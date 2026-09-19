defmodule Absinthe.Incremental.ExecutionTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def plugins, do: [Absinthe.Middleware.Dataloader | Absinthe.Plugin.defaults()]

    def context(context) do
      loader = Dataloader.new() |> Dataloader.add_source(:values, Dataloader.KV.new(&load/2))
      Map.put(context, :loader, loader)
    end

    def load({:value, %{pid: pid}}, inputs) do
      send(pid, {:loaded, Enum.map(inputs, & &1.id)})
      Map.new(inputs, &{&1, "loaded #{&1.id}"})
    end

    def batch_values(pid, ids) do
      send(pid, {:batched, Enum.sort(ids)})
      Map.new(ids, &{&1, "batch #{&1}"})
    end

    object :item do
      field :id, :integer
      field :name, :string
      field :children, list_of(:item)

      field :batch_value, :string do
        resolve fn item, _, %{context: %{pid: pid}} ->
          batch({__MODULE__, :batch_values, pid}, item.id, fn values ->
            {:ok, values[item.id]}
          end)
        end
      end

      field :loaded, :string do
        resolve dataloader(:values, fn _, _, %{context: %{pid: pid}} ->
                  {:value, %{pid: pid}}
                end)
      end

      field :asynchronous, :string do
        resolve fn item, _, %{context: %{pid: pid}} ->
          async(fn ->
            send(pid, {:async, item.id})
            async(fn -> {:ok, "async #{item.id}"} end)
          end)
        end
      end

      field :fail, non_null(:string) do
        resolve fn _, _, _ -> {:error, "failed"} end
      end
    end

    query do
      field :items, list_of(:item)
      field :item, :item
      field :required_items, list_of(non_null(:item))

      field :remember, :string do
        middleware fn res, _ ->
          %{res | context: Map.put(res.context, :remembered, "remembered")}
        end

        resolve fn _, _, _ -> {:ok, "done"} end
      end

      field :recall, :string do
        resolve fn _, _, %{context: context} -> {:ok, context[:remembered]} end
      end
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    items = [%{id: 1, name: "one"}, %{id: 2, name: "two"}]

    [
      options: [
        context: %{pid: self()},
        root_value: %{
          item: %{id: 0, children: items},
          items: items,
          required_items: items
        }
      ]
    ]
  end

  test "deferred groups and stream items finish nested async and batch suspension", %{
    options: options
  } do
    {:ok, result} =
      Absinthe.run_incremental(
        """
        { ... @defer { items { batchValue asynchronous } } }
        """,
        Schema,
        options
      )

    assert result.initial_result.data == %{}
    refute_received {:batched, _}
    refute_received {:async, _}

    assert [%{incremental: [%{data: %{"items" => items}}], hasNext: false}] =
             Enum.to_list(result.subsequent_results)

    assert items == [
             %{"batchValue" => "batch 1", "asynchronous" => "async 1"},
             %{"batchValue" => "batch 2", "asynchronous" => "async 2"}
           ]

    assert_received {:batched, [1, 2]}

    {:ok, streamed} =
      Absinthe.run_incremental("{ items @stream { batchValue asynchronous } }", Schema, options)

    assert [first] = Enum.take(streamed.subsequent_results, 1)

    assert [%{items: [%{"batchValue" => "batch 1", "asynchronous" => "async 1"}]}] =
             first.incremental

    assert_received {:batched, [1]}
    refute_received {:batched, [2]}
  end

  test "dataloader context and middleware context persist between delivery frames", %{
    options: options
  } do
    {:ok, result} =
      Absinthe.run_incremental(
        """
        { remember item { loaded ... @defer { loaded name } } ... @defer { recall } }
        """,
        Schema,
        options
      )

    assert result.initial_result.data == %{
             "remember" => "done",
             "item" => %{"loaded" => "loaded 0"}
           }

    assert_received {:loaded, [0]}
    payloads = Enum.to_list(result.subsequent_results)

    assert Enum.any?(
             payloads,
             &Enum.any?(&1.incremental, fn entry -> entry.data == %{"recall" => "remembered"} end)
           )

    refute_received {:loaded, _}

    {:ok, streamed} =
      Absinthe.run_incremental("{ items @stream(initialCount: 1) { loaded } }", Schema, options)

    assert streamed.initial_result.data == %{"items" => [%{"loaded" => "loaded 1"}]}
    assert_received {:loaded, [1]}
    refute_received {:loaded, [2]}

    assert [%{incremental: [%{items: [%{"loaded" => "loaded 2"}]}]}] =
             Enum.to_list(streamed.subsequent_results)

    assert_received {:loaded, [2]}
  end

  test "a failed deferred frame cancels streams it discovered before failing", %{options: options} do
    {:ok, result} =
      Absinthe.run_incremental(
        """
        { item { id ... @defer { children @stream { asynchronous } fail } } }
        """,
        Schema,
        options
      )

    assert [%{completed: [%{errors: [_]}], hasNext: false} = payload] =
             Enum.to_list(result.subsequent_results)

    refute Map.has_key?(payload, :pending)
    refute Map.has_key?(payload, :incremental)
    refute_received {:async, _}
  end

  test "non-null streamed object failure closes its stream and suppresses nested defer", %{
    options: options
  } do
    {:ok, result} =
      Absinthe.run_incremental(
        """
        { requiredItems @stream { fail ... @defer { asynchronous } } }
        """,
        Schema,
        options
      )

    assert result.initial_result.data == %{"requiredItems" => []}

    assert [%{completed: [%{errors: [%{path: ["requiredItems", 0, "fail"]}]}], hasNext: false}] =
             Enum.to_list(result.subsequent_results)

    refute_received {:async, _}
  end

  test "a nullable streamed object error delivers null and continues later items", %{
    options: options
  } do
    {:ok, result} = Absinthe.run_incremental("{ items @stream { fail } }", Schema, options)
    assert [first, second] = Enum.to_list(result.subsequent_results)
    assert [%{items: [nil], errors: [%{path: ["items", 0, "fail"]}]}] = first.incremental
    assert [%{items: [nil], errors: [%{path: ["items", 1, "fail"]}]}] = second.incremental
    assert second.hasNext == false
  end

  test "a shared stream starts fresh item contexts after its parent groups complete", %{
    options: options
  } do
    {:ok, result} =
      Absinthe.run_incremental(
        """
        {
          ... @defer(label: "left") { ...Shared }
          ... @defer(label: "right") { ...Shared }
        }
        fragment Shared on RootQueryType {
          items @stream { id ... @defer { loaded } }
        }
        """,
        Schema,
        options
      )

    initial_ids = MapSet.new(result.initial_result.pending, & &1.id)
    [parent | items] = Enum.to_list(result.subsequent_results)
    assert [%{data: %{"items" => []}}] = parent.incremental
    assert MapSet.new(parent.completed, & &1.id) == initial_ids

    assert Enum.count(
             items,
             &Enum.any?(Map.get(&1, :incremental, []), fn entry -> Map.has_key?(entry, :items) end)
           ) == 2

    for payload <- items, entry <- Map.get(payload, :incremental, []) do
      refute MapSet.member?(initial_ids, entry.id)
    end

    assert List.last(items).hasNext == false
    assert_received {:loaded, [1]}
    assert_received {:loaded, [2]}
  end

  test "negative stream counts are execution errors in eager mode", %{options: options} do
    assert {:ok, %{data: %{"items" => nil}, errors: [%{path: ["items"]}]}} =
             Absinthe.run("{ items @stream(initialCount: -1) { id } }", Schema, options)

    assert {:ok, %{data: %{"items" => [%{"id" => 1}, %{"id" => 2}]}}} =
             Absinthe.run(
               "{ items @stream(if: false, initialCount: -1) { id } }",
               Schema,
               options
             )
  end
end
