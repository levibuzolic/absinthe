defmodule Absinthe.Integration.Execution.IncrementalMiddlewareTest do
  use Absinthe.Case, async: false

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives
    import Absinthe.Resolution.Helpers

    object :person do
      field :id, :id

      field :async_name, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          send(pid, {:resolver_entered, :async_name, id})

          async(fn ->
            send(pid, {:async_resolved, id})
            {:ok, async_name(id)}
          end)
        end
      end

      field :batch_name, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          send(pid, {:resolver_entered, :batch_name, id})

          batch({__MODULE__, :load_batch_name, pid}, id, fn values ->
            send(pid, {:batch_post_resolved, id})
            {:ok, Map.fetch!(values, id)}
          end)
        end
      end

      field :batch_alias, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          batch({__MODULE__, :load_batch_name, pid}, id, fn values ->
            send(pid, {:batch_alias_post_resolved, id})
            {:ok, Map.fetch!(values, id)}
          end)
        end
      end

      field :dataloader_name, :string do
        resolve fn %{id: id}, _, %{context: %{loader: loader, test_pid: pid}} ->
          send(pid, {:resolver_entered, :dataloader_name, id})

          loader
          |> Dataloader.load(:test, {:name, pid}, id)
          |> on_load(fn loader ->
            send(pid, {:dataloader_post_resolved, id})

            {:ok, Dataloader.get(loader, :test, {:name, pid}, id)}
          end)
        end
      end

      field :dataloader_friend, :person do
        resolve fn %{id: id}, _, %{context: %{loader: loader, test_pid: pid}} ->
          loader
          |> Dataloader.load(:test, {:friend, pid}, id)
          |> on_load(fn loader ->
            {:ok, Dataloader.get(loader, :test, {:friend, pid}, id)}
          end)
        end
      end

      field :dataloader_friend_name, :string do
        resolve fn %{id: id}, _, %{context: %{loader: loader, test_pid: pid}} ->
          loader
          |> Dataloader.load(:test, {:friend, pid}, id)
          |> on_load(fn loader ->
            %{id: friend_id} = Dataloader.get(loader, :test, {:friend, pid}, id)

            loader
            |> Dataloader.load(:test, {:name, pid}, friend_id)
            |> on_load(fn loader ->
              send(pid, {:friend_name_resolved, id})
              {:ok, Dataloader.get(loader, :test, {:name, pid}, friend_id)}
            end)
          end)
        end
      end
    end

    query do
      field :person, :person

      field :people, list_of(:person) do
        resolve fn source, _, %{context: %{test_pid: pid}} ->
          send(pid, :people_resolved)
          {:ok, Map.fetch!(source, :people)}
        end
      end
    end

    def load_batch_name(pid, keys) do
      send(pid, {:batch_loaded, keys})
      Map.new(keys, &{&1, batch_name(&1)})
    end

    def load_dataloader({:name, pid}, keys) do
      send(pid, {:dataloader_loaded, keys})
      Map.new(keys, &{&1, dataloader_name(&1)})
    end

    def load_dataloader({:friend, pid}, keys) do
      send(pid, {:dataloader_friends_loaded, keys})
      friends = %{1 => 2, 2 => 3, 3 => 1}
      Map.new(keys, &{&1, %{id: Map.fetch!(friends, &1)}})
    end

    def async_name(1), do: "Ada"
    def async_name(id), do: "Async #{id}"

    def batch_name(1), do: "Grace"
    def batch_name(id), do: "Batch #{id}"

    def dataloader_name(1), do: "Edsger"
    def dataloader_name(id), do: "Dataloader #{id}"

    def loader do
      Dataloader.new()
      |> Dataloader.add_source(:test, Dataloader.KV.new(&__MODULE__.load_dataloader/2))
    end

    def plugins do
      [Absinthe.Middleware.Dataloader] ++ Absinthe.Plugin.defaults()
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    person = %{id: 1}
    people = [%{id: 1}, %{id: 2}]

    {:ok,
     options: [
       root_value: %{person: person, people: people},
       context: %{test_pid: self(), loader: Schema.loader()}
     ]}
  end

  test "deferred child fields preserve async, batch, and dataloader middleware", %{
    options: options
  } do
    query = """
    {
      person {
        id
        ... @defer(label: "middleware") {
          asyncName
          batchName
          dataloaderName
        }
      }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"person" => %{"id" => "1"}}

    refute_received {:resolver_entered, _, _}
    refute_received {:async_resolved, _}
    refute_received {:batch_loaded, _}
    refute_received {:batch_post_resolved, _}
    refute_received {:dataloader_loaded, _}
    refute_received {:dataloader_post_resolved, _}

    payloads = Enum.to_list(result.subsequent_results)
    assert List.last(payloads).hasNext == false

    fields =
      payloads
      |> Enum.flat_map(&Map.get(&1, :incremental, []))
      |> Enum.flat_map(&Map.get(&1, :data, []))
      |> Map.new()

    assert fields == %{
             "asyncName" => "Ada",
             "batchName" => "Grace",
             "dataloaderName" => "Edsger"
           }

    assert_received {:resolver_entered, :async_name, 1}
    assert_received {:resolver_entered, :batch_name, 1}
    assert_received {:resolver_entered, :dataloader_name, 1}
    refute_received {:resolver_entered, _, _}
    assert_received {:async_resolved, 1}
    assert_received {:batch_loaded, [1]}
    assert_received {:batch_post_resolved, 1}
    keys = MapSet.new([1])
    assert_received {:dataloader_loaded, ^keys}
    assert_received {:dataloader_post_resolved, 1}
  end

  test "streamed items defer async, batch, and dataloader child work", %{options: options} do
    query = """
    {
      people @stream(initialCount: 1) {
        id
        asyncName
        batchName
        dataloaderName
      }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)

    assert result.initial_result.data == %{
             "people" => [
               %{
                 "id" => "1",
                 "asyncName" => "Ada",
                 "batchName" => "Grace",
                 "dataloaderName" => "Edsger"
               }
             ]
           }

    assert_received :people_resolved
    refute_received :people_resolved
    assert_received {:resolver_entered, :async_name, 1}
    assert_received {:resolver_entered, :batch_name, 1}
    assert_received {:resolver_entered, :dataloader_name, 1}
    refute_received {:resolver_entered, _, _}
    assert_received {:async_resolved, 1}
    refute_received {:async_resolved, 2}
    assert_received {:batch_loaded, [1]}
    refute_received {:batch_loaded, [2]}
    assert_received {:batch_post_resolved, 1}
    refute_received {:batch_post_resolved, 2}
    keys = MapSet.new([1])
    assert_received {:dataloader_loaded, ^keys}
    keys = MapSet.new([2])
    refute_received {:dataloader_loaded, ^keys}
    assert_received {:dataloader_post_resolved, 1}
    refute_received {:dataloader_post_resolved, 2}

    payloads = Enum.to_list(result.subsequent_results)
    assert List.last(payloads).hasNext == false

    assert Enum.any?(payloads, fn payload ->
             Enum.any?(Map.get(payload, :incremental, []), fn entry ->
               entry[:items] == [
                 %{
                   "id" => "2",
                   "asyncName" => "Async 2",
                   "batchName" => "Batch 2",
                   "dataloaderName" => "Dataloader 2"
                 }
               ]
             end)
           end)

    assert_received {:resolver_entered, :async_name, 2}
    assert_received {:resolver_entered, :batch_name, 2}
    assert_received {:resolver_entered, :dataloader_name, 2}
    refute_received {:resolver_entered, _, _}
    assert_received {:async_resolved, 2}
    assert_received {:batch_loaded, [2]}
    assert_received {:batch_post_resolved, 2}
    keys = MapSet.new([2])
    assert_received {:dataloader_loaded, ^keys}
    assert_received {:dataloader_post_resolved, 2}
    refute_received :people_resolved
  end

  test "sibling deferred batch fields share one middleware batch", %{options: options} do
    query = """
    {
      person {
        id
        ... @defer(label: "batch") {
          batchName
          batchAlias
        }
      }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"person" => %{"id" => "1"}}
    refute_received {:resolver_entered, _, _}
    refute_received {:batch_loaded, _}

    payloads = Enum.to_list(result.subsequent_results)
    assert List.last(payloads).hasNext == false

    assert Enum.any?(payloads, fn payload ->
             Enum.any?(Map.get(payload, :incremental, []), fn entry ->
               entry[:data] == %{"batchName" => "Grace", "batchAlias" => "Grace"}
             end)
           end)

    assert_receive {:batch_loaded, keys}
    assert Enum.sort(keys) == [1, 1]
    refute_received {:batch_loaded, _}
    assert_received {:resolver_entered, :batch_name, 1}
    refute_received {:resolver_entered, _, _}
    assert_received {:batch_post_resolved, 1}
    assert_received {:batch_alias_post_resolved, 1}
  end

  test "separate deferred groups share dataloader keys with each other and the initial result", %{
    options: options
  } do
    for preload? <- [false, true] do
      initial_field = if preload?, do: "initial: dataloaderName", else: ""

      query = """
      { people {
        id
        #{initial_field}
        ... @defer(label: "first") { first: dataloaderName }
        ... @defer(label: "second") { second: dataloaderName }
      } }
      """

      assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)

      initial_people =
        for id <- 1..2 do
          person = %{"id" => Integer.to_string(id)}
          if preload?, do: Map.put(person, "initial", Schema.dataloader_name(id)), else: person
        end

      assert result.initial_result.data == %{"people" => initial_people}

      for id <- 1..2, preload? do
        assert_received {:resolver_entered, :dataloader_name, ^id}
        assert_received {:dataloader_post_resolved, ^id}
      end

      refute_received {:resolver_entered, :dataloader_name, _}
      refute_received {:dataloader_post_resolved, _}
      unless preload?, do: refute_received({:dataloader_loaded, _})

      expected =
        for {person, id} <- Enum.with_index(initial_people, 1) do
          Map.merge(person, %{
            "first" => Schema.dataloader_name(id),
            "second" => Schema.dataloader_name(id)
          })
        end

      assert {%{"people" => ^expected}, payloads} = Incremental.consume(result)

      assert Enum.sort(received_dataloader_keys()) == [1, 2]

      for id <- 1..2, _group <- 1..2 do
        assert_received {:resolver_entered, :dataloader_name, ^id}
        assert_received {:dataloader_post_resolved, ^id}
      end

      refute_received {:resolver_entered, :dataloader_name, _}
      refute_received {:dataloader_post_resolved, _}

      assert Enum.sort(
               for payload <- payloads,
                   notice <- Map.get(payload, :pending, []),
                   do: {notice.path, notice.label}
             ) == [
               {["people", 0], "first"},
               {["people", 0], "second"},
               {["people", 1], "first"},
               {["people", 1], "second"}
             ]
    end
  end

  test "chained dataloader callbacks inside nested defers reuse initial and deferred loads", %{
    options: options
  } do
    query = """
    { people {
      id
      dataloaderName
      ... @defer(label: "friend") {
        dataloaderFriend {
          id
          ... @defer(label: "name") { dataloaderFriendName }
        }
      }
    } }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)

    assert result.initial_result.data == %{
             "people" => [
               %{"id" => "1", "dataloaderName" => "Edsger"},
               %{"id" => "2", "dataloaderName" => "Dataloader 2"}
             ]
           }

    refute_received {:dataloader_friends_loaded, _}
    refute_received {:friend_name_resolved, _}

    assert {data, payloads} = Incremental.consume(result)

    assert data == %{
             "people" => [
               %{
                 "id" => "1",
                 "dataloaderName" => "Edsger",
                 "dataloaderFriend" => %{
                   "id" => "2",
                   "dataloaderFriendName" => "Dataloader 3"
                 }
               },
               %{
                 "id" => "2",
                 "dataloaderName" => "Dataloader 2",
                 "dataloaderFriend" => %{
                   "id" => "3",
                   "dataloaderFriendName" => "Edsger"
                 }
               }
             ]
           }

    assert Enum.sort(received_dataloader_keys()) == [1, 2, 3]
    assert Enum.sort(received_dataloader_keys(:dataloader_friends_loaded)) == [1, 2, 3]
    assert_received {:friend_name_resolved, 2}
    assert_received {:friend_name_resolved, 3}
    refute_received {:friend_name_resolved, _}

    assert Enum.sort(
             for payload <- payloads,
                 notice <- Map.get(payload, :pending, []),
                 do: {notice.path, notice.label}
           ) == [
             {["people", 0], "friend"},
             {["people", 0, "dataloaderFriend"], "name"},
             {["people", 1], "friend"},
             {["people", 1, "dataloaderFriend"], "name"}
           ]
  end

  defp received_dataloader_keys(tag \\ :dataloader_loaded) do
    receive do
      {^tag, keys} -> MapSet.to_list(keys) ++ received_dataloader_keys(tag)
    after
      0 -> []
    end
  end
end
