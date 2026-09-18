defmodule Absinthe.Integration.Execution.IncrementalMiddlewareTest do
  use Absinthe.Case, async: false

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives
    import Absinthe.Resolution.Helpers

    object :person do
      field :id, :id

      field :async_name, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          async(fn ->
            send(pid, {:async_resolved, id})
            {:ok, async_name(id)}
          end)
        end
      end

      field :batch_name, :string do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
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
          loader
          |> Dataloader.load(:test, {:name, pid}, id)
          |> on_load(fn loader ->
            send(pid, {:dataloader_post_resolved, id})

            {:ok, Dataloader.get(loader, :test, {:name, pid}, id)}
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
    refute_received {:batch_loaded, _}

    payloads = Enum.to_list(result.subsequent_results)
    assert List.last(payloads).hasNext == false

    assert Enum.any?(payloads, fn payload ->
             Enum.any?(Map.get(payload, :incremental, []), fn entry ->
               entry[:data] == %{"batchName" => "Grace", "batchAlias" => "Grace"}
             end)
           end)

    assert_received {:batch_loaded, keys}
    assert Enum.sort(keys) == [1, 1]
    refute_received {:batch_loaded, _}
    assert_received {:batch_post_resolved, 1}
    assert_received {:batch_alias_post_resolved, 1}
  end
end
