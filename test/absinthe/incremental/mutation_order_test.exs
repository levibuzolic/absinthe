defmodule Absinthe.Incremental.MutationOrderTest do
  use Absinthe.Case, async: true

  defmodule PassBarrier do
    @behaviour Absinthe.Plugin

    def before_resolution(execution) do
      update_in(execution.context[:passes], &((&1 || 0) + 1))
    end

    def after_resolution(execution) do
      if execution.acc[__MODULE__] do
        execution
      else
        send(execution.context.test_pid, {:first_resolution_pass_finished, self()})
        put_in(execution.acc[__MODULE__], true)
      end
    end

    def pipeline(pipeline, execution) do
      if execution.context[:disable_resume_callbacks] && execution.pending != [] &&
           not Map.has_key?(execution.context, :completed) do
        [{Absinthe.Phase.Document.Execution.Resolution, plugin_callbacks: false}]
      else
        pipeline
      end
    end
  end

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import Absinthe.Resolution.Helpers
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def plugins,
      do: [Absinthe.Middleware.Dataloader | Absinthe.Plugin.defaults()] ++ [PassBarrier]

    def context(context) do
      loader = Dataloader.new() |> Dataloader.add_source(:ids, Dataloader.KV.new(&load_ids/2))
      Map.put(context, :loader, loader)
    end

    def load_ids(:id, ids), do: Map.new(ids, &{&1, &1})
    def batch_ids(_, ids), do: Map.new(ids, &{&1, &1})

    def first(_, args, %{context: %{test_pid: pid}}) do
      send(pid, {:mutation_trace, :first_started})

      async(fn ->
        wait_for_release(pid)
        if args.fail, do: {:error, "failed"}, else: {:ok, %{id: 1}}
      end)
    end

    def completed(resolution, _) do
      send(resolution.context.test_pid, {:mutation_trace, :first_completed})
      put_in(resolution.context[:completed], "first completed")
    end

    defp wait_for_release(pid) do
      send(pid, {:first_waiting, self()})

      receive do
        :release_first -> :ok
      after
        5_000 -> raise "test did not release the first mutation"
      end
    end

    object :person do
      field :id, :integer
      field :observed, :string
      field :passes, :integer

      field :delayed_id, non_null(:integer) do
        arg :fail, :boolean, default_value: false

        resolve fn %{id: id}, args, %{context: %{test_pid: pid}} ->
          async(fn ->
            wait_for_release(pid)
            async(fn -> if args.fail, do: {:error, "child failed"}, else: {:ok, id} end)
          end)
        end

        middleware fn resolution, _ ->
          send(resolution.context.test_pid, {:mutation_trace, :child_completed})
          resolution
        end
      end

      field :batch_id, :integer do
        resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
          batch({__MODULE__, :batch_ids, nil}, id, fn values ->
            send(pid, {:mutation_trace, :batch_completed})
            {:ok, Map.fetch!(values, id)}
          end)
        end
      end

      field :loaded_id, :integer do
        resolve fn %{id: id}, _, %{context: %{loader: loader, test_pid: pid}} ->
          loader
          |> Dataloader.load(:ids, :id, id)
          |> on_load(fn loader ->
            send(pid, {:mutation_trace, :loader_completed})
            {:ok, Dataloader.get(loader, :ids, :id, id)}
          end)
        end
      end

      field :name, :string do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, {:mutation_trace, :name})
          {:ok, "Ada"}
        end
      end
    end

    query do
      field :person, :person
    end

    mutation do
      field :first, :person do
        arg :fail, :boolean, default_value: false
        resolve &__MODULE__.first/3
        middleware &__MODULE__.completed/2
      end

      field :required_first, non_null(:person) do
        arg :fail, :boolean, default_value: false
        resolve &__MODULE__.first/3
        middleware &__MODULE__.completed/2
      end

      field :immediate, :person do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, {:mutation_trace, :first_started})
          {:ok, %{id: 1}}
        end
      end

      field :required_immediate, non_null(:person) do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, {:mutation_trace, :first_started})
          {:ok, %{id: 1}}
        end
      end

      field :immediate_failure, non_null(:person) do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, {:mutation_trace, :first_started})
          {:error, "failed"}
        end
      end

      field :second_batch, :person do
        resolve fn _, _, %{context: %{test_pid: pid} = context} ->
          send(pid, {:mutation_trace, :second_started})

          batch({__MODULE__, :batch_ids, nil}, 2, fn values ->
            send(pid, {:mutation_trace, :second_completed})
            {:ok, %{id: Map.fetch!(values, 2), passes: context[:passes]}}
          end)
        end
      end

      field :second_loaded, :person do
        resolve fn _, _, %{context: %{test_pid: pid, loader: loader} = context} ->
          send(pid, {:mutation_trace, :second_started})

          loader
          |> Dataloader.load(:ids, :id, 2)
          |> on_load(fn loader ->
            send(pid, {:mutation_trace, :second_completed})
            {:ok, %{id: Dataloader.get(loader, :ids, :id, 2), passes: context[:passes]}}
          end)
        end
      end

      field :second_async, :person do
        resolve fn _, _, %{context: %{test_pid: pid} = context} ->
          send(pid, {:mutation_trace, :second_started})
          async(fn -> {:ok, %{id: 2, passes: context[:passes]}} end)
        end

        middleware fn resolution, _ ->
          send(resolution.context.test_pid, {:mutation_trace, :second_completed})
          resolution
        end
      end

      field :second, :person do
        resolve fn _, _, %{context: %{test_pid: pid} = context} ->
          send(pid, {:mutation_trace, :second_started})
          {:ok, %{id: 2, observed: context[:completed], passes: context[:passes]}}
        end
      end
    end
  end

  @query "mutation { first { id ... @defer { name } } second { id } }"

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "ordinary execution finishes a suspended mutation before starting the next root field" do
    assert {:ok, %{data: data}} = execute(:run)

    assert data == %{
             "first" => %{"id" => 1, "name" => "Ada"},
             "second" => %{"id" => 2}
           }

    assert trace() == [:first_started, :first_completed, :name, :second_started]
  end

  test "incremental execution preserves suspended mutation order while deferring child work" do
    assert {:ok, %Absinthe.Incremental{} = result} = execute(:run_incremental)
    assert result.initial_result.data == %{"first" => %{"id" => 1}, "second" => %{"id" => 2}}
    initial_trace = trace()

    assert {%{"first" => %{"id" => 1, "name" => "Ada"}, "second" => %{"id" => 2}}, _} =
             Absinthe.Case.Assertions.Incremental.consume(result)

    assert initial_trace == [:first_started, :first_completed, :second_started]
    assert trace() == [:name]
  end

  test "an eager child must finish repeated Async suspension before the next mutation root" do
    query = "mutation { immediate { delayedId ... @defer { name } } second { id } }"

    assert {:ok, result} = execute(:run_incremental, query)

    assert result.initial_result.data == %{
             "immediate" => %{"delayedId" => 1},
             "second" => %{"id" => 2}
           }

    assert trace() == [:first_started, :child_completed, :second_started]

    assert {%{"immediate" => %{"delayedId" => 1, "name" => "Ada"}, "second" => %{"id" => 2}}, _} =
             Absinthe.Case.Assertions.Incremental.consume(result)

    assert trace() == [:name]
  end

  test "Batch and Dataloader children finish after root suspension before the next mutation" do
    query = "mutation { first { batchId loadedId ... @defer { name } } second { observed } }"

    assert {:ok, result} = execute(:run_incremental, query)

    assert result.initial_result.data == %{
             "first" => %{"batchId" => 1, "loadedId" => 1},
             "second" => %{"observed" => "first completed"}
           }

    assert trace() == [
             :first_started,
             :first_completed,
             :batch_completed,
             :loader_completed,
             :second_started
           ]

    Absinthe.Case.Assertions.Incremental.consume(result)
    assert trace() == [:name]
  end

  test "a suspended nullable mutation failure permits the next root" do
    query = "mutation { first(fail: true) { id ... @defer { name } } second { id } }"

    for api <- [:run, :run_incremental] do
      assert {:ok, %{data: %{"first" => nil, "second" => %{"id" => 2}}, errors: [error]}} =
               execute(api, query)

      assert error.path == ["first"]
      assert trace() == [:first_started, :first_completed, :second_started]
    end
  end

  test "a suspended non-null mutation failure stops before the next root" do
    query = "mutation { requiredFirst(fail: true) { id ... @defer { name } } second { id } }"

    for api <- [:run, :run_incremental] do
      assert {:ok, %{data: nil, errors: [error]}} = execute(api, query)
      assert error.path == ["requiredFirst"]
      assert trace() == [:first_started, :first_completed]
    end
  end

  test "a suspended eager child failure respects the nullability of its mutation root" do
    for {field, data, expected_trace} <- [
          {"immediate", %{"immediate" => nil, "second" => %{"id" => 2}},
           [:first_started, :child_completed, :second_started]},
          {"requiredImmediate", nil, [:first_started, :child_completed]}
        ],
        api <- [:run, :run_incremental] do
      query = "mutation { #{field} { delayedId(fail: true) ... @defer { name } } second { id } }"
      assert {:ok, %{data: ^data, errors: [error]}} = execute(api, query)
      assert error.path == [field, "delayedId"]

      expected_trace =
        if api == :run,
          do: List.insert_at(expected_trace, 1, :name),
          else: expected_trace

      assert trace() == expected_trace
    end
  end

  test "an immediate non-null root failure also stops remaining mutations" do
    query = "mutation { immediateFailure { id } second { id } }"

    for api <- [:run, :run_incremental] do
      assert {:ok, %{data: nil, errors: [%{path: ["immediateFailure"]}]}} =
               apply(Absinthe, api, [query, Schema, [context: %{test_pid: self()}]])

      assert trace() == [:first_started]
    end
  end

  test "root null propagation discards deferred jobs collected by earlier mutations" do
    prefix = "immediate { id ... @defer { name } }"
    sync = "mutation { #{prefix} immediateFailure { id } second { id } }"
    suspended = "mutation { #{prefix} requiredFirst(fail: true) { id } second { id } }"

    assert {:ok, %{data: nil, errors: [%{path: ["immediateFailure"]}]}} =
             Absinthe.run_incremental(sync, Schema, context: %{test_pid: self()})

    assert trace() == [:first_started, :first_started]

    assert {:ok, %{data: nil, errors: [%{path: ["requiredFirst"]}]}} =
             execute(:run_incremental, suspended)

    assert trace() == [:first_started, :first_started, :first_completed]
  end

  test "a resume honors plugin options and the next root restores the original phase options" do
    query = "mutation { first { id } second { passes observed } }"

    for api <- [:run, :run_incremental] do
      assert {:ok, %{data: data}} = execute(api, query, %{disable_resume_callbacks: true})

      assert data == %{
               "first" => %{"id" => 1},
               "second" => %{"passes" => 2, "observed" => "first completed"}
             }

      assert trace() == [:first_started, :first_completed, :second_started]
    end
  end

  test "a plugin disabling callbacks for one resume does not disable later root middleware" do
    for field <- ["secondAsync", "secondBatch", "secondLoaded"],
        api <- [:run, :run_incremental] do
      query = "mutation { first { id } #{field} { id passes } }"
      assert {:ok, %{data: data}} = execute(api, query, %{disable_resume_callbacks: true})
      assert data == %{"first" => %{"id" => 1}, field => %{"id" => 2, "passes" => 2}}
      assert trace() == [:first_started, :first_completed, :second_started, :second_completed]
    end
  end

  test "repeated aliased roots merge once and carry resumed middleware context forward" do
    query = """
    mutation {
      one: first { id }
      one: first { ... @defer { name } }
      two: second { observed }
    }
    """

    assert {:ok, result} = execute(:run_incremental, query)

    assert result.initial_result.data == %{
             "one" => %{"id" => 1},
             "two" => %{"observed" => "first completed"}
           }

    assert trace() == [:first_started, :first_completed, :second_started]

    assert {%{
              "one" => %{"id" => 1, "name" => "Ada"},
              "two" => %{"observed" => "first completed"}
            }, _} =
             Absinthe.Case.Assertions.Incremental.consume(result)

    assert trace() == [:name]
  end

  defp execute(api, query \\ @query, context \\ %{}) do
    supervisor = start_supervised!(Task.Supervisor, id: make_ref())
    pid = self()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        apply(Absinthe, api, [query, Schema, [context: Map.put(context, :test_pid, pid)]])
      end)

    assert_receive {:first_waiting, resolver}, 5_000
    # This callback is the barrier before Async resumes, not a timing delay.
    # All trace events come from the execution process, including the middleware
    # that records completion after the asynchronous result has been awaited.
    execution_pid = task.pid
    assert_receive {:first_resolution_pass_finished, ^execution_pid}, 5_000
    send(resolver, :release_first)
    Task.await(task, 5_000)
  end

  defp trace do
    receive do
      {:mutation_trace, event} -> [event | trace()]
    after
      0 -> []
    end
  end
end
