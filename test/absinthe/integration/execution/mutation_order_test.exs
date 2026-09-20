defmodule Absinthe.Integration.Execution.MutationOrderTest do
  use Absinthe.Case, async: true

  defmodule PassBarrier do
    @behaviour Absinthe.Plugin

    def before_resolution(execution) do
      execution = update_in(execution.context[:passes], &((&1 || 0) + 1))
      send(execution.context.test_pid, {:plugin_pass, :before, execution.context.passes})
      execution
    end

    def after_resolution(execution) do
      send(execution.context.test_pid, {:plugin_pass, :after, execution.context.passes})

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

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "a suspended mutation and its children finish before the next root starts" do
    assert {:ok, %{data: data}} = execute("mutation { first { id name } second { observed } }")

    assert data == %{
             "first" => %{"id" => 1, "name" => "Ada"},
             "second" => %{"observed" => "first completed"}
           }

    assert trace() == [:first_started, :first_completed, :name, :second_started]
  end

  test "repeated Async suspension in an eager child blocks the next mutation root" do
    assert {:ok, %{data: data}} = execute("mutation { immediate { delayedId } second { id } }")
    assert data == %{"immediate" => %{"delayedId" => 1}, "second" => %{"id" => 2}}
    assert trace() == [:first_started, :child_completed, :second_started]
  end

  test "Batch and Dataloader children settle after root suspension and before the next root" do
    assert {:ok, %{data: data}} =
             execute("mutation { first { batchId loadedId } second { observed } }")

    assert data == %{
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
  end

  test "suspended root and child failures stop only at non-null mutation boundaries" do
    for {field, selection, path, expected_data, expected_trace} <- [
          {"first(fail: true)", "id", ["first"], %{"first" => nil, "second" => %{"id" => 2}},
           [:first_started, :first_completed, :second_started]},
          {"requiredFirst(fail: true)", "id", ["requiredFirst"], nil,
           [:first_started, :first_completed]},
          {"immediate", "delayedId(fail: true)", ["immediate", "delayedId"],
           %{"immediate" => nil, "second" => %{"id" => 2}},
           [:first_started, :child_completed, :second_started]},
          {"requiredImmediate", "delayedId(fail: true)", ["requiredImmediate", "delayedId"], nil,
           [:first_started, :child_completed]}
        ] do
      assert {:ok, %{data: ^expected_data, errors: [error]}} =
               execute("mutation { #{field} { #{selection} } second { id } }")

      assert error.path == path
      assert trace() == expected_trace
    end
  end

  test "an immediate non-null root failure stops remaining mutations" do
    assert {:ok, %{data: nil, errors: [%{path: ["immediateFailure"]}]}} =
             Absinthe.run("mutation { immediateFailure { id } second { id } }", Schema,
               context: %{test_pid: self()}
             )

    assert trace() == [:first_started]
  end

  test "a resume honors plugin options and the next root restores the original options" do
    assert {:ok, %{data: data}} =
             execute("mutation { first { id } second { passes observed } }", %{
               disable_resume_callbacks: true
             })

    assert data == %{
             "first" => %{"id" => 1},
             "second" => %{"passes" => 2, "observed" => "first completed"}
           }

    assert trace() == [:first_started, :first_completed, :second_started]
  end

  test "disabling callbacks for one resume does not disable subsequent root middleware" do
    for field <- ["secondAsync", "secondBatch", "secondLoaded"] do
      assert {:ok, %{data: data}} =
               execute("mutation { first { id } #{field} { id passes } }", %{
                 disable_resume_callbacks: true
               })

      assert data == %{"first" => %{"id" => 1}, field => %{"id" => 2, "passes" => 2}}
      assert trace() == [:first_started, :first_completed, :second_started, :second_completed]
    end
  end

  test "repeated aliased roots merge once and carry resumed context forward" do
    assert {:ok, %{data: data}} =
             execute(
               "mutation { one: first { id } one: first { name } two: second { observed } }"
             )

    assert data == %{
             "one" => %{"id" => 1, "name" => "Ada"},
             "two" => %{"observed" => "first completed"}
           }

    assert trace() == [:first_started, :first_completed, :name, :second_started]
  end

  test "BatchResolver owns plugin callbacks across two documents and serial root resumes" do
    alias Absinthe.{Phase, Pipeline}
    query = "mutation { first: secondAsync { id passes } second: secondBatch { id passes } }"

    pipeline =
      Schema
      |> Pipeline.for_document(context: %{test_pid: self()})
      |> Pipeline.before(Phase.Document.Execution.Resolution)

    blueprints =
      for _ <- 1..2 do
        assert {:ok, blueprint, _} = Pipeline.run(query, pipeline)
        blueprint
      end

    results = Pipeline.BatchResolver.run(blueprints, schema: Schema)
    assert length(results) == 2

    for blueprint <- results do
      assert blueprint.execution.pending == []
      assert blueprint.execution.mutation == nil
      assert {:ok, %{result: %{data: data}}} = Phase.Document.Result.run(blueprint, [])

      assert data == %{
               "first" => %{"id" => 2, "passes" => 1},
               "second" => %{"id" => 2, "passes" => 2}
             }
    end

    for pass <- 1..3 do
      assert_received {:plugin_pass, :before, ^pass}
      assert_received {:plugin_pass, :after, ^pass}
    end

    refute_received {:plugin_pass, _, _}

    assert trace() == [
             :second_started,
             :second_started,
             :second_completed,
             :second_started,
             :second_completed,
             :second_started,
             :second_completed,
             :second_completed
           ]
  end

  defp execute(query, context \\ %{}) do
    supervisor = start_supervised!(Task.Supervisor, id: make_ref())
    pid = self()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Absinthe.run(query, Schema, context: Map.put(context, :test_pid, pid))
      end)

    assert_receive {:first_waiting, resolver}, 5_000
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
