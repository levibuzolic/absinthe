defmodule Absinthe.Integration.Execution.MutationOrderTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_types Absinthe.Fixtures.MutationOrder

    defdelegate plugins(), to: Absinthe.Fixtures.MutationOrder
    defdelegate context(context), to: Absinthe.Fixtures.MutationOrder

    query do
      import_fields :mutation_order_queries
    end

    mutation do
      import_fields :mutation_order_mutations
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

  test "BatchResolver completes unequal mutation documents independently" do
    alias Absinthe.{Phase, Pipeline}

    queries = [
      "mutation { one: secondAsync { id } two: secondBatch { id } three: secondLoaded { id } }",
      "mutation { immediateFailure { id } second { id } }",
      "mutation { only: secondBatch { id } }"
    ]

    options = [context: %{test_pid: self()}]

    expected =
      Enum.map(queries, fn query ->
        assert {:ok, result} = Absinthe.run(query, Schema, options)
        result
      end)

    expected_events = trace() |> Enum.frequencies()

    pipeline =
      Schema
      |> Pipeline.for_document(options)
      |> Pipeline.before(Phase.Document.Execution.Resolution)

    for documents <- [Enum.zip(queries, expected), Enum.zip(queries, expected) |> Enum.reverse()] do
      blueprints =
        Enum.map(documents, fn {query, _expected} ->
          assert {:ok, blueprint, _} = Pipeline.run(query, pipeline)
          blueprint
        end)

      results = Pipeline.BatchResolver.run(blueprints, schema: Schema)
      assert length(results) == length(documents)

      for {blueprint, {_query, expected}} <- Enum.zip(results, documents) do
        assert blueprint.execution.pending == []
        assert blueprint.execution.resolved == %{}
        assert blueprint.execution.mutation == nil
        assert {:ok, %{result: ^expected}} = Phase.Document.Result.run(blueprint, [])
      end

      assert trace() |> Enum.frequencies() == expected_events
    end
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
