defmodule Absinthe.Incremental.MutationOrderTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives
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

    assert {data, _} = Absinthe.Case.Assertions.Incremental.consume(result)

    assert data == %{
             "one" => %{"id" => 1, "name" => "Ada"},
             "two" => %{"observed" => "first completed"}
           }

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
