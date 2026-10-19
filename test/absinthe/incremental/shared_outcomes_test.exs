defmodule Absinthe.Incremental.SharedOutcomesTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :value, :integer
      field :shared, :integer
      field :numbers, list_of(:integer)
      field :failure, non_null(:string)

      field :hidden, :string do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, :resolved_hidden)
          {:ok, "hidden"}
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

  test "a shared failure is announced for a nested fragment after its parent succeeds" do
    query = """
    {
      ... @defer(label: "R") { failure }
      ... @defer(label: "P") {
        value
        ... @defer(label: "C") {
          failure
          hidden
          ... @defer(label: "cancelled") { hidden }
        }
      }
    }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{value: 1},
               context: %{test_pid: self()}
             )

    assert {%{"value" => 1}, payloads} = Incremental.consume(result, expect_errors: true)
    notices = for payload <- payloads, pending <- Map.get(payload, :pending, []), do: pending
    assert Enum.map(notices, & &1.label) == ["R", "P", "C"]
    child = Enum.find(notices, &(&1.label == "C"))

    completions =
      for payload <- payloads, completed <- Map.get(payload, :completed, []), do: completed

    assert [%{errors: [%{path: ["failure"]}]}] = Enum.filter(completions, &(&1.id == child.id))
    terminal = List.last(payloads)
    assert terminal.pending == [child]
    assert Enum.any?(terminal.completed, &(&1.id == child.id))
    refute_received :resolved_hidden
  end

  test "multiple shared failures complete an unannounced owner only once" do
    query = """
    {
      ... @defer(label: "R1") { first: failure }
      ... @defer(label: "R2") { second: failure }
      ... @defer(label: "P") {
        value
        ... @defer(label: "C") { first: failure second: failure }
      }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, root_value: %{value: 1})
    assert {%{"value" => 1}, payloads} = Incremental.consume(result, expect_errors: true)
    terminal = List.last(payloads)
    assert [%{id: child_id, label: "C"}] = terminal.pending

    assert [%{errors: [%{path: ["first"]}]}] =
             Enum.filter(terminal.completed, &(&1.id == child_id))
  end

  test "a successful shared stream survives its first owner's private failure" do
    query = """
    {
      ... @defer(label: "R") { failure ...Shared }
      ... @defer(label: "P") { value ... @defer(label: "C") { ...Shared } }
    }
    fragment Shared on RootQueryType { numbers @stream }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema, root_value: %{value: 1, numbers: [2, 3]})

    assert {%{"value" => 1, "numbers" => [2, 3]}, payloads} =
             Incremental.consume(result, expect_errors: true)

    notices = for payload <- payloads, pending <- Map.get(payload, :pending, []), do: pending
    assert Enum.map(notices, &Map.get(&1, :label)) == ["R", "P", "C", nil]
  end

  test "distinct deferred spreads preserve both labels and eager fragment reuse" do
    for {selections, labels} <- [
          {"...Shared @defer(label: \"A\") ...Shared @defer(label: \"B\")", ["A", "B"]},
          {"... @defer(label: \"A\") { ...Shared } ...Shared", []}
        ] do
      query = "{ #{selections} } fragment Shared on RootQueryType { shared }"
      assert {:ok, result} = Absinthe.run_incremental(query, Schema, root_value: %{shared: 2})
      assert {%{"shared" => 2}, payloads} = Incremental.consume(result)

      if labels == [] do
        assert result == %{data: %{"shared" => 2}}
      else
        assert %Absinthe.Incremental{} = result
        assert result.initial_result.data == %{}
        assert Enum.map(hd(payloads).pending, & &1.label) == labels
      end
    end
  end
end
