defmodule Absinthe.Incremental.CancellationTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :node do
      field :id, :integer
      field :value, :integer
      field :child, :node
      field :children, list_of(:node)

      field :failure, non_null(:string) do
        resolve fn _, _ -> {:error, "failed"} end
      end

      field :hidden, :string do
        resolve fn _, _, %{context: %{test_pid: pid}} ->
          send(pid, :resolved_hidden)
          {:ok, "hidden"}
        end
      end
    end

    query do
      field :node, :node
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "a surviving shared job drops occurrences belonging to a failed no-work descendant" do
    query = """
    { node {
      ... @defer(label: "fails") {
        failure
        child { id }
        ... @defer(label: "dominated") { child { hidden } }
      }
      ... @defer(label: "survives") { child { value } }
    } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{node: %{child: %{id: 1, value: 2}}},
               context: %{test_pid: self()}
             )

    assert {%{"node" => %{"child" => %{"value" => 2}}}, payloads} =
             Incremental.consume(result)

    assert labels(payloads) == ["fails", "survives"]
    refute_received :resolved_hidden
  end

  test "a shared private frame cancels its streams and defers when its last owner fails" do
    query = """
    { node {
      ... @defer(label: "a") { ...Shared failure }
      ... @defer(label: "b") { ...Shared other: failure }
    } }
    fragment Shared on Node {
      child {
        children @stream(label: "stream") { hidden }
        ... @defer(label: "nested") { hidden }
      }
    }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{node: %{child: %{children: [%{}, %{}]}}},
               context: %{test_pid: self()}
             )

    assert {%{"node" => %{}}, payloads} = Incremental.consume(result)
    assert labels(payloads) == ["a", "b"]

    assert 2 ==
             Enum.count(
               for(
                 payload <- payloads,
                 completion <- Map.get(payload, :completed, []),
                 do: completion
               ),
               &Map.has_key?(&1, :errors)
             )

    refute_received :resolved_hidden
  end

  defp labels(payloads) do
    for payload <- payloads, pending <- Map.get(payload, :pending, []), do: pending.label
  end
end
