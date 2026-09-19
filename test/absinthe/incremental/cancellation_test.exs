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

      field :observed_child, :node do
        resolve fn source, _, %{context: %{test_pid: pid}} ->
          send(pid, :resolved_shared_child)
          {:ok, source.child}
        end
      end

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
             Incremental.consume(result, expect_errors: true)

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

    assert {%{"node" => %{}}, payloads} = Incremental.consume(result, expect_errors: true)
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

  test "a published three-owner frame keeps its nested stream when another owner fails" do
    query = """
    { node {
      ... @defer(label: "a") { ...Shared }
      ... @defer(label: "b") { ...Shared private: value failure }
      ... @defer(label: "c") { ...Shared value }
    } }
    fragment Shared on Node {
      child: observedChild { children @stream(label: "stream") { hidden } }
    }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{node: %{value: 9, child: %{children: [%{}, %{}]}}},
               context: %{test_pid: self()}
             )

    refute_received :resolved_shared_child
    refute_received :resolved_hidden

    assert {%{
              "node" => %{
                "child" => %{"children" => [%{"hidden" => "hidden"}, %{"hidden" => "hidden"}]},
                "value" => 9
              }
            },
            [initial, shared, failed | later]} =
             Incremental.consume(result, expect_errors: true)

    assert [%{id: a, label: "a"}, %{id: b, label: "b"}, %{id: c, label: "c"}] =
             initial.pending

    assert shared.incremental == [%{id: a, data: %{"child" => %{"children" => []}}}]
    assert shared.completed == [%{id: a}]
    assert [%{id: stream, label: "stream", path: ["node", "child", "children"]}] = shared.pending

    assert [%{id: ^b, errors: [%{message: "failed", path: ["node", "failure"]}]}] =
             failed.completed

    refute Map.has_key?(failed, :incremental)

    assert [%{id: ^c}, %{id: ^stream}] = Enum.flat_map(later, &Map.get(&1, :completed, []))

    assert [
             %{id: ^c, data: %{"value" => 9}},
             %{id: ^stream, items: [%{"hidden" => "hidden"}]},
             %{id: ^stream, items: [%{"hidden" => "hidden"}]}
           ] = Enum.flat_map(later, &Map.get(&1, :incremental, []))

    assert_received :resolved_shared_child
    refute_received :resolved_shared_child
    assert_received :resolved_hidden
    assert_received :resolved_hidden
    refute_received :resolved_hidden
  end

  defp labels(payloads) do
    for payload <- payloads, pending <- Map.get(payload, :pending, []), do: pending.label
  end
end
