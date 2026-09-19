defmodule Absinthe.Incremental.SuspendedFailureTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :child do
      field :value, :integer do
        resolve fn %{value: value}, _, %{context: %{pid: pid}} ->
          send(pid, {:streamed, value})
          {:ok, value}
        end
      end
    end

    object :node do
      field :children, list_of(:child)

      field :delayed, :string do
        resolve fn _, _, %{context: %{pid: pid}} ->
          send(pid, :nested_defer_resolved)
          {:ok, "canceled"}
        end
      end

      field :required_failure, non_null(:string) do
        resolve fn _, _, %{context: %{pid: pid}} ->
          async(fn ->
            send(pid, :async_failure_resolved)
            {:error, "failed asynchronously"}
          end)
        end
      end

      field :batch_failure, non_null(:string) do
        resolve fn _, _, %{context: %{pid: pid}} ->
          batch({__MODULE__, :load_failures, pid}, :failure, fn failures ->
            send(pid, :batch_failure_resolved)
            {:error, Map.fetch!(failures, :failure)}
          end)
        end
      end
    end

    def load_failures(pid, keys) do
      send(pid, {:failures_loaded, keys})
      Map.new(keys, &{&1, "failed after batching"})
    end

    query do
      field :broken, :node
      field :items, list_of(:node)
      field :required_items, list_of(non_null(:node))

      field :survivor, :string do
        resolve fn _, _, %{context: %{pid: pid}} ->
          send(pid, :survivor_resolved)
          {:ok, "alive"}
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

  test "a suspended non-null failure cancels queued defer and stream work before announcement" do
    query = """
    {
      broken {
        ... @defer(label: "nested") { delayed }
        children @stream(initialCount: 1, label: "children") { value }
        requiredFailure
      }
      ... @defer(label: "survivor") { survivor }
    }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               context: %{pid: self()},
               root_value: %{broken: %{children: [%{value: 1}, %{value: 2}]}}
             )

    assert %{
             data: %{"broken" => nil},
             errors: [
               %{message: "failed asynchronously", path: ["broken", "requiredFailure"]}
             ],
             pending: [%{id: survivor_id, label: "survivor", path: []}],
             hasNext: true
           } = result.initial_result

    assert_received :async_failure_resolved
    assert_received {:streamed, 1}
    refute_received :nested_defer_resolved
    refute_received {:streamed, 2}
    refute_received :survivor_resolved

    assert Enum.to_list(result.subsequent_results) == [
             %{
               incremental: [%{id: survivor_id, data: %{"survivor" => "alive"}}],
               completed: [%{id: survivor_id}],
               hasNext: false
             }
           ]

    assert_received :survivor_resolved
    refute_received :survivor_resolved
    refute_received :async_failure_resolved
    refute_received {:streamed, _}
    refute_received :nested_defer_resolved
  end

  test "a suspended failure in a deferred frame cancels its newly queued descendants" do
    assert_deferred_failure("requiredFailure", "failed asynchronously", :async_failure_resolved)
  end

  test "a batch callback failure cancels descendants after the batch resumes" do
    assert_deferred_failure("batchFailure", "failed after batching", :batch_failure_resolved)
    assert_received {:failures_loaded, [:failure]}
    refute_received {:failures_loaded, _}
  end

  test "nullable streamed items fail independently after suspension and keep their sibling alive" do
    assert_stream_failure("items", [nil, nil], [0, 1])
  end

  test "a suspended non-null streamed item failure cancels the remaining stream" do
    assert_stream_failure("requiredItems", [], [0])
  end

  defp assert_deferred_failure(field, message, resolved_message) do
    query = """
    {
      broken {
        ... @defer(label: "failed") {
          ... @defer(label: "nested") { delayed }
          children @stream(initialCount: 0, label: "children") { value }
          #{field}
        }
      }
      ... @defer(label: "survivor") { survivor }
    }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               context: %{pid: self()},
               root_value: %{broken: %{children: [%{value: 1}]}}
             )

    assert result.initial_result.data == %{"broken" => %{}}

    assert Enum.sort(Enum.map(result.initial_result.pending, & &1.label)) == [
             "failed",
             "survivor"
           ]

    failed_id = Enum.find(result.initial_result.pending, &(&1.label == "failed")).id
    refute_received ^resolved_message
    refute_received :survivor_resolved

    assert {%{"broken" => %{}, "survivor" => "alive"}, payloads} = Incremental.consume(result)

    assert [%{id: ^failed_id, errors: [%{message: ^message, path: ["broken", ^field]}]}] =
             for(
               payload <- payloads,
               completion <- Map.get(payload, :completed, []),
               Map.has_key?(completion, :errors),
               do: completion
             )

    assert_no_descendant_announcements(payloads)
    assert_received ^resolved_message
    refute_received ^resolved_message
    assert_received :survivor_resolved
    refute_received :survivor_resolved
    refute_received :nested_defer_resolved
    refute_received {:streamed, _}
  end

  defp assert_stream_failure(field, expected_items, failed_indices) do
    query = """
    {
      #{field} @stream(initialCount: 0, label: "items") {
        ... @defer(label: "nested") { delayed }
        children @stream(initialCount: 0, label: "children") { value }
        requiredFailure
      }
      ... @defer(label: "survivor") { survivor }
    }
    """

    items = [%{children: [%{value: 1}]}, %{children: [%{value: 2}]}]

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               context: %{pid: self()},
               root_value: %{items: items, required_items: items}
             )

    assert result.initial_result.data == %{field => []}
    assert Enum.sort(Enum.map(result.initial_result.pending, & &1.label)) == ["items", "survivor"]
    refute_received :async_failure_resolved
    refute_received :survivor_resolved

    expected = %{field => expected_items, "survivor" => "alive"}
    assert {^expected, payloads} = Incremental.consume(result)

    errors =
      for payload <- payloads,
          entry <- Map.get(payload, :incremental, []) ++ Map.get(payload, :completed, []),
          error <- Map.get(entry, :errors, []),
          do: Map.take(error, [:message, :path])

    assert errors ==
             Enum.map(failed_indices, fn index ->
               %{message: "failed asynchronously", path: [field, index, "requiredFailure"]}
             end)

    assert_no_descendant_announcements(payloads)

    for _ <- failed_indices do
      assert_received :async_failure_resolved
    end

    refute_received :async_failure_resolved
    assert_received :survivor_resolved
    refute_received :survivor_resolved
    refute_received :nested_defer_resolved
    refute_received {:streamed, _}
  end

  defp assert_no_descendant_announcements(payloads) do
    for payload <- payloads, pending <- Map.get(payload, :pending, []) do
      refute pending.label in ["nested", "children"]
    end
  end
end
