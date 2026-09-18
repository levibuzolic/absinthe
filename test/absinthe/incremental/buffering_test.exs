defmodule Absinthe.Incremental.BufferingTest do
  use Absinthe.Case, async: true

  alias Absinthe.{Phase, Pipeline}

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :row do
      field :id, :integer
      field :value, non_null(:integer)
      field :nullable_value, :integer
      field :child, :row
      field :children, list_of(:row)
    end

    query do
      field :rows, list_of(:row)
      field :numbers, list_of(:integer)
    end
  end

  defmodule Result do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)

      case blueprint.result do
        %{data: %{"value" => value}} ->
          {:ok,
           %{blueprint | result: Map.put(blueprint.result, :extensions, %{last_value: value})}}

        _ ->
          {:ok, blueprint}
      end
    end
  end

  defmodule HideErrorsResult do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)
      {:ok, %{blueprint | result: Map.delete(blueprint.result, :errors)}}
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "one group spanning a large list publishes values in execution order with the last extensions" do
    rows = Enum.map(1..1024, &%{id: &1, value: &1})

    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ rows { id } ... @defer { rows { value } } }",
               Schema,
               root_value: %{rows: rows},
               pipeline_modifier: fn pipeline, _ ->
                 Pipeline.replace(pipeline, Phase.Document.Result, Result)
               end
             )

    assert [%{id: id, path: []}] = result.initial_result.pending
    assert [payload] = Enum.to_list(result.subsequent_results)
    assert payload.hasNext == false
    assert payload.completed == [%{id: id}]
    assert payload.extensions == %{last_value: 1024}

    assert payload.incremental ==
             Enum.map(0..1023, fn index ->
               %{id: id, subPath: ["rows", index], data: %{"value" => index + 1}}
             end)
  end

  test "ordinary null scalars, objects and lists preserve deferred work elsewhere" do
    rows = Enum.map(1..256, &%{id: &1, value: &1})

    query = """
    {
      rows { id }
      ... @defer {
        rows {
          nullableValue
          child { ... @defer { value } }
          children @stream { ... @defer { value } }
          ... @defer { value }
        }
      }
    }
    """

    assert {:ok, %{data: expected}} = Absinthe.run(query, Schema, root_value: %{rows: rows})
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, root_value: %{rows: rows})
    assert {^expected, payloads} = Absinthe.Case.Assertions.Incremental.consume(result)
    refute Enum.any?(payloads, &Map.has_key?(&1, :errors))
  end

  test "null propagation cancels descendants even when a result phase hides errors" do
    selections = "child { ... @defer { id } value }"

    for query <- [
          "{ rows { id #{selections} } ... @defer { numbers } }",
          "{ rows { id } ... @defer { rows { #{selections} } numbers } }"
        ] do
      assert {:ok, result} =
               Absinthe.run_incremental(query, Schema,
                 root_value: %{rows: [%{id: 1, child: %{id: 2, value: nil}}], numbers: [3]},
                 pipeline_modifier: fn pipeline, _ ->
                   Pipeline.replace(pipeline, Phase.Document.Result, HideErrorsResult)
                 end
               )

      assert {%{"rows" => [%{"id" => 1, "child" => nil}], "numbers" => [3]}, _} =
               Absinthe.Case.Assertions.Incremental.consume(result)
    end
  end

  test "a late non-null failure discards all earlier private values in the group" do
    rows = Enum.map(1..256, &%{id: &1, value: &1}) ++ [%{id: 257, value: nil}]

    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ rows { id } ... @defer { rows { value } } }",
               Schema,
               root_value: %{rows: rows}
             )

    assert [%{id: id}] = result.initial_result.pending

    assert [%{hasNext: false, completed: [%{id: ^id, errors: [error]}]} = payload] =
             Enum.to_list(result.subsequent_results)

    assert error.path == ["rows", 256, "value"]
    refute Map.has_key?(payload, :incremental)
  end

  test "a late group failure cancels a stream whose private parent value was buffered earlier" do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ rows { id } ... @defer { numbers @stream rows { value } } }",
               Schema,
               root_value: %{numbers: [1, 2], rows: [%{id: 1, value: nil}]}
             )

    assert [%{id: id}] = result.initial_result.pending

    assert [%{hasNext: false, completed: [%{id: ^id, errors: [_]}]} = payload] =
             Enum.to_list(result.subsequent_results)

    refute Map.has_key?(payload, :pending)
    refute Map.has_key?(payload, :incremental)
  end

  test "a later failure preserves a stream whose shared parent value was already published" do
    assert {:ok, result} =
             Absinthe.run_incremental(
               """
               {
                 rows { id }
                 ... @defer(label: "fails") { ...Numbers rows { value } }
                 ... @defer(label: "survives") { ...Numbers }
               }
               fragment Numbers on RootQueryType { numbers @stream }
               """,
               Schema,
               root_value: %{numbers: [1, 2], rows: [%{id: 1, value: nil}]}
             )

    %{id: failed_id} = Enum.find(result.initial_result.pending, &(&1.label == "fails"))
    %{id: survived_id} = Enum.find(result.initial_result.pending, &(&1.label == "survives"))
    [first | rest] = Enum.to_list(result.subsequent_results)
    assert [%{id: ^survived_id, data: %{"numbers" => []}}] = first.incremental
    assert [%{id: ^survived_id}] = first.completed
    assert [%{id: stream_id, path: ["numbers"]}] = first.pending

    assert [%{id: ^failed_id, errors: [_]}, %{id: ^stream_id}] =
             Enum.flat_map(rest, &Map.get(&1, :completed, []))

    assert [%{id: ^stream_id, items: [1]}, %{id: ^stream_id, items: [2]}] =
             Enum.flat_map(rest, &Map.get(&1, :incremental, []))

    assert List.last(rest).hasNext == false
  end
end
