defmodule Absinthe.Incremental.PruningTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :row do
      field :id, :integer
      field :value, :integer

      field :failure, :integer do
        resolve fn _, _ -> {:error, "unavailable"} end
      end
    end

    query do
      field :rows, list_of(:row)
      field :numbers, list_of(non_null(:integer))
      field :marker, :string
    end
  end

  defmodule HideErrors do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Absinthe.Phase.Document.Result.run(blueprint, options)
      {:ok, %{blueprint | result: Map.delete(blueprint.result, :errors)}}
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "aliased nullable errors preserve deferred work within the same object and other rows" do
    rows = Enum.map(1..4, &%{id: &1, value: &1})

    query = """
    { rows { id ... @defer { result: failure ... @defer { value } } } }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, root_value: %{rows: rows})
    assert {data, payloads} = Incremental.consume(result)

    assert data == %{
             "rows" => Enum.map(1..4, &%{"id" => &1, "result" => nil, "value" => &1})
           }

    paths =
      for payload <- payloads,
          entry <- Map.get(payload, :incremental, []),
          error <- Map.get(entry, :errors, []),
          do: error.path

    assert Enum.sort(paths) == Enum.map(0..3, &["rows", &1, "result"])
  end

  test "a failed scalar-list prefix cancels its stream even when result formatting hides errors" do
    query = """
    { values: numbers @stream(initialCount: 1) ... @defer(label: "survives") { marker } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{numbers: [nil, 2], marker: "ready"},
               pipeline_modifier: fn pipeline, _ ->
                 Absinthe.Pipeline.replace(pipeline, Absinthe.Phase.Document.Result, HideErrors)
               end
             )

    assert result.initial_result.data == %{"values" => nil}
    assert [%{label: "survives"}] = result.initial_result.pending
    assert {%{"values" => nil, "marker" => "ready"}, _} = Incremental.consume(result)
  end
end
