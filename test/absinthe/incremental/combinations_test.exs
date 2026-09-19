defmodule Absinthe.Incremental.CombinationsTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def traced(source, _, resolution) do
      send(resolution.context.test_pid, {:resolved, Absinthe.Resolution.path(resolution)})
      {:ok, Map.get(source, resolution.definition.schema_node.identifier)}
    end

    object :row do
      field :id, :integer, resolve: &__MODULE__.traced/3
      field :value, :integer, resolve: &__MODULE__.traced/3
      field :children, list_of(:row), resolve: &__MODULE__.traced/3
    end

    query do
      field :rows, list_of(:row), resolve: &__MODULE__.traced/3
    end
  end

  @query """
  query Combinations(
    $parent: Boolean!, $sibling: Boolean!, $details: Boolean!, $nested: Boolean!,
    $rows: Boolean!, $children: Boolean!, $rowCount: Int!, $childCount: Int!
  ) {
    ...Rows @defer(if: $parent)
    ...Rows @defer(if: $sibling)
  }
  fragment Rows on RootQueryType {
    roster: rows @stream(if: $rows, initialCount: $rowCount) {
      id
      ...Details @defer(if: $details)
      descendants: children @stream(if: $children, initialCount: $childCount) {
        id
        ...Details @defer(if: $nested)
      }
    }
  }
  fragment Details on Row { value }
  """

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "combined defer and stream settings preserve data, demand and resolver identity" do
    populated = [
      %{id: 1, value: 10, children: [%{id: 2, value: 20}, nil, %{id: 3, value: 30}]},
      nil,
      %{id: 4, value: nil, children: nil},
      %{id: 5, value: 50, children: []}
    ]

    # Exhaust this bounded product rather than relying on a random seed: shared
    # defer owners, nested fragments, disabled streams and three prefix sizes.
    cases =
      for rows <- [populated, [], nil],
          parent <- [false, true],
          sibling <- [false, true],
          details <- [false, true],
          nested <- [false, true],
          {stream_rows, row_count} <- [{false, 0}, {true, 0}, {true, 1}, {true, 9}],
          {stream_children, child_count} <- [{false, 0}, {true, 0}, {true, 1}, {true, 9}] do
        {rows,
         %{
           "parent" => parent,
           "sibling" => sibling,
           "details" => details,
           "nested" => nested,
           "rows" => stream_rows,
           "children" => stream_children,
           "rowCount" => row_count,
           "childCount" => child_count
         }}
      end

    for {rows, variables} <- cases do
      options = [root_value: %{rows: rows}, context: %{test_pid: self()}, variables: variables]
      assert {:ok, %{data: expected} = eager} = Absinthe.run(@query, Schema, options)
      refute Map.has_key?(eager, :errors)
      resolved_paths()

      expected_initial = initial_data(rows, variables)
      assert {:ok, result} = Absinthe.run_incremental(@query, Schema, options)

      if expected_initial == expected do
        assert %{data: ^expected_initial} = result
        refute Map.has_key?(result, :hasNext)
      else
        assert %Absinthe.Incremental{initial_result: %{pending: [_ | _], hasNext: true}} = result
      end

      initial = Map.get(result, :initial_result, result)

      assert initial.data == expected_initial,
             "initial data mismatch for #{inspect({rows, variables})}"

      initial_paths = resolved_paths()

      assert Enum.sort(initial_paths) == Enum.sort(data_paths(expected_initial, [])),
             "resolved fields absent from initial data: #{inspect(variables)}"

      assert {data, payloads} = Incremental.consume(result)
      assert data == expected, "data mismatch for #{inspect({rows, variables})}"

      assert Enum.sort(initial_paths ++ resolved_paths()) == Enum.sort(data_paths(expected, [])),
             "missing or repeated resolver calls: #{inspect(variables)}"

      for payload <- payloads,
          entry <- [
            payload | Map.get(payload, :incremental, []) ++ Map.get(payload, :completed, [])
          ] do
        refute Map.has_key?(entry, :errors)
      end
    end

    assert length(cases) == 768
  end

  defp initial_data(_rows, %{"parent" => true, "sibling" => true}), do: %{}

  defp initial_data(rows, variables) do
    roster =
      initial_list(rows, variables["rows"], variables["rowCount"], fn row ->
        children =
          initial_list(row.children, variables["children"], variables["childCount"], fn child ->
            initial_fields(child, variables["nested"])
          end)

        row
        |> initial_fields(variables["details"])
        |> Map.put("descendants", children)
      end)

    %{"roster" => roster}
  end

  defp initial_list(nil, _stream, _count, _render), do: nil

  defp initial_list(values, stream, count, render) do
    values = if stream, do: Enum.take(values, count), else: values

    Enum.map(values, fn
      nil -> nil
      value -> render.(value)
    end)
  end

  defp initial_fields(row, true), do: %{"id" => row.id}
  defp initial_fields(row, false), do: %{"id" => row.id, "value" => row.value}

  defp resolved_paths do
    receive do
      {:resolved, path} -> [path | resolved_paths()]
    after
      0 -> []
    end
  end

  defp data_paths(data, prefix) when is_map(data) do
    Enum.flat_map(data, fn {key, value} ->
      path = prefix ++ [key]
      [path | data_paths(value, path)]
    end)
  end

  defp data_paths(data, prefix) when is_list(data) do
    data
    |> Enum.with_index()
    |> Enum.flat_map(fn {value, index} -> data_paths(value, prefix ++ [index]) end)
  end

  defp data_paths(_, _), do: []
end
