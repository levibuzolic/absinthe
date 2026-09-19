defmodule Absinthe.Incremental.NestedSuspensionTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :node do
      field :id, :integer

      field :required, non_null(:integer) do
        resolve fn source, _, resolution ->
          resolve_value(resolution, :leaves, fn ->
            if source.id == 2, do: {:error, "required failed"}, else: {:ok, source.id}
          end)
        end
      end

      field :optional, :integer do
        resolve fn source, _, resolution ->
          resolve_value(resolution, :leaves, fn ->
            if source.id == 3, do: {:error, "optional failed"}, else: {:ok, source.id}
          end)
        end
      end

      field :matrix, list_of(list_of(:node)) do
        resolve fn source, _, resolution ->
          resolve_value(resolution, :containers, fn -> {:ok, source.matrix} end)
        end
      end
    end

    query do
      field :node, :node
    end

    defp resolve_value(%{context: %{mode: mode}}, boundary, fun) do
      if mode in [:all, boundary], do: async(fun), else: fun.()
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "nested container and leaf suspension preserve nullable matrix data and error paths" do
    assert_suspension_boundaries(:ordinary)
  end

  test "deferred aliased matrices preserve complete draft and Relay payloads under suspension" do
    for format <- [:draft, :relay], do: assert_suspension_boundaries(format)
  end

  defp assert_suspension_boundaries(format) do
    row = [%{id: 1}, %{id: 2}, nil, %{id: 3}]
    root = %{node: %{id: 0, matrix: [row, nil, row]}}
    expected_row = [%{"r" => 1, "o" => 1}, nil, nil, %{"r" => 3, "o" => nil}]
    expected_matrix = %{"m" => [expected_row, nil, expected_row]}
    expected_data = %{"n" => Map.put(expected_matrix, "id", 0)}

    relative_errors =
      for row <- [0, 2],
          {column, field, message} <- [
            {1, "r", "required failed"},
            {3, "o", "optional failed"}
          ] do
        %{path: ["m", row, column, field], message: message}
      end

    for fields <- ["r: required o: optional", "o: optional r: required"] do
      query = """
      { n: node { id ... @defer(label: "Node$defer$Matrix") { m: matrix { #{fields} } } } }
      """

      payloads =
        for mode <- [:sync, :containers, :leaves, :all] do
          options = [root_value: root, context: %{mode: mode}]

          case format do
            :ordinary ->
              assert {:ok, result} = Absinthe.run(query, Schema, options)
              assert result.data == expected_data
              assert_errors(result.errors, relative_errors, ["n"])
              [result]

            :draft ->
              assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)

              assert {^expected_data, payloads} =
                       Absinthe.Case.Assertions.Incremental.consume(result)

              assert [_, %{incremental: [patch]}] = payloads
              assert patch.data == expected_matrix
              assert_errors(patch.errors, relative_errors, ["n"])
              payloads

            :relay ->
              assert {:ok, result} =
                       Absinthe.run_incremental(
                         query,
                         Schema,
                         Keyword.put(options, :incremental_format, :relay)
                       )

              assert result.initial_result.data == %{"n" => %{"id" => 0}}
              subsequent = Enum.to_list(result.subsequent_results)
              assert [patch, %{hasNext: false}] = subsequent
              assert patch.path == ["n"]
              assert patch.label == "Node$defer$Matrix"
              assert patch.data == expected_matrix
              assert_errors(patch.errors, relative_errors, [])
              [result.initial_result | subsequent]
          end
        end

      assert length(Enum.uniq(payloads)) == 1,
             "#{format} payloads changed across suspension boundaries for #{fields}"
    end
  end

  defp assert_errors(errors, expected, prefix) do
    assert Enum.sort(Enum.map(errors, &Map.take(&1, [:path, :message]))) ==
             Enum.sort(Enum.map(expected, &%{&1 | path: prefix ++ &1.path}))
  end
end
