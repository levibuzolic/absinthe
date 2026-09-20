defmodule Absinthe.Integration.Execution.SuspendedNullPropagationTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers

    object :node do
      field :required, non_null(:string) do
        resolve fn _, _ -> {:error, "required failed"} end
      end

      field :delayed, :string do
        resolve fn _, _, resolution ->
          %{mode: mode, pid: pid} = resolution.context
          path = Absinthe.Resolution.path(resolution)

          result = fn ->
            send(pid, {:completed, path})
            {:error, "delayed failed"}
          end

          case mode do
            :sync -> result.()
            :async -> async(result)
            :batch -> batch({__MODULE__, :batch}, path, fn _ -> result.() end)
          end
        end
      end

      field :child, non_null(:node)
    end

    query do
      field :node, :node
      field :nodes, list_of(non_null(:node))
      field :matrix, list_of(non_null(list_of(non_null(:node))))

      field :outside, :string do
        resolve fn _, _ -> async(fn -> {:ok, "ready"} end) end
      end
    end

    def batch(_, _), do: :ok
  end

  defmodule MatrixSchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers

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
    for schema <- [Schema, MatrixSchema],
        schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, schema}, id: schema)
    end

    :ok
  end

  test "all completed sibling errors survive null propagation across suspension boundaries" do
    for fields <- ["delayed required", "required delayed"], outside <- ["", "outside"] do
      for {selection, path, key} <- [
            {"node { #{fields} }", ["node"], "node"},
            {"node { child { #{fields} } }", ["node", "child"], "node"},
            {"nodes { #{fields} }", ["nodes", 0], "nodes"},
            {"matrix { #{fields} }", ["matrix", 0, 0], "matrix"}
          ] do
        query = "{ #{outside} #{selection} }"
        expected = execute(query, :sync)

        assert expected.data ==
                 Map.merge(
                   %{key => nil},
                   if(outside == "", do: %{}, else: %{"outside" => "ready"})
                 )

        assert Enum.sort(Enum.map(expected.errors, & &1.path)) ==
                 Enum.sort([path ++ ["delayed"], path ++ ["required"]])

        assert_received {:completed, completed_path}
        assert completed_path == path ++ ["delayed"]

        for mode <- [:async, :batch] do
          assert execute(query, mode) == expected, "#{mode} changed the response for #{query}"
          assert_received {:completed, completed_path}
          assert completed_path == path ++ ["delayed"]
          refute_received {:completed, _}
        end
      end
    end
  end

  test "an unrelated pending sibling does not delay a completed container's null propagation" do
    assert %{data: %{"outside" => "ready", "node" => nil}, errors: [error]} =
             execute("{ outside node { required } }", :sync)

    assert error.path == ["node", "required"]
  end

  test "aliased nullable matrices preserve data and errors when containers and leaves suspend" do
    row = [%{id: 1}, %{id: 2}, nil, %{id: 3}]
    root = %{node: %{id: 0, matrix: [row, nil, row]}}
    expected_row = [%{"r" => 1, "o" => 1}, nil, nil, %{"r" => 3, "o" => nil}]
    expected_data = %{"n" => %{"id" => 0, "m" => [expected_row, nil, expected_row]}}

    expected_errors =
      for row <- [0, 2],
          {column, field, message} <- [
            {1, "r", "required failed"},
            {3, "o", "optional failed"}
          ],
          do: %{path: ["n", "m", row, column, field], message: message}

    for fields <- ["r: required o: optional", "o: optional r: required"] do
      results =
        for mode <- [:sync, :containers, :leaves, :all] do
          assert {:ok, result} =
                   Absinthe.run("{ n: node { id m: matrix { #{fields} } } }", MatrixSchema,
                     root_value: root,
                     context: %{mode: mode}
                   )

          assert result.data == expected_data

          assert Enum.sort(Enum.map(result.errors, &Map.take(&1, [:message, :path]))) ==
                   Enum.sort(expected_errors)

          result
        end

      assert length(Enum.uniq(results)) == 1
    end
  end

  defp execute(query, mode) do
    assert {:ok, result} =
             Absinthe.run(query, Schema,
               context: %{mode: mode, pid: self()},
               root_value: %{node: %{child: %{}}, nodes: [%{}], matrix: [[%{}]]}
             )

    result
  end
end
