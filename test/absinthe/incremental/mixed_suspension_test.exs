defmodule Absinthe.Incremental.MixedSuspensionTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import Absinthe.Resolution.Helpers
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

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

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "mixed suspension preserves every completed sibling error through objects, lists and deliveries" do
    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3],
        fields <- ["delayed required", "required delayed"],
        outside <- ["", "outside"] do
      selections = [
        {"node { #{fields} }", ["node"]},
        {"node { child { #{fields} } }", ["node", "child"]},
        {"nodes { #{fields} }", ["nodes", 0]},
        {"matrix { #{fields} }", ["matrix", 0, 0]},
        {"node { ... @defer { #{fields} } }", ["node"]},
        {"... @defer { node { #{fields} } }", ["node"]},
        {"nodes @stream { #{fields} }", ["nodes", 0]},
        {"matrix @stream { #{fields} }", ["matrix", 0, 0]}
      ]

      for {selection, path} <- selections do
        query = "{ #{outside} #{selection} }"
        expected = execute(query, :sync, run)

        assert Enum.sort(error_paths(expected)) ==
                 Enum.sort([path ++ ["delayed"], path ++ ["required"]])

        assert_received {:completed, completed_path}
        assert completed_path == path ++ ["delayed"]

        for mode <- [:async, :batch] do
          assert execute(query, mode, run) == expected,
                 "#{mode} changed the response for #{query}"

          assert_received {:completed, completed_path}
          assert completed_path == path ++ ["delayed"]
          refute_received {:completed, _}
        end
      end
    end
  end

  test "an unrelated pending sibling does not postpone a completed container's null propagation" do
    for mode <- [:sync, :async, :batch] do
      assert [%{data: %{"outside" => "ready", "node" => nil}, errors: [error]}] =
               execute("{ outside node { required } }", mode)

      assert error.path == ["node", "required"]
    end
  end

  defp execute(query, mode, run \\ &Absinthe.run_incremental/3) do
    assert {:ok, result} =
             run.(query, Schema,
               context: %{mode: mode, pid: self()},
               root_value: %{node: %{child: %{}}, nodes: [%{}], matrix: [[%{}]]}
             )

    case result do
      %Absinthe.Incremental{} -> [result.initial_result | Enum.to_list(result.subsequent_results)]
      result -> [result]
    end
  end

  defp error_paths(payloads) do
    for payload <- payloads,
        entry <- [
          payload | Map.get(payload, :incremental, []) ++ Map.get(payload, :completed, [])
        ],
        error <- Map.get(entry, :errors, []),
        do: error.path
  end
end
