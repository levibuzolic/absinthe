defmodule Absinthe.Incremental.InputTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :value, :integer
      field :numbers, list_of(:integer)
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "source strings, source structs and parsed documents have the same execution contract" do
    query = "{ numbers @stream(initialCount: 1) ... @defer { value } }"
    source = %Absinthe.Language.Source{body: query, name: "incremental.graphql"}
    assert {:ok, %{input: document}} = Absinthe.Phase.Parse.run(source)
    options = [root_value: %{value: 42, numbers: [1, 2]}]

    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3] do
      assert {:ok, expected} = run.(query, Schema, options)
      {data, payloads} = Incremental.consume(expected)
      assert data == %{"value" => 42, "numbers" => [1, 2]}

      for input <- [source, document] do
        assert {:ok, result} = run.(input, Schema, options)
        assert Incremental.consume(result) == {data, payloads}
      end
    end

    assert Absinthe.run!(document, Schema, options) ==
             Absinthe.run!(query, Schema, options)

    for format <- [:graphql_draft, :relay] do
      options = Keyword.put(options, :incremental_format, format)

      assert payloads(Absinthe.run_incremental!(document, Schema, options)) ==
               payloads(Absinthe.run_incremental!(query, Schema, options))
    end
  end

  test "parsed documents still undergo ordinary document validation" do
    assert {:ok, %{input: document}} = Absinthe.Phase.Parse.run("{ unknown }")

    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3] do
      assert {:ok, %{errors: [error]}} = run.(document, Schema, [])
      assert error.message == ~s(Cannot query field "unknown" on type "RootQueryType".)
      assert error.locations == [%{line: 1, column: 3}]
    end
  end

  defp payloads(result), do: [result.initial_result | Enum.to_list(result.subsequent_results)]
end
