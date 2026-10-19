defmodule Absinthe.Integration.Execution.ParsedInputTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

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

  test "strings, sources, parsed documents and parsed blueprints have the same execution contract" do
    query = "{ numbers value }"
    source = %Absinthe.Language.Source{body: query, name: "ordinary.graphql"}
    assert {:ok, blueprint = %{input: document}} = Absinthe.Phase.Parse.run(source)
    options = [root_value: %{value: 42, numbers: [1, 2]}]
    expected = %{data: %{"value" => 42, "numbers" => [1, 2]}}

    for input <- [query, source, document, blueprint] do
      assert Absinthe.run(input, Schema, options) == {:ok, expected}
      assert Absinthe.run!(input, Schema, options) == expected
    end
  end

  test "parsed documents still undergo ordinary document validation" do
    assert {:ok, %{input: document}} = Absinthe.Phase.Parse.run("{ unknown }")
    assert {:ok, %{errors: [error]}} = Absinthe.run(document, Schema)
    assert error.message == ~s(Cannot query field "unknown" on type "RootQueryType".)
    assert error.locations == [%{line: 1, column: 3}]
  end
end
