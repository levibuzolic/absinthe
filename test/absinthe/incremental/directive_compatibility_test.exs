defmodule Absinthe.Incremental.DirectiveCompatibilityTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule BuiltinDeferSchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives, only: [:defer]

    directive :stream do
      arg :if, :boolean, default_value: true
      arg :label, :string
      on [:field]
    end

    query do
      field :value, :string
      field :delayed, :string
    end
  end

  defmodule BuiltinStreamSchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives, only: [:stream]

    directive :defer do
      arg :if, :boolean, default_value: true
      arg :label, :string
      on [:inline_fragment]
    end

    query do
      field :value, :string
      field :other, :string
      field :numbers, list_of(:integer)
    end
  end

  setup_all do
    for schema <- [BuiltinDeferSchema, BuiltinStreamSchema],
        schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!(Supervisor.child_spec({Absinthe.Schema.Manager, schema}, id: schema))
    end

    :ok
  end

  test "builtin defer leaves custom stream labels and overlapping scalar fields untouched" do
    query = """
    query($label: String!) {
      value @stream(if: true, label: $label)
      value @stream(if: true, label: "shared")
      ... @defer(label: "shared") { delayed }
    }
    """

    options = [
      variables: %{"label" => "shared"},
      root_value: %{value: "eager", delayed: "later"}
    ]

    assert {:ok, result} = Absinthe.run_incremental(query, BuiltinDeferSchema, options)
    assert result.initial_result.data == %{"value" => "eager"}
    assert [%{label: "shared", path: []}] = result.initial_result.pending

    assert {%{"value" => "eager", "delayed" => "later"} = expected, _} =
             Incremental.consume(result)

    assert {:ok, %{data: ^expected}} = Absinthe.run(query, BuiltinDeferSchema, options)
  end

  test "builtin stream leaves active custom defer fragments and variable labels eager" do
    query = """
    query($label: String!) {
      numbers @stream(initialCount: 1, label: "shared")
      ... @defer(if: true, label: $label) { value }
      ... @defer(if: true, label: "shared") { other }
    }
    """

    options = [
      variables: %{"label" => "shared"},
      root_value: %{value: "eager", other: "also eager", numbers: [1, 2]}
    ]

    assert {:ok, result} = Absinthe.run_incremental(query, BuiltinStreamSchema, options)

    assert result.initial_result.data == %{
             "value" => "eager",
             "other" => "also eager",
             "numbers" => [1]
           }

    assert [%{label: "shared", path: ["numbers"]}] = result.initial_result.pending

    assert {%{"value" => "eager", "other" => "also eager", "numbers" => [1, 2]} = expected, _} =
             Incremental.consume(result)

    assert {:ok, %{data: ^expected}} = Absinthe.run(query, BuiltinStreamSchema, options)
  end
end
