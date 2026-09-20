defmodule Absinthe.Phase.Document.Validation.StreamOverlapTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    interface :parent do
      field :child, :child
      field :names, list_of(:string)
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    interface :other_parent do
      field :child, :child
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    object :a do
      interface :parent
      field :child, :child
      field :names, list_of(:string)
    end

    object :b do
      interfaces [:parent, :other_parent]
      field :child, :child
      field :names, list_of(:string)
    end

    object :child do
      field :names, list_of(:string)
      field :child, :child
    end

    union :result do
      types [:a, :b]
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    query do
      field :result, :result
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "nested streams remain independent beneath different concrete parent types" do
    queries = [
      """
      { result {
        ... on A { child { names @stream } }
        ... on B { child { names } }
      } }
      """,
      """
      { result {
        ... on A { ... { child { names @stream } } }
        ... on B { child { names } }
      } }
      """,
      """
      { result { ...AFields ...BFields } }
      fragment AFields on A { child { names @stream } }
      fragment BFields on B { child { names } }
      """
    ]

    expected = %{"result" => %{"child" => %{"names" => ["one", "two"]}}}

    for query <- queries, kind <- [:a, :b] do
      options = [root_value: %{result: %{kind: kind, child: %{names: ["one", "two"]}}}]
      assert {:ok, %{data: ^expected}} = Absinthe.run(query, Schema, options)
      assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
      assert {^expected, _payloads} = Incremental.consume(result)

      if kind == :a do
        assert %Absinthe.Incremental{} = result
      else
        assert %{data: ^expected} = result
      end
    end
  end

  test "concrete parent exclusivity persists through deeper children with the same type" do
    query = """
    { result {
      ... on A { child { child { names @stream } } }
      ... on B { child { child { names } } }
    } }
    """

    expected = %{"result" => %{"child" => %{"child" => %{"names" => ["one", "two"]}}}}

    for kind <- [:a, :b] do
      options = [
        root_value: %{result: %{kind: kind, child: %{child: %{names: ["one", "two"]}}}}
      ]

      assert {:ok, %{data: ^expected}} = Absinthe.run(query, Schema, options)
      assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
      assert {^expected, _payloads} = Incremental.consume(result)
    end
  end

  test "direct stream overlap is still invalid across different concrete parent types" do
    assert_overlap("""
    { result {
      ... on A { names @stream }
      ... on B { names }
    } }
    """)
  end

  test "matching concrete parents and abstract parents still merge child selections" do
    for selections <- [
          "... on A { child { names @stream } } ... on A { child { names } }",
          "... on Parent { child { names @stream } } ... on A { child { names } }",
          "... on Parent { child { names @stream } } ... on OtherParent { child { names } }",
          "... on A { child { names @stream } } ... on OtherParent { child { names } }"
        ] do
      assert_overlap("{ result { #{selections} } }")
    end
  end

  test "every independent child set is validated after separating concrete alternatives" do
    assert_overlap("""
    { result {
      ... on A { child { names @stream names } }
      ... on B { child { names } }
    } }
    """)
  end

  test "abstract overlap errors are deduplicated across concrete child groups" do
    assert_overlap("""
    { result {
      ... on Parent { child { names @stream } }
      ... on OtherParent { child { names } }
      ... on A { child { __typename } }
      ... on B { child { __typename } }
    } }
    """)
  end

  test "unknown fields and fragment types retain validation errors" do
    for query <- [
          "{ result { ... on A { unknown { names @stream } } } }",
          "{ result { ... on Missing { child { names @stream } } } }"
        ] do
      assert {:ok, %{errors: [_ | _]}} = Absinthe.run_incremental(query, Schema)
    end
  end

  defp assert_overlap(query) do
    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3] do
      assert {:ok, %{errors: [%{message: message}]}} = run.(query, Schema, [])
      assert message == "Fields `names` overlap and cannot use the `stream` directive."
    end
  end
end
