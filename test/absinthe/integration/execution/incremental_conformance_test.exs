defmodule Absinthe.Integration.Execution.IncrementalConformanceTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule ReplaceLocations do
    use Absinthe.Phase

    def run(blueprint, options) do
      location = Keyword.fetch!(options, :location)

      {:ok,
       Absinthe.Blueprint.prewalk(blueprint, fn
         %{source_location: _} = node -> %{node | source_location: location}
         node -> node
       end)}
    end
  end

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def traced(source, _, resolution) do
      send(
        resolution.context.test_pid,
        {:conformance_resolved, Absinthe.Resolution.path(resolution)}
      )

      {:ok, Map.get(source, resolution.definition.schema_node.identifier)}
    end

    object :node do
      field :a, :string, resolve: &__MODULE__.traced/3
      field :b, :string, resolve: &__MODULE__.traced/3
      field :c, :string, resolve: &__MODULE__.traced/3
      field :child, :node, resolve: &__MODULE__.traced/3
      field :nodes, list_of(:node), resolve: &__MODULE__.traced/3
      field :bad, non_null(:string), resolve: &__MODULE__.traced/3
    end

    query do
      field :node, :node, resolve: &__MODULE__.traced/3
      field :other, :node, resolve: &__MODULE__.traced/3
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    leaf = %{a: "a", b: "b", c: "c"}
    node = Map.merge(leaf, %{child: Map.put(leaf, :child, leaf), nodes: [leaf, leaf]})
    {:ok, options: [root_value: %{node: node, other: node}, context: %{test_pid: self()}]}
  end

  test "shared nested work does not deadlock two different defer parents", %{options: options} do
    query = """
    {
      ... @defer(label: "left") {
        node { a ... @defer(label: "leftChild") { child { a b } } }
      }
      ... @defer(label: "right") {
        node { b ... @defer(label: "rightChild") { child { b c } } }
      }
    }
    """

    assert_data(query, options, %{
      "node" => %{"a" => "a", "b" => "b", "child" => %{"a" => "a", "b" => "b", "c" => "c"}}
    })

    assert_once(["node"])
    assert_once(["node", "child"])
    assert_once(["node", "child", "b"])
  end

  test "a repeated named fragment retains children from different parent contexts", %{
    options: options
  } do
    query = """
    {
      node {
        ... @defer(label: "left") { a ...Shared }
        ... @defer(label: "right") { b ...Shared }
      }
    }
    fragment Shared on Node {
      child { a ...Deep @defer(label: "deep") }
    }
    fragment Deep on Node { b c }
    """

    assert_data(query, options, %{
      "node" => %{"a" => "a", "b" => "b", "child" => %{"a" => "a", "b" => "b", "c" => "c"}}
    })

    assert_once(["node", "child"])
    assert_once(["node", "child", "b"])
  end

  test "a child shared with an independent group survives its other parent's failure", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "fails") {
        bad
        ... @defer(label: "cancelledChild") { child { a b } }
      }
      ... @defer(label: "survives") { child { b c } }
    } }
    """

    {data, payloads} = execute(query, options, expect_errors: true)
    assert data == %{"node" => %{"child" => %{"b" => "b", "c" => "c"}}}
    assert completion(payloads, "fails").errors |> length() == 1
    refute Map.has_key?(completion(payloads, "survives"), :errors)
    assert_once(["node", "child", "b"])
    refute_received {:conformance_resolved, ["node", "child", "a"]}
  end

  test "a failing shared group does not discard surviving owners of its descendants", %{
    options: options
  } do
    query = """
    {
      ... @defer(label: "outer") {
        node { a child { a } }
      }
      node {
        ... @defer(label: "fails") {
          bad
          child { b }
        }
      }
    }
    """

    {data, payloads} = execute(query, options, expect_errors: true)
    assert data == %{"node" => %{"a" => "a", "child" => %{"a" => "a"}}}
    assert Map.has_key?(completion(payloads, "fails"), :errors)
    refute Map.has_key?(completion(payloads, "outer"), :errors)
  end

  test "nullable failure cancels descendants for only the affected list item", %{options: options} do
    query = """
    { node { nodes @stream(initialCount: 1) {
      a
      ... @defer(label: "item") { child { bad ... @defer(label: "unreachable") { a } } }
      ... @defer(label: "sibling") { b }
    } } }
    """

    leaf = %{a: "a", b: "b", child: %{a: "hidden"}}
    options = Keyword.put(options, :root_value, %{node: %{nodes: [leaf, leaf]}})
    {data, _} = execute(query, options, expect_errors: true)
    expected = %{"a" => "a", "b" => "b", "child" => nil}
    assert data == %{"node" => %{"nodes" => [expected, expected]}}
    refute_received {:conformance_resolved, ["node", "nodes", 0, "child", "a"]}
    refute_received {:conformance_resolved, ["node", "nodes", 1, "child", "a"]}
  end

  test "deferred selections across three depths retain each unique field exactly once", %{
    options: options
  } do
    query = """
    {
      node { child { a } }
      ... @defer(label: "root") {
        node { a child { a b ... @defer(label: "deep") { c } } }
      }
      node { ... @defer(label: "middle") { b child { b c } } }
    }
    """

    assert_data(query, options, %{
      "node" => %{"a" => "a", "b" => "b", "child" => %{"a" => "a", "b" => "b", "c" => "c"}}
    })

    for path <- [
          ["node"],
          ["node", "child"],
          ["node", "child", "a"],
          ["node", "child", "b"],
          ["node", "child", "c"]
        ],
        do: assert_once(path)
  end

  test "a group failure discards its buffered private data while shared data survives", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "fails") { a child { bad } }
      ... @defer(label: "survives") { child { b } }
    } }
    """

    {data, payloads} = execute(query, options, expect_errors: true)
    assert Map.has_key?(completion(payloads, "fails"), :errors)
    refute Map.has_key?(completion(payloads, "survives"), :errors)
    assert data == %{"node" => %{"child" => %{"b" => "b"}}}
  end

  test "a repeated directive node is visited once and cancelled with its owning parent", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "fails") { bad ...Shared }
      ... @defer(label: "survives") { a ...Shared }
    } }
    fragment Shared on Node { ...Deep @defer(label: "deep") }
    fragment Deep on Node { child { b } }
    """

    {data, payloads} = execute(query, options, expect_errors: true)
    assert Map.has_key?(completion(payloads, "fails"), :errors)
    assert data == %{"node" => %{"a" => "a"}}
    refute Enum.any?(Enum.flat_map(payloads, &Map.get(&1, :pending, [])), &(&1[:label] == "deep"))
    refute_received {:conformance_resolved, ["node", "child"]}
  end

  test "distinct directive occurrences retain surviving owners without unique locations", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "fails") { bad ...Shared @defer }
      ... @defer(label: "survives") { a ...Shared @defer }
    } }
    fragment Shared on Node { child { b } }
    """

    for location <- [nil, %Absinthe.Blueprint.SourceLocation{line: 1, column: 1}] do
      {data, payloads} = execute(query, with_locations(options, location), expect_errors: true)

      assert Map.has_key?(completion(payloads, "fails"), :errors)
      assert data == %{"node" => %{"a" => "a", "child" => %{"b" => "b"}}}
      assert_once(["node", "child"])
    end
  end

  test "location-free fragment reuse preserves directive identity for every streamed item", %{
    options: options
  } do
    query = """
    { node { nodes @stream(initialCount: 1) { ...Outer ...Outer } } }
    fragment Outer on Node { ...Shared @defer(label: "item") }
    fragment Shared on Node { a b }
    """

    {data, payloads} = execute(query, with_locations(options, nil))

    assert data == %{
             "node" => %{"nodes" => [%{"a" => "a", "b" => "b"}, %{"a" => "a", "b" => "b"}]}
           }

    item_paths =
      for payload <- payloads,
          notice <- Map.get(payload, :pending, []),
          notice[:label] == "item",
          do: notice.path

    assert item_paths == [["node", "nodes", 0], ["node", "nodes", 1]]

    for index <- [0, 1], name <- ["a", "b"], do: assert_once(["node", "nodes", index, name])
  end

  test "child groups of an otherwise empty parent are released without a parent notice", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "empty") {
        ... @defer(label: "one") { child { a } }
        ... @defer(label: "two") { child { b } }
      }
    } }
    """

    {data, payloads} = execute(query, options)
    assert data == %{"node" => %{"child" => %{"a" => "a", "b" => "b"}}}

    refute Enum.any?(
             Enum.flat_map(payloads, &Map.get(&1, :pending, [])),
             &(&1[:label] == "empty")
           )

    assert_once(["node", "child"])
  end

  test "the same fragment reached initially and through several defers resolves once", %{
    options: options
  } do
    for selections <- [
          "...Shared ... @defer { a ...Shared } ... @defer { b ...Shared }",
          "... @defer { a ...Shared } ... @defer { b ...Shared } ...Shared"
        ] do
      query = "{ node { #{selections} } } fragment Shared on Node { child { a b } }"

      assert_data(query, options, %{
        "node" => %{"a" => "a", "b" => "b", "child" => %{"a" => "a", "b" => "b"}}
      })

      for path <- [
            ["node"],
            ["node", "a"],
            ["node", "b"],
            ["node", "child"],
            ["node", "child", "a"],
            ["node", "child", "b"]
          ],
          do: assert_once(path)
    end
  end

  test "one failed task completes all its owners while preserving a third overlapping group", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "first") { child { bad } }
      ... @defer(label: "second") { child { bad } }
      ... @defer(label: "third") { child { b } }
    } }
    """

    {data, payloads} = execute(query, options, expect_errors: true)
    assert data == %{"node" => %{"child" => %{"b" => "b"}}}

    for label <- ["first", "second"] do
      assert [%{path: ["node", "child", "bad"]}] = completion(payloads, label).errors
    end

    refute Map.has_key?(completion(payloads, "third"), :errors)
    assert_once(["node", "child", "bad"])
    assert_once(["node", "child", "b"])
  end

  test "nested streamed children of shared objects retain per-item defer identities", %{
    options: options
  } do
    query = """
    { node {
      ... @defer(label: "left") { child { a } }
      ... @defer(label: "right") {
        child { b nodes @stream(label: "nodes", initialCount: 1) {
          a ... @defer(label: "item") { b ... @defer(label: "inner") { c } }
        } }
      }
    } }
    """

    leaf = %{a: "a", b: "b", c: "c"}

    options =
      Keyword.put(options, :root_value, %{node: %{child: %{a: "a", b: "b", nodes: [leaf, leaf]}}})

    item = %{"a" => "a", "b" => "b", "c" => "c"}

    assert_data(query, options, %{
      "node" => %{"child" => %{"a" => "a", "b" => "b", "nodes" => [item, item]}}
    })

    for index <- [0, 1],
        name <- ["a", "b", "c"],
        do: assert_once(["node", "child", "nodes", index, name])
  end

  defp assert_data(query, options, expected) do
    {actual, _payloads} = execute(query, options)
    assert actual == expected
  end

  defp execute(query, options, consume_options \\ []) do
    assert {:ok, %Absinthe.Incremental{} = result} =
             Absinthe.run_incremental(query, Schema, options)

    Incremental.consume(result, consume_options)
  end

  defp completion(payloads, label) do
    %{id: id} =
      payloads |> Enum.flat_map(&Map.get(&1, :pending, [])) |> Enum.find(&(&1[:label] == label))

    payloads |> Enum.flat_map(&Map.get(&1, :completed, [])) |> Enum.find(&(&1.id == id))
  end

  defp assert_once(path) do
    assert_received {:conformance_resolved, ^path}
    refute_received {:conformance_resolved, ^path}
  end

  defp with_locations(options, location) do
    Keyword.put(options, :pipeline_modifier, fn pipeline, _ ->
      Absinthe.Pipeline.insert_before(
        pipeline,
        Absinthe.Incremental.Start,
        {ReplaceLocations, location: location}
      )
    end)
  end
end
