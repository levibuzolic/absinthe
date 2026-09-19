defmodule Absinthe.Phase.Document.Validation.StreamFragmentReuseTest do
  use Absinthe.Case, async: true

  alias Absinthe.Phase.Document.Validation.IncrementalStreams

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    interface :node do
      field :child, :node
      field :value, :integer
      field :names, list_of(:string)
      resolve_type fn _, _ -> :a end
    end

    object :a do
      interface :node
      field :child, :node
      field :value, :integer
      field :names, list_of(:string)
    end

    object :b do
      interface :node
      field :child, :node
      field :value, :integer
      field :names, list_of(:string)
    end

    query do
      field :node, :node
      field :values, list_of(:integer)
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "validation work stays bounded across reused fragments and ordinary selections" do
    queries = [
      fragment_graph(12, "value"),
      fragment_graph(12, "names @stream"),
      "{ node { #{String.duplicate("child { value } ", 1_000)} } values @stream }"
    ]

    for query <- queries do
      pipeline =
        Schema |> Absinthe.Pipeline.for_document() |> Absinthe.Pipeline.before(IncrementalStreams)

      assert {:ok, blueprint, _} = Absinthe.Pipeline.run(query, pipeline)

      {:reductions, before} = Process.info(self(), :reductions)
      assert {:ok, result} = IncrementalStreams.run(blueprint)
      {:reductions, after_count} = Process.info(self(), :reductions)

      assert result.errors == []
      # A generous work budget catches repeated merged-set expansion without
      # depending on machine speed. The original fragment walk used over 12M.
      assert after_count - before < 1_000_000
      assert {:ok, %{data: %{"node" => nil, "values" => nil}}} = Absinthe.run(query, Schema)
    end
  end

  test "reused fragment pairs report one conflict across compatible parent alternatives" do
    for stream <- ["@stream", "@stream(if: false) @skip(if: true)"] do
      query = """
      { node {
        ... on A { child { ...Left ...Left } }
        ... on B { child { ...Left } }
        ... on Node { child { ...Right } }
      } }
      fragment Left on Node { names #{stream} }
      fragment Right on Node { names }
      """

      assert {:ok, %{errors: [error]}} = Absinthe.run_incremental(query, Schema)
      assert error.message == "Fields `names` overlap and cannot use the `stream` directive."
      assert Enum.map(error.locations, & &1.line) == [6, 7]
    end
  end

  defp fragment_graph(depth, leaf) do
    branches =
      for index <- 0..(depth - 1) do
        """
        fragment F#{index} on Node {
          ... on A { child { ...F#{index + 1} ...M0 } }
          ... on B { child { ...F#{index + 1} } }
        }
        """
      end

    markers =
      for index <- 0..(depth - 1) do
        "fragment M#{index} on Node { child { ...M#{index + 1} } }"
      end

    Enum.join(
      ["{ node { ...F0 } values @stream }"] ++
        branches ++
        markers ++
        ["fragment F#{depth} on Node { #{leaf} }", "fragment M#{depth} on Node { value }"],
      "\n"
    )
  end
end
