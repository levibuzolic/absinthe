defmodule Absinthe.Incremental.AbstractTypesTest do
  use Absinthe.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    interface :named do
      field :name, :string
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    object :person do
      interface :named
      field :name, :string
      field :age, :integer
    end

    object :organization do
      interface :named
      field :name, :string
      field :slug, :string
    end

    object :rock do
      field :hardness, :integer
    end

    union :search_result do
      types [:person, :organization, :rock]
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    query do
      field :search, list_of(:search_result)
      field :named, list_of(:named)
    end
  end

  defmodule ContextSchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def resolve_kind(item, resolution) do
      send(resolution.context.test_pid, {:type_context, item, resolution})
      if resolution.arguments[:person], do: :person
    end

    def resolve_rows(source, _, resolution),
      do: {:ok, Map.fetch!(source, resolution.definition.schema_node.identifier)}

    def parent_metadata(resolution, _) do
      resolution
      |> put_in([Access.key(:private), :marker], :parent)
      |> put_in([Access.key(:extensions), :marker], :parent)
    end

    interface :named do
      field :name, :string
      resolve_type &__MODULE__.resolve_kind/2
    end

    union :search_result do
      types [:person]
      resolve_type &__MODULE__.resolve_kind/2
    end

    object :person do
      interface :named

      field :name, :string do
        resolve fn source, _, _ -> {:ok, source.name} end

        middleware fn resolution, _ ->
          resolution
          |> put_in([Access.key(:context), :completed], resolution.value)
          |> put_in([Access.key(:acc), :completed], resolution.value)
          |> put_in([Access.key(:private), :marker], :child)
          |> put_in([Access.key(:extensions), :marker], :child)
        end
      end
    end

    query do
      field :named, list_of(:named) do
        arg :person, :boolean
        resolve &__MODULE__.resolve_rows/3
        middleware &__MODULE__.parent_metadata/2
      end

      field :search, list_of(:search_result) do
        arg :person, :boolean
        resolve &__MODULE__.resolve_rows/3
        middleware &__MODULE__.parent_metadata/2
      end

      field :nested, list_of(list_of(:named)) do
        arg :person, :boolean
        resolve &__MODULE__.resolve_rows/3
        middleware &__MODULE__.parent_metadata/2
      end
    end
  end

  setup_all do
    for schema <- [Schema, ContextSchema] do
      if schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
        start_supervised!({Absinthe.Schema.Manager, schema}, id: schema)
      end
    end

    :ok
  end

  test "deferred interface and union fragments apply independently to streamed concrete types" do
    query = """
    { results: search @stream(initialCount: 1) {
      __typename
      ...Names @defer(label: "names")
      ...Details @defer(label: "details")
    } }
    fragment Names on Named { display: name }
    fragment Details on SearchResult {
      ... on Person { age }
      ... on Organization { slug }
      ... on Rock { hardness }
    }
    """

    rows = [
      %{kind: :person, name: "Ada", age: 37},
      %{kind: :organization, name: "ACME", slug: "acme"},
      %{kind: :rock, hardness: 7}
    ]

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, root_value: %{search: rows})
    assert result.initial_result.data == %{"results" => [%{"__typename" => "Person"}]}

    assert {data, payloads} = Incremental.consume(result)

    assert data == %{
             "results" => [
               %{"__typename" => "Person", "display" => "Ada", "age" => 37},
               %{"__typename" => "Organization", "display" => "ACME", "slug" => "acme"},
               %{"__typename" => "Rock", "hardness" => 7}
             ]
           }

    assert [["results", 0], ["results", 1]] ==
             for(
               payload <- payloads,
               notice <- Map.get(payload, :pending, []),
               notice[:label] == "names",
               do: notice.path
             )
  end

  test "an interface field preserves shared and type-specific deferred selections for every item" do
    query = """
    { named {
      __typename
      ... on Named @defer { name }
      ... on Person @defer { name age }
      ... on Organization @defer { name slug }
    } }
    """

    assert {:ok, %Absinthe.Incremental{} = result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{
                 named: [
                   %{kind: :person, name: "Ada", age: 37},
                   %{kind: :organization, name: "ACME", slug: "acme"}
                 ]
               }
             )

    assert result.initial_result.data == %{
             "named" => [%{"__typename" => "Person"}, %{"__typename" => "Organization"}]
           }

    assert %{pending: [_ | _], hasNext: true} = result.initial_result

    assert {%{
              "named" => [
                %{"__typename" => "Person", "name" => "Ada", "age" => 37},
                %{"__typename" => "Organization", "name" => "ACME", "slug" => "acme"}
              ]
            }, _payloads} = Incremental.consume(result)
  end

  test "a streamed abstract item resolves with the same field arguments as an eager item" do
    options = [root_value: %{named: [%{name: "Ada"}]}, context: %{test_pid: self()}]
    selection = "{ ... on Person { name } }"

    assert {:ok, %{data: expected}} =
             Absinthe.run("{ named(person: true) #{selection} }", ContextSchema, options)

    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ named(person: true) @stream(initialCount: 0) #{selection} }",
               ContextSchema,
               options
             )

    assert {^expected, _} = Incremental.consume(result)
  end

  test "abstract list completion preserves field metadata and carries shared state between items" do
    rows = [%{name: "Ada"}, %{name: "Grace"}]

    for field <- [:named, :search, :nested], count <- [:eager, 0, 1, 2] do
      nested? = field == :nested
      values = if nested?, do: [rows, rows], else: rows
      root = %{field => values}
      directive = if count == :eager, do: "", else: "@stream(initialCount: #{count})"
      query = "{ results: #{field}(person: true) #{directive} { ... on Person { name } } }"
      api = if count == :eager, do: :run, else: :run_incremental

      assert {:ok, result} =
               apply(Absinthe, api, [
                 query,
                 ContextSchema,
                 [root_value: root, context: %{test_pid: self()}]
               ])

      expected_rows = Enum.map(rows, &%{"name" => &1.name})
      expected = if nested?, do: [expected_rows, expected_rows], else: expected_rows
      assert {%{"results" => ^expected}, _} = Incremental.consume(result)

      paths = if nested?, do: [[0, 0], [0, 1], [1, 0], [1, 1]], else: [[0], [1]]

      for {indices, index} <- Enum.with_index(paths) do
        assert_receive {:type_context, item, resolution}
        assert item == Enum.at(rows, rem(index, 2))
        assert resolution.arguments == %{person: true}
        assert resolution.definition.name == Atom.to_string(field)
        assert resolution.definition.alias == "results"
        assert resolution.parent_type.identifier == :query
        assert resolution.source == root
        assert resolution.root_value == root
        assert resolution.schema == ContextSchema
        assert resolution.state == :resolved
        assert resolution.value == values
        assert resolution.private.marker == :parent
        assert resolution.extensions.marker == :parent
        assert Absinthe.Resolution.path(resolution) == ["results" | indices]
        previous = if index > 0, do: Enum.at(rows, rem(index - 1, 2)).name
        assert resolution.context[:completed] == previous
        assert resolution.acc[:completed] == previous
      end
    end
  end
end
