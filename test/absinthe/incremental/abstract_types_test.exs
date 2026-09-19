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

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
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
    assert_no_errors(payloads)

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
            }, payloads} = Incremental.consume(result)

    assert_no_errors(payloads)
  end

  defp assert_no_errors(payloads) do
    for payload <- payloads,
        entry <- [
          payload | Map.get(payload, :incremental, []) ++ Map.get(payload, :completed, [])
        ] do
      refute Map.has_key?(entry, :errors)
    end
  end
end
