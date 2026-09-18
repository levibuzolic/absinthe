defmodule Absinthe.Phase.Document.Validation.IncrementalTest do
  use Absinthe.Case, async: true

  alias Absinthe.{Phase, Pipeline}

  defmodule RenamedAdapter do
    use Absinthe.Adapter

    @names %{
      directive: %{
        "later" => "defer",
        "chunks" => "stream",
        "omit" => "skip",
        "keep" => "include"
      },
      argument: %{"when" => "if", "tag" => "label", "first" => "initial_count"}
    }

    def to_internal_name(name, role) do
      get_in(@names, [role, name]) ||
        Absinthe.Adapter.LanguageConventions.to_internal_name(name, role)
    end

    def to_external_name(name, role) do
      Enum.find_value(Map.get(@names, role, %{}), fn {external, internal} ->
        if internal == name, do: external
      end) || Absinthe.Adapter.LanguageConventions.to_external_name(name, role)
    end
  end

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :node do
      field :name, :string
      field :names, non_null(list_of(non_null(:string)))
      field :matrix, list_of(list_of(:string))
      field :child, :node
    end

    object :other_node do
      field :names, list_of(:string)
    end

    union :result do
      types [:node, :other_node]
      resolve_type fn _, _ -> :node end
    end

    query do
      field :node, :node, resolve: fn _, _ -> {:ok, %{name: "hello", names: ["hello"]}} end
      field :search, :result
    end

    mutation do
      field :update, :node
      field :updates, list_of(:node)
    end

    subscription do
      field :changed, :node
      field :changes, list_of(:node)
    end
  end

  defmodule CustomDirectiveSchema do
    use Absinthe.Schema
    use Absinthe.Fixture

    directive :defer do
      arg :label, :string
      on [:inline_fragment]
    end

    directive :stream do
      arg :label, :string
      on [:field]
    end

    query do
      field :name, :string, resolve: fn _, _ -> {:ok, "hello"} end
    end

    mutation do
      field :name, :string, resolve: fn _, _ -> {:ok, "hello"} end
    end
  end

  defp errors(document, options \\ []) do
    pipeline =
      Schema
      |> Pipeline.for_document(Keyword.put(options, :jump_phases, false))
      |> Pipeline.upto(Phase.Document.Validation.Result)

    {_, blueprint, _} = Pipeline.run(document, pipeline)
    blueprint.execution.validation_errors
  end

  defp messages(document, options \\ []) do
    document |> errors(options) |> Enum.map(& &1.message)
  end

  test "adapted stream names still require list fields and prohibit overlapping fields" do
    assert ["Directive `chunks` may only be used on list fields."] ==
             messages("{ node { name @chunks } }", adapter: RenamedAdapter)

    assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
             messages("{ node { names @chunks names } }", adapter: RenamedAdapter)

    assert [] == errors("{ node { names @chunks(first: -1) } }", adapter: RenamedAdapter)
  end

  test "adapted labels must remain unique literal values" do
    assert ["Incremental directive label `same` must be unique."] ==
             messages(
               "{ node { ... @later(tag: \"same\") { name } names @chunks(tag: \"same\") } }",
               adapter: RenamedAdapter
             )

    assert ["Directive `chunks` label must be a string literal."] ==
             messages("query ($label: String) { node { names @chunks(tag: $label) } }",
               adapter: RenamedAdapter,
               variables: %{"label" => "unique"}
             )
  end

  test "adapted conditional arguments retain static subscription disablement semantics" do
    assert [] ==
             errors(
               "subscription { changed { ... @later(when: false) { name } names @chunks(when: false) } }",
               adapter: RenamedAdapter
             )

    assert [] ==
             errors(
               "subscription ($condition: Boolean!) { changed { names @chunks(when: $condition) } }",
               adapter: RenamedAdapter,
               variables: %{"condition" => true}
             )

    assert ["Directive `chunks` must be disableable in a subscription operation."] ==
             messages("subscription { changed { names @chunks(when: true) } }",
               adapter: RenamedAdapter
             )
  end

  test "adapted skip and include directives control static subscription exclusion" do
    for condition <- ["@omit(when: true)", "@keep(when: false)"] do
      assert [] ==
               errors("subscription { changed #{condition} { names @chunks } }",
                 adapter: RenamedAdapter
               )
    end

    for condition <- ["@omit(when: false)", "@keep(when: true)"] do
      assert ["Directive `chunks` must be disableable in a subscription operation."] ==
               messages("subscription { changed #{condition} { names @chunks } }",
                 adapter: RenamedAdapter
               )
    end
  end

  test "adapted directive names retain mutation root restrictions" do
    assert ["Directive `chunks` is not allowed at a mutation or subscription root."] ==
             messages("mutation { updates @chunks(when: false) { name } }",
               adapter: RenamedAdapter
             )

    assert ["Directive `later` is not allowed at a mutation or subscription root."] ==
             messages("mutation { ... @later { update { name } } }", adapter: RenamedAdapter)
  end

  test "unrelated custom directives stay untouched when their names are adapted" do
    assert {:ok, %{data: %{"name" => "hello"}}} =
             Absinthe.run(
               """
               mutation ($label: String) {
                 ... @later(tag: $label) { name @chunks(tag: "same") }
                 name @chunks(tag: "same")
               }
               """,
               CustomDirectiveSchema,
               adapter: RenamedAdapter,
               variables: %{"label" => "same"}
             )
  end

  test "unique labels across defer and stream, null labels and omitted labels are valid" do
    assert [] ==
             errors("""
             {
               node {
                 ... @defer(label: "detail") { name }
                 ... @defer(label: null) { name }
                 ... @defer(label: null) { name }
                 names @stream(label: "names")
                 matrix @stream
               }
             }
             """)
  end

  test "labels must be unique across directives and unselected operations" do
    assert ["Incremental directive label `same` must be unique."] ==
             messages(
               """
               query First { node { ... @defer(label: "same") { name } } }
               query Second { node { names @stream(label: "same") } }
               """,
               operation_name: "First"
             )
  end

  test "labels must be unique even on statically disabled or skipped directives" do
    assert ["Incremental directive label `same` must be unique."] ==
             messages("""
             { node {
               ... @defer(if: false, label: "same") { name }
               names @stream(label: "same") @skip(if: true)
             } }
             """)
  end

  test "label variables are forbidden even if they resolve to null or a unique string" do
    for value <- [nil, "unique"] do
      assert ["Directive `stream` label must be a string literal."] ==
               messages("query ($label: String) { node { names @stream(label: $label) } }",
                 variables: %{"label" => value}
               )
    end
  end

  test "fragment reuse does not multiply a directive's label" do
    assert [] ==
             errors("""
             { node { ...Names ...Names } }
             fragment Names on Node { names @stream(label: "names") }
             """)
  end

  test "stream accepts nullable, non-null, and nested list fields" do
    assert [] == errors("{ node { names @stream matrix @stream } }")
  end

  test "stream rejects scalar and object fields even when disabled" do
    for field <- ["name", "child { name }"] do
      selection = String.replace(field, ~r/^\w+/, "\\0 @stream(if: false)")

      assert ["Directive `stream` may only be used on list fields."] ==
               messages("{ node { #{selection} } }")
    end
  end

  test "negative initialCount is not a validation error" do
    assert [] == errors("{ node { names @stream(initialCount: -1) } }")
  end

  test "ordinary execution eagerly handles valid imported directives" do
    assert {:ok, %{data: %{"node" => %{"name" => "hello", "names" => ["hello"]}}}} =
             Absinthe.run("{ node { ... @defer { name } names @stream } }", Schema)
  end

  test "ordinary execution still validates incremental directives" do
    assert {:ok, %{errors: [%{message: "Directive `stream` may only be used on list fields."}]}} =
             Absinthe.run("{ node { name @stream } }", Schema)
  end

  test "custom directives with the same names retain their existing semantics" do
    assert {:ok, %{data: %{"name" => "hello"}}} =
             Absinthe.run(
               """
               mutation ($label: String) {
                 ... @defer(label: $label) { name @stream(label: "same") }
                 name @stream(label: "same")
               }
               """,
               CustomDirectiveSchema,
               variables: %{"label" => "same"}
             )
  end

  test "root mutation and subscription stream are forbidden even when disabled and skipped" do
    for {operation, field} <- [{"mutation", "updates"}, {"subscription", "changes"}] do
      assert ["Directive `stream` is not allowed at a mutation or subscription root."] ==
               messages("#{operation} { #{field} @stream(if: false) @skip(if: true) { name } }")
    end
  end

  test "root defer is forbidden through a chain of named and inline fragments" do
    assert ["Directive `defer` is not allowed at a mutation or subscription root."] ==
             messages("""
             mutation { ...Outer }
             fragment Outer on RootMutationType { ...Inner }
             fragment Inner on RootMutationType { ... @defer(if: false) { update { name } } }
             """)
  end

  test "root stream is forbidden through named fragments" do
    assert ["Directive `stream` is not allowed at a mutation or subscription root."] ==
             messages("""
             mutation { ...Root }
             fragment Root on RootMutationType { updates @stream { name } }
             """)
  end

  test "nested mutation directives are permitted" do
    assert [] == errors("mutation { update { ... @defer { name } names @stream } }")
  end

  test "root restrictions validate unselected operations" do
    assert ["Directive `stream` is not allowed at a mutation or subscription root."] ==
             messages(
               """
               query Selected { node { name } }
               mutation Other { updates @stream { name } }
               """,
               operation_name: "Selected"
             )
  end

  test "subscriptions reject unconditional nested defer and stream" do
    for selection <- ["... @defer { name }", "names @stream", "names @stream(if: true)"] do
      assert [message] = messages("subscription { changed { #{selection} } }")
      assert message =~ "must be disableable in a subscription operation"
    end
  end

  test "subscriptions allow disabled directives and variable conditions independent of values" do
    assert [] ==
             errors(
               "subscription { changed { ... @defer(if: false) { name } names @stream(if: false) } }"
             )

    for value <- [true, false] do
      assert [] ==
               errors(
                 """
                 subscription ($condition: Boolean!) {
                   changed { ... @defer(if: $condition) { name } names @stream(if: $condition) }
                 }
                 """,
                 variables: %{"condition" => value}
               )
    end
  end

  test "subscriptions allow potentially excluding skip and include on ancestors" do
    for condition <- ["@skip(if: true)", "@include(if: false)"] do
      assert [] == errors("subscription { changed #{condition} { names @stream } }")
    end

    for directive <- ["skip", "include"] do
      assert [] ==
               errors(
                 """
                 subscription ($condition: Boolean!) {
                   changed @#{directive}(if: $condition) { names @stream }
                 }
                 """,
                 variables: %{"condition" => true}
               )
    end
  end

  test "subscriptions still reject directives that skip and include cannot exclude" do
    for condition <- ["@skip(if: false)", "@include(if: true)"] do
      assert ["Directive `stream` must be disableable in a subscription operation."] ==
               messages("subscription { changed #{condition} { names @stream } }")
    end
  end

  test "skipped fragment visitation does not hide an unconditional use" do
    assert ["Directive `stream` must be disableable in a subscription operation."] ==
             messages("""
             subscription { changed { ...Names @skip(if: true) ...Names } }
             fragment Names on Node { names @stream }
             """)
  end

  test "overlapping stream occurrences are forbidden even if one stream is disabled" do
    for selections <- [
          "names @stream names",
          "names names @stream",
          "names @stream names @stream",
          "names @stream(if: false) names"
        ] do
      assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
               messages("{ node { #{selections} } }")
    end
  end

  test "stream overlap is based on response names" do
    assert [] == errors("{ node { first: names @stream second: names @stream } }")

    assert ["Fields `same` overlap and cannot use the `stream` directive."] ==
             messages("{ node { same: names @stream same: matrix } }")
  end

  test "stream overlap visits named and inline fragments" do
    assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
             messages("""
             { node { ...Names ... on Node { names } } }
             fragment Names on Node { names @stream }
             """)
  end

  test "stream overlap compares nested selections of merged parents" do
    assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
             messages("{ node { child { names @stream } } node { child { names } } }")
  end

  test "stream overlap is forbidden across mutually exclusive types" do
    assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
             messages("""
             { search {
               ... on Node { names @stream }
               ... on OtherNode { names }
             } }
             """)
  end

  test "overlap errors are deduplicated across repeated fragment graphs and operations" do
    assert ["Fields `names` overlap and cannot use the `stream` directive."] ==
             messages(
               """
               query First { node { ...Outer ...Outer } }
               query Second { node { ...Outer } }
               fragment Outer on Node { ...Inner ...Inner }
               fragment Inner on Node { names @stream names }
               """,
               operation_name: "First"
             )
  end

  test "overlap errors report the locations of both field occurrences" do
    assert [%{locations: [%{line: 2}, %{line: 3}]}] =
             errors("""
             { node {
               names @stream
               names
             } }
             """)
  end
end
