defmodule Absinthe.Schema.SdlRenderTest do
  use ExUnit.Case, async: true

  defmodule SdlTestSchema do
    use Absinthe.Schema

    alias Absinthe.Blueprint.Schema

    @sdl """
    "Schema description"
    schema {
      query: Query
    }

    directive @foo(name: String!) repeatable on OBJECT | SCALAR

    interface Animal {
      legCount: Int!
    }

    \"""
    A submitted post
    Multiline description
    \"""
    type Post {
      old: String @deprecated(reason: \"""
        It's old
        Really old
      \""")

      sweet: SweetScalar

      "Something"
      title: String!
    }

    input ComplexInput {
      foo: String
    }

    scalar SweetScalar

    type Query {
      echo(
        category: Category!

        "The number of times"
        times: Int = 10
      ): [Category!]!
      posts: Post
      search(limit: Int, sort: SorterInput!): [SearchResult]
      defaultBooleanArg(boolean: Boolean = false): String
      defaultInputArg(input: ComplexInput = {foo: "bar"}): String
      defaultListArg(things: [String] = ["ThisThing"]): [String]
      defaultEnumArg(category: Category = NEWS): Category
      defaultNullStringArg(name: String = null): String
      animal: Animal
    }

    type Dog implements Pet & Animal {
      legCount: Int!
      name: String!
    }

    "Simple description"
    enum Category {
      "Just the facts"
      NEWS

      \"""
      What some rando thinks

      Take with a grain of salt
      \"""
      OPINION

      CLASSIFIED
    }

    interface Pet implements Animal {
      name: String!
      legCount: Int!
    }

    "One or the other"
    union SearchResult = Post | User

    "Sort this thing"
    input SorterInput {
      "By this field"
      field: String!
    }

    type User {
      name: String!
    }
    """
    import_sdl @sdl
    def sdl, do: @sdl

    def hydrate(%Schema.InterfaceTypeDefinition{identifier: :animal}, _) do
      {:resolve_type, &__MODULE__.resolve_type/1}
    end

    def hydrate(%{identifier: :pet}, _) do
      {:resolve_type, &__MODULE__.resolve_type/1}
    end

    def hydrate(_node, _ancestors), do: []

    def resolve_type(_), do: false
  end

  test "Render SDL from blueprint defined with SDL" do
    assert Absinthe.Schema.to_sdl(SdlTestSchema) ==
             SdlTestSchema.sdl()
  end

  defmodule DefaultValuesSchema do
    use Absinthe.Schema
    @schema_provider Absinthe.Schema.PersistentTerm

    @string_default "  " <> ~S(#{literal}) <> " \n\t\0  " <> String.duplicate("x", 5_001)

    def string_default, do: @string_default

    directive :export_config do
      arg :enabled, non_null(:boolean), default_value: true
      arg :count, non_null(:integer), default_value: 0
      on [:field]
    end

    scalar :code do
      serialize fn value -> "code:#{value}" end
      parse fn _ -> :error end
    end

    enum :export_order do
      value :ascending, as: :asc
      value :descending, as: :desc
    end

    input_object :export_options do
      field :sort_order, :export_order, default_value: :asc
      field :enabled, :boolean, default_value: false
      field :codes, list_of(:code), default_value: [:blue, nil]
    end

    input_object :export_batch do
      field :options, list_of(:export_options),
        default_value: [%{sort_order: :asc, codes: [:blue, nil]}, nil]
    end

    import_sdl """
    input ImportedExportOptions {
      enabled: Boolean = false
    }
    """

    extend input_object(:imported_export_options) do
      field :sort_order, :export_order, default_value: :desc
    end

    query do
      field :fallback, :string, default_value: "fallback"

      field :export, :string do
        arg :code, :code, default_value: :blue
        arg :order, non_null(:export_order), default_value: :asc
        arg :literal_string, :string, default_value: @string_default
        arg :batch, :export_batch
        arg :imported_options, :imported_export_options

        arg :options, :export_options,
          default_value: %{sort_order: :desc, enabled: false, codes: [:blue, nil]}
      end
    end
  end

  test "macro defaults use their GraphQL types without starting the schema provider" do
    sdl = Absinthe.Schema.to_sdl(DefaultValuesSchema)

    assert sdl =~ "directive @exportConfig(count: Int! = 0, enabled: Boolean! = true) on FIELD"

    assert sdl =~ ~s(code: Code = "code:blue")
    assert sdl =~ "order: ExportOrder! = ASCENDING"

    assert sdl =~
             ~s(options: ExportOptions = {codes: ["code:blue", null], enabled: false, sortOrder: DESCENDING})

    assert {:ok, _} = Absinthe.Phase.Parse.run(sdl)

    assert {:ok, ^sdl} =
             Mix.Tasks.Absinthe.Schema.Sdl.generate_schema(%Mix.Tasks.Absinthe.Schema.Sdl.Options{
               schema: DefaultValuesSchema
             })
  end

  test "renders macro input field defaults alongside SDL defaults without output field defaults" do
    sdl = Absinthe.Schema.to_sdl(DefaultValuesSchema)

    assert sdl =~ "sortOrder: ExportOrder = ASCENDING"
    assert sdl =~ "enabled: Boolean = false"
    assert sdl =~ ~s(codes: [Code] = ["code:blue", null])

    assert sdl =~
             ~s(options: [ExportOptions] = [{codes: ["code:blue", null], sortOrder: ASCENDING}, null])

    assert sdl =~ """
           input ImportedExportOptions {
             enabled: Boolean = false
             sortOrder: ExportOrder = DESCENDING
           }
           """

    assert sdl =~ "fallback: String\n"
    assert {:ok, _} = Absinthe.Phase.Parse.run(sdl)
  end

  test "renders long string defaults as parseable GraphQL strings" do
    sdl = Absinthe.Schema.to_sdl(DefaultValuesSchema)

    assert {:ok, %{input: %Absinthe.Language.Document{definitions: definitions}}} =
             Absinthe.Phase.Parse.run(sdl)

    default_value =
      definitions
      |> Enum.find(&match?(%Absinthe.Language.ObjectTypeDefinition{name: "RootQueryType"}, &1))
      |> Map.fetch!(:fields)
      |> Enum.find(&(&1.name == "export"))
      |> Map.fetch!(:arguments)
      |> Enum.find(&(&1.name == "literalString"))
      |> Map.fetch!(:default_value)

    assert %Absinthe.Language.StringValue{value: value} = default_value
    assert value == DefaultValuesSchema.string_default()
  end

  defmodule StructuredDefaultsSchema do
    use Absinthe.Schema
    use Absinthe.Fixture

    scalar :json, open_ended: true do
      parse fn value -> {:ok, value} end

      serialize fn
        :no_value -> nil
        value -> value
      end
    end

    query do
      field :export, :string do
        arg :config, :json,
          default_value: %{
            enabled_flag: true,
            nested: %{"raw_key" => "  padded  ", "missing" => nil}
          }

        arg :codes, :json, default_value: [65, 66]
        arg :many, :json, default_value: Enum.to_list(1..64)
        arg :missing, :json, default_value: :no_value
      end
    end
  end

  defmodule InvalidScalarKeySchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_types StructuredDefaultsSchema, only: [:json]

    query do
      field :export, :string do
        arg :config, :json, default_value: %{"not-a-name" => true}
      end
    end
  end

  test "SDL export reports custom scalar object keys that cannot be GraphQL Names" do
    sdl = Absinthe.Schema.to_sdl(InvalidScalarKeySchema)

    assert sdl =~ "ArgumentError"
    assert sdl =~ "Cannot render scalar default object key"
    assert sdl =~ "not-a-name"
    assert sdl =~ "expected a GraphQL Name"
    assert {:error, _} = Absinthe.Phase.Parse.run(sdl)
  end

  defmodule CollidingScalarKeysSchema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_types StructuredDefaultsSchema, only: [:json]

    query do
      field :export, :string do
        arg :config, :json, default_value: %{"foo" => 2, foo: 1}
      end
    end
  end

  test "SDL export reports distinct scalar object keys that produce the same GraphQL Name" do
    sdl = Absinthe.Schema.to_sdl(CollidingScalarKeysSchema)

    assert sdl =~ "ArgumentError"
    assert sdl =~ "Duplicate scalar default object key"
    assert sdl =~ "foo"
    assert {:error, _} = Absinthe.Phase.Parse.run(sdl)
  end

  test "custom scalar defaults render nested objects and serialized null as GraphQL literals" do
    alias Absinthe.Language
    defaults = structured_scalar_defaults()

    assert %Language.ObjectValue{fields: fields} = defaults["config"]

    assert %{
             "enabled_flag" => %Language.BooleanValue{value: true},
             "nested" => %Language.ObjectValue{fields: nested}
           } = Map.new(fields, &{&1.name, &1.value})

    assert %{
             "raw_key" => %Language.StringValue{value: "  padded  "},
             "missing" => %Language.NullValue{}
           } = Map.new(nested, &{&1.name, &1.value})

    assert %Language.NullValue{} = defaults["missing"]
  end

  test "custom scalar list defaults are neither charlists nor truncated inspection output" do
    alias Absinthe.Language
    defaults = structured_scalar_defaults()

    assert %Language.ListValue{values: codes} = defaults["codes"]
    assert Enum.map(codes, & &1.value) == [65, 66]
    assert %Language.ListValue{values: many} = defaults["many"]
    assert Enum.map(many, & &1.value) == Enum.to_list(1..64)
  end

  test "introspection and SDL export preserve the same structured scalar default literals" do
    if StructuredDefaultsSchema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, StructuredDefaultsSchema})
    end

    query = """
    { __type(name: "RootQueryType") { fields { args { name defaultValue } } } }
    """

    assert {:ok, %{data: %{"__type" => %{"fields" => [%{"args" => arguments}]}}}} =
             Absinthe.run(query, StructuredDefaultsSchema)

    assert Enum.sort(Enum.map(arguments, & &1["name"])) == ["codes", "config", "many", "missing"]
    sdl_defaults = structured_scalar_defaults()

    for %{"name" => name, "defaultValue" => literal} <- arguments do
      assert {:ok, %{input: %{definitions: [%{fields: [field]}]}}} =
               Absinthe.Phase.Parse.run("input Defaults { value: Json = #{literal} }")

      assert scalar_literal_value(field.default_value) == scalar_literal_value(sdl_defaults[name])
    end
  end

  defp scalar_literal_value(%Absinthe.Language.ObjectValue{fields: fields}),
    do: Map.new(fields, &{&1.name, scalar_literal_value(&1.value)})

  defp scalar_literal_value(%Absinthe.Language.ListValue{values: values}),
    do: Enum.map(values, &scalar_literal_value/1)

  defp scalar_literal_value(%Absinthe.Language.NullValue{}), do: nil
  defp scalar_literal_value(%{value: value}), do: value

  defp structured_scalar_defaults do
    sdl = Absinthe.Schema.to_sdl(StructuredDefaultsSchema)
    assert {:ok, %{input: %{definitions: definitions}}} = Absinthe.Phase.Parse.run(sdl)

    query = Enum.find(definitions, &match?(%Absinthe.Language.ObjectTypeDefinition{}, &1))
    field = Enum.find(query.fields, &(&1.name == "export"))
    Map.new(field.arguments, &{&1.name, &1.default_value})
  end

  describe "Render SDL" do
    test "for a type" do
      assert_rendered("""
      type Person implements Entity {
        name: String!
        baz: Int
      }
      """)
    end

    test "for an interface" do
      assert_rendered("""
      interface Entity implements Node {
        name: String!
      }
      """)
    end

    test "for an input" do
      assert_rendered("""
      "Description for Profile"
      input Profile {
        "Description for name"
        name: String!
      }
      """)
    end

    test "for a union with types" do
      assert_rendered("""
      union Foo = Bar | Baz
      """)
    end

    test "for a union without types" do
      assert_rendered("""
      union Foo
      """)
    end

    test "for a scalar" do
      assert_rendered("""
      scalar MyGreatScalar
      """)
    end

    test "for a directive" do
      assert_rendered("""
      directive @foo(name: String!) on OBJECT | SCALAR
      """)
    end

    test "for a schema declaration" do
      assert_rendered("""
      schema {
        query: Query
      }
      """)
    end

    test "for a type with directive input object" do
      assert_rendered("""
      type TypeWithDirective {
        some: String @additionalInfo(input: {enabled: true})
      }
      """)
    end
  end

  defp assert_rendered(sdl) do
    rendered_sdl =
      with {:ok, %{input: doc}} <- Absinthe.Phase.Parse.run(sdl),
           %Absinthe.Language.Document{definitions: [node]} <- doc,
           blueprint = Absinthe.Blueprint.Draft.convert(node, doc) do
        Inspect.inspect(blueprint, %Inspect.Opts{pretty: true})
      end

    assert sdl == rendered_sdl
  end

  defmodule SchemaPrototypeTest do
    use Absinthe.Schema.Prototype

    input_object :info do
      field :enabled, :boolean
    end

    directive :additional_info do
      arg :input, :info
      on [:field_definition]
    end
  end

  defmodule MacroTestSchema do
    use Absinthe.Schema
    @prototype_schema SchemaPrototypeTest

    query do
      description "Escaped\t\"descrição/description\""

      field :echo, :string do
        arg :times, :integer, default_value: 10, description: "The number of times"
        arg :time_interval, :integer
      end

      field :search, :search_result

      field :documented_field, :string do
        directive :additional_info, input: %{enabled: true}
      end
    end

    directive :foo do
      arg :baz, :string
      on :field
    end

    directive :my_custom do
      on :input_object
    end

    enum :order_status do
      value :delivered
      value :processing
      value :picking
    end

    enum :status, values: [:one, :two, :three]

    object :order do
      field :id, :id
      field :name, :string
      field :status, :order_status
      field :other_status, :status
      import_fields :imported_fields
    end

    object :category do
      field :name, :string
    end

    object :imported_fields do
      field :imported, non_null(:boolean)
    end

    union :search_result do
      types [:order, :category]
    end
  end

  test "Render SDL from blueprint defined with macros" do
    assert Absinthe.Schema.to_sdl(MacroTestSchema) ==
             """
             schema {
               query: RootQueryType
             }

             directive @myCustom on INPUT_OBJECT

             directive @foo(baz: String) on FIELD

             \"Escaped\\t\\\"descrição\\/description\\\"\"
             type RootQueryType {
               echo(
                 \"The number of times\"
                 times: Int = 10

                 timeInterval: Int
               ): String
               search: SearchResult
               documentedField: String @additionalInfo(input: {enabled: true})
             }

             enum OrderStatus {
               DELIVERED
               PROCESSING
               PICKING
             }

             enum Status {
               ONE
               TWO
               THREE
             }

             type Order {
               imported: Boolean!
               id: ID
               name: String
               status: OrderStatus
               otherStatus: Status
             }

             type Category {
               name: String
             }

             union SearchResult = Order | Category
             """
  end

  defmodule TestModifier do
    def pipeline(pipeline, opts) do
      send(self(), type: :module, opts: opts)
      pipeline
    end
  end

  defmodule ModifiedTestSchema do
    use Absinthe.Schema

    def custom_pipeline(pipeline, opts) do
      send(self(), type: :function, opts: opts)
      pipeline
    end

    @pipeline_modifier TestModifier
    @pipeline_modifier {__MODULE__, :custom_pipeline}

    @sdl """
    type Query {
      echo: String
    }
    """
    import_sdl @sdl
  end

  test "Render SDL takes opts" do
    Absinthe.Schema.to_sdl(ModifiedTestSchema, sdl_render: true)
    assert_received type: :function, opts: [sdl_render: true]
    assert_received type: :module, opts: [sdl_render: true]
  end
end
