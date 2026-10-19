defmodule Absinthe.Type.IncrementalDirectivesTest do
  use Absinthe.Case, async: false

  alias Absinthe.Schema

  defmodule OptInSchema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :value, :string
    end
  end

  defmodule DefaultSchema do
    use Absinthe.Schema
    use Absinthe.Fixture

    query do
      field :value, :string
    end
  end

  @introspection_query """
  {
    __schema {
      directives {
        name
        isRepeatable
        locations
        args {
          name
          defaultValue
          type {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
  }
  """

  test "incremental directives are absent unless imported" do
    refute Schema.lookup_directive(DefaultSchema, :defer)
    refute Schema.lookup_directive(DefaultSchema, :stream)

    assert {:ok, %{data: %{"__schema" => %{"directives" => directives}}}} =
             Absinthe.run(@introspection_query, DefaultSchema)

    names = Enum.map(directives, & &1["name"])
    refute "defer" in names
    refute "stream" in names
  end

  test "SDL export preserves the imported directives and their defaults" do
    sdl = Schema.to_sdl(OptInSchema)

    assert sdl =~
             "directive @defer(label: String, if: Boolean! = true) on FRAGMENT_SPREAD | INLINE_FRAGMENT"

    assert sdl =~
             "directive @stream(initialCount: Int! = 0, label: String, if: Boolean! = true) on FIELD"

    refute Schema.to_sdl(DefaultSchema) =~ "directive @defer"
    refute Schema.to_sdl(DefaultSchema) =~ "directive @stream"
  end

  test "imported directives expose the draft definition through introspection" do
    assert Schema.lookup_directive(OptInSchema, :defer)
    assert Schema.lookup_directive(OptInSchema, :stream)

    assert {:ok, %{data: %{"__schema" => %{"directives" => directives}}}} =
             Absinthe.run(@introspection_query, OptInSchema)

    assert %{
             "isRepeatable" => false,
             "locations" => ["FRAGMENT_SPREAD", "INLINE_FRAGMENT"],
             "args" => defer_args
           } = directive(directives, "defer")

    assert %{
             "if" => %{
               "defaultValue" => "true",
               "type" => %{
                 "kind" => "NON_NULL",
                 "name" => nil,
                 "ofType" => %{"kind" => "SCALAR", "name" => "Boolean"}
               }
             },
             "label" => %{
               "defaultValue" => nil,
               "type" => %{"kind" => "SCALAR", "name" => "String"}
             }
           } = args_by_name(defer_args)

    assert %{
             "isRepeatable" => false,
             "locations" => ["FIELD"],
             "args" => stream_args
           } = directive(directives, "stream")

    assert %{
             "if" => %{
               "defaultValue" => "true",
               "type" => %{
                 "kind" => "NON_NULL",
                 "name" => nil,
                 "ofType" => %{"kind" => "SCALAR", "name" => "Boolean"}
               }
             },
             "label" => %{
               "defaultValue" => nil,
               "type" => %{"kind" => "SCALAR", "name" => "String"}
             },
             "initialCount" => %{
               "defaultValue" => "0",
               "type" => %{
                 "kind" => "NON_NULL",
                 "name" => nil,
                 "ofType" => %{"kind" => "SCALAR", "name" => "Int"}
               }
             }
           } = args_by_name(stream_args)
  end

  defp directive(directives, name) do
    Enum.find(directives, &(&1["name"] == name))
  end

  defp args_by_name(args), do: Map.new(args, &{&1["name"], &1})
end
