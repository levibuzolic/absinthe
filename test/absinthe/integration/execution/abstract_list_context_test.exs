defmodule Absinthe.Integration.Execution.AbstractListContextTest do
  use Absinthe.Case, async: true

  defmodule ContextSchema do
    use Absinthe.Schema
    use Absinthe.Fixture

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
    if ContextSchema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, ContextSchema})
    end

    :ok
  end

  test "every abstract list item keeps parent field metadata and observes shared-state updates" do
    rows = [%{name: "Ada"}, %{name: "Grace"}]

    for field <- [:named, :search, :nested] do
      nested? = field == :nested
      values = if nested?, do: [rows, rows], else: rows
      root = %{field => values}
      query = "{ results: #{field}(person: true) { ... on Person { name } } }"

      assert {:ok, %{data: data}} =
               Absinthe.run(query, ContextSchema, root_value: root, context: %{test_pid: self()})

      expected_rows = Enum.map(rows, &%{"name" => &1.name})
      expected = if nested?, do: [expected_rows, expected_rows], else: expected_rows
      assert data == %{"results" => expected}
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

      refute_received {:type_context, _, _}
    end
  end
end
