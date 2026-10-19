defmodule Absinthe.Fixtures.AbstractListContext do
  use Absinthe.Schema.Notation

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

  object :abstract_list_queries do
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
