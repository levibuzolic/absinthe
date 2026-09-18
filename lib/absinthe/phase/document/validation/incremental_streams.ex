defmodule Absinthe.Phase.Document.Validation.IncrementalStreams do
  @moduledoc false

  use Absinthe.Phase

  alias Absinthe.{Blueprint, Phase}
  alias Absinthe.Blueprint.Document
  alias Absinthe.Phase.Document.Validation.IncrementalDirectives

  @spec run(Blueprint.t(), Keyword.t()) :: Phase.result_t()
  def run(input, _options \\ []) do
    if IncrementalDirectives.enabled?(input.schema) and Blueprint.find(input, &stream_field?/1) do
      validate(input)
    else
      {:ok, input}
    end
  end

  defp stream_field?(%Document.Field{} = field), do: stream?(field)
  defp stream_field?(_), do: false

  defp validate(input) do
    fragments = Map.new(input.fragments, &{&1.name, &1})
    state = %{groups: MapSet.new(), pairs: MapSet.new(), errors: []}

    state =
      Enum.reduce(input.operations ++ input.fragments, state, fn definition, state ->
        validate_set(definition.selections, fragments, state)
      end)

    {:ok, %{input | errors: input.errors ++ Enum.reverse(state.errors)}}
  end

  defp validate_set([], _fragments, state), do: state

  defp validate_set(selections, fragments, state) do
    {fields, _} = collect_fields(selections, fragments, {[], MapSet.new()})
    fields = fields |> Enum.uniq_by(& &1.source_location) |> Enum.reverse()
    key = fields |> Enum.map(& &1.source_location) |> Enum.sort()

    if MapSet.member?(state.groups, key) do
      state
    else
      state = %{state | groups: MapSet.put(state.groups, key)}

      fields
      |> Enum.group_by(&(&1.alias || &1.name))
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce(state, fn {name, fields}, state ->
        state = validate_pairs(fields, name, state)
        validate_set(Enum.flat_map(fields, & &1.selections), fragments, state)
      end)
    end
  end

  defp collect_fields(selections, fragments, state) do
    Enum.reduce(selections, state, fn
      %Document.Field{} = field, {fields, visited} ->
        {[field | fields], visited}

      %Document.Fragment.Inline{selections: nested}, state ->
        collect_fields(nested, fragments, state)

      %Document.Fragment.Spread{name: name}, {fields, visited} = state ->
        if MapSet.member?(visited, name) do
          state
        else
          state = {fields, MapSet.put(visited, name)}

          case Map.get(fragments, name) do
            nil -> state
            fragment -> collect_fields(fragment.selections, fragments, state)
          end
        end
    end)
  end

  defp validate_pairs(fields, name, state) do
    {streams, others} = Enum.split_with(fields, &stream?/1)
    validate_stream_pairs(streams, others, name, state)
  end

  defp validate_stream_pairs([], _others, _name, state), do: state

  defp validate_stream_pairs([field | rest], others, name, state) do
    state =
      Enum.reduce(rest ++ others, state, fn other, state ->
        key = Enum.sort([field.source_location, other.source_location])

        if MapSet.member?(state.pairs, key) do
          state
        else
          error = %Phase.Error{
            phase: __MODULE__,
            message: "Fields `#{name}` overlap and cannot use the `stream` directive.",
            locations: [field.source_location, other.source_location]
          }

          %{state | pairs: MapSet.put(state.pairs, key), errors: [error | state.errors]}
        end
      end)

    validate_stream_pairs(rest, others, name, state)
  end

  defp stream?(field) do
    Enum.any?(
      field.directives,
      &(IncrementalDirectives.incremental?(&1) and &1.schema_node.identifier == :stream)
    )
  end
end
