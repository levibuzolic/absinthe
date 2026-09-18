defmodule Absinthe.Phase.Document.Validation.IncrementalStreams do
  @moduledoc false

  use Absinthe.Phase

  alias Absinthe.{Blueprint, Phase}
  alias Absinthe.Blueprint.Document
  alias Absinthe.Incremental.Directives

  @spec run(Blueprint.t(), Keyword.t()) :: Phase.result_t()
  def run(input, _options \\ []) do
    if Directives.enabled?(input.schema) and Blueprint.find(input, &stream_field?/1) do
      validate(input)
    else
      {:ok, input}
    end
  end

  defp stream_field?(%Document.Field{} = field), do: stream?(field)
  defp stream_field?(_), do: false

  defp validate(input) do
    definitions = Enum.with_index(input.operations ++ input.fragments)

    fragments =
      for {%Document.Fragment.Named{} = fragment, id} <- definitions, into: %{} do
        {fragment.name, {[id], fragment.selections}}
      end

    state = %{groups: MapSet.new(), pairs: MapSet.new(), errors: []}

    state =
      Enum.reduce(definitions, state, fn {definition, id}, state ->
        validate_set([{[id], definition.selections}], fragments, state)
      end)

    {:ok, %{input | errors: input.errors ++ Enum.reverse(state.errors)}}
  end

  defp validate_set([], _fragments, state), do: state

  defp validate_set(selection_sets, fragments, state) do
    {fields, _} =
      Enum.reduce(selection_sets, {[], MapSet.new()}, fn {origin, selections}, acc ->
        collect_fields(selections, origin, fragments, acc)
      end)

    fields = fields |> Enum.uniq_by(&elem(&1, 0)) |> Enum.reverse()
    key = fields |> Enum.map(&elem(&1, 0)) |> Enum.sort()

    if MapSet.member?(state.groups, key) do
      state
    else
      state = %{state | groups: MapSet.put(state.groups, key)}

      fields
      |> Enum.group_by(fn {_id, field} -> field.alias || field.name end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce(state, fn {name, fields}, state ->
        state = validate_pairs(fields, name, state)

        children =
          for {id, %{selections: [_ | _] = selections}} <- fields, do: {id, selections}

        validate_set(children, fragments, state)
      end)
    end
  end

  # Identity is the definition and selection-index path, independent of optional
  # source locations. A named fragment always resumes from its own origin.
  defp collect_fields(selections, origin, fragments, state) do
    selections
    |> Enum.with_index()
    |> Enum.reduce(state, fn
      {%Document.Field{} = field, index}, {fields, visited} ->
        {[{[index | origin], field} | fields], visited}

      {%Document.Fragment.Inline{selections: nested}, index}, state ->
        collect_fields(nested, [index | origin], fragments, state)

      {%Document.Fragment.Spread{name: name}, _index}, {fields, visited} = state ->
        if MapSet.member?(visited, name) do
          state
        else
          state = {fields, MapSet.put(visited, name)}

          case Map.get(fragments, name) do
            nil -> state
            {origin, selections} -> collect_fields(selections, origin, fragments, state)
          end
        end
    end)
  end

  defp validate_pairs(fields, name, state) do
    {streams, others} = Enum.split_with(fields, fn {_id, field} -> stream?(field) end)
    validate_stream_pairs(streams, others, name, state)
  end

  defp validate_stream_pairs([], _others, _name, state), do: state

  defp validate_stream_pairs([{id, field} | rest], others, name, state) do
    state =
      Enum.reduce(rest ++ others, state, fn {other_id, other}, state ->
        key = Enum.sort([id, other_id])

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
    Enum.any?(field.directives, &(Directives.identifier(&1) == :stream))
  end
end
