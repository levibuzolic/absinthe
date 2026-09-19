defmodule Absinthe.Phase.Document.Validation.IncrementalStreams do
  @moduledoc false

  use Absinthe.Phase

  alias Absinthe.{Blueprint, Phase, Schema, Type}
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
        {fragment.name, {[id], fragment.schema_node, fragment.selections}}
      end

    state = %{schema: input.schema, groups: MapSet.new(), pairs: MapSet.new(), errors: []}

    state =
      Enum.reduce(definitions, state, fn {definition, id}, state ->
        validate_set([{[id], definition.schema_node, definition.selections}], fragments, state)
      end)

    {:ok, %{input | errors: input.errors ++ Enum.reverse(state.errors)}}
  end

  defp validate_set([], _fragments, state), do: state

  defp validate_set(selection_sets, fragments, state) do
    {fields, _} =
      Enum.reduce(selection_sets, {[], MapSet.new()}, fn {origin, parent_type, selections}, acc ->
        collect_fields(selections, origin, parent_type, fragments, acc)
      end)

    fields = fields |> Enum.uniq_by(&elem(&1, 0)) |> Enum.reverse()
    key = fields |> Enum.map(&elem(&1, 0)) |> Enum.sort()

    if MapSet.member?(state.groups, key) do
      state
    else
      state = %{state | groups: MapSet.put(state.groups, key)}

      fields
      |> Enum.group_by(fn {_id, field, _parent_type} -> field.alias || field.name end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.reduce(state, fn {name, fields}, state ->
        state = validate_pairs(fields, name, state)

        fields
        |> child_groups()
        |> Enum.reduce(state, fn fields, state ->
          children =
            for {id, %{selections: [_ | _] = selections} = field, _parent_type} <- fields do
              {id, child_type(field, state.schema), selections}
            end

          validate_set(children, fragments, state)
        end)
      end)
    end
  end

  # Identity is the definition and selection-index path, independent of optional
  # source locations. A named fragment always resumes from its own origin.
  defp collect_fields(selections, origin, parent_type, fragments, state) do
    selections
    |> Enum.with_index()
    |> Enum.reduce(state, fn
      {%Document.Field{} = field, index}, {fields, visited} ->
        {[{[index | origin], field, parent_type} | fields], visited}

      {%Document.Fragment.Inline{selections: nested, schema_node: type}, index}, state ->
        collect_fields(nested, [index | origin], type, fragments, state)

      {%Document.Fragment.Spread{name: name}, _index}, {fields, visited} = state ->
        if MapSet.member?(visited, name) do
          state
        else
          state = {fields, MapSet.put(visited, name)}

          case Map.get(fragments, name) do
            nil ->
              state

            {origin, type, selections} ->
              collect_fields(selections, origin, type, fragments, state)
          end
        end
    end)
  end

  # FieldsInSetCanMerge checks streams on every pair, but merges child sets only
  # when the parent types match or either parent is abstract. Keep concrete
  # alternatives separate through recursion, including abstract selections in
  # each group so every potentially overlapping pair is still checked.
  defp child_groups(fields) do
    groups =
      Enum.group_by(fields, fn
        {_id, _field, %Type.Object{identifier: identifier}} -> identifier
        _ -> nil
      end)

    {abstract, concrete} = Map.pop(groups, nil, [])

    case Enum.sort_by(concrete, &elem(&1, 0)) do
      [] -> [abstract]
      groups -> Enum.map(groups, fn {_type, fields} -> fields ++ abstract end)
    end
  end

  defp child_type(%{schema_node: %{type: type}}, schema), do: Schema.lookup_type(schema, type)
  defp child_type(_field, _schema), do: nil

  defp validate_pairs(fields, name, state) do
    {streams, others} = Enum.split_with(fields, fn {_id, field, _type} -> stream?(field) end)
    validate_stream_pairs(streams, others, name, state)
  end

  defp validate_stream_pairs([], _others, _name, state), do: state

  defp validate_stream_pairs([{id, field, _type} | rest], others, name, state) do
    state =
      Enum.reduce(rest ++ others, state, fn {other_id, other, _type}, state ->
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
