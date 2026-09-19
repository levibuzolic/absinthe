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

    state = %{schema: input.schema, fields: %{}, pairs: MapSet.new(), errors: []}

    state =
      Enum.reduce(definitions, state, fn {definition, id}, state ->
        {_fields, state} =
          validate_set({[id], definition.schema_node, definition.selections}, fragments, state)

        state
      end)

    {:ok, %{input | errors: input.errors ++ Enum.reverse(state.errors)}}
  end

  # Cache original selection sets and pairs, rather than every combination of
  # merged sets. Reused fragments can otherwise create exponentially many sets.
  defp validate_set({origin, parent_type, selections}, fragments, state) do
    case Map.fetch(state.fields, origin) do
      {:ok, {fields, _contains_stream?}} ->
        {fields, state}

      :error ->
        {fields, _} =
          collect_fields(selections, origin, parent_type, fragments, {[], MapSet.new()})

        fields =
          fields
          |> Enum.reverse()
          |> Enum.group_by(fn {_id, field, _parent_type} -> field.alias || field.name end)

        state = %{state | fields: Map.put(state.fields, origin, {fields, false})}
        ordered_fields = Enum.sort_by(fields, &elem(&1, 0))

        state =
          Enum.reduce(ordered_fields, state, fn {_name, fields}, state ->
            Enum.reduce(fields, state, fn field, state ->
              {_fields, state} = validate_set(children(field, state.schema), fragments, state)
              state
            end)
          end)

        contains_stream? =
          Enum.any?(fields, fn {_name, fields} ->
            Enum.any?(fields, &streaming_field?(&1, state))
          end)

        state = %{state | fields: Map.put(state.fields, origin, {fields, contains_stream?})}

        state =
          if contains_stream? do
            Enum.reduce(ordered_fields, state, fn {name, fields}, state ->
              validate_pairs(fields, fields, name, state)
            end)
          else
            state
          end

        {fields, state}
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

  defp children({id, field, _parent_type}, schema),
    do: {id, child_type(field, schema), field.selections}

  defp child_type(%{schema_node: %{type: type}}, schema), do: Schema.lookup_type(schema, type)
  defp child_type(_field, _schema), do: nil

  defp streaming_field?({id, field, _type}, state) do
    {_fields, contains_stream?} = Map.fetch!(state.fields, id)
    stream?(field) or contains_stream?
  end

  defp validate_pairs(left, right, name, state) do
    streamed = Enum.filter(right, &streaming_field?(&1, state))

    Enum.reduce(left, state, fn field, state ->
      others = if streaming_field?(field, state), do: right, else: streamed
      Enum.reduce(others, state, &validate_pair(field, &1, name, &2))
    end)
  end

  defp validate_pair({id, _, _}, {id, _, _}, _name, state), do: state

  defp validate_pair(
         {id, field, type},
         {other_id, other, other_type},
         name,
         state
       ) do
    key = Enum.sort([id, other_id])

    if MapSet.member?(state.pairs, key) do
      state
    else
      state = %{state | pairs: MapSet.put(state.pairs, key)}
      state = validate_streams(field, other, name, state)

      # Different concrete parents are exclusive. An abstract parent may
      # overlap either concrete parent, but cannot make them overlap each other.
      if compatible_parents?(type, other_type) do
        {left, _} = Map.fetch!(state.fields, id)
        {right, _} = Map.fetch!(state.fields, other_id)

        left
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce(state, fn {name, fields}, state ->
          validate_pairs(fields, Map.get(right, name, []), name, state)
        end)
      else
        state
      end
    end
  end

  defp compatible_parents?(%Type.Object{identifier: left}, %Type.Object{identifier: right}),
    do: left == right

  defp compatible_parents?(_, _), do: true

  defp validate_streams(field, other, name, state) do
    if stream?(field) or stream?(other) do
      fields = if stream?(field), do: [field, other], else: [other, field]

      error = %Phase.Error{
        phase: __MODULE__,
        message: "Fields `#{name}` overlap and cannot use the `stream` directive.",
        locations: Enum.map(fields, & &1.source_location)
      }

      %{state | errors: [error | state.errors]}
    else
      state
    end
  end

  defp stream?(field) do
    Enum.any?(field.directives, &(Directives.identifier(&1) == :stream))
  end
end
