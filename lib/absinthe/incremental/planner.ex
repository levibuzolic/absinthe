defmodule Absinthe.Incremental.Planner do
  @moduledoc false

  alias Absinthe.{Blueprint, Type}
  alias Absinthe.Incremental.{Directives, State}

  def project(parent, parent_type, source, path, res) do
    details = Map.get(parent, :field_details) || [{parent, nil}]

    initial = %{fields: %{}, order: [], visited: %{}, state: res.incremental}

    collected =
      Enum.reduce(details, initial, fn {field, usage}, acc ->
        collect(field.selections, usage, parent_type, path, res, acc)
      end)

    {fields, partitions, partition_order} =
      Enum.reduce(Enum.reverse(collected.order), {[], %{}, []}, fn key,
                                                                   {fields, partitions, order} ->
        details = collected.fields |> Map.fetch!(key) |> Enum.reverse()
        usages = filtered_usages(details, collected.state)
        field = merge(details)

        if usages == res.delivery do
          {[field | fields], partitions, order}
        else
          order = if Map.has_key?(partitions, usages), do: order, else: [usages | order]
          {fields, Map.update(partitions, usages, [field], &[field | &1]), order}
        end
      end)

    state =
      Enum.reduce(Enum.reverse(partition_order), collected.state, fn usages, state ->
        State.enqueue(state, %{
          kind: :defer,
          groups: usages,
          fields: partitions |> Map.fetch!(usages) |> Enum.reverse(),
          emitter: parent,
          source: source,
          parent_type: parent_type,
          path: path
        })
      end)

    {:ok, Enum.reverse(fields), %{res | incremental: state}}
  catch
    {:incremental_subscription, directive} -> {:error, directive}
  end

  def prepare_stream(nil, _field, res), do: {nil, res, []}

  def prepare_stream(value, field, res) do
    case Directives.active(field, :stream) do
      {_, _} when res.incremental_subscription ->
        {nil, res, ["The @stream directive is not supported on subscription operations."]}

      {_, %{initial_count: count}} when count < 0 ->
        {nil, res, ["The initialCount argument to @stream must be a non-negative integer."]}

      {_, args} when not is_nil(res.incremental) and is_list(value) ->
        {prefix, tail} = Enum.split(value, args.initial_count)

        if tail == [] do
          {prefix, res, []}
        else
          %Type.List{of_type: item_type} = Type.unwrap_non_null(field.schema_node.type)

          {group, state} =
            State.group(res.incremental, %{
              kind: :stream,
              path: State.path(res.path),
              label: args[:label],
              parent: nil
            })

          state =
            State.enqueue(state, %{
              kind: :stream,
              groups: MapSet.new([group]),
              values: tail,
              index: length(prefix),
              path: res.path,
              emitter: %{
                field
                | field_details: Enum.map(field.field_details, fn {node, _} -> {node, nil} end)
              },
              item_type: item_type,
              source: res.source,
              extensions: res.extensions
            })

          {prefix, %{res | incremental: state}, []}
        end

      _ ->
        {value, res, []}
    end
  end

  defp collect(selections, usage, type, path, res, acc) do
    Enum.reduce(selections, acc, fn selection, acc ->
      collect_selection(selection, usage, type, path, res, acc)
    end)
  end

  defp collect_selection(%{flags: %{skip: _}}, _, _, _, _, acc), do: acc

  defp collect_selection(%Blueprint.Document.Field{} = field, usage, type, _, _, acc) do
    field = concrete_field(field, type)
    key = field.alias || field.name
    order = if Map.has_key?(acc.fields, key), do: acc.order, else: [key | acc.order]
    fields = Map.update(acc.fields, key, [{field, usage}], &[{field, usage} | &1])
    %{acc | fields: fields, order: order}
  end

  defp collect_selection(
         %Blueprint.Document.Fragment.Inline{} = fragment,
         usage,
         type,
         path,
         res,
         acc
       ) do
    if applies?(fragment.type_condition, type, res.schema) do
      {usage, acc} = defer(Directives.active(fragment, :defer), usage, path, res, acc)
      collect(fragment.selections, usage, type, path, res, acc)
    else
      acc
    end
  end

  defp collect_selection(
         %Blueprint.Document.Fragment.Spread{name: name} = spread,
         usage,
         type,
         path,
         res,
         acc
       ) do
    fragment = Map.fetch!(res.fragments, name)
    directive = Directives.active(spread, :defer)
    token = if directive, do: elem(directive, 0), else: usage_token(usage, acc.state)
    visited = Map.get(acc.visited, name, MapSet.new())

    if applies?(fragment.type_condition, type, res.schema) and
         not MapSet.member?(visited, nil) and not MapSet.member?(visited, token) do
      acc = %{acc | visited: Map.put(acc.visited, name, MapSet.put(visited, token))}
      {usage, acc} = defer(directive, usage, path, res, acc)
      collect(fragment.selections, usage, type, path, res, acc)
    else
      acc
    end
  end

  defp usage_token(nil, _), do: nil
  defp usage_token(usage, state), do: state.groups[usage].directive

  defp defer(nil, parent, _path, _res, acc), do: {parent, acc}

  defp defer({directive, args}, parent, path, res, acc) do
    if res.incremental_subscription do
      throw({:incremental_subscription, directive})
    end

    {usage, state} =
      State.group(acc.state, %{
        kind: :defer,
        parent: parent,
        path: State.path(path),
        label: args[:label],
        directive: directive
      })

    {usage, %{acc | state: state}}
  end

  defp filtered_usages(details, state) do
    usages = MapSet.new(details, &elem(&1, 1))

    if MapSet.member?(usages, nil) do
      MapSet.new()
    else
      MapSet.reject(usages, &ancestor_in_set?(state.groups[&1].parent, usages, state))
    end
  end

  defp ancestor_in_set?(nil, _, _), do: false

  defp ancestor_in_set?(usage, usages, state) do
    MapSet.member?(usages, usage) or ancestor_in_set?(state.groups[usage].parent, usages, state)
  end

  defp merge([{first, _} | _] = details) do
    %{
      first
      | selections: Enum.flat_map(details, fn {field, _} -> field.selections end),
        field_details: details
    }
  end

  def retain_fields(fields, failed) do
    Enum.flat_map(fields, fn field ->
      case Enum.reject(field.field_details, fn {_, usage} -> MapSet.member?(failed, usage) end) do
        [] -> []
        details -> [merge(details)]
      end
    end)
  end

  defp concrete_field(%{name: "__" <> _} = field, type), do: %{field | parent_type: type}

  defp concrete_field(field, type) do
    %{
      field
      | parent_type: type,
        schema_node: Map.fetch!(type.fields, field.schema_node.identifier)
    }
  end

  defp applies?(nil, _, _), do: true
  defp applies?(%{schema_node: node}, type, schema), do: applies?(node, type, schema)

  defp applies?(condition, type, schema) when is_atom(condition) do
    applies?(Absinthe.Schema.lookup_type(schema, condition), type, schema)
  end

  defp applies?(%Type.Object{name: name}, %Type.Object{name: name}, _), do: true

  defp applies?(%Type.Interface{} = condition, type, _),
    do: Type.Interface.member?(condition, type)

  defp applies?(%Type.Union{} = condition, type, _), do: Type.Union.member?(condition, type)
  defp applies?(_, _, _), do: false
end
