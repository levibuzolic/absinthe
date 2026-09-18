defmodule Absinthe.Incremental.Planner do
  @moduledoc false

  alias Absinthe.{Blueprint, Type}
  alias Absinthe.Incremental.State

  def project(parent, parent_type, source, path, res) do
    details = Map.get(parent, :field_details) || [{parent, nil}]

    initial = %{fields: %{}, order: [], visited: %{}, state: res.incremental}

    collected =
      Enum.reduce(details, initial, fn {field, usage}, acc ->
        collect(field.selections, usage, parent_type, path, res, acc)
      end)

    {fields, partitions, partition_order} =
      Enum.reduce(collected.order, {[], %{}, []}, fn key, {fields, partitions, order} ->
        details = Map.fetch!(collected.fields, key)
        usages = filtered_usages(details, collected.state)
        field = merge(details)

        if usages == res.delivery do
          {[field | fields], partitions, order}
        else
          order = if Map.has_key?(partitions, usages), do: order, else: order ++ [usages]
          {fields, Map.update(partitions, usages, [field], &(&1 ++ [field])), order}
        end
      end)

    state =
      Enum.reduce(partition_order, collected.state, fn usages, state ->
        State.enqueue(state, %{
          kind: :defer,
          groups: usages,
          fields: Map.fetch!(partitions, usages),
          emitter: parent,
          source: source,
          parent_type: parent_type,
          path: path
        })
      end)

    {Enum.reverse(fields), %{res | incremental: state}}
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
    order = if Map.has_key?(acc.fields, key), do: acc.order, else: acc.order ++ [key]
    fields = Map.update(acc.fields, key, [{field, usage}], &(&1 ++ [{field, usage}]))
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
      {usage, acc} = defer(fragment, usage, path, res, acc)
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
    directive = State.directive(spread, "defer")
    token = if directive, do: elem(directive, 0), else: usage_token(usage, acc.state)
    visited = Map.get(acc.visited, name, MapSet.new())

    if applies?(fragment.type_condition, type, res.schema) and
         not MapSet.member?(visited, nil) and not MapSet.member?(visited, token) do
      acc = %{acc | visited: Map.put(acc.visited, name, MapSet.put(visited, token))}
      {usage, acc} = defer(spread, usage, path, res, acc)
      collect(fragment.selections, usage, type, path, res, acc)
    else
      acc
    end
  end

  defp usage_token(nil, _), do: nil
  defp usage_token(usage, state), do: state.groups[usage].directive

  defp defer(fragment, parent, path, res, acc) do
    case State.directive(fragment, "defer") do
      nil ->
        {parent, acc}

      {directive, args} ->
        if res.operation_type == :subscription do
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
