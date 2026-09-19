defmodule Absinthe.Incremental.Planner do
  @moduledoc false

  alias Absinthe.{Blueprint, Type}
  alias Absinthe.Incremental.{Directives, State}

  def project(parent, parent_type, source, path, res) do
    details = Map.get(parent, :field_details) || [{parent, nil}]

    initial = %{
      fields: %{},
      order: [],
      visited: %{},
      response_path: State.path(path),
      state: res.incremental
    }

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
            State.group(
              res.incremental,
              Map.merge(Map.take(args, [:label]), %{
                kind: :stream,
                path: State.path(res.path)
              })
            )

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
    state = record_response_key(acc.state, usage, acc.response_path, key)
    %{acc | fields: fields, order: order, state: state}
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
    directive = Directives.active(spread, :defer)

    token =
      if directive,
        do: directive_token(elem(directive, 0), res),
        else: usage_token(usage, acc.state)

    visited = Map.get(acc.visited, name, MapSet.new())

    cond do
      not applies?(fragment.type_condition, type, res.schema) ->
        acc

      MapSet.member?(visited, nil) or MapSet.member?(visited, token) ->
        # A reused fragment's fields are not visited again. Preserve the full
        # snapshot rather than infer an incomplete projection (which can omit
        # Relay's abstract-type discriminators as well as ordinary fields).
        state = record_response_key(acc.state, usage, acc.response_path, :all)
        %{acc | state: state}

      true ->
        acc = %{acc | visited: Map.put(acc.visited, name, MapSet.put(visited, token))}
        {usage, acc} = defer(spread, usage, path, res, acc)
        collect(fragment.selections, usage, type, path, res, acc)
    end
  end

  defp record_response_key(state, nil, _path, _key), do: state

  defp record_response_key(state, usage, path, key) do
    group = state.groups[usage]

    if group.path == path do
      keys =
        if key == :all or group.response_keys == :all,
          do: :all,
          else: MapSet.put(group.response_keys, key)

      state = put_in(state.groups[usage].response_keys, keys)
      record_response_key(state, group.parent, path, key)
    else
      state
    end
  end

  defp usage_token(nil, _), do: nil
  defp usage_token(usage, state), do: state.groups[usage].directive_id

  # Ordinary subscription execution does not run Start; an applicable active
  # directive is rejected below before any deferred usage can be created.
  defp directive_token(_, %{incremental_subscription: true}), do: :subscription

  defp directive_token(directive, _),
    do: Keyword.fetch!(directive.__private__, :__absinthe_incremental_id)

  defp defer(fragment, parent, path, res, acc) do
    case Directives.active(fragment, :defer) do
      nil ->
        {parent, acc}

      {directive, args} ->
        if res.incremental_subscription do
          throw({:incremental_subscription, directive})
        end

        {usage, state} =
          State.group(
            acc.state,
            Map.merge(Map.take(args, [:label]), %{
              kind: :defer,
              parent: parent,
              path: State.path(path),
              directive_id: directive_token(directive, res)
            })
          )

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
