defmodule Absinthe.Incremental.Delivery do
  @moduledoc false

  alias Absinthe.Incremental.State

  def next(%{execution: %{incremental: %{jobs: []}}}, _options), do: nil

  def next(blueprint, pipeline) do
    state = blueprint.execution.incremental
    frame = Enum.find(state.jobs, &announced?(&1, state))
    state = %{State.remove(state, frame) | frame: frame}

    execution = %{
      blueprint.execution
      | incremental: state,
        result: nil,
        pending: [],
        resolved: %{}
    }

    blueprint = %{blueprint | execution: execution, result: %{}}

    case Absinthe.Pipeline.run(blueprint, pipeline) do
      {:ok, blueprint, _} ->
        case deliver(blueprint, frame) do
          {%{hasNext: true} = payload, blueprint} when map_size(payload) == 1 ->
            next(blueprint, pipeline)

          result ->
            result
        end

      {:error, error, _} ->
        raise Absinthe.ExecutionError, message: inspect(error)
    end
  end

  defp deliver(blueprint, frame) do
    result = blueprint.result
    state = %{blueprint.execution.incremental | frame: nil}

    {stream_entries, emitted_extensions, state} =
      if failed?(frame, result) do
        {[], Map.get(result, :extensions, %{}), fail(state, frame, Map.get(result, :errors, []))}
      else
        state = continue_stream(state, frame)
        state = prune(state, frame_path(frame), result.data)

        case frame.kind do
          :stream -> {[entry(frame, result, state)], Map.get(result, :extensions, %{}), state}
          :defer -> {[], %{}, %{state | buffered: state.buffered ++ [{frame, result}]}}
        end
      end

    {entries, buffered_extensions, state} = flush(state)
    {completed, state} = complete(state)
    {pending, state} = announce(state)
    payload = %{hasNext: state.jobs != []}
    extensions = Map.merge(buffered_extensions, emitted_extensions)

    payload =
      if map_size(extensions) == 0, do: payload, else: Map.put(payload, :extensions, extensions)

    payload =
      payload
      |> put_nonempty(:incremental, entries ++ stream_entries)
      |> put_nonempty(:completed, completed)
      |> put_nonempty(:pending, pending)

    {payload, put_in(blueprint.execution.incremental, state)}
  end

  defp flush(state) do
    # Private values belong to the whole delivery group. An earlier successful
    # task must not leak them if a later task fails. Shared values become safe
    # once any of their owners succeeds, and are then delivered only once.
    {entries, extensions, buffered} =
      Enum.reduce(state.buffered, {[], %{}, []}, fn {frame, result},
                                                    {entries, extensions, buffered} ->
        ready =
          MapSet.filter(frame.groups, fn ref ->
            group = state.groups[ref]

            group.id != nil and not State.has_work?(state, ref) and
              not Map.has_key?(group, :errors)
          end)

        if MapSet.size(ready) > 0 do
          {[entry(%{frame | groups: ready}, result, state) | entries],
           Map.merge(extensions, Map.get(result, :extensions, %{})), buffered}
        else
          {entries, extensions, [{frame, result} | buffered]}
        end
      end)

    {Enum.reverse(entries), extensions, %{state | buffered: Enum.reverse(buffered)}}
  end

  defp failed?(%{kind: :defer}, %{data: nil}), do: true
  defp failed?(%{kind: :stream, item_type: %Absinthe.Type.NonNull{}}, %{data: nil}), do: true
  defp failed?(_, _), do: false

  defp continue_stream(state, %{kind: :stream, values: [_ | [_ | _] = rest]} = frame) do
    # Keep each stream ordered, while allowing work discovered in earlier items
    # to run before the next item is requested.
    State.enqueue(state, %{frame | values: rest, index: frame.index + 1})
  end

  defp continue_stream(state, _), do: state

  defp entry(%{kind: :stream, groups: groups}, result, state) do
    [group] = MapSet.to_list(groups)

    %{id: state.groups[group].id, items: [result.data]}
    |> put_nonempty(:errors, Map.get(result, :errors, []))
  end

  defp entry(frame, result, state) do
    group =
      frame.groups
      |> Enum.map(&state.groups[&1])
      |> Enum.filter(& &1.id)
      |> Enum.max_by(&length(&1.path))

    sub_path = Enum.drop(State.path(frame.path), length(group.path))

    %{id: group.id, data: result.data}
    |> put_nonempty(:subPath, sub_path)
    |> put_nonempty(:errors, Map.get(result, :errors, []))
  end

  defp announced?(frame, state), do: Enum.any?(frame.groups, &(state.groups[&1].id != nil))

  def announce(state) do
    {notices, state, unannounced} =
      Enum.reduce(Enum.reverse(state.unannounced), {[], state, []}, fn ref,
                                                                       {notices, state,
                                                                        unannounced} ->
        group = state.groups[ref]

        if State.has_work?(state, ref) and released?(state, group.parent) and
             owner_delivered?(state, group.owner) do
          id = Integer.to_string(state.next_id)

          state = %{
            state
            | groups: Map.put(state.groups, ref, %{group | id: id}),
              next_id: state.next_id + 1
          }

          notice = %{id: id, path: group.path}
          notice = if is_nil(group.label), do: notice, else: Map.put(notice, :label, group.label)
          {[notice | notices], state, unannounced}
        else
          unannounced = if State.has_work?(state, ref), do: [ref | unannounced], else: unannounced
          {notices, state, unannounced}
        end
      end)

    {Enum.reverse(notices), %{state | unannounced: unannounced}}
  end

  defp owner_delivered?(_, nil), do: true

  defp owner_delivered?(state, owner) do
    not Enum.any?(state.buffered, fn {frame, _} -> frame.ref == owner end)
  end

  defp released?(_, nil), do: true

  defp released?(state, ref) do
    group = state.groups[ref]
    (group.done or not State.has_work?(state, ref)) and released?(state, group.parent)
  end

  defp complete(state) do
    refs = Enum.sort_by(state.completion_candidates, &state.groups[&1].ordinal)
    state = %{state | completion_candidates: MapSet.new()}

    Enum.reduce(refs, {[], state}, fn ref, {notices, state} ->
      group = state.groups[ref]

      if group.id != nil and not group.done and not State.has_work?(state, ref) do
        notice = %{id: group.id} |> put_nonempty(:errors, Map.get(group, :errors, []))
        state = %{state | groups: Map.put(state.groups, ref, %{group | done: true})}
        {notices ++ [notice], state}
      else
        {notices, state}
      end
    end)
  end

  defp fail(state, frame, errors) do
    groups =
      Enum.reduce(state.order, frame.groups, fn ref, groups ->
        if state.groups[ref].owner == frame.ref, do: MapSet.put(groups, ref), else: groups
      end)

    failed =
      Enum.reduce(Enum.reverse(state.order), groups, fn ref, failed ->
        if MapSet.member?(failed, state.groups[ref].parent),
          do: MapSet.put(failed, ref),
          else: failed
      end)

    jobs =
      Enum.flat_map(state.jobs, fn job ->
        groups = MapSet.difference(job.groups, failed)

        cond do
          MapSet.size(groups) == 0 -> []
          groups == job.groups -> [job]
          true -> [%{job | groups: groups, fields: retain_fields(job.fields, failed)}]
        end
      end)

    buffered =
      Enum.flat_map(state.buffered, fn {frame, result} ->
        groups = MapSet.difference(frame.groups, failed)
        if MapSet.size(groups) == 0, do: [], else: [{%{frame | groups: groups}, result}]
      end)

    state = %{State.replace_jobs(state, jobs) | buffered: buffered}

    Enum.reduce(failed, state, fn ref, state ->
      %{state | groups: Map.update!(state.groups, ref, &Map.put(&1, :errors, errors))}
    end)
  end

  defp retain_fields(fields, failed) do
    Enum.flat_map(fields, fn field ->
      details =
        Enum.reject(field.field_details, fn {_, usage} -> MapSet.member?(failed, usage) end)

      case details do
        [] ->
          []

        [{first, _} | _] ->
          [
            %{
              first
              | field_details: details,
                selections: Enum.flat_map(details, fn {node, _} -> node.selections end)
            }
          ]
      end
    end)
  end

  # Work is meaningful only while the object/list containing it exists in the
  # just-completed result. Missing fields belong to other delivery groups.
  def prune(state, base, data) do
    if contains_null?(data) do
      jobs =
        Enum.reject(state.jobs, fn job ->
          path = State.path(job.path)
          List.starts_with?(path, base) and null_at?(data, Enum.drop(path, length(base)))
        end)

      State.replace_jobs(state, jobs)
    else
      state
    end
  end

  defp contains_null?(nil), do: true

  defp contains_null?(data) when is_map(data),
    do: Enum.any?(data, fn {_, value} -> contains_null?(value) end)

  defp contains_null?(data) when is_list(data), do: Enum.any?(data, &contains_null?/1)
  defp contains_null?(_), do: false

  defp null_at?(nil, _), do: true
  defp null_at?(_, []), do: false

  defp null_at?(data, [key | rest]) when is_map(data) do
    case Map.fetch(data, key) do
      {:ok, value} -> null_at?(value, rest)
      :error -> false
    end
  end

  defp null_at?(data, [index | rest]) when is_list(data) and is_integer(index) do
    case Enum.fetch(data, index) do
      {:ok, value} -> null_at?(value, rest)
      :error -> false
    end
  end

  defp null_at?(_, _), do: false

  defp frame_path(%{kind: :stream} = frame), do: State.path([frame.index | frame.path])
  defp frame_path(frame), do: State.path(frame.path)

  defp put_nonempty(map, _, []), do: map
  defp put_nonempty(map, key, values), do: Map.put(map, key, values)
end
