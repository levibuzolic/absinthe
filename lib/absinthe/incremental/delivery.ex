defmodule Absinthe.Incremental.Delivery do
  @moduledoc false

  alias Absinthe.Incremental.State

  def next(blueprint, pipeline) do
    if State.pending?(blueprint.execution.incremental), do: execute_next(blueprint, pipeline)
  end

  defp execute_next(blueprint, pipeline) do
    state = blueprint.execution.incremental
    {frame, state} = State.take(state)

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
    state = blueprint.execution.incremental

    {stream_entries, emitted_extensions, state} =
      if failed?(frame, result) do
        state = State.finish(state, frame)
        {[], Map.get(result, :extensions, %{}), fail(state, frame, Map.get(result, :errors, []))}
      else
        state = continue_stream(state, frame)
        state = State.finish(state, frame)
        state = prune(state, frame_path(frame), result.data, blueprint.execution.result)

        case frame.kind do
          :stream -> {[entry(frame, result, state)], Map.get(result, :extensions, %{}), state}
          :defer -> {[], %{}, State.buffer(state, frame, result)}
        end
      end

    {entries, buffered_extensions, completed, pending, state} = publish(state)
    payload = %{hasNext: State.pending?(state)}
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

  # Publishing a parent can release a child whose shared work already finished.
  # Settle those publications here even when there is no resolver work left.
  defp publish(state) do
    ready =
      state.completion_candidates
      |> :gb_sets.to_list()
      |> Enum.filter(fn ref ->
        group = state.groups[ref]
        group.id != nil and not group.done and not State.has_work?(state, ref)
      end)

    state = %{state | completion_candidates: :gb_sets.empty()}
    {entries, extensions, state} = flush(state, ready)
    {completed, state} = complete(state, ready)
    {pending, state} = announce(state)

    if :gb_sets.is_empty(state.completion_candidates) do
      {entries, extensions, completed, pending, state}
    else
      {more, later_extensions, later_completed, later_pending, state} = publish(state)

      {entries ++ more, Map.merge(extensions, later_extensions), completed ++ later_completed,
       pending ++ later_pending, state}
    end
  end

  defp flush(state, ready) do
    # Private values belong to the whole delivery group. An earlier successful
    # task must not leak them if a later task fails. Shared values become safe
    # once any of their owners succeeds, and are then delivered only once.
    successful = ready |> Enum.reject(&Map.has_key?(state.groups[&1], :errors)) |> MapSet.new()

    refs = state |> State.buffered_refs(successful) |> Enum.sort()

    {entries, extensions, state} =
      Enum.reduce(refs, {[], %{}, state}, fn ref, {entries, extensions, state} ->
        {{frame, result}, state} = State.pop_buffer(state, ref)
        frame = %{frame | groups: MapSet.intersection(frame.groups, successful)}
        state = State.wake(state, {:frame, ref})

        {[entry(frame, result, state) | entries],
         Map.merge(extensions, Map.get(result, :extensions, %{})), state}
      end)

    {Enum.reverse(entries), extensions, state}
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
      |> Enum.max_by(&length(&1.path))

    sub_path = Enum.drop(State.path(frame.path), length(group.path))

    %{id: group.id, data: result.data}
    |> put_nonempty(:subPath, sub_path)
    |> put_nonempty(:errors, Map.get(result, :errors, []))
  end

  def announce(state) do
    refs = Enum.sort(state.unannounced)
    state = %{state | unannounced: []}

    {notices, state} =
      Enum.reduce(refs, {[], state}, fn ref, {notices, state} ->
        group = state.groups[ref]

        if State.has_work?(state, ref) or State.has_buffered?(state, ref) do
          case blocked_on(state, group) do
            nil ->
              id = Integer.to_string(map_size(state.group_ids))

              state = State.announced(state, ref, id)

              notice = Map.merge(%{id: id, path: group.path}, Map.take(group, [:label]))

              {[notice | notices], state}

            dependency ->
              {notices, State.wait_for(state, dependency, ref)}
          end
        else
          {notices, state}
        end
      end)

    {Enum.reverse(notices), state}
  end

  defp blocked_on(state, group) do
    blocking_parent(state, group.parent) ||
      if Map.has_key?(state.buffered, group.owner), do: {:frame, group.owner}
  end

  defp blocking_parent(_, nil), do: nil

  defp blocking_parent(state, ref) do
    group = state.groups[ref]
    if State.has_work?(state, ref), do: {:group, ref}, else: blocking_parent(state, group.parent)
  end

  defp complete(state, ready) do
    Enum.map_reduce(ready, state, fn ref, state ->
      group = state.groups[ref]
      notice = %{id: group.id} |> put_nonempty(:errors, Map.get(group, :errors, []))
      {notice, put_in(state.groups[ref].done, true)}
    end)
  end

  defp fail(state, frame, errors) do
    {failed, unpublished} = failed_descendants(state, frame)

    state =
      State.restrict_jobs(state, fn job ->
        groups = MapSet.difference(job.groups, failed)

        cond do
          MapSet.size(groups) == 0 ->
            nil

          groups == job.groups ->
            job

          true ->
            %{
              job
              | groups: groups,
                fields: Absinthe.Incremental.Planner.retain_fields(job.fields, failed)
            }
        end
      end)

    refs = State.buffered_refs(state, failed)

    state =
      Enum.reduce(refs, state, fn ref, state ->
        {{frame, result}, state} = State.pop_buffer(state, ref)
        groups = MapSet.difference(frame.groups, failed)

        if MapSet.size(groups) == 0,
          do: state,
          else: State.buffer(state, %{frame | groups: groups}, result)
      end)

    dependencies = Enum.map(failed, &{:group, &1}) ++ Enum.map(unpublished, &{:frame, &1})
    state = State.cancel_waiters(state, dependencies)

    Enum.reduce(failed, state, fn ref, state ->
      put_in(state.groups[ref][:errors], errors)
    end)
  end

  defp failed_descendants(state, frame) do
    # A group's parents and the owners of its source value always precede it.
    # This lets one pass cancel both nested defers and streams belonging to
    # earlier private values that were buffered before this frame failed.
    Enum.reduce(
      Enum.sort(Map.keys(state.groups)),
      {frame.groups, MapSet.new([frame.ref])},
      fn ref, {failed, unpublished} ->
        group = state.groups[ref]

        if MapSet.member?(failed, ref) or MapSet.member?(failed, group.parent) or
             MapSet.member?(unpublished, group.owner) do
          failed = MapSet.put(failed, ref)

          unpublished =
            Enum.reduce(group.buffered, unpublished, fn value_ref, unpublished ->
              {value_frame, _} = Map.fetch!(state.buffered, value_ref)

              if MapSet.subset?(value_frame.groups, failed),
                do: MapSet.put(unpublished, value_ref),
                else: unpublished
            end)

          {failed, unpublished}
        else
          {failed, unpublished}
        end
      end
    )
  end

  # Work is meaningful only while the object/list containing it exists in the
  # just-completed result. Missing fields belong to other delivery groups.
  def prune(state, base, data, execution_result) do
    if may_invalidate_work?(execution_result, data) do
      State.restrict_jobs(state, fn job ->
        path = State.path(job.path)

        unless List.starts_with?(path, base) and null_at?(data, Enum.drop(path, length(base))),
          do: job
      end)
    else
      state
    end
  end

  # An ordinary null leaf never scheduled descendants. Error propagation or a
  # result phase nulling a completed container can invalidate queued work.
  defp may_invalidate_work?(%{errors: [_ | _]}, _data), do: true
  defp may_invalidate_work?(%{fields: fields}, nil) when is_list(fields), do: true
  defp may_invalidate_work?(%{values: _}, nil), do: true

  defp may_invalidate_work?(%{fields: fields}, data) when is_list(fields) and is_map(data) do
    Enum.any?(fields, fn field ->
      key = Map.get(field.emitter, :alias) || field.emitter.name

      case Map.fetch(data, key) do
        {:ok, value} -> may_invalidate_work?(field, value)
        :error -> false
      end
    end)
  end

  defp may_invalidate_work?(%{values: values}, data) when is_list(data),
    do:
      Enum.zip(values, data)
      |> Enum.any?(fn {node, value} -> may_invalidate_work?(node, value) end)

  defp may_invalidate_work?(_, _), do: false

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
