defmodule Absinthe.Incremental.State do
  @moduledoc false

  defstruct jobs: %{},
            ready: :gb_sets.empty(),
            next_job: 0,
            groups: %{},
            group_ids: %{},
            unannounced: [],
            next_frame: 0,
            frame: nil,
            waiting: %{},
            completion_candidates: MapSet.new(),
            buffered: %{}

  # Group identities outlive their notices: queued occurrences can still refer
  # to their ancestry. Membership sets track queued and running work, while
  # ready IDs track only runnable queued jobs. Frame IDs record execution order.

  def group(state, attributes) do
    ref = map_size(state.groups)
    owner = if state.frame, do: state.frame.ref

    group =
      Map.merge(
        %{
          id: nil,
          done: false,
          parent: nil,
          owner: owner,
          jobs: MapSet.new(),
          buffered: MapSet.new()
        },
        attributes
      )

    {ref,
     %{
       state
       | groups: Map.put(state.groups, ref, group),
         unannounced: [ref | state.unannounced]
     }}
  end

  def enqueue(state, job) do
    id = state.next_job
    job = job |> Map.delete(:ref) |> Map.put(:job_id, id)
    state = change_memberships(state, job.groups, id, :add)
    ready = if announced?(state, job), do: :gb_sets.add(id, state.ready), else: state.ready

    %{state | jobs: Map.put(state.jobs, id, job), ready: ready, next_job: id + 1}
  end

  def pending?(state), do: map_size(state.jobs) > 0

  def take(state) do
    if :gb_sets.is_empty(state.ready),
      do: raise("Incremental work has no announced delivery group")

    {id, ready} = :gb_sets.take_smallest(state.ready)
    {job, jobs} = Map.pop!(state.jobs, id)
    frame = Map.put(job, :ref, state.next_frame)

    {frame, %{state | jobs: jobs, ready: ready, frame: frame, next_frame: state.next_frame + 1}}
  end

  def finish(state, frame) do
    state = change_memberships(state, frame.groups, frame.job_id, :remove)
    %{state | frame: nil}
  end

  def announced(state, ref, id) do
    group = state.groups[ref]
    ready = Enum.reduce(group.jobs, state.ready, &:gb_sets.add/2)

    candidates =
      if MapSet.size(group.jobs) == 0,
        do: MapSet.put(state.completion_candidates, ref),
        else: state.completion_candidates

    %{
      state
      | groups: Map.put(state.groups, ref, %{group | id: id}),
        group_ids: Map.put(state.group_ids, id, ref),
        ready: ready,
        completion_candidates: candidates
    }
  end

  # Cancellation removes only affected owner memberships. Ready IDs preserve
  # enqueue order among runnable jobs without repeatedly walking blocked jobs.
  def restrict_jobs(state, restrict) do
    Enum.reduce(state.jobs, state, fn {id, job}, state ->
      case restrict.(job) do
        nil ->
          state = change_memberships(state, job.groups, id, :remove)
          %{state | jobs: Map.delete(state.jobs, id), ready: :gb_sets.delete_any(id, state.ready)}

        retained ->
          removed = MapSet.difference(job.groups, retained.groups)
          state = change_memberships(state, removed, id, :remove)

          ready =
            if announced?(state, retained),
              do: :gb_sets.add(id, state.ready),
              else: :gb_sets.delete_any(id, state.ready)

          %{state | jobs: Map.put(state.jobs, id, retained), ready: ready}
      end
    end)
  end

  defp announced?(state, job), do: Enum.any?(job.groups, &(state.groups[&1].id != nil))

  def buffer(state, frame, result) do
    groups =
      Enum.reduce(frame.groups, state.groups, fn ref, groups ->
        Map.update!(groups, ref, &%{&1 | buffered: MapSet.put(&1.buffered, frame.ref)})
      end)

    %{state | groups: groups, buffered: Map.put(state.buffered, frame.ref, {frame, result})}
  end

  # Owner memberships contain only live buffers. Removing a buffer is separate
  # from publishing it: discarded data must not wake its dependent groups.
  def pop_buffer(state, ref) do
    {{frame, result}, buffered} = Map.pop!(state.buffered, ref)

    groups =
      Enum.reduce(frame.groups, state.groups, fn owner, groups ->
        Map.update!(groups, owner, &%{&1 | buffered: MapSet.delete(&1.buffered, ref)})
      end)

    {{frame, result}, %{state | groups: groups, buffered: buffered}}
  end

  def buffered_refs(state, groups) do
    Enum.reduce(groups, MapSet.new(), fn ref, refs ->
      MapSet.union(refs, state.groups[ref].buffered)
    end)
  end

  def has_work?(state, ref), do: MapSet.size(state.groups[ref].jobs) > 0

  def has_buffered?(state, ref), do: MapSet.size(state.groups[ref].buffered) > 0

  def wait_for(state, dependency, ref) do
    %{state | waiting: Map.update(state.waiting, dependency, [ref], &[ref | &1])}
  end

  def wake(state, dependency) do
    case Map.pop(state.waiting, dependency) do
      {nil, _} -> state
      {refs, waiting} -> %{state | waiting: waiting, unannounced: refs ++ state.unannounced}
    end
  end

  def cancel_waiters(state, dependencies),
    do: %{state | waiting: Map.drop(state.waiting, dependencies)}

  defp change_memberships(state, groups, id, action) do
    Enum.reduce(groups, state, fn ref, state ->
      group = state.groups[ref]

      jobs =
        case action do
          :add -> MapSet.put(group.jobs, id)
          :remove -> MapSet.delete(group.jobs, id)
        end

      finished = MapSet.size(jobs) == 0

      candidates =
        if finished,
          do: MapSet.put(state.completion_candidates, ref),
          else: MapSet.delete(state.completion_candidates, ref)

      state = %{
        state
        | groups: Map.put(state.groups, ref, %{group | jobs: jobs}),
          completion_candidates: candidates
      }

      if finished, do: wake(state, {:group, ref}), else: state
    end)
  end

  def path(path), do: Absinthe.Resolution.path(%{path: path})
end
