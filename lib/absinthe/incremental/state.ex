defmodule Absinthe.Incremental.State do
  @moduledoc false

  defstruct jobs: [],
            groups: %{},
            order: [],
            unannounced: [],
            next_id: 0,
            frame: nil,
            work: %{},
            completion_candidates: MapSet.new(),
            buffered: []

  # Group identities outlive their notices: queued occurrences can still refer
  # to their ancestry. Work counts keep those identities out of scheduler scans.

  def group(state, attributes) do
    ref = make_ref()
    owner = if state.frame, do: state.frame.ref

    group =
      Map.merge(
        %{
          id: nil,
          done: false,
          parent: nil,
          label: nil,
          owner: owner,
          ordinal: map_size(state.groups)
        },
        attributes
      )

    {ref,
     %{
       state
       | groups: Map.put(state.groups, ref, group),
         order: [ref | state.order],
         unannounced: [ref | state.unannounced]
     }}
  end

  def enqueue(state, job) do
    state = change_work(state, job.groups, 1)
    %{state | jobs: state.jobs ++ [Map.put(job, :ref, make_ref())]}
  end

  def remove(state, job) do
    state = change_work(state, job.groups, -1)
    %{state | jobs: List.delete(state.jobs, job)}
  end

  def replace_jobs(state, jobs) do
    work =
      Enum.reduce(jobs, %{}, fn job, work ->
        Enum.reduce(job.groups, work, &Map.update(&2, &1, 1, fn count -> count + 1 end))
      end)

    candidates =
      Enum.reduce(state.work, state.completion_candidates, fn {ref, _}, candidates ->
        if Map.get(work, ref, 0) == 0,
          do: MapSet.put(candidates, ref),
          else: MapSet.delete(candidates, ref)
      end)

    %{state | jobs: jobs, work: work, completion_candidates: candidates}
  end

  def has_work?(state, ref), do: Map.get(state.work, ref, 0) > 0

  defp change_work(state, groups, change) do
    Enum.reduce(groups, state, fn ref, state ->
      count = Map.get(state.work, ref, 0) + change

      candidates =
        if count == 0,
          do: MapSet.put(state.completion_candidates, ref),
          else: MapSet.delete(state.completion_candidates, ref)

      work = if count == 0, do: Map.delete(state.work, ref), else: Map.put(state.work, ref, count)
      %{state | work: work, completion_candidates: candidates}
    end)
  end

  def directive(%{directives: directives}, name) do
    identifier =
      case name do
        "defer" -> :defer
        "stream" -> :stream
      end

    Enum.find_value(directives, fn directive ->
      if match?(
           %{identifier: ^identifier, definition: Absinthe.Type.BuiltIns.IncrementalDirectives},
           directive.schema_node
         ) do
        args = Absinthe.Blueprint.Input.Argument.value_map(directive.arguments)
        if Map.get(args, :if, true), do: {directive, args}
      end
    end)
  end

  def path(path), do: Absinthe.Resolution.path(%{path: path})
end
