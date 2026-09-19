defmodule Absinthe.Incremental.State do
  @moduledoc false

  alias Absinthe.{Blueprint, Type}

  @typedoc "An internal group identity, distinct from its public string ID."
  @type group_ref :: non_neg_integer()

  @typedoc "A delivery group and the queued or buffered work belonging to it."
  @type group :: %{
          required(:kind) => :defer | :stream,
          required(:path) => [String.t() | non_neg_integer()],
          required(:id) => String.t() | nil,
          required(:done) => boolean(),
          required(:parent) => group_ref() | nil,
          required(:owner) => non_neg_integer() | nil,
          required(:jobs) => MapSet.t(non_neg_integer()),
          required(:buffered) => MapSet.t(non_neg_integer()),
          required(:response_keys) => MapSet.t(String.t()) | :all,
          optional(:directive_id) => non_neg_integer(),
          optional(:label) => String.t() | nil,
          optional(:errors) => [term()]
        }

  @typep emitter :: Blueprint.Document.Field.t() | Blueprint.Document.Operation.t()
  @typep dependency :: {:group, group_ref()} | {:frame, non_neg_integer()}

  @typedoc """
  Deferred fields or streamed values awaiting execution. Enqueue assigns
  `job_id`; taking the work assigns its execution frame `ref`.
  """
  @type work :: %{
          required(:kind) => :defer | :stream,
          required(:groups) => MapSet.t(group_ref()),
          required(:emitter) => emitter(),
          required(:source) => term(),
          required(:path) => [emitter() | non_neg_integer()],
          optional(:job_id) => non_neg_integer(),
          optional(:ref) => non_neg_integer(),
          optional(:fields) => [Blueprint.Document.Field.t()],
          optional(:parent_type) => Type.Object.t(),
          optional(:values) => nonempty_list(term()),
          optional(:index) => non_neg_integer(),
          optional(:item_type) => Type.reference_t(),
          optional(:extensions) => map()
        }

  @typep buffered_frame :: %{
           required(:ref) => non_neg_integer(),
           required(:groups) => MapSet.t(group_ref()),
           required(:path) => [emitter() | non_neg_integer()]
         }

  defstruct jobs: %{},
            ready: :gb_sets.empty(),
            next_job: 0,
            groups: %{},
            group_ids: %{},
            unannounced: :gb_sets.empty(),
            next_frame: 0,
            frame: nil,
            waiting: %{},
            completion_candidates: :gb_sets.empty(),
            buffered: %{}

  @typedoc "Scheduling state shared by incremental planning, execution, and delivery."
  @type t :: %__MODULE__{
          jobs: %{optional(non_neg_integer()) => work()},
          ready: :gb_sets.set(non_neg_integer()),
          next_job: non_neg_integer(),
          groups: %{optional(group_ref()) => group()},
          group_ids: %{optional(String.t()) => group_ref()},
          unannounced: :gb_sets.set(group_ref()),
          next_frame: non_neg_integer(),
          frame: work() | nil,
          waiting: %{optional(dependency()) => [group_ref()]},
          completion_candidates: :gb_sets.set(group_ref()),
          buffered: %{optional(non_neg_integer()) => {buffered_frame(), map()}}
        }

  # Group identities outlive their notices: queued occurrences can still refer
  # to their ancestry. Membership sets track queued and running work, while
  # ready IDs track only runnable queued jobs. Frame IDs record execution order.

  @spec group(t(), map()) :: {group_ref(), t()}
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
          buffered: MapSet.new(),
          response_keys: MapSet.new()
        },
        attributes
      )

    {ref,
     %{
       state
       | groups: Map.put(state.groups, ref, group),
         unannounced: :gb_sets.add(ref, state.unannounced)
     }}
  end

  @spec enqueue(t(), work()) :: t()
  def enqueue(state, job) do
    id = state.next_job
    job = job |> Map.delete(:ref) |> Map.put(:job_id, id)
    state = change_memberships(state, job.groups, id, :add)
    ready = if announced?(state, job), do: :gb_sets.add(id, state.ready), else: state.ready

    %{state | jobs: Map.put(state.jobs, id, job), ready: ready, next_job: id + 1}
  end

  @spec pending?(t()) :: boolean()
  def pending?(state), do: map_size(state.jobs) > 0

  @spec take(t()) :: {work(), t()}
  def take(state) do
    if :gb_sets.is_empty(state.ready),
      do: raise("Incremental work has no announced delivery group")

    {id, ready} = :gb_sets.take_smallest(state.ready)
    {job, jobs} = Map.pop!(state.jobs, id)
    frame = Map.put(job, :ref, state.next_frame)

    {frame, %{state | jobs: jobs, ready: ready, frame: frame, next_frame: state.next_frame + 1}}
  end

  @spec finish(t(), work()) :: t()
  def finish(state, frame) do
    state = change_memberships(state, frame.groups, frame.job_id, :remove)
    %{state | frame: nil}
  end

  @spec announced(t(), group_ref(), String.t()) :: t()
  def announced(state, ref, id) do
    group = state.groups[ref]
    ready = Enum.reduce(group.jobs, state.ready, &:gb_sets.add/2)

    candidates =
      if MapSet.size(group.jobs) == 0,
        do: :gb_sets.add(ref, state.completion_candidates),
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
  @spec restrict_jobs(t(), (work() -> work() | nil)) :: t()
  def restrict_jobs(state, restrict) do
    Enum.reduce(state.jobs, state, fn {id, job}, state ->
      case restrict.(job) do
        nil ->
          state = change_memberships(state, job.groups, id, :remove)
          %{state | jobs: Map.delete(state.jobs, id), ready: :gb_sets.delete_any(id, state.ready)}

        ^job ->
          state

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

  @spec buffer(t(), work() | buffered_frame(), map()) :: t()
  def buffer(state, frame, result) do
    groups =
      Enum.reduce(frame.groups, state.groups, fn ref, groups ->
        Map.update!(groups, ref, &%{&1 | buffered: MapSet.put(&1.buffered, frame.ref)})
      end)

    # Publication needs only identity, ownership, and path. Finished resolver
    # sources can be large and must not live as long as a group's private data.
    buffered_frame = Map.take(frame, [:ref, :groups, :path])

    %{
      state
      | groups: groups,
        buffered: Map.put(state.buffered, frame.ref, {buffered_frame, result})
    }
  end

  # Owner memberships contain only live buffers. Removing a buffer is separate
  # from publishing it: discarded data must not wake its dependent groups.
  @spec pop_buffer(t(), non_neg_integer()) :: {{buffered_frame(), map()}, t()}
  def pop_buffer(state, ref) do
    {{frame, result}, buffered} = Map.pop!(state.buffered, ref)

    groups =
      Enum.reduce(frame.groups, state.groups, fn owner, groups ->
        Map.update!(groups, owner, &%{&1 | buffered: MapSet.delete(&1.buffered, ref)})
      end)

    {{frame, result}, %{state | groups: groups, buffered: buffered}}
  end

  @spec buffered_refs(t(), MapSet.t(group_ref())) :: MapSet.t(non_neg_integer())
  def buffered_refs(state, groups) do
    Enum.reduce(groups, MapSet.new(), fn ref, refs ->
      MapSet.union(refs, state.groups[ref].buffered)
    end)
  end

  @spec has_work?(t(), group_ref()) :: boolean()
  def has_work?(state, ref), do: MapSet.size(state.groups[ref].jobs) > 0

  @spec has_buffered?(t(), group_ref()) :: boolean()
  def has_buffered?(state, ref), do: MapSet.size(state.groups[ref].buffered) > 0

  @spec wait_for(t(), dependency(), group_ref()) :: t()
  def wait_for(state, dependency, ref) do
    %{state | waiting: Map.update(state.waiting, dependency, [ref], &[ref | &1])}
  end

  @spec wake(t(), dependency()) :: t()
  def wake(state, dependency) do
    case Map.pop(state.waiting, dependency) do
      {nil, _} ->
        state

      {refs, waiting} ->
        unannounced = Enum.reduce(refs, state.unannounced, &:gb_sets.add/2)
        %{state | waiting: waiting, unannounced: unannounced}
    end
  end

  @spec cancel_waiters(t(), [dependency()]) :: t()
  def cancel_waiters(state, dependencies),
    do: %{state | waiting: Map.drop(state.waiting, dependencies)}

  defp change_memberships(state, groups, id, action) do
    Enum.reduce(groups, state, fn ref, state ->
      group = state.groups[ref]

      {jobs, unannounced} =
        case action do
          :add ->
            # A group can first acquire work while completing a shared object
            # selected by an earlier group, after the initial announcement pass.
            unannounced =
              if is_nil(group.id) and MapSet.size(group.jobs) == 0,
                do: :gb_sets.add(ref, state.unannounced),
                else: state.unannounced

            {MapSet.put(group.jobs, id), unannounced}

          :remove ->
            {MapSet.delete(group.jobs, id), state.unannounced}
        end

      finished = MapSet.size(jobs) == 0

      candidates =
        if finished,
          do: :gb_sets.add(ref, state.completion_candidates),
          else: :gb_sets.delete_any(ref, state.completion_candidates)

      state = %{
        state
        | groups: Map.put(state.groups, ref, %{group | jobs: jobs}),
          unannounced: unannounced,
          completion_candidates: candidates
      }

      if finished, do: wake(state, {:group, ref}), else: state
    end)
  end

  def path(path), do: Absinthe.Resolution.path(%{path: path})
end
