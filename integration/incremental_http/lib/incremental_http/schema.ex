defmodule IncrementalHTTP.Schema do
  use Absinthe.Schema

  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  defmodule MutationGate do
    @behaviour Absinthe.Plugin

    def before_resolution(execution), do: execution
    def pipeline(pipeline, _execution), do: pipeline

    def after_resolution(execution) do
      case Map.pop(execution.context, :mutation_gate) do
        {nil, _} ->
          execution

        {{pid, path}, context} ->
          IncrementalHTTP.Bridge.event(context.request_id, %{event: "blocked", path: path})

          receive do
            :release_resolver -> send(pid, :release)
          after
            15_000 -> raise "test did not release the mutation child"
          end

          %{execution | context: context}
      end
    end
  end

  def plugins, do: [MutationGate | Absinthe.Plugin.defaults()]

  def delayed_id(resolution, _) do
    id = resolution.source.id
    path = Absinthe.Resolution.path(resolution)

    task =
      Task.async(fn ->
        receive do
          :release -> {:ok, id}
        end
      end)

    resolution
    |> put_in([Access.key(:context), :mutation_gate], {task.pid, path})
    |> Absinthe.Middleware.Async.call(task)
  end

  def trace(source, args, resolution) do
    IncrementalHTTP.Bridge.event(resolution.context.request_id, %{
      event: "resolved",
      path: Absinthe.Resolution.path(resolution)
    })

    if args[:wait], do: wait_for_release(resolution)

    key = resolution.definition.schema_node.identifier
    {:ok, Map.get(source, key)}
  end

  def slow(source, args, resolution) do
    trace(source, args, resolution)
    wait_for_release(resolution)
    {:ok, "released"}
  end

  defp wait_for_release(resolution) do
    IncrementalHTTP.Bridge.event(resolution.context.request_id, %{
      event: "blocked",
      path: Absinthe.Resolution.path(resolution)
    })

    receive do
      :release_resolver -> :ok
    after
      15_000 -> raise "timed out waiting for :release_resolver"
    end
  end

  interface :node do
    field :id, non_null(:id)
    resolve_type fn _, _ -> :person end
  end

  object :person do
    interface :node
    field :id, non_null(:id), resolve: &__MODULE__.trace/3

    field :delayed_id, non_null(:id) do
      middleware &__MODULE__.delayed_id/2

      middleware fn resolution, _ ->
        IncrementalHTTP.Bridge.event(resolution.context.request_id, %{
          event: "resolved",
          path: Absinthe.Resolution.path(resolution)
        })

        resolution
      end
    end

    field :name, :string, resolve: &__MODULE__.trace/3
    field :age, :integer, resolve: &__MODULE__.trace/3

    field :friend, :person do
      arg :wait, :boolean, default_value: false
      resolve &__MODULE__.trace/3
    end

    field :friends, list_of(:person), resolve: &__MODULE__.trace/3

    field :failure, :string do
      resolve fn _, _, _ -> {:error, %{message: "unavailable", code: "OFFLINE"}} end
    end

    field :required_failure, non_null(:string) do
      resolve fn _, _, _ -> {:error, "required value unavailable"} end
    end

    field :slow, :string, resolve: &__MODULE__.slow/3
  end

  object :person_edge do
    field :cursor, non_null(:string)
    field :node, :person
  end

  object :page_info do
    field :has_next_page, non_null(:boolean)
    field :has_previous_page, non_null(:boolean)
    field :start_cursor, :string
    field :end_cursor, :string
  end

  object :person_connection do
    field :edges, list_of(:person_edge)
    field :page_info, non_null(:page_info)
  end

  def connection(source, args, resolution) do
    trace(source, args, resolution)
    offset = if args[:after], do: String.to_integer(args.after), else: 0
    people = source.people
    selected = Enum.slice(people, offset, args.first)

    edges =
      Enum.with_index(selected, offset + 1)
      |> Enum.map(fn {person, index} ->
        unless resolution.definition.schema_node.identifier == :nullable_people_connection and
                 index == 2 do
          %{cursor: Integer.to_string(index), node: person}
        end
      end)

    {:ok,
     %{
       edges: edges,
       page_info: %{
         has_next_page: offset + length(edges) < length(people),
         has_previous_page: offset > 0,
         start_cursor: if(selected != [], do: Integer.to_string(offset + 1)),
         end_cursor: if(selected != [], do: Integer.to_string(offset + length(selected)))
       }
     }}
  end

  query do
    field :node, :node, resolve: &__MODULE__.trace/3

    field :nullable_people_connection, :person_connection do
      arg :first, non_null(:integer)
      arg :after, :string
      resolve &__MODULE__.connection/3
    end

    field :people_connection, :person_connection do
      arg :first, non_null(:integer)
      arg :after, :string
      resolve &__MODULE__.connection/3
    end

    field :person, :person, resolve: &__MODULE__.trace/3
    field :required_people, list_of(non_null(:person)), resolve: &__MODULE__.trace/3
    field :nullable_people, list_of(:person), resolve: &__MODULE__.trace/3
    field :people, list_of(:person), resolve: &__MODULE__.trace/3
    field :numbers, list_of(:integer), resolve: &__MODULE__.trace/3
    field :required_numbers, list_of(non_null(:integer)), resolve: &__MODULE__.trace/3
    field :matrix, list_of(list_of(non_null(:integer))), resolve: &__MODULE__.trace/3

    field :required_rows, list_of(non_null(list_of(non_null(:integer)))),
      resolve: &__MODULE__.trace/3

    field :empty, list_of(:person), resolve: &__MODULE__.trace/3
    field :absent, :person, resolve: &__MODULE__.trace/3
  end

  mutation do
    field :first, :person, resolve: &__MODULE__.trace/3
    field :second, :person, resolve: &__MODULE__.trace/3
  end

  def root_value do
    grace = %{id: 2, name: "Grace", age: 40}
    edsger = %{id: 3, name: "Edsger", age: 41}

    ada = %{
      id: 1,
      name: "Ada",
      age: 37,
      friend: grace,
      friends: [grace, edsger]
    }

    %{
      first: ada,
      second: grace,
      person: ada,
      node: ada,
      people: [ada, grace, edsger],
      nullable_people: [ada, nil, grace],
      required_people: [ada, nil, grace],
      numbers: [1, 2, 3],
      required_numbers: [1, nil, 3],
      matrix: [[1], [nil], [3]],
      required_rows: [[1], [nil], [3]],
      empty: [],
      absent: nil
    }
  end
end
