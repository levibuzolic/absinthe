defmodule IncrementalHTTP.Schema do
  use Absinthe.Schema

  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  def trace(source, _, resolution) do
    IncrementalHTTP.Bridge.event(resolution.context.request_id, %{
      event: "resolved",
      path: Absinthe.Resolution.path(resolution)
    })

    key = resolution.definition.schema_node.identifier
    {:ok, Map.get(source, key)}
  end

  def slow(source, args, resolution) do
    trace(source, args, resolution)

    IncrementalHTTP.Bridge.event(resolution.context.request_id, %{
      event: "blocked",
      path: Absinthe.Resolution.path(resolution)
    })

    receive do
      :release_resolver -> {:ok, "released"}
    after
      15_000 -> raise "timed out waiting for :release_resolver"
    end
  end

  object :person do
    field :id, :id, resolve: &__MODULE__.trace/3
    field :name, :string, resolve: &__MODULE__.trace/3
    field :age, :integer, resolve: &__MODULE__.trace/3
    field :friend, :person, resolve: &__MODULE__.trace/3
    field :friends, list_of(:person), resolve: &__MODULE__.trace/3

    field :failure, :string do
      resolve fn _, _, _ -> {:error, %{message: "unavailable", code: "OFFLINE"}} end
    end

    field :required_failure, non_null(:string) do
      resolve fn _, _, _ -> {:error, "required value unavailable"} end
    end

    field :slow, :string, resolve: &__MODULE__.slow/3
  end

  query do
    field :person, :person, resolve: &__MODULE__.trace/3
    field :people, list_of(:person), resolve: &__MODULE__.trace/3
    field :numbers, list_of(:integer), resolve: &__MODULE__.trace/3
    field :required_numbers, list_of(non_null(:integer)), resolve: &__MODULE__.trace/3
    field :empty, list_of(:person), resolve: &__MODULE__.trace/3
    field :absent, :person, resolve: &__MODULE__.trace/3
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
      person: ada,
      people: [ada, grace, edsger],
      numbers: [1, 2, 3],
      required_numbers: [1, nil, 3],
      empty: [],
      absent: nil
    }
  end
end
