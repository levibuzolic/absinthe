defmodule Absinthe.Integration.Execution.IncrementalSubscriptionTest do
  use Absinthe.Case, async: true

  defmodule PubSub do
    @behaviour Absinthe.Subscription.Pubsub

    def node_name, do: node()

    def subscribe(topic) do
      Registry.register(__MODULE__, topic, [])
      :ok
    end

    def publish_subscription(topic, data) do
      Registry.dispatch(__MODULE__, topic, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:event, topic, data})
      end)
    end

    def publish_mutation(_, _, _), do: :ok
  end

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :version, :integer
    end

    object :person do
      field :name, :string
      field :friends, list_of(:string)

      field :observed_name, :string do
        resolve fn %{name: name, pid: pid}, _, _ ->
          send(pid, :person_name_resolved)
          {:ok, name}
        end
      end
    end

    object :dog do
      field :name, :string
    end

    union :subject do
      types [:person, :dog]
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    object :event do
      field :subject, :subject
    end

    subscription do
      field :event, :event do
        arg :topic, non_null(:string)
        config fn %{topic: topic}, _ -> {:ok, topic: topic} end
      end
    end
  end

  defmodule StreamOnlySchema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives, only: [:stream]

    query do
      field :version, :integer
    end

    object :event do
      field :numbers, list_of(:integer)
    end

    subscription do
      field :event, :event do
        arg :topic, non_null(:string)
        config fn %{topic: topic}, _ -> {:ok, topic: topic} end
      end
    end
  end

  setup_all do
    start_supervised!({Registry, keys: :duplicate, name: PubSub})
    start_supervised!({Absinthe.Subscription, PubSub})

    for schema <- [Schema, StreamOnlySchema],
        schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!(Supervisor.child_spec({Absinthe.Schema.Manager, schema}, id: schema))
    end

    :ok
  end

  test "registration accepts a variable directive and only matching events fail" do
    document = """
    subscription($later: Boolean!, $topic: String!) {
      event(topic: $topic) {
        subject {
          __typename
          ... on Person @defer(if: $later) { name }
          ... on Dog { name }
        }
      }
    }
    """

    {key, topic} = subscribe(document, true)
    publish(key, %{subject: %{kind: :dog, name: "Spot"}})

    assert_receive {:event, ^topic,
                    %{
                      data: %{
                        "event" => %{"subject" => %{"__typename" => "Dog", "name" => "Spot"}}
                      }
                    }}

    publish(key, %{subject: nil})
    assert_receive {:event, ^topic, %{data: %{"event" => %{"subject" => nil}}}}

    publish(key, %{subject: %{kind: :person, name: "Ada"}})
    assert_receive {:event, ^topic, %{data: %{"event" => %{"subject" => nil}}, errors: [error]}}
    assert error.path == ["event", "subject"]
    assert error.message =~ "subscription"
  end

  test "disabled defer is eager during subscription publication" do
    document = """
    subscription($later: Boolean!, $topic: String!) {
      event(topic: $topic) { subject { ... on Person @defer(if: $later) { name } } }
    }
    """

    {key, topic} = subscribe(document, false)
    publish(key, %{subject: %{kind: :person, name: "Ada"}})
    assert_receive {:event, ^topic, %{data: %{"event" => %{"subject" => %{"name" => "Ada"}}}}}
  end

  test "active stream fails the reached subscription field and disabled stream remains eager" do
    document = """
    subscription($later: Boolean!, $topic: String!) {
      event(topic: $topic) { subject { ... on Person { name friends @stream(if: $later) } } }
    }
    """

    {key, topic} = subscribe(document, true)
    publish(key, %{subject: %{kind: :person, name: "Ada", friends: ["Grace"]}})

    assert_receive {:event, ^topic,
                    %{
                      data: %{"event" => %{"subject" => %{"name" => "Ada", "friends" => nil}}},
                      errors: [error]
                    }}

    assert error.path == ["event", "subject", "friends"]
    assert error.message =~ "subscription"

    publish(key, %{subject: %{kind: :person, name: "Ada", friends: nil}})

    assert_receive {:event, ^topic,
                    %{
                      data: %{"event" => %{"subject" => %{"name" => "Ada", "friends" => nil}}}
                    } = result}

    refute Map.has_key?(result, :errors)

    {key, topic} = subscribe(document, false)
    publish(key, %{subject: %{kind: :person, name: "Ada", friends: ["Grace"]}})

    assert_receive {:event, ^topic,
                    %{
                      data: %{
                        "event" => %{"subject" => %{"name" => "Ada", "friends" => ["Grace"]}}
                      }
                    }}
  end

  test "skip and include conditions take precedence during event execution" do
    document = """
    subscription($later: Boolean!, $topic: String!) {
      event(topic: $topic) {
        subject {
          __typename
          ... on Person @defer(if: $later) @skip(if: true) { name }
        }
      }
    }
    """

    {key, topic} = subscribe(document, true)
    publish(key, %{subject: %{kind: :person, name: "Ada"}})

    assert_receive {:event, ^topic,
                    %{data: %{"event" => %{"subject" => %{"__typename" => "Person"}}}}}
  end

  test "subscription execution guards a schema importing only stream" do
    document = """
    subscription($later: Boolean!, $topic: String!) {
      event(topic: $topic) { numbers @stream(if: $later) }
    }
    """

    {key, topic} = subscribe(document, true, StreamOnlySchema)
    publish(key, %{numbers: [1, 2]})

    assert_receive {:event, ^topic, %{data: %{"event" => %{"numbers" => nil}}, errors: [error]}}
    assert error.path == ["event", "numbers"]
    assert error.message =~ "subscription"

    {key, topic} = subscribe(document, false, StreamOnlySchema)
    publish(key, %{numbers: [1, 2]})
    assert_receive {:event, ^topic, %{data: %{"event" => %{"numbers" => [1, 2]}}}}
  end

  test "ordinary registration honors variable inclusion on a deferred named fragment" do
    document = """
    subscription($later: Boolean!, $included: Boolean!, $topic: String!) {
      event(topic: $topic) {
        subject {
          __typename
          ...PersonFields @defer(if: $later) @include(if: $included)
        }
      }
    }
    fragment PersonFields on Person { observedName }
    """

    for {later, included} <- [{true, false}, {true, true}, {false, true}] do
      {key, topic} = subscribe_ordinary(document, %{"later" => later, "included" => included})
      publish(key, %{subject: %{kind: :person, name: "Ada", pid: self()}})
      assert_receive {:event, ^topic, result}

      case {later, included} do
        {true, false} ->
          assert result == %{data: %{"event" => %{"subject" => %{"__typename" => "Person"}}}}
          refute_received :person_name_resolved

        {true, true} ->
          assert %{data: %{"event" => %{"subject" => nil}}, errors: [error]} = result
          assert error.path == ["event", "subject"]

          assert error.message ==
                   "The @defer directive is not supported on subscription operations."

          refute_received :person_name_resolved

        {false, true} ->
          assert result == %{
                   data: %{
                     "event" => %{
                       "subject" => %{"__typename" => "Person", "observedName" => "Ada"}
                     }
                   }
                 }

          assert_received :person_name_resolved
          refute_received :person_name_resolved
      end
    end
  end

  test "an excluded named fragment does not hide a later included active defer" do
    document = """
    subscription($later: Boolean!, $excluded: Boolean!, $included: Boolean!, $topic: String!) {
      event(topic: $topic) {
        subject {
          ...PersonFields @include(if: $excluded)
          ...PersonFields @defer(if: $later) @include(if: $included)
        }
      }
    }
    fragment PersonFields on Person { observedName }
    """

    {key, topic} =
      subscribe_ordinary(document, %{"later" => true, "excluded" => false, "included" => true})

    publish(key, %{subject: %{kind: :person, name: "Ada", pid: self()}})

    assert_receive {:event, ^topic, %{data: %{"event" => %{"subject" => nil}}, errors: [error]}}
    assert error.path == ["event", "subject"]
    assert error.message == "The @defer directive is not supported on subscription operations."
    refute_received :person_name_resolved
  end

  defp subscribe_ordinary(document, variables) do
    key = Integer.to_string(System.unique_integer([:positive]))

    assert {:ok, %{"subscribed" => topic}} =
             Absinthe.run(document, Schema,
               variables: Map.put(variables, "topic", key),
               context: %{pubsub: PubSub}
             )

    PubSub.subscribe(topic)
    {key, topic}
  end

  defp subscribe(document, later, schema \\ Schema) do
    key = Integer.to_string(System.unique_integer([:positive]))

    assert {:ok, %{"subscribed" => topic}} =
             Absinthe.run_incremental(document, schema,
               variables: %{"topic" => key, "later" => later},
               context: %{pubsub: PubSub}
             )

    PubSub.subscribe(topic)
    {key, topic}
  end

  defp publish(key, value), do: Absinthe.Subscription.publish(PubSub, value, event: key)
end
