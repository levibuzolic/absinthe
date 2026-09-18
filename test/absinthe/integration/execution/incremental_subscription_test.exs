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

  setup_all do
    start_supervised!({Registry, keys: :duplicate, name: PubSub})
    start_supervised!({Absinthe.Subscription, PubSub})

    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
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

  defp subscribe(document, later) do
    key = Integer.to_string(System.unique_integer([:positive]))

    assert {:ok, %{"subscribed" => topic}} =
             Absinthe.run_incremental(document, Schema,
               variables: %{"topic" => key, "later" => later},
               context: %{pubsub: PubSub}
             )

    PubSub.subscribe(topic)
    {key, topic}
  end

  defp publish(key, value), do: Absinthe.Subscription.publish(PubSub, value, event: key)
end
