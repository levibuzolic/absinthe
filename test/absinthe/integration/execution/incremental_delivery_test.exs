defmodule Absinthe.Integration.Execution.IncrementalDeliveryTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    def traced(source, _, resolution) do
      key = resolution.definition.schema_node.identifier
      send(resolution.context.test_pid, {:resolved, Absinthe.Resolution.path(resolution)})
      {:ok, Map.get(source, key)}
    end

    object :person do
      field :id, :id, resolve: &__MODULE__.traced/3
      field :name, :string, resolve: &__MODULE__.traced/3
      field :age, :integer, resolve: &__MODULE__.traced/3
      field :friend, :person, resolve: &__MODULE__.traced/3
      field :friends, list_of(:person), resolve: &__MODULE__.traced/3

      field :failure, :string do
        resolve fn _, _, _ -> {:error, %{message: "unavailable", code: "OFFLINE"}} end
      end

      field :required_failure, non_null(:string) do
        resolve fn _, _, _ -> {:error, "required value unavailable"} end
      end
    end

    object :organization do
      field :name, :string, resolve: &__MODULE__.traced/3
    end

    union :search_result do
      types [:person, :organization]
      resolve_type fn %{kind: kind}, _ -> kind end
    end

    scalar :encoded do
      serialize fn value -> "encoded-#{value}" end
    end

    enum :status do
      value :ready
      value :waiting
    end

    query do
      field :person, :person, resolve: &__MODULE__.traced/3
      field :people, list_of(:person), resolve: &__MODULE__.traced/3
      field :numbers, list_of(:integer), resolve: &__MODULE__.traced/3
      field :required_numbers, list_of(non_null(:integer)), resolve: &__MODULE__.traced/3
      field :matrix, list_of(list_of(non_null(:integer))), resolve: &__MODULE__.traced/3
      field :search, list_of(:search_result), resolve: &__MODULE__.traced/3
      field :encoded_value, :encoded
      field :statuses, list_of(:status)

      field :tick, :integer do
        middleware fn resolution, _ ->
          counter = Map.get(resolution.context, :counter, 0) + 1

          %{resolution | context: Map.put(resolution.context, :counter, counter)}
          |> Absinthe.Resolution.put_result({:ok, counter})
        end
      end
    end

    mutation do
      field :first, :person, resolve: &__MODULE__.traced/3
      field :second, :person, resolve: &__MODULE__.traced/3
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    person = %{
      id: 1,
      name: "Ada",
      age: 37,
      friend: %{id: 2, name: "Grace", age: 40},
      friends: [%{id: 2, name: "Grace"}, %{id: 3, name: "Edsger"}]
    }

    options = [
      root_value: %{
        person: person,
        people: [person, %{id: 2, name: "Grace", age: 40}],
        numbers: [1, 2, 3],
        required_numbers: [1, nil, 3],
        matrix: [[1, 2], [3, 4]],
        first: person,
        second: person
      },
      context: %{test_pid: self()}
    ]

    {:ok, options: options}
  end

  test "ordinary execution and disabled directives preserve one response", %{options: options} do
    query = "{ person { id ... @defer(if: false) { name } } numbers @stream(if: false) }"

    assert Absinthe.run_incremental(query, Schema, options) ==
             Absinthe.run(query, Schema, options)

    assert {:ok, %{data: %{"person" => %{"id" => "1", "name" => "Ada"}}}} =
             Absinthe.run("{ person { id ... @defer { name } } }", Schema, options)
  end

  test "deferred resolvers run only when a continuation is consumed", %{options: options} do
    query = "{ person { id ... @defer(label: \"details\") { name } } }"
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"person" => %{"id" => "1"}}
    assert [%{id: id, path: ["person"], label: "details"}] = result.initial_result.pending
    assert is_binary(id)
    assert_received {:resolved, ["person"]}
    assert_received {:resolved, ["person", "id"]}
    refute_received {:resolved, ["person", "name"]}

    assert reconstruct(result) == %{"person" => %{"id" => "1", "name" => "Ada"}}
    assert_received {:resolved, ["person", "name"]}
    refute_received {:resolved, ["person"]}
  end

  test "streaming resolves a list once and postpones tail child resolution", %{options: options} do
    query = "{ people @stream(initialCount: 1, label: \"people\") { id name } }"
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"people" => [%{"id" => "1", "name" => "Ada"}]}
    assert_received {:resolved, ["people"]}
    assert_received {:resolved, ["people", 0, "name"]}
    refute_received {:resolved, ["people", 1, "name"]}

    assert reconstruct(result) == %{
             "people" => [%{"id" => "1", "name" => "Ada"}, %{"id" => "2", "name" => "Grace"}]
           }

    assert_received {:resolved, ["people", 1, "name"]}
    refute_received {:resolved, ["people"]}
  end

  test "halting consumption leaves later stream items unresolved", %{options: options} do
    assert {:ok, result} =
             Absinthe.run_incremental("{ people @stream { name } }", Schema, options)

    assert result.initial_result.data == %{"people" => []}
    assert [_] = Enum.take(result.subsequent_results, 1)
    assert_received {:resolved, ["people", 0, "name"]}
    refute_received {:resolved, ["people", 1, "name"]}
  end

  test "eager and deferred occurrences share an object resolver but retain child timing", %{
    options: options
  } do
    query = """
    {
      person { friend { id } }
      ... @defer(label: "details") { person { friend { name } } }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"person" => %{"friend" => %{"id" => "2"}}}
    assert_received {:resolved, ["person"]}
    assert_received {:resolved, ["person", "friend"]}
    refute_received {:resolved, ["person", "friend", "name"]}

    assert reconstruct(result) == %{"person" => %{"friend" => %{"id" => "2", "name" => "Grace"}}}
    assert_received {:resolved, ["person", "friend", "name"]}
    refute_received {:resolved, ["person"]}
    refute_received {:resolved, ["person", "friend"]}
  end

  test "overlapping sibling defers deliver shared fields once and complete both labels", %{
    options: options
  } do
    query = """
    { person {
      id
      ... @defer(label: "a") { name friend { id name } }
      ... @defer(label: "b") { name age friend { name age } }
    } }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)

    assert reconstruct(result) == %{
             "person" => %{
               "id" => "1",
               "name" => "Ada",
               "age" => 37,
               "friend" => %{"id" => "2", "name" => "Grace", "age" => 40}
             }
           }

    assert_received {:resolved, ["person", "name"]}
    refute_received {:resolved, ["person", "name"]}
    assert_received {:resolved, ["person", "friend"]}
    refute_received {:resolved, ["person", "friend"]}
    assert_received {:resolved, ["person", "friend", "name"]}
    refute_received {:resolved, ["person", "friend", "name"]}
  end

  test "nested defer and stream combinations reconstruct the eager result", %{options: options} do
    queries = [
      "{ person { id ... @defer { friends @stream(initialCount: 1) { id ... @defer { name } } } } }",
      "{ people @stream { id ... @defer { name ... @defer { age } } } }",
      "{ person { ... @defer { name ... @defer { name age } } } }",
      "{ matrix @stream(initialCount: 1) }",
      "{ person { ...A @defer } } fragment A on Person { name ...B @defer } fragment B on Person { age }"
    ]

    for query <- queries do
      assert {:ok, %{data: expected}} = Absinthe.run(query, Schema, options)
      assert {:ok, incremental} = Absinthe.run_incremental(query, Schema, options)
      assert reconstruct(incremental) == expected
    end
  end

  test "aliases and repeated fragment paths remain distinct for each list item", %{
    options: options
  } do
    query = """
    { members: people { identifier: id ...Details @defer(label: "details") } }
    fragment Details on Person { displayName: name }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert Enum.map(result.initial_result.pending, & &1.path) == [["members", 0], ["members", 1]]

    assert reconstruct(result) == %{
             "members" => [
               %{"identifier" => "1", "displayName" => "Ada"},
               %{"identifier" => "2", "displayName" => "Grace"}
             ]
           }
  end

  test "a nullable deferred error retains its response path and extra values", %{options: options} do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ person { id ... @defer { problem: failure } } }",
               Schema,
               options
             )

    payloads = Enum.to_list(result.subsequent_results)

    errors =
      for payload <- payloads,
          entry <- Map.get(payload, :incremental, []),
          error <- Map.get(entry, :errors, []),
          do: error

    assert [
             %{
               message: "unavailable",
               path: ["person", "problem"],
               code: "OFFLINE",
               locations: [_]
             }
           ] = errors

    assert List.last(payloads).hasNext == false
  end

  test "non-null deferred failure appears in completion without invalid data", %{options: options} do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ person { id ... @defer { requiredFailure } } }",
               Schema,
               options
             )

    assert result.initial_result.data == %{"person" => %{"id" => "1"}}
    payloads = Enum.to_list(result.subsequent_results)
    assert [] == Enum.flat_map(payloads, &Map.get(&1, :incremental, []))

    assert [%{errors: [%{path: ["person", "requiredFailure"]}]}] =
             Enum.flat_map(payloads, &Map.get(&1, :completed, []))

    assert List.last(payloads).hasNext == false
  end

  test "a non-null tail failure cannot poison the initial stream prefix", %{options: options} do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ requiredNumbers @stream(initialCount: 1) }",
               Schema,
               options
             )

    assert result.initial_result.data == %{"requiredNumbers" => [1]}
    refute Map.has_key?(result.initial_result, :errors)
    payloads = Enum.to_list(result.subsequent_results)

    assert [%{errors: [%{path: ["requiredNumbers", 1]}]}] =
             Enum.flat_map(payloads, &Map.get(&1, :completed, []))

    assert List.last(payloads).hasNext == false
  end

  test "an initial null parent suppresses unreachable incremental work", %{options: options} do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ person { requiredFailure ... @defer { name } } }",
               Schema,
               options
             )

    assert %{data: %{"person" => nil}, errors: [_]} = result
    refute Map.has_key?(result, :pending)
    refute_received {:resolved, ["person", "name"]}
  end

  test "empty and fully included lists need no incremental envelope", %{options: options} do
    for {numbers, count} <- [{[], 0}, {[1], 1}, {[1, 2], 9}] do
      options = Keyword.put(options, :root_value, %{numbers: numbers})
      query = "{ numbers @stream(initialCount: #{count}) }"

      assert {:ok, %{data: %{"numbers" => ^numbers}}} =
               Absinthe.run_incremental(query, Schema, options)
    end
  end

  test "nested mutation defers preserve serial root mutation order", %{options: options} do
    query = "mutation { first { id ... @defer { name } } second { id ... @defer { age } } }"
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert_received {:resolved, ["first"]}
    assert_received {:resolved, ["first", "id"]}
    assert_received {:resolved, ["second"]}
    assert_received {:resolved, ["second", "id"]}
    refute_received {:resolved, ["first", "name"]}

    assert reconstruct(result) == %{
             "first" => %{"id" => "1", "name" => "Ada"},
             "second" => %{"id" => "1", "age" => 37}
           }
  end

  test "operation selection, variables, and the raising API use the same pipeline", %{
    options: options
  } do
    query =
      "query A { person { id } } query B($later: Boolean!) { person { ... @defer(if: $later) { name } } }"

    options = Keyword.merge(options, operation_name: "B", variables: %{"later" => true})

    assert reconstruct(Absinthe.run_incremental!(query, Schema, options)) == %{
             "person" => %{"name" => "Ada"}
           }
  end

  test "context updates from middleware survive separate delivery frames", %{options: options} do
    query = "{ first: tick ... @defer { second: tick } ... @defer { third: tick } }"
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"first" => 1}
    assert reconstruct(result) == %{"first" => 1, "second" => 2, "third" => 3}
  end

  test "streams discovered in shared deferred objects wait for the parent data", %{
    options: options
  } do
    query = """
    {
      ... @defer(label: "left") {
        person { friends @stream { name } friend { friend { name } } }
      }
      ... @defer(label: "right") {
        person { id friend { friend { id } } }
      }
    }
    """

    source = %{
      person: %{
        id: 1,
        friends: [%{name: "Grace"}],
        friend: %{friend: %{id: 2, name: "Ada"}}
      }
    }

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema, Keyword.put(options, :root_value, source))

    assert reconstruct(result) == %{
             "person" => %{
               "id" => "1",
               "friends" => [%{"name" => "Grace"}],
               "friend" => %{"friend" => %{"id" => "2", "name" => "Ada"}}
             }
           }
  end

  test "abstract list items apply deferred type conditions independently", %{options: options} do
    source = %{
      search: [
        %{kind: :person, id: 1, name: "Ada"},
        %{kind: :organization, name: "ACME"},
        %{kind: :person, id: 2, name: "Grace"}
      ]
    }

    query = """
    { search @stream(initialCount: 1) {
      __typename
      ... on Person { id ... @defer(label: "personName") { name } }
      ... on Organization { ... @defer(label: "organizationName") { name } }
    } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema, Keyword.put(options, :root_value, source))

    assert reconstruct(result) == %{
             "search" => [
               %{"__typename" => "Person", "id" => "1", "name" => "Ada"},
               %{"__typename" => "Organization", "name" => "ACME"},
               %{"__typename" => "Person", "id" => "2", "name" => "Grace"}
             ]
           }
  end

  test "negative initial count is an execution error only for a selected active stream", %{
    options: options
  } do
    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3] do
      assert {:ok, %{data: %{"numbers" => nil}, errors: [%{path: ["numbers"]}]}} =
               run.("{ numbers @stream(initialCount: -1) }", Schema, options)

      assert {:ok, %{data: %{"numbers" => [1, 2, 3]}}} =
               run.("{ numbers @stream(initialCount: -1, if: false) }", Schema, options)

      assert {:ok, %{data: %{}}} =
               run.("{ numbers @stream(initialCount: -1) @skip(if: true) }", Schema, options)
    end
  end

  test "null list completion does not evaluate stream arguments", %{options: options} do
    options = Keyword.put(options, :root_value, %{numbers: nil})

    assert {:ok, %{data: %{"numbers" => nil}}} =
             Absinthe.run_incremental("{ numbers @stream(initialCount: -1) }", Schema, options)
  end

  test "deferred scalars and streamed enums use their schema serialization", %{options: options} do
    options =
      Keyword.put(options, :root_value, %{encoded_value: 42, statuses: [:ready, :waiting]})

    query = "{ ... @defer { encodedValue } statuses @stream(initialCount: 1) }"
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert result.initial_result.data == %{"statuses" => ["READY"]}

    assert reconstruct(result) == %{
             "encodedValue" => "encoded-42",
             "statuses" => ["READY", "WAITING"]
           }
  end

  test "only the outer list streams and inner non-null failures retain the full item path", %{
    options: options
  } do
    options = Keyword.put(options, :root_value, %{matrix: [[1], [nil], [3]]})

    assert {:ok, result} =
             Absinthe.run_incremental("{ matrix @stream(initialCount: 1) }", Schema, options)

    assert result.initial_result.data == %{"matrix" => [[1]]}
    payloads = Enum.to_list(result.subsequent_results)
    entries = Enum.flat_map(payloads, &Map.get(&1, :incremental, []))
    assert Enum.flat_map(entries, & &1.items) == [nil, [3]]
    assert [%{path: ["matrix", 1, 0]}] = Enum.flat_map(entries, &Map.get(&1, :errors, []))

    refute Enum.any?(
             Enum.flat_map(payloads, &Map.get(&1, :completed, [])),
             &Map.has_key?(&1, :errors)
           )
  end

  test "complexity limits apply before any deferred resolver can execute", %{options: options} do
    options = Keyword.merge(options, analyze_complexity: true, max_complexity: 1)

    assert {:ok, %{errors: [_ | _]}} =
             Absinthe.run_incremental(
               "{ person { ... @defer { id name age } } }",
               Schema,
               options
             )

    refute_received {:resolved, _}
  end

  # A small transport consumer also checks the relationships between notices,
  # results, and completion, independently of the executor's scheduling policy.
  defp reconstruct(%{data: data}), do: data

  defp reconstruct(%{initial_result: initial, subsequent_results: subsequent}) do
    payloads = [initial | Enum.to_list(subsequent)]
    assert List.last(payloads).hasNext == false
    assert Enum.all?(Enum.drop(payloads, -1), & &1.hasNext)

    {data, pending, completed} =
      Enum.reduce(payloads, {initial.data, %{}, MapSet.new()}, fn payload,
                                                                  {data, pending, completed} ->
        pending =
          Enum.reduce(Map.get(payload, :pending, []), pending, fn notice, acc ->
            assert is_binary(notice.id)
            refute Map.has_key?(acc, notice.id)
            Map.put(acc, notice.id, notice.path)
          end)

        data =
          Enum.reduce(Map.get(payload, :incremental, []), data, fn entry, acc ->
            assert Map.has_key?(pending, entry.id)
            refute MapSet.member?(completed, entry.id)
            path = Map.fetch!(pending, entry.id) ++ Map.get(entry, :subPath, [])

            update_path(acc, path, fn previous ->
              case entry do
                %{data: fields} -> Map.merge(previous, fields)
                %{items: items} -> previous ++ items
              end
            end)
          end)

        completed =
          Enum.reduce(Map.get(payload, :completed, []), completed, fn notice, acc ->
            assert Map.has_key?(pending, notice.id)
            refute MapSet.member?(acc, notice.id)
            refute Map.has_key?(notice, :errors)
            MapSet.put(acc, notice.id)
          end)

        {data, pending, completed}
      end)

    assert MapSet.new(Map.keys(pending)) == completed
    data
  end

  defp update_path(value, [], fun), do: fun.(value)

  defp update_path(values, [index | rest], fun) when is_integer(index) do
    List.update_at(values, index, &update_path(&1, rest, fun))
  end

  defp update_path(value, [key | rest], fun) do
    Map.update!(value, key, &update_path(&1, rest, fun))
  end
end
