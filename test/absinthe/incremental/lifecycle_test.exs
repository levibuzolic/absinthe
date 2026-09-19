defmodule Absinthe.Incremental.LifecycleTest do
  use Absinthe.Case, async: true

  alias Absinthe.{Phase, Pipeline}
  alias Absinthe.Case.Assertions.Incremental

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    object :person do
      field :name, :string
      field :age, :integer
      field :other, :string
      field :child, :person

      field :children, list_of(:person) do
        resolve fn source, _, resolution ->
          send(
            resolution.context.test_pid,
            {:children_resolved, Absinthe.Resolution.path(resolution)}
          )

          {:ok, source.children}
        end
      end

      field :observed_name, :string do
        resolve fn source, _, %{context: %{test_pid: pid}} ->
          send(pid, :observed_name_resolved)
          {:ok, source.name}
        end
      end

      field :required_failure, non_null(:string) do
        resolve fn _, _ -> {:error, "failed"} end
      end
    end

    query do
      field :person, :person
      field :people, list_of(:person)
    end
  end

  defmodule RedactPerson do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)

      case blueprint.result do
        %{data: %{"redactedPerson" => _}} ->
          {:ok, put_in(blueprint.result.data["redactedPerson"], nil)}

        %{data: %{"redactedPeople" => _}} ->
          {:ok, put_in(blueprint.result.data["redactedPeople"], nil)}

        _ ->
          {:ok, blueprint}
      end
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    {:ok, options: [root_value: %{person: %{name: "Ada", age: 37, other: "ready"}}]}
  end

  test "an unannounced child publishes its buffered data when its independent owner later fails",
       %{
         options: options
       } do
    query = """
    { person {
      ... @defer(label: "a") {
        ... @defer(label: "child") { name }
        age
      }
      ... @defer(label: "b") { name requiredFailure }
    } }
    """

    assert_surviving_child(query, options)
  end

  test "finishing the final queued frame publishes and completes an already executed child", %{
    options: options
  } do
    query = """
    { person {
      ... @defer(label: "b") { name requiredFailure }
      ... @defer(label: "a") {
        ... @defer(label: "child") { name }
        age
      }
    } }
    """

    assert_surviving_child(query, options)
  end

  test "shared buffered child data publishes once when all owners succeed", %{options: options} do
    parent = "... @defer(label: \"a\") { ... @defer(label: \"child\") { name } age }"
    independent = "... @defer(label: \"b\") { name other }"

    for selections <- [[parent, independent], [independent, parent]] do
      query = "{ person { #{Enum.join(selections, " ")} } }"
      assert {:ok, %{data: expected}} = Absinthe.run(query, Schema, options)
      assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
      assert {^expected, payloads} = Incremental.consume(result)

      for payload <- payloads, completion <- Map.get(payload, :completed, []) do
        refute Map.has_key?(completion, :errors)
      end
    end
  end

  test "an initially empty group is announced when a shared object's children add work" do
    inner_fields = """
      child { observedName ... @defer(label: "innerLeaf") { age name } }
    """

    outer = "child { name ... @defer(label: \"outerLeaf\") { age name } }"
    child = %{name: "Ada", age: 37}
    expected = %{"child" => %{"name" => "Ada", "age" => 37, "observedName" => "Ada"}}

    for {inner, fragment} <- [
          {"... @defer(label: \"inner\") { #{inner_fields} }", ""},
          {"...Inner @defer(label: \"inner\")", "fragment Inner on Person { #{inner_fields} }"}
        ],
        selections <- [[inner, outer], [outer, inner]],
        {field, source, data, calls} <- [
          {"entry: person", %{child: child}, %{"entry" => expected}, 1},
          {"entries: people", [%{child: child}, %{child: child}],
           %{"entries" => [expected, expected]}, 2},
          {"entries: people @stream(initialCount: 1)", [%{child: child}, %{child: nil}],
           %{"entries" => [expected, %{"child" => nil}]}, 1}
        ] do
      query = """
      { #{field} { ... @defer(label: "outer") { #{Enum.join(selections, " ")} } } }
      #{fragment}
      """

      assert {:ok, result} =
               Absinthe.run_incremental(query, Schema,
                 root_value: %{person: source, people: source},
                 context: %{test_pid: self()}
               )

      refute_received :observed_name_resolved
      assert {^data, payloads} = Incremental.consume(result)

      assert calls ==
               Enum.count(Enum.flat_map(payloads, &Map.get(&1, :pending, [])), fn notice ->
                 notice[:label] == "inner"
               end)

      for _ <- 1..calls, do: assert_received(:observed_name_resolved)
      refute_received :observed_name_resolved
    end
  end

  test "a failure cancels work added to an initially empty group before announcing it" do
    query = """
    { person {
      ... @defer(label: "outer") {
        ... @defer(label: "inner") { child { observedName } }
        child { name requiredFailure }
      }
    } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               root_value: %{person: %{child: %{name: "Ada"}}},
               context: %{test_pid: self()}
             )

    assert {%{"person" => %{"child" => nil}}, payloads} = Incremental.consume(result)
    assert [%{label: "outer"}] = Enum.flat_map(payloads, &Map.get(&1, :pending, []))

    assert [%{message: "failed", path: ["person", "child", "requiredFailure"]}] =
             for(
               payload <- payloads,
               entry <- Map.get(payload, :incremental, []),
               error <- Map.get(entry, :errors, []),
               do: Map.take(error, [:message, :path])
             )

    refute_received :observed_name_resolved
  end

  test "overlapping sibling defers over nested lists keep one pending ID per fragment" do
    fragments = %{
      "A" => "branches: children { leaves: children { observedName } }",
      "B" => "branches: children { name leaves: children { observedName age } }",
      "C" => "branches: children { age leaves: children { observedName other } }"
    }

    for width <- [1, 4, 12],
        labels <- [["A", "B"], ["A", "B", "C"]],
        order <- [labels, Enum.reverse(labels)] do
      branches =
        for branch <- 1..width do
          %{
            name: "Branch #{branch}",
            age: branch,
            children:
              for leaf <- 1..2 do
                %{name: "Leaf #{branch}:#{leaf}", age: leaf, other: "ready"}
              end
          }
        end

      selections = Enum.map_join(order, " ", &"...#{&1} @defer(label: \"#{&1}\")")

      definitions =
        Enum.map_join(labels, " ", &"fragment #{&1} on Person { #{Map.fetch!(fragments, &1)} }")

      query = "{ entry: person { name #{selections} } } #{definitions}"

      assert {:ok, result} =
               Absinthe.run_incremental(query, Schema,
                 root_value: %{person: %{name: "Ada", children: branches}},
                 context: %{test_pid: self()}
               )

      assert result.initial_result.data == %{"entry" => %{"name" => "Ada"}}
      refute_received {:children_resolved, _}
      refute_received :observed_name_resolved

      expected_branches =
        for branch <- branches do
          leaves =
            for leaf <- branch.children do
              data = %{"observedName" => leaf.name, "age" => leaf.age}
              if "C" in labels, do: Map.put(data, "other", leaf.other), else: data
            end

          data = %{"name" => branch.name, "leaves" => leaves}
          if "C" in labels, do: Map.put(data, "age", branch.age), else: data
        end

      assert {%{"entry" => %{"name" => "Ada", "branches" => ^expected_branches}}, payloads} =
               Incremental.consume(result)

      assert Enum.sort(
               for payload <- payloads,
                   notice <- Map.get(payload, :pending, []),
                   do: {notice.path, notice.label}
             ) == Enum.map(labels, &{["entry"], &1})

      assert_received {:children_resolved, ["entry", "branches"]}

      for index <- 0..(width - 1) do
        assert_received {:children_resolved, ["entry", "branches", ^index, "leaves"]}
        assert_received :observed_name_resolved
        assert_received :observed_name_resolved
      end

      refute_received {:children_resolved, _}
      refute_received :observed_name_resolved
    end
  end

  test "initial formatter redaction prunes deferred children without execution errors", %{
    options: options
  } do
    options = with_redaction(options)

    assert {:ok, %{data: %{"redactedPerson" => nil}}} =
             Absinthe.run_incremental(
               "{ redactedPerson: person { age ... @defer { name } } }",
               Schema,
               options
             )
  end

  test "deferred formatter redaction cancels children before announcing their work", %{
    options: options
  } do
    query = """
    { ... @defer(label: "outer") {
      redactedPerson: person { age ... @defer(label: "inner") { name } }
    } }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, with_redaction(options))
    assert {%{"redactedPerson" => nil}, payloads} = Incremental.consume(result)

    assert [%{label: "outer"}] = Enum.flat_map(payloads, &Map.get(&1, :pending, []))

    for payload <- payloads, completion <- Map.get(payload, :completed, []) do
      refute Map.has_key?(completion, :errors)
    end
  end

  test "redacting a list cancels its stream tail and deferred items during initial and later work" do
    selection = """
    redactedPeople: people @stream(initialCount: 1, label: "tail") {
      name
      ... @defer(label: "item") { observedName }
    }
    """

    for {fields, labels} <- [
          {selection, ["survives"]},
          {"... @defer(label: \"outer\") { #{selection} }", ["outer", "survives"]}
        ] do
      query = "{ #{fields} ... @defer(label: \"survives\") { person { name } } }"

      assert {:ok, result} =
               Absinthe.run_incremental(
                 query,
                 Schema,
                 with_redaction(
                   context: %{test_pid: self()},
                   root_value: %{
                     people: [%{name: "Ada"}, %{name: "Grace"}],
                     person: %{name: "Lin"}
                   }
                 )
               )

      assert {%{"redactedPeople" => nil, "person" => %{"name" => "Lin"}}, payloads} =
               Incremental.consume(result)

      assert labels ==
               Enum.sort(
                 for payload <- payloads,
                     notice <- Map.get(payload, :pending, []),
                     do: notice.label
               )

      refute_received :observed_name_resolved
    end
  end

  defp with_redaction(options) do
    Keyword.put(options, :pipeline_modifier, fn pipeline, _ ->
      Pipeline.replace(pipeline, Phase.Document.Result, RedactPerson)
    end)
  end

  defp assert_surviving_child(query, options) do
    assert {:ok, result} = Absinthe.run_incremental(query, Schema, options)
    assert {data, payloads} = Incremental.consume(result)
    assert data == %{"person" => %{"name" => "Ada", "age" => 37}}

    %{id: failed_id} = Enum.find(result.initial_result.pending, &(&1.label == "b"))

    assert [%{id: ^failed_id, errors: [%{message: "failed", path: path}]}] =
             for(
               payload <- payloads,
               completion <- Map.get(payload, :completed, []),
               Map.has_key?(completion, :errors),
               do: completion
             )

    assert path == ["person", "requiredFailure"]
  end
end
