defmodule Absinthe.Incremental.RelayTest do
  use Absinthe.Case, async: true

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture
    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    scalar :opaque do
      serialize &Function.identity/1
    end

    object :person do
      field :id, :id
      field :friends, list_of(:person)
      field :opaque, :opaque
      field :later_opaque, :opaque

      field :failure, :string do
        resolve fn _, _ -> {:error, "unavailable"} end
      end

      field :required_failure, non_null(:string) do
        resolve fn _, _ -> {:error, "required value unavailable"} end
      end

      field :name, :string do
        resolve fn person, _, resolution ->
          send(resolution.context.test_pid, :resolved_name)
          {:ok, person.name}
        end
      end
    end

    query do
      field :person, :person
    end
  end

  defmodule Result do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Absinthe.Phase.Document.Result.run(blueprint, options)

      {:ok,
       %{
         blueprint
         | result:
             Map.put(blueprint.result, :extensions, %{
               :trace => "present",
               :is_final => "reserved atom",
               "is_final" => "reserved string"
             })
       }}
    end
  end

  defmodule PathlessErrors do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Absinthe.Phase.Document.Result.run(blueprint, options)

      result =
        Map.update(blueprint.result, :errors, [], fn errors ->
          [%{message: "before"} | errors] ++ [%{message: "after"}]
        end)

      {:ok, %{blueprint | result: result}}
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  test "Relay receives a labeled deferred snapshot and a separate final marker on demand" do
    query = """
    { person { id ... @defer(label: "Query$defer$Details") { name } } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1, name: "Ada"}},
               context: %{test_pid: self()}
             )

    assert result.initial_result == %{
             data: %{"person" => %{"id" => "1"}},
             hasNext: true,
             extensions: %{is_final: false}
           }

    refute_received :resolved_name

    assert Enum.to_list(result.subsequent_results) == [
             %{
               data: %{"name" => "Ada"},
               label: "Query$defer$Details",
               path: ["person"],
               hasNext: true,
               extensions: %{is_final: false}
             },
             %{data: nil, hasNext: false, extensions: %{is_final: true}}
           ]

    assert_received :resolved_name
    refute_received :resolved_name
  end

  test "independent deferred siblings emit only their selected response fields" do
    fragments =
      for index <- 1..64 do
        "... @defer(label: \"Q$defer$Details#{index}\") { selected#{index}: id }"
      end

    query = "{ person { eager: id #{Enum.join(fragments, " ")} } }"

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1}}
             )

    patches = Enum.filter(result.subsequent_results, &Map.has_key?(&1, :label))
    assert Enum.sum(Enum.map(patches, &map_size(&1.data))) == 64

    for {patch, index} <- Enum.with_index(patches, 1) do
      assert patch.data == %{"selected#{index}" => "1"}
    end
  end

  test "a deferred ancestor with no independent work is delivered before its child" do
    query = """
    { person { id ...Outer @defer(label: "Query$defer$Outer") } }
    fragment Outer on Person { id ...Inner @defer(label: "Outer$defer$Inner") }
    fragment Inner on Person { name }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1, name: "Ada"}},
               context: %{test_pid: self()}
             )

    assert [outer, inner, %{hasNext: false}] = Enum.to_list(result.subsequent_results)
    assert outer.label == "Query$defer$Outer"
    assert inner.label == "Outer$defer$Inner"
    assert outer.path == inner.path
    assert outer.data == %{"id" => "1", "name" => "Ada"}
    assert inner.data == %{"name" => "Ada"}
  end

  test "stream items use their absolute list indices after the initial prefix" do
    query = """
    { person { friends @stream(label: "Query$stream$Friends", initialCount: 1) { id name } } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{friends: [%{id: 1, name: "Ada"}, %{id: 2, name: "Grace"}]}},
               context: %{test_pid: self()}
             )

    assert result.initial_result.data == %{
             "person" => %{"friends" => [%{"id" => "1", "name" => "Ada"}]}
           }

    assert_received :resolved_name
    refute_received :resolved_name

    assert [item, %{hasNext: false}] = Enum.to_list(result.subsequent_results)
    assert item.label == "Query$stream$Friends"
    assert item.path == ["person", "friends", 1]
    assert item.data == %{"id" => "2", "name" => "Grace"}
    assert_received :resolved_name
    refute_received :resolved_name
  end

  test "errors are relative to a deferred or streamed patch's data root" do
    for query <- [
          "{ person { id ... @defer(label: \"Q$defer$Details\") { failure } } }",
          "{ person { friends @stream(label: \"Q$stream$Friends\") { id failure } } }"
        ] do
      assert {:ok, result} =
               Absinthe.run_incremental(query, Schema,
                 incremental_format: :relay,
                 root_value: %{person: %{id: 1, friends: [%{id: 2}]}}
               )

      assert [patch, %{hasNext: false}] = Enum.to_list(result.subsequent_results)
      assert patch.data["failure"] == nil
      assert [%{message: "unavailable", path: ["failure"]}] = patch.errors
    end
  end

  test "a failed boundary terminates the Relay operation without executing later work" do
    query = """
    { person {
      id
      ... @defer(label: "Q$defer$Failure") { requiredFailure }
      ... @defer(label: "Q$defer$Later") { name }
    } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1, name: "Ada"}},
               context: %{test_pid: self()}
             )

    assert [%{data: nil, errors: [error], hasNext: false, extensions: %{is_final: true}}] =
             Enum.to_list(result.subsequent_results)

    assert error.message == "required value unavailable"
    assert error.path == ["person", "requiredFailure"]
    refute_received :resolved_name
  end

  test "nullable streamed items retain their positions through a final snapshot" do
    query = """
    { person { friends @stream(label: "Q$stream$Friends", initialCount: 1) { id } } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{friends: [%{id: 1}, nil, %{id: 3}]}}
             )

    assert [heartbeat, item, final] = Enum.to_list(result.subsequent_results)
    assert heartbeat == %{data: nil, hasNext: true, extensions: %{is_final: false}}
    assert item.path == ["person", "friends", 2]
    assert item.data == %{"id" => "3"}
    assert final.hasNext == false
    assert final.extensions.is_final
    refute Map.has_key?(final, :label)
    assert final.data == %{"person" => %{"friends" => [%{"id" => "1"}, nil, %{"id" => "3"}]}}
  end

  test "result phase extensions survive formatting with Relay's final marker reserved" do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ person { id ... @defer(label: \"Q$defer$Details\") { name } } }",
               Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1, name: "Ada"}},
               context: %{test_pid: self()},
               pipeline_modifier: fn pipeline, _ ->
                 Absinthe.Pipeline.replace(pipeline, Absinthe.Phase.Document.Result, Result)
               end
             )

    assert result.initial_result.extensions == %{trace: "present", is_final: false}
    assert [patch, final] = Enum.to_list(result.subsequent_results)
    assert patch.extensions == %{trace: "present", is_final: false}
    assert final.extensions == %{trace: "present", is_final: true}
  end

  test "custom scalar structs and JSON containers survive deferred snapshots and null-slot replay" do
    date = ~D[2026-09-19]
    object = %{"dates" => [date], "details" => %{"enabled" => true}}

    root = %{
      person: %{
        opaque: date,
        later_opaque: [object],
        friends: [%{id: 1, opaque: object}, nil]
      }
    }

    query = """
    { person {
      opaque
      ... @defer(label: "Q$defer$Details") {
        laterOpaque
        friends @stream(label: "Q$stream$Friends") { id opaque }
      }
    } }
    """

    assert {:ok, eager} = Absinthe.run(query, Schema, root_value: root)

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: root
             )

    assert result.initial_result.data == %{"person" => %{"opaque" => date}}
    assert [deferred, item, final] = Enum.to_list(result.subsequent_results)

    assert deferred.data == %{
             "laterOpaque" => [object],
             "friends" => []
           }

    assert item.data == %{"id" => "1", "opaque" => object}
    assert final.data == eager.data
    assert final.extensions.is_final
  end

  test "ordinary Relay results reserve the final extension for atom and string keys" do
    assert {:ok, result} =
             Absinthe.run_incremental("{ person { id } }", Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1}},
               pipeline_modifier: fn pipeline, _ ->
                 Absinthe.Pipeline.replace(pipeline, Absinthe.Phase.Document.Result, Result)
               end
             )

    assert result.data == %{"person" => %{"id" => "1"}}
    assert result.hasNext == false
    assert result.extensions == %{trace: "present", is_final: true}
  end

  test "deferred errors follow projected fields and retain order within each list item" do
    query = """
    { person { friends { id failure ... @defer(label: "Q$defer$Details") {
      later: failure after: failure
    } } } }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{friends: Enum.map(1..256, &%{id: &1})}}
             )

    patches = Enum.filter(result.subsequent_results, &Map.has_key?(&1, :label))
    assert length(patches) == 256

    for {patch, index} <- Enum.with_index(patches) do
      assert patch.path == ["person", "friends", index]

      assert patch.data == %{"later" => nil, "after" => nil}
      assert Enum.map(patch.errors, & &1.path) == [["after"], ["later"]]
      assert Enum.all?(patch.errors, &(&1.message == "unavailable"))
    end
  end

  test "pathless result errors preserve their order among errors scoped to the deferred object" do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ person { id ... @defer(label: \"Q$defer$Details\") { failure } } }",
               Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1}},
               pipeline_modifier: fn pipeline, _ ->
                 Absinthe.Pipeline.replace(
                   pipeline,
                   Absinthe.Phase.Document.Result,
                   PathlessErrors
                 )
               end
             )

    assert [patch, %{hasNext: false}] = Enum.to_list(result.subsequent_results)

    assert [
             %{message: "before"},
             %{message: "unavailable", path: ["failure"]},
             %{message: "after"}
           ] =
             patch.errors
  end

  test "halting within a batch of deferred snapshots leaves later resolvers unexecuted" do
    query = """
    { person {
      id
      ...Outer @defer(label: "Q$defer$Outer")
      ... @defer(label: "Q$defer$Later") { name }
    } }
    fragment Outer on Person { id ...Inner @defer(label: "Outer$defer$Inner") }
    fragment Inner on Person { failure }
    """

    assert {:ok, result} =
             Absinthe.run_incremental(query, Schema,
               incremental_format: :relay,
               root_value: %{person: %{id: 1, name: "Ada"}},
               context: %{test_pid: self()}
             )

    assert [%{label: "Q$defer$Outer"}] = Enum.take(result.subsequent_results, 1)
    refute_received :resolved_name
  end
end
