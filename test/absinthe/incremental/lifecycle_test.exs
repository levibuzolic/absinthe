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

      field :required_failure, non_null(:string) do
        resolve fn _, _ -> {:error, "failed"} end
      end
    end

    query do
      field :person, :person
    end
  end

  defmodule RedactPerson do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)

      case blueprint.result do
        %{data: %{"redactedPerson" => _}} ->
          {:ok, put_in(blueprint.result.data["redactedPerson"], nil)}

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
