defmodule Absinthe.Integration.Execution.IncrementalCoercionTest do
  use Absinthe.Case, async: false

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :numbers, list_of(:integer)
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup do
    {:ok, options: [root_value: %{numbers: [1, 2, 3]}]}
  end

  test "accepts variable and default initial counts", %{options: options} do
    variable_query = """
    query($count: Int!) {
      numbers @stream(initialCount: $count)
    }
    """

    assert {:ok, %{data: %{"numbers" => [1, 2, 3]}}} =
             Absinthe.run(
               variable_query,
               Schema,
               Keyword.put(options, :variables, %{"count" => 2})
             )

    assert {:ok, result} =
             Absinthe.run_incremental(
               variable_query,
               Schema,
               Keyword.put(options, :variables, %{"count" => 2})
             )

    assert result.initial_result.data == %{"numbers" => [1, 2]}

    assert [%{incremental: [%{items: [3]}], hasNext: false}] =
             Enum.to_list(result.subsequent_results)

    default_query = """
    query($count: Int = 1) {
      numbers @stream(initialCount: $count)
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(default_query, Schema, options)
    assert result.initial_result.data == %{"numbers" => [1]}

    assert {:ok, result} = Absinthe.run_incremental("{ numbers @stream }", Schema, options)
    assert result.initial_result.data == %{"numbers" => []}

    omitted_query = "query($count: Int) { numbers @stream(initialCount: $count) }"
    assert {:ok, result} = Absinthe.run_incremental(omitted_query, Schema, options)
    assert result.initial_result.data == %{"numbers" => []}
  end

  test "rejects null and wrong initialCount values through both APIs", %{options: options} do
    assert_error_contains(
      "{ numbers @stream(initialCount: null) }",
      "initialCount",
      options
    )

    assert_error_contains(
      "{ numbers @stream(initialCount: \"one\") }",
      "initialCount",
      options
    )

    null_variable_query = """
    query($count: Int) {
      numbers @stream(initialCount: $count)
    }
    """

    assert_error_contains(
      null_variable_query,
      "initialCount",
      Keyword.put(options, :variables, %{"count" => nil})
    )

    wrong_variable_query = """
    query($count: String!) {
      numbers @stream(initialCount: $count)
    }
    """

    assert_error_contains(
      wrong_variable_query,
      "String!",
      Keyword.put(options, :variables, %{"count" => "one"})
    )
  end

  test "rejects null defer conditions through both APIs", %{options: options} do
    query = "{ ... @defer(if: null) { numbers } }"
    assert_error_contains(query, "if", options)
  end

  test "rejects invalid placement through both APIs", %{options: options} do
    assert_error_contains("{ numbers @defer }", "may not be used on FIELD", options)

    assert_error_contains(
      "{ ... @stream { numbers } }",
      "may not be used on INLINE_FRAGMENT",
      options
    )
  end

  test "rejects repeated incremental directives through both APIs", %{options: options} do
    assert_error_contains("{ numbers @stream @stream }", "cannot be applied repeatedly", options)

    assert_error_contains(
      "{ ... @defer @defer { numbers } }",
      "cannot be applied repeatedly",
      options
    )
  end

  defp assert_error_contains(query, fragment, options) do
    for run <- [&Absinthe.run/3, &Absinthe.run_incremental/3] do
      assert {:ok, %{errors: errors}} = run.(query, Schema, options)

      assert Enum.any?(errors, fn %{message: message} ->
               String.contains?(message, fragment)
             end),
             "expected an error containing #{inspect(fragment)}, got: #{inspect(errors)}"
    end
  end
end
