defmodule Absinthe.Incremental.ConsumerTest do
  use ExUnit.Case, async: true

  alias Absinthe.Case.Assertions.Incremental

  test "rejects a deferred data patch whose pending path is out of bounds" do
    result = incremental_result(%{"rows" => [%{}]}, ["rows", 3], %{"value" => 1})

    assert_raise ExUnit.AssertionError, fn -> Incremental.consume(result) end
  end

  test "rejects a deferred data patch with a negative list index" do
    result = incremental_result(%{"rows" => [%{}]}, ["rows", -1], %{"value" => 1})

    assert_raise ExUnit.AssertionError, fn -> Incremental.consume(result) end
  end

  test "applies a deferred patch at the last list index" do
    result = incremental_result(%{"rows" => [%{}]}, ["rows", 0], %{"value" => 1})

    assert {%{"rows" => [%{"value" => 1}]}, [_initial, _update]} =
             Incremental.consume(result)
  end

  test "applies a deferred patch through nested object and list paths" do
    data = %{"rows" => [%{"children" => [%{}]}]}
    result = incremental_result(data, ["rows", 0, "children", 0], %{"value" => 1})

    assert {%{"rows" => [%{"children" => [%{"value" => 1}]}]}, [_initial, _update]} =
             Incremental.consume(result)
  end

  test "rejects unexpected errors in ordinary results, initial results, patches and completions" do
    result = incremental_result(%{}, [], %{"value" => 1})
    [update] = result.subsequent_results
    [patch] = update.incremental
    [completion] = update.completed
    errors = [%{message: "unexpected", path: ["value"]}]

    for result <- [
          %{data: %{"value" => 1}, errors: errors},
          %{result | initial_result: Map.put(result.initial_result, :errors, errors)},
          %{
            result
            | subsequent_results: [%{update | incremental: [Map.put(patch, :errors, errors)]}]
          },
          %{
            result
            | subsequent_results: [%{update | completed: [Map.put(completion, :errors, errors)]}]
          }
        ] do
      assert_raise ExUnit.AssertionError, fn -> Incremental.consume(result) end
      assert {%{"value" => 1}, _} = Incremental.consume(result, expect_errors: true)
    end

    assert_raise ExUnit.AssertionError, fn ->
      Incremental.consume(result, expect_errors: true)
    end
  end

  test "rejects empty error lists even when errors are expected" do
    result = %{data: %{"value" => 1}, errors: []}

    for options <- [[], [expect_errors: true]] do
      assert_raise ExUnit.AssertionError, fn -> Incremental.consume(result, options) end
    end
  end

  defp incremental_result(data, path, fields) do
    %Absinthe.Incremental{
      initial_result: %{
        data: data,
        pending: [%{id: "deferred", path: path}],
        hasNext: true
      },
      subsequent_results: [
        %{
          incremental: [%{id: "deferred", data: fields}],
          completed: [%{id: "deferred"}],
          hasNext: false
        }
      ]
    }
  end
end
