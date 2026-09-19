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
