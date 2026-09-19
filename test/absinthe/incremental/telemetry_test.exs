defmodule Absinthe.Incremental.TelemetryTest do
  use ExUnit.Case, async: false

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :value, :integer do
        resolve fn _, _ -> async(fn -> {:ok, 1} end) end
      end

      field :values, list_of(:item) do
        resolve fn _, _ -> {:ok, [1, 2, 3]} end
      end
    end

    object :item do
      field :value, :integer do
        resolve fn source, _, _ -> async(fn -> {:ok, source} end) end
      end
    end
  end

  setup_all do
    if Schema.__absinthe_schema_provider__() == Absinthe.Schema.PersistentTerm do
      start_supervised!({Absinthe.Schema.Manager, Schema})
    end

    :ok
  end

  setup %{test: test} do
    events =
      for scope <- [[:execute, :operation], [:resolve, :field]], event <- [:start, :stop] do
        [:absinthe | scope] ++ [event]
      end

    :ok = :telemetry.attach_many(test, events, &Absinthe.TestTelemetryHelper.send_to_pid/4, %{})
    on_exit(fn -> :telemetry.detach(test) end)
    :ok
  end

  test "operation telemetry ends with the initial result while field telemetry follows demand" do
    for format <- [:draft, :relay] do
      result = execute(format)
      assert_initial_events()

      Enum.to_list(result.subsequent_results)

      assert_field_events(events(), [
        ["values", 0, "value"],
        ["values", 1, "value"],
        ["values", 2, "value"]
      ])
    end
  end

  test "halting the continuation leaves later field spans unstarted" do
    for format <- [:draft, :relay] do
      result =
        Absinthe.run_incremental!(
          """
          {
            eager: value
            ... @defer(label: "first") { first: value }
            ... @defer(label: "last") { last: value }
          }
          """,
          Schema,
          incremental_format: format
        )

      assert_initial_events(["eager"])

      assert [_payload] = Enum.take(result.subsequent_results, 1)

      assert_field_events(events(), [["first"]])
    end
  end

  defp execute(format) do
    Absinthe.run_incremental!(
      """
      {
        values @stream(initialCount: 1, label: "items") {
          ... @defer(label: "details") { value }
        }
      }
      """,
      Schema,
      incremental_format: format
    )
  end

  defp assert_initial_events(path \\ ["values"]) do
    assert [
             {[:absinthe, :execute, :operation, :start], _, %{id: id}},
             field_start,
             field_stop,
             {[:absinthe, :execute, :operation, :stop], %{duration: duration}, %{id: id}}
           ] = events()

    assert duration >= 0
    assert_field_events([field_start, field_stop], [path])
  end

  defp assert_field_events(events, paths) do
    actual =
      for [start, stop] <- Enum.chunk_every(events, 2) do
        assert {[:absinthe, :resolve, :field, :start], _, %{id: id, resolution: resolution}} =
                 start

        assert {[:absinthe, :resolve, :field, :stop], %{duration: duration}, %{id: ^id}} = stop
        assert duration >= 0
        Absinthe.Resolution.path(resolution)
      end

    assert length(events) == length(paths) * 2
    assert actual == paths
  end

  defp events(acc \\ []) do
    receive do
      {:telemetry_event, {event, measurements, metadata, _}} ->
        events([{event, measurements, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
