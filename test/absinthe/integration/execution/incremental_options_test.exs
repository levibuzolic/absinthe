defmodule Absinthe.Integration.Execution.IncrementalOptionsTest do
  use Absinthe.Case, async: true

  alias Absinthe.{Phase, Pipeline}

  defmodule RenamingAdapter do
    use Absinthe.Adapter

    def to_internal_name("later", :directive), do: "defer"
    def to_internal_name("chunks", :directive), do: "stream"
    def to_internal_name("enabled", :argument), do: "if"
    def to_internal_name("tag", :argument), do: "label"
    def to_internal_name("first", :argument), do: "initial_count"

    def to_internal_name(name, role),
      do: Absinthe.Adapter.LanguageConventions.to_internal_name(name, role)

    def to_external_name(name, role),
      do: Absinthe.Adapter.LanguageConventions.to_external_name(name, role)
  end

  defmodule Schema do
    use Absinthe.Schema
    use Absinthe.Fixture

    import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

    query do
      field :numbers, list_of(:integer) do
        resolve fn _, _ -> {:ok, [1, 2]} end
      end

      field :failure, :string do
        resolve fn _, _ -> {:error, %{message: "unavailable", code: "OFFLINE"}} end
      end

      field :required_failure, non_null(:string) do
        resolve fn _, _ -> {:error, %{message: "unavailable", code: "OFFLINE"}} end
      end

      field :required_numbers, list_of(non_null(:integer)) do
        resolve fn _, _ -> {:ok, [1, nil, 3]} end
      end

      field :value, :string do
        resolve fn _, resolution ->
          send(resolution.context.test_pid, {:resolved, resolution.definition.alias})
          {:ok, "value"}
        end
      end
    end
  end

  defmodule CustomResult do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)

      result =
        Map.update(blueprint.result, :errors, [], fn errors ->
          Enum.map(errors, &Map.update!(&1, :message, fn message -> "custom: " <> message end))
        end)

      {:ok, %{blueprint | result: Map.put(result, :extensions, %{custom: true})}}
    end
  end

  defmodule TraceResolution do
    use Absinthe.Phase

    def run(blueprint, options) do
      send(
        blueprint.execution.context.test_pid,
        {:resolution_phase, Keyword.fetch!(options, :tag)}
      )

      Phase.Document.Execution.Resolution.run(blueprint, options)
    end
  end

  defmodule ExtensionResult do
    use Absinthe.Phase

    def run(blueprint, options) do
      {:ok, blueprint} = Phase.Document.Result.run(blueprint, options)
      {:ok, put_in(blueprint.result[:extensions], %{formatted: true})}
    end
  end

  defmodule RejectExecution do
    use Absinthe.Phase

    def run(blueprint, options) do
      if options[:initial] || blueprint.execution.incremental.frame do
        {:error, "execution rejected"}
      else
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

  test "initial pipeline errors retain the public return and raising contracts" do
    options = [
      context: %{test_pid: self()},
      pipeline_modifier: fn pipeline, _ ->
        Pipeline.insert_after(
          pipeline,
          Absinthe.Incremental.Start,
          {RejectExecution, initial: true}
        )
      end
    ]

    query = "{ value ... @defer { later: value } }"
    assert {:error, "execution rejected"} = Absinthe.run_incremental(query, Schema, options)

    assert_raise Absinthe.ExecutionError, "execution rejected", fn ->
      Absinthe.run_incremental!(query, Schema, options)
    end

    refute_received {:resolved, _}
  end

  test "a failed continuation raises without starting later resolvers" do
    assert {:ok, result} =
             Absinthe.run_incremental(
               "{ ready: value ... @defer { first: value } ... @defer { second: value } }",
               Schema,
               context: %{test_pid: self()},
               pipeline_modifier: fn pipeline, _ ->
                 Pipeline.insert_after(pipeline, Absinthe.Incremental.Start, RejectExecution)
               end
             )

    assert result.initial_result.data == %{"ready" => "value"}
    assert_received {:resolved, "ready"}

    assert_raise Absinthe.ExecutionError, ~s("execution rejected"), fn ->
      Enum.to_list(result.subsequent_results)
    end

    refute_received {:resolved, _}
  end

  test "custom formatting survives failed defer and stream completion packets" do
    for {query, path, message} <- [
          {"{ ... @defer { requiredFailure } }", ["requiredFailure"], "unavailable"},
          {"{ requiredNumbers @stream(initialCount: 1) }", ["requiredNumbers", 1],
           "Cannot return null for non-nullable field"}
        ] do
      assert {:ok, result} =
               Absinthe.run_incremental(query, Schema,
                 pipeline_modifier: fn pipeline, _ ->
                   Pipeline.replace(pipeline, Phase.Document.Result, CustomResult)
                 end
               )

      assert result.initial_result.extensions == %{custom: true}

      assert [
               %{
                 completed: [%{errors: [error]}],
                 extensions: %{custom: true},
                 hasNext: false
               }
             ] = Enum.to_list(result.subsequent_results)

      assert error.path == path
      assert error.message =~ "custom: " <> message
    end
  end

  test "result phase options from a pipeline modifier also format deferred errors" do
    modifier = fn pipeline, _ ->
      Pipeline.replace(
        pipeline,
        Phase.Document.Result,
        {Phase.Document.Result, spec_compliant_errors: true}
      )
    end

    assert {:ok, result} =
             Absinthe.run_incremental("{ failure ... @defer { delayed: failure } }", Schema,
               pipeline_modifier: modifier
             )

    assert [%{extensions: %{code: "OFFLINE"}}] = result.initial_result.errors

    assert [%{incremental: [%{errors: [%{extensions: %{code: "OFFLINE"}}]}]}] =
             Enum.to_list(result.subsequent_results)
  end

  test "custom result phases format both initial and deferred errors" do
    modifier = fn pipeline, _ ->
      Pipeline.replace(pipeline, Phase.Document.Result, CustomResult)
    end

    assert {:ok, result} =
             Absinthe.run_incremental("{ failure ... @defer { delayed: failure } }", Schema,
               pipeline_modifier: modifier
             )

    assert [%{message: "custom: unavailable"}] = result.initial_result.errors

    assert [%{incremental: [%{errors: [%{message: "custom: unavailable"}]}]}] =
             Enum.to_list(result.subsequent_results)
  end

  test "custom result extensions reach initial, deferred, and streamed payloads" do
    modifier = fn pipeline, _ ->
      Pipeline.replace(pipeline, Phase.Document.Result, ExtensionResult)
    end

    assert {:ok, result} =
             Absinthe.run_incremental("{ numbers @stream ... @defer { failure } }", Schema,
               pipeline_modifier: modifier
             )

    assert result.initial_result.extensions == %{formatted: true}
    payloads = Enum.to_list(result.subsequent_results)
    assert Enum.all?(payloads, &(&1.extensions == %{formatted: true}))

    assert Enum.any?(
             payloads,
             &Enum.any?(&1.incremental, fn entry -> Map.has_key?(entry, :items) end)
           )

    assert Enum.any?(
             payloads,
             &Enum.any?(&1.incremental, fn entry -> Map.has_key?(entry, :data) end)
           )
  end

  test "adapter-normalized directive and argument identities control execution" do
    query = """
    {
      numbers @chunks(first: 1, tag: "numbers")
      ... @later(enabled: true, tag: "details") { failure }
    }
    """

    assert {:ok, result} = Absinthe.run_incremental(query, Schema, adapter: RenamingAdapter)
    assert result.initial_result.data == %{"numbers" => [1]}

    assert Enum.sort(Enum.map(result.initial_result.pending, & &1.label)) == [
             "details",
             "numbers"
           ]

    payloads = Enum.to_list(result.subsequent_results)
    assert Enum.any?(payloads, &Enum.any?(&1.incremental, fn entry -> entry[:items] == [2] end))

    assert Enum.any?(
             payloads,
             &Enum.any?(&1.incremental, fn entry -> entry[:data] == %{"failure" => nil} end)
           )

    assert {:ok, %{data: %{"numbers" => [1, 2]}}} =
             Absinthe.run_incremental("{ numbers @chunks(enabled: false) }", Schema,
               adapter: RenamingAdapter
             )
  end

  test "replacement resolution phases also run for deferred work with their configured options" do
    modifier = fn pipeline, _ ->
      Pipeline.replace(
        pipeline,
        Phase.Document.Execution.Resolution,
        {TraceResolution, tag: :configured}
      )
    end

    assert {:ok, result} =
             Absinthe.run_incremental("{ value ... @defer { delayed: value } }", Schema,
               context: %{test_pid: self()},
               pipeline_modifier: modifier
             )

    assert_received {:resolution_phase, :configured}
    assert_received {:resolved, nil}
    refute_received {:resolved, "delayed"}

    assert [%{incremental: [%{data: %{"delayed" => "value"}}]}] =
             Enum.to_list(result.subsequent_results)

    assert_received {:resolution_phase, :configured}
    assert_received {:resolved, "delayed"}
  end

  test "removing the incremental boundary fails before executing any resolver" do
    modifier = fn pipeline, _ -> Pipeline.without(pipeline, Absinthe.Incremental.Start) end

    assert_raise RuntimeError, "Could not find phase Elixir.Absinthe.Incremental.Start", fn ->
      Absinthe.run_incremental("{ value ... @defer { delayed: value } }", Schema,
        context: %{test_pid: self()},
        pipeline_modifier: modifier
      )
    end

    refute_received {:resolved, _}
  end
end
