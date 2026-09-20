defmodule Absinthe.Incremental do
  @moduledoc """
  A transport-independent incremental GraphQL response.

  `initial_result` is ready when execution returns. Enumerating
  `subsequent_results` executes the remaining work in the consuming process.
  Halting enumeration leaves later work unexecuted. Consume the enumerable once;
  enumerating it again repeats that work.

  The default `:graphql_draft` response format follows GraphQL spec proposal #1110
  at revision `045e19363c2b55f127960bd3b5e8072a15b29aec`, which remains a draft.
  It supports Apollo's `GraphQL17Alpha9Handler` (`incrementalSpec=v0.2`);
  the older `Defer20220824Handler` / `GraphQL17Alpha2Handler` format is unsupported.
  `Absinthe.run_incremental/3` also accepts `incremental_format: :relay` for
  Relay's labeled response format.
  """

  alias Absinthe.{Phase, Pipeline}
  alias Absinthe.Incremental.{Delivery, Start, State}

  @enforce_keys [:initial_result, :subsequent_results]
  defstruct [:initial_result, :subsequent_results]

  @typedoc "An incremental response with an eager result and lazy subsequent results."
  @type t :: %__MODULE__{initial_result: map(), subsequent_results: Enumerable.t()}

  @doc false
  def run(document, schema, options) do
    format = Keyword.get(options, :incremental_format, :graphql_draft)

    unless format in [:graphql_draft, :relay] do
      raise ArgumentError, "expected :incremental_format to be :graphql_draft or :relay"
    end

    modifier = options[:pipeline_modifier] || fn pipeline, _ -> pipeline end

    pipeline =
      schema
      |> Pipeline.for_document(options)
      |> Pipeline.insert_before(Phase.Document.Execution.Resolution, Start)
      |> modifier.(options)

    continuation = pipeline |> Pipeline.from(Start) |> tl() |> Pipeline.without(Phase.Telemetry)

    case Pipeline.run(document, pipeline) do
      {:ok, blueprint, _} ->
        finish(blueprint, continuation, format)

      {:error, error, _} ->
        {:error, error}
    end
  end

  defp finish(%{execution: %{incremental: nil}, result: result}, _, format),
    do: {:ok, ordinary_result(result, format)}

  defp finish(blueprint, pipeline, format) do
    state =
      Delivery.prune(
        blueprint.execution.incremental,
        [],
        blueprint.result[:data],
        blueprint.execution.result
      )

    {pending, state} = Delivery.announce(state)

    if State.pending?(state) do
      blueprint = put_in(blueprint.execution.incremental, state)
      initial = Map.merge(blueprint.result, %{pending: pending, hasNext: true})

      result =
        case format do
          :graphql_draft ->
            subsequent = Stream.unfold(blueprint, &Delivery.next(&1, pipeline))
            %__MODULE__{initial_result: initial, subsequent_results: subsequent}

          :relay ->
            Absinthe.Incremental.Relay.format(initial, blueprint, pipeline)
        end

      {:ok, result}
    else
      {:ok, ordinary_result(blueprint.result, format)}
    end
  end

  defp ordinary_result(result, :graphql_draft), do: result
  defp ordinary_result(result, :relay), do: Absinthe.Incremental.Relay.final(result)
end
