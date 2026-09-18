defmodule Absinthe.Incremental do
  @moduledoc """
  A transport-independent incremental GraphQL response.

  `initial_result` is ready when execution returns. Enumerating
  `subsequent_results` executes the remaining work in the consuming process.
  Halting enumeration leaves later work unexecuted. Consume the enumerable once;
  enumerating it again repeats that work.

  The response format follows GraphQL spec proposal #1110, revision
  `045e19363c2b55f127960bd3b5e8072a15b29aec`, which remains a draft.
  """

  alias Absinthe.{Phase, Pipeline}
  alias Absinthe.Incremental.{Delivery, Start, State}

  @enforce_keys [:initial_result, :subsequent_results]
  defstruct [:initial_result, :subsequent_results]

  @type t :: %__MODULE__{initial_result: map(), subsequent_results: Enumerable.t()}

  @doc false
  def run(document, schema, options) do
    modifier = options[:pipeline_modifier] || fn pipeline, _ -> pipeline end

    pipeline =
      schema
      |> Pipeline.for_document(options)
      |> Pipeline.insert_before(Phase.Document.Execution.Resolution, Start)
      |> modifier.(options)

    continuation = pipeline |> Pipeline.from(Start) |> tl() |> Pipeline.without(Phase.Telemetry)

    case Pipeline.run(document, pipeline) do
      {:ok, blueprint, _} ->
        finish(blueprint, continuation)

      {:error, error, _} ->
        {:error, error}
    end
  end

  defp finish(%{execution: %{incremental: nil}, result: result}, _), do: {:ok, result}

  defp finish(blueprint, pipeline) do
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
      subsequent = Stream.unfold(blueprint, &Delivery.next(&1, pipeline))
      {:ok, %__MODULE__{initial_result: initial, subsequent_results: subsequent}}
    else
      {:ok, blueprint.result}
    end
  end
end
