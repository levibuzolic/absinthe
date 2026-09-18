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

  alias Absinthe.Incremental.Delivery

  @enforce_keys [:initial_result, :subsequent_results]
  defstruct [:initial_result, :subsequent_results]

  @type t :: %__MODULE__{initial_result: map(), subsequent_results: Enumerable.t()}

  @doc false
  def run(document, schema, options) do
    options = Keyword.put(options, :incremental, true)
    modifier = options[:pipeline_modifier] || fn pipeline, _ -> pipeline end

    pipeline =
      schema
      |> Absinthe.Pipeline.for_document(options)
      |> Absinthe.Pipeline.insert_before(
        Absinthe.Phase.Document.Execution.Resolution,
        Absinthe.Incremental.Start
      )
      |> modifier.(options)

    case Absinthe.Pipeline.run(document, pipeline) do
      {:ok, blueprint, _} ->
        finish(blueprint, continuation(pipeline))

      {:error, error, _} ->
        {:error, error}
    end
  end

  defp finish(%{execution: %{incremental: nil}, result: result}, _), do: {:ok, result}

  defp finish(blueprint, pipeline) do
    state = Delivery.prune(blueprint.execution.incremental, [], blueprint.result[:data])
    {pending, state} = Delivery.announce(state)

    if state.jobs == [] do
      {:ok, blueprint.result}
    else
      blueprint = put_in(blueprint.execution.incremental, state)
      initial = Map.merge(blueprint.result, %{pending: pending, hasNext: true})
      subsequent = Stream.unfold(blueprint, &Delivery.next(&1, pipeline))
      {:ok, %__MODULE__{initial_result: initial, subsequent_results: subsequent}}
    end
  end

  defp continuation(pipeline) do
    pipeline
    |> List.flatten()
    |> Enum.drop_while(&(phase_module(&1) != Absinthe.Incremental.Start))
    |> Enum.drop(1)
    |> Enum.reject(&(phase_module(&1) == Absinthe.Phase.Telemetry))
  end

  defp phase_module({module, _options}), do: module
  defp phase_module(module), do: module
end
