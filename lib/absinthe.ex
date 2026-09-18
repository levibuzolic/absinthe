defmodule Absinthe do
  @moduledoc """
  Documentation for the Absinthe package, a toolkit for building GraphQL
  APIs with Elixir.

  For usage information, see [the documentation](https://hexdocs.pm/absinthe), which
  includes guides, API information for important modules, and links to useful resources.
  """

  defmodule SerializationError do
    @moduledoc """
    An error during serialization.
    """
    defexception message: "serialization failed"
  end

  defmodule ExecutionError do
    @moduledoc """
    An error during execution.
    """
    defexception message: "execution failed"
  end

  defmodule AnalysisError do
    @moduledoc """
    An error during analysis.
    """
    defexception message: "analysis failed"
  end

  @type result_selection_t :: %{
          String.t() =>
            nil
            | integer
            | float
            | boolean
            | binary
            | atom
            | result_selection_t
            | [result_selection_t]
        }

  @type result_error_t ::
          %{message: String.t()}
          | %{message: String.t(), locations: [%{line: pos_integer, column: integer}]}

  @type result_t ::
          %{data: nil | result_selection_t}
          | %{data: nil | result_selection_t, errors: [result_error_t]}
          | %{errors: [result_error_t]}

  @type pipeline_modifier_fun :: (Absinthe.Pipeline.t(), Keyword.t() -> Absinthe.Pipeline.t())

  @doc """
  Evaluates a query document against a schema, with options.

  ## Options

  * `:adapter` - The name of the adapter to use. See the `Absinthe.Adapter`
    behaviour and the `Absinthe.Adapter.Passthrough` and
    `Absinthe.Adapter.LanguageConventions` modules that implement it.
    (`Absinthe.Adapter.LanguageConventions` is the default value for this option.)
  * `:operation_name` - If more than one operation is present in the provided
    query document, this must be provided to select which operation to execute.
  * `:variables` - A map of provided variable values to be used when filling in
    arguments in the provided query document.
  * `:context` -> A map of the execution context.
  * `:root_value` -> A root value to use as the source for toplevel fields.
  * `:analyze_complexity` -> Whether to analyze the complexity before
  executing an operation.
  * `:max_complexity` -> An integer (or `:infinity`) for the maximum allowed
  complexity for the operation being executed.

  ## Examples

  ```
  \"""
  query GetItemById($id: ID) {
    item(id: $id) {
      name
    }
  }
  \"""
  |> Absinthe.run(App.Schema, variables: %{"id" => params[:item_id]})
  ```

  See the `Absinthe` module documentation for more examples.

  """
  @type run_opts :: [
          context: %{},
          adapter: Absinthe.Adapter.t(),
          root_value: term,
          operation_name: String.t(),
          analyze_complexity: boolean,
          variables: %{optional(String.t()) => any()},
          max_complexity: non_neg_integer | :infinity,
          pipeline_modifier: pipeline_modifier_fun()
        ]

  @type run_result :: {:ok, result_t} | {:error, String.t()}

  @spec run(
          binary | Absinthe.Language.Source.t() | Absinthe.Language.Document.t(),
          Absinthe.Schema.t(),
          run_opts
        ) :: run_result
  def run(document, schema, options \\ []) do
    pipeline_modifier = options[:pipeline_modifier] || (&pipeline_identity/2)

    pipeline =
      schema
      |> Absinthe.Pipeline.for_document(options)
      |> pipeline_modifier.(options)

    case Absinthe.Pipeline.run(document, pipeline) do
      {:ok, %{result: result}, _phases} ->
        {:ok, result}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end

  @doc """
  Evaluates a query document against a schema, without options.

  ## Options

  See `run/3` for the available options.
  """
  @spec run!(
          binary | Absinthe.Language.Source.t() | Absinthe.Language.Document.t(),
          Absinthe.Schema.t(),
          run_opts
        ) :: result_t | no_return
  def run!(input, schema, options \\ []) do
    case run(input, schema, options) do
      {:ok, result} -> result
      {:error, err} -> raise ExecutionError, message: err
    end
  end

  @doc """
  Evaluates a document with support for incremental delivery.

  The schema must explicitly import
  `Absinthe.Type.BuiltIns.IncrementalDirectives` to expose `@defer` and `@stream`.
  This API accepts the same options as `run/3`.

  When execution leaves deferred fields or streamed list items, the result is
  an `Absinthe.Incremental` struct. Its `initial_result` is ready to deliver;
  enumerating `subsequent_results` resolves and delivers the remaining work on
  demand. Consume that enumerable once, in the request process. Stopping
  enumeration leaves subsequent work unexecuted.

  Documents with no effective incremental work, including invalid documents,
  return the same result maps as `run/3`. `run/3` itself retains its single-result
  behavior and executes imported incremental directives eagerly.

  Incremental payloads follow the draft protocol identified in
  `Absinthe.Incremental`. A transport adapter must explicitly support these
  payloads; they are not an ordinary GraphQL response map.
  """
  @spec run_incremental(
          binary | Absinthe.Language.Source.t() | Absinthe.Language.Document.t(),
          Absinthe.Schema.t(),
          run_opts
        ) :: {:ok, result_t | Absinthe.Incremental.t()} | {:error, String.t()}
  def run_incremental(document, schema, options \\ []) do
    Absinthe.Incremental.run(document, schema, options)
  end

  @doc """
  Like `run_incremental/3`, but raises `Absinthe.ExecutionError` on a pipeline error.

  GraphQL validation and execution errors remain in the returned response,
  matching `run!/3`.
  """
  @spec run_incremental!(
          binary | Absinthe.Language.Source.t() | Absinthe.Language.Document.t(),
          Absinthe.Schema.t(),
          run_opts
        ) :: result_t | Absinthe.Incremental.t() | no_return
  def run_incremental!(document, schema, options \\ []) do
    case run_incremental(document, schema, options) do
      {:ok, result} -> result
      {:error, err} -> raise ExecutionError, message: err
    end
  end

  defp pipeline_identity(pipeline, _options), do: pipeline
end
