defmodule Absinthe.Incremental.Start do
  @moduledoc false
  use Absinthe.Phase

  alias Absinthe.Blueprint
  alias Absinthe.Incremental.Directives

  # A stable boundary lets pipeline modifiers replace the resolution phase
  # without losing the configured execution and result phases on later pulls.
  def run(blueprint, _options) do
    {blueprint, _} = Blueprint.prewalk(blueprint, 0, &identify_directive/2)
    {:ok, put_in(blueprint.execution.incremental, %Absinthe.Incremental.State{})}
  end

  # Source locations are diagnostic metadata, so each original occurrence needs
  # its own identity before fragment reuse and field merging copy the directive.
  defp identify_directive(%Blueprint.Directive{} = directive, id) do
    if Directives.identifier(directive) == :defer do
      private = Keyword.put(directive.__private__, :__absinthe_incremental_id, id)
      {:halt, %{directive | __private__: private}, id + 1}
    else
      {:halt, directive, id}
    end
  end

  defp identify_directive(node, id), do: {node, id}
end
