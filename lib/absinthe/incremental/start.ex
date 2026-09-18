defmodule Absinthe.Incremental.Start do
  @moduledoc false
  use Absinthe.Phase

  # A stable boundary lets pipeline modifiers replace the resolution phase
  # without losing the configured execution and result phases on later pulls.
  def run(blueprint, _options) do
    {:ok, put_in(blueprint.execution.incremental, %Absinthe.Incremental.State{})}
  end
end
