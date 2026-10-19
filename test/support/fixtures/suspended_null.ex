defmodule Absinthe.Fixtures.SuspendedNull do
  use Absinthe.Schema.Notation
  import Absinthe.Resolution.Helpers

  object :node do
    field :required, non_null(:string) do
      resolve fn _, _ -> {:error, "required failed"} end
    end

    field :delayed, :string do
      resolve fn _, _, resolution ->
        %{mode: mode, pid: pid} = resolution.context
        path = Absinthe.Resolution.path(resolution)

        result = fn ->
          send(pid, {:completed, path})
          {:error, "delayed failed"}
        end

        case mode do
          :sync -> result.()
          :async -> async(result)
          :batch -> batch({__MODULE__, :batch}, path, fn _ -> result.() end)
        end
      end
    end

    field :child, non_null(:node)
  end

  object :suspended_null_queries do
    field :node, :node
    field :nodes, list_of(non_null(:node))
    field :matrix, list_of(non_null(list_of(non_null(:node))))

    field :outside, :string do
      resolve fn _, _ -> async(fn -> {:ok, "ready"} end) end
    end
  end

  def batch(_, _), do: :ok
end
