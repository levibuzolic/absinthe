defmodule Absinthe.Fixtures.MutationOrder do
  use Absinthe.Schema.Notation

  import Absinthe.Resolution.Helpers

  defmodule PassBarrier do
    @behaviour Absinthe.Plugin

    def before_resolution(execution) do
      execution = update_in(execution.context[:passes], &((&1 || 0) + 1))
      send(execution.context.test_pid, {:plugin_pass, :before, execution.context.passes})
      execution
    end

    def after_resolution(execution) do
      send(execution.context.test_pid, {:plugin_pass, :after, execution.context.passes})

      if execution.acc[__MODULE__] do
        execution
      else
        send(execution.context.test_pid, {:first_resolution_pass_finished, self()})
        put_in(execution.acc[__MODULE__], true)
      end
    end

    def pipeline(pipeline, execution) do
      if execution.context[:disable_resume_callbacks] && execution.pending != [] &&
           not Map.has_key?(execution.context, :completed) do
        [{Absinthe.Phase.Document.Execution.Resolution, plugin_callbacks: false}]
      else
        pipeline
      end
    end
  end

  def plugins,
    do: [Absinthe.Middleware.Dataloader | Absinthe.Plugin.defaults()] ++ [PassBarrier]

  def context(context) do
    loader = Dataloader.new() |> Dataloader.add_source(:ids, Dataloader.KV.new(&load_ids/2))
    Map.put(context, :loader, loader)
  end

  def load_ids(:id, ids), do: Map.new(ids, &{&1, &1})
  def batch_ids(_, ids), do: Map.new(ids, &{&1, &1})

  def first(_, args, %{context: %{test_pid: pid}}) do
    send(pid, {:mutation_trace, :first_started})

    async(fn ->
      wait_for_release(pid)
      if args.fail, do: {:error, "failed"}, else: {:ok, %{id: 1}}
    end)
  end

  def completed(resolution, _) do
    send(resolution.context.test_pid, {:mutation_trace, :first_completed})
    put_in(resolution.context[:completed], "first completed")
  end

  defp wait_for_release(pid) do
    send(pid, {:first_waiting, self()})

    receive do
      :release_first -> :ok
    after
      5_000 -> raise "test did not release the first mutation"
    end
  end

  object :person do
    field :id, :integer
    field :observed, :string
    field :passes, :integer

    field :delayed_id, non_null(:integer) do
      arg :fail, :boolean, default_value: false

      resolve fn %{id: id}, args, %{context: %{test_pid: pid}} ->
        async(fn ->
          wait_for_release(pid)
          async(fn -> if args.fail, do: {:error, "child failed"}, else: {:ok, id} end)
        end)
      end

      middleware fn resolution, _ ->
        send(resolution.context.test_pid, {:mutation_trace, :child_completed})
        resolution
      end
    end

    field :batch_id, :integer do
      resolve fn %{id: id}, _, %{context: %{test_pid: pid}} ->
        batch({__MODULE__, :batch_ids, nil}, id, fn values ->
          send(pid, {:mutation_trace, :batch_completed})
          {:ok, Map.fetch!(values, id)}
        end)
      end
    end

    field :loaded_id, :integer do
      resolve fn %{id: id}, _, %{context: %{loader: loader, test_pid: pid}} ->
        loader
        |> Dataloader.load(:ids, :id, id)
        |> on_load(fn loader ->
          send(pid, {:mutation_trace, :loader_completed})
          {:ok, Dataloader.get(loader, :ids, :id, id)}
        end)
      end
    end

    field :name, :string do
      resolve fn _, _, %{context: %{test_pid: pid}} ->
        send(pid, {:mutation_trace, :name})
        {:ok, "Ada"}
      end
    end
  end

  object :mutation_order_queries do
    field :person, :person
  end

  object :mutation_order_mutations do
    field :first, :person do
      arg :fail, :boolean, default_value: false
      resolve &__MODULE__.first/3
      middleware &__MODULE__.completed/2
    end

    field :required_first, non_null(:person) do
      arg :fail, :boolean, default_value: false
      resolve &__MODULE__.first/3
      middleware &__MODULE__.completed/2
    end

    field :immediate, :person do
      resolve fn _, _, %{context: %{test_pid: pid}} ->
        send(pid, {:mutation_trace, :first_started})
        {:ok, %{id: 1}}
      end
    end

    field :required_immediate, non_null(:person) do
      resolve fn _, _, %{context: %{test_pid: pid}} ->
        send(pid, {:mutation_trace, :first_started})
        {:ok, %{id: 1}}
      end
    end

    field :immediate_failure, non_null(:person) do
      resolve fn _, _, %{context: %{test_pid: pid}} ->
        send(pid, {:mutation_trace, :first_started})
        {:error, "failed"}
      end
    end

    field :second_batch, :person do
      resolve fn _, _, %{context: %{test_pid: pid} = context} ->
        send(pid, {:mutation_trace, :second_started})

        batch({__MODULE__, :batch_ids, nil}, 2, fn values ->
          send(pid, {:mutation_trace, :second_completed})
          {:ok, %{id: Map.fetch!(values, 2), passes: context[:passes]}}
        end)
      end
    end

    field :second_loaded, :person do
      resolve fn _, _, %{context: %{test_pid: pid, loader: loader} = context} ->
        send(pid, {:mutation_trace, :second_started})

        loader
        |> Dataloader.load(:ids, :id, 2)
        |> on_load(fn loader ->
          send(pid, {:mutation_trace, :second_completed})
          {:ok, %{id: Dataloader.get(loader, :ids, :id, 2), passes: context[:passes]}}
        end)
      end
    end

    field :second_async, :person do
      resolve fn _, _, %{context: %{test_pid: pid} = context} ->
        send(pid, {:mutation_trace, :second_started})
        async(fn -> {:ok, %{id: 2, passes: context[:passes]}} end)
      end

      middleware fn resolution, _ ->
        send(resolution.context.test_pid, {:mutation_trace, :second_completed})
        resolution
      end
    end

    field :second, :person do
      resolve fn _, _, %{context: %{test_pid: pid} = context} ->
        send(pid, {:mutation_trace, :second_started})
        {:ok, %{id: 2, observed: context[:completed], passes: context[:passes]}}
      end
    end
  end
end
