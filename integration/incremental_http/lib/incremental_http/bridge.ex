defmodule IncrementalHTTP.Bridge do
  @moduledoc false

  defmodule RelayExtensions do
    use Absinthe.Phase

    # Probe mixed atom/string extension keys before Relay formatting and JSON encoding.
    def run(blueprint, options) do
      {:ok, blueprint} = Absinthe.Phase.Document.Result.run(blueprint, options)

      extensions = %{
        :is_final => "reserved atom",
        "is_final" => "reserved string",
        "trace" => "preserved"
      }

      {:ok, put_in(blueprint.result[:extensions], extensions)}
    end
  end

  # A test-only JSON-lines bridge. Each request owns its enumerable and executes
  # in one monitored process; no GraphQL payload fields are rewritten here.
  def run do
    controller = self()

    spawn_link(fn ->
      IO.stream(:stdio, :line)
      |> Enum.each(&send(controller, {:command, Jason.decode!(&1)}))

      send(controller, :eof)
    end)

    event(nil, %{event: "ready"})
    control(%{})
  end

  def event(id, value), do: IO.puts(Jason.encode!(Map.put(value, :id, id)))

  defp control(workers) do
    receive do
      {:command, %{"command" => "start", "id" => id} = request} ->
        {pid, ref} = spawn_monitor(fn -> execute(request) end)
        control(Map.put(workers, id, {pid, ref}))

      {:command, %{"command" => command, "id" => id}} ->
        case workers[id] do
          {pid, _} ->
            case command do
              "next" -> send(pid, :next)
              "release_resolver" -> send(pid, :release_resolver)
              "cancel" -> Process.exit(pid, :kill)
            end

          nil ->
            :ok
        end

        control(workers)

      {:DOWN, ref, :process, _pid, reason} ->
        {id, _} = Enum.find(workers, fn {_id, {_pid, monitor}} -> monitor == ref end)
        event(id, %{event: "stopped", reason: inspect(reason)})
        control(Map.delete(workers, id))

      :eof ->
        Enum.each(workers, fn {_id, {pid, _}} -> Process.exit(pid, :kill) end)

        Enum.each(workers, fn {id, {_pid, ref}} ->
          receive do
            {:DOWN, ^ref, :process, _, reason} ->
              event(id, %{event: "stopped", reason: inspect(reason)})
          end
        end)
    end
  end

  defp execute(request) do
    id = request["id"]

    options = [
      root_value: IncrementalHTTP.Schema.root_value(),
      context: %{request_id: id},
      variables: request["variables"] || %{},
      operation_name: request["operationName"]
    ]

    result =
      case request["mode"] do
        "eager" ->
          Absinthe.run!(request["query"], IncrementalHTTP.Schema, options)

        format when format in ["graphql_draft", "relay"] ->
          format = if format == "relay", do: :relay, else: :graphql_draft
          options = Keyword.put(options, :incremental_format, format)

          options =
            if format == :relay do
              Keyword.put(options, :pipeline_modifier, fn pipeline, _ ->
                Absinthe.Pipeline.replace(
                  pipeline,
                  Absinthe.Phase.Document.Result,
                  RelayExtensions
                )
              end)
            else
              options
            end

          Absinthe.run_incremental!(request["query"], IncrementalHTTP.Schema, options)
      end

    case result do
      %Absinthe.Incremental{} = result ->
        event(id, %{event: "payload", payload: result.initial_result})

        result.subsequent_results
        |> Enumerable.reduce({:suspend, nil}, fn payload, _ ->
          event(id, %{event: "payload", payload: payload})
          if payload.hasNext, do: {:suspend, nil}, else: {:halt, nil}
        end)
        |> continue()

      result ->
        event(id, %{event: "payload", payload: result})
    end
  end

  defp continue({:suspended, acc, continuation}) do
    receive do
      :next -> continue(continuation.({:cont, acc}))
    after
      15_000 -> raise "test did not release the next incremental result"
    end
  end

  defp continue({status, _}) when status in [:done, :halted], do: :ok
end
