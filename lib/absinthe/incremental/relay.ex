defmodule Absinthe.Incremental.Relay do
  @moduledoc false

  alias Absinthe.Incremental
  alias Absinthe.Incremental.Delivery

  def format(initial, blueprint, pipeline) do
    state = %{
      blueprint: blueprint,
      pipeline: pipeline,
      data: snapshot(initial.data),
      errors: index_errors({0, %{}}, Map.get(initial, :errors, [])),
      sent: MapSet.new(),
      replay: false,
      done: false
    }

    %Incremental{
      initial_result: initial |> Map.take([:data, :errors, :extensions]) |> continuing(),
      subsequent_results: state |> Stream.unfold(&next/1) |> Stream.flat_map(& &1)
    }
  end

  def final(result), do: mark(result, true)
  defp continuing(result), do: mark(result, false)

  defp mark(result, final?) do
    result
    |> Map.put(:hasNext, not final?)
    |> Map.update(:extensions, %{is_final: final?}, &Map.put(&1, :is_final, final?))
  end

  defp next(%{done: true}), do: nil

  defp next(state) do
    {payload, blueprint} = Delivery.next(state.blueprint, state.pipeline)
    core = blueprint.execution.incremental
    completed = Map.get(payload, :completed, [])
    state = %{state | blueprint: blueprint}

    {stream_packets, state} =
      Enum.reduce(Map.get(payload, :incremental, []), {[], state}, fn entry, acc ->
        apply_entry(entry, core.groups[Map.fetch!(core.group_ids, entry.id)], acc)
      end)

    {packets, state} =
      Enum.reduce(completed, {[], state}, fn
        %{errors: [_ | _]}, acc ->
          acc

        completion, {packets, state} ->
          ref = Map.fetch!(core.group_ids, completion.id)

          case core.groups[ref].kind do
            :defer -> defer_packets(ref, packets, state)
            :stream -> {packets, state}
          end
      end)

    packets = Enum.reverse(packets, Enum.reverse(stream_packets))

    failures =
      completed
      |> Enum.flat_map(&Map.get(&1, :errors, []))
      |> Enum.uniq()

    done = failures != [] or not payload.hasNext

    packets =
      cond do
        done -> packets ++ [terminal(state, failures)]
        packets == [] -> [continuing(%{data: nil})]
        true -> packets
      end

    extensions = Map.get(payload, :extensions, %{})

    packets =
      Enum.map(packets, fn packet ->
        Map.update!(packet, :extensions, &Map.merge(extensions, &1))
      end)

    {packets, %{state | done: done}}
  end

  defp apply_entry(%{data: fields} = entry, group, {packets, state}) do
    path = group.path ++ Map.get(entry, :subPath, [])
    data = update(state.data, path, &Map.merge(&1, snapshot(fields)))
    errors = index_errors(state.errors, Map.get(entry, :errors, []))
    {packets, %{state | data: data, errors: errors}}
  end

  defp apply_entry(%{items: items} = entry, group, {packets, state}) do
    {:list, values} = fetch(state.data, group.path)
    errors = Map.get(entry, :errors, [])
    item_errors = index_errors({0, %{}}, errors)

    {values, packets, replay} =
      Enum.reduce(items, {values, packets, state.replay}, fn item, {values, packets, replay} ->
        index = :array.size(values)
        values = :array.set(index, snapshot(item), values)
        path = group.path ++ [index]

        if is_nil(item) do
          # Relay treats null patch data as a control message or an operation
          # error. Keep the slot and replay ordinary data when execution ends.
          {values, packets, true}
        else
          packet =
            %{data: item, label: group.label, path: path}
            |> with_errors(relative_errors(item_errors, path))
            |> continuing()

          {values, [packet | packets], replay}
        end
      end)

    data = update(state.data, group.path, fn _ -> {:list, values} end)
    {packets, %{state | data: data, errors: index_errors(state.errors, errors), replay: replay}}
  end

  defp terminal(%{replay: true} = state, []) do
    %{data: restore(state.data)}
    |> with_errors(relative_errors(state.errors, []))
    |> final()
  end

  defp terminal(_, failures), do: %{data: nil} |> with_errors(failures) |> final()

  defp defer_packets(nil, packets, state), do: {packets, state}

  defp defer_packets(ref, packets, state) do
    if MapSet.member?(state.sent, ref) do
      {packets, state}
    else
      group = state.blueprint.execution.incremental.groups[ref]
      {packets, state} = defer_packets(group.parent, packets, state)

      packet =
        %{data: restore(fetch(state.data, group.path)), path: group.path, label: group.label}
        |> with_errors(relative_errors(state.errors, group.path))
        |> continuing()

      {[packet | packets], %{state | sent: MapSet.put(state.sent, ref)}}
    end
  end

  # Index each response-path prefix so a deferred fragment reads only its own
  # errors. Sequence numbers preserve order when merging errors without paths.
  defp index_errors(index, errors) do
    Enum.reduce(errors, index, fn error, {sequence, by_path} ->
      paths =
        case error do
          %{path: path} -> Enum.scan(path, [], fn key, prefix -> prefix ++ [key] end)
          _ -> [nil]
        end

      by_path =
        Enum.reduce([[] | paths], by_path, fn path, by_path ->
          Map.update(by_path, path, [{sequence, error}], &[{sequence, error} | &1])
        end)

      {sequence + 1, by_path}
    end)
  end

  defp relative_errors({_, by_path}, path) do
    errors = Map.get(by_path, path, [])

    errors =
      if path == [],
        do: errors,
        else: :lists.rkeymerge(1, errors, Map.get(by_path, nil, []))

    for {_, error} <- Enum.reverse(errors) do
      case error do
        %{path: error_path} -> %{error | path: Enum.drop(error_path, length(path))}
        _ -> error
      end
    end
  end

  defp with_errors(packet, []), do: packet
  defp with_errors(packet, errors), do: Map.put(packet, :errors, errors)

  # Lists are indexed while accumulating patches; repeatedly updating a linked
  # list at increasing indices would make a large deferred list quadratic.
  defp snapshot(value) when is_list(value),
    do: {:list, value |> Enum.map(&snapshot/1) |> :array.from_list()}

  defp snapshot(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {key, snapshot(child)} end)

  defp snapshot(value), do: value

  defp restore({:list, values}), do: values |> :array.to_list() |> Enum.map(&restore/1)

  defp restore(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {key, restore(child)} end)

  defp restore(value), do: value

  defp fetch(data, []), do: data
  defp fetch({:list, values}, [index | rest]), do: fetch(:array.get(index, values), rest)
  defp fetch(data, [key | rest]), do: fetch(Map.fetch!(data, key), rest)

  defp update(data, [], fun), do: fun.(data)

  defp update({:list, values}, [index | rest], fun),
    do: {:list, :array.set(index, update(:array.get(index, values), rest, fun), values)}

  defp update(data, [key | rest], fun), do: Map.update!(data, key, &update(&1, rest, fun))
end
