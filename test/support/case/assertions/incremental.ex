defmodule Absinthe.Case.Assertions.Incremental do
  import ExUnit.Assertions

  @doc """
  Consume an in-process incremental result and reconstruct its delivered data.

  Ordinary result maps are returned unchanged with a one-element payload list.
  Incremental results are checked for valid pending/completion lifecycles and
  reconstructed by applying data and item entries at their announced paths.
  """
  def consume(%{data: data} = result), do: {data, [result]}

  def consume(%{initial_result: initial, subsequent_results: subsequent}) do
    payloads = [initial | Enum.to_list(subsequent)]
    assert List.last(payloads).hasNext == false
    assert Enum.all?(Enum.drop(payloads, -1), & &1.hasNext)

    {data, pending, completed} =
      Enum.reduce(payloads, {initial.data, %{}, MapSet.new()}, fn payload,
                                                                  {data, pending, completed} ->
        pending =
          Enum.reduce(Map.get(payload, :pending, []), pending, fn notice, acc ->
            assert is_binary(notice.id)
            refute Map.has_key?(acc, notice.id)
            Map.put(acc, notice.id, notice.path)
          end)

        data =
          Enum.reduce(Map.get(payload, :incremental, []), data, fn entry, acc ->
            assert Map.has_key?(pending, entry.id)
            refute MapSet.member?(completed, entry.id)
            path = Map.fetch!(pending, entry.id) ++ Map.get(entry, :subPath, [])

            update_path(acc, path, fn previous ->
              case entry do
                %{data: fields} ->
                  assert MapSet.disjoint?(
                           MapSet.new(Map.keys(previous)),
                           MapSet.new(Map.keys(fields))
                         ),
                         "duplicate response keys at #{inspect(path)}"

                  Map.merge(previous, fields)

                %{items: items} ->
                  previous ++ items
              end
            end)
          end)

        completed =
          Enum.reduce(Map.get(payload, :completed, []), completed, fn notice, acc ->
            assert Map.has_key?(pending, notice.id)
            refute MapSet.member?(acc, notice.id)
            MapSet.put(acc, notice.id)
          end)

        {data, pending, completed}
      end)

    assert MapSet.new(Map.keys(pending)) == completed
    {data, payloads}
  end

  defp update_path(value, [], fun), do: fun.(value)

  defp update_path(values, [index | rest], fun) when is_integer(index) do
    assert is_list(values), "expected a list at index #{inspect(index)}, got: #{inspect(values)}"
    assert index >= 0, "expected a non-negative list index, got: #{inspect(index)}"

    assert index < length(values),
           "list index #{inspect(index)} is out of bounds for a list of length #{length(values)}"

    List.update_at(values, index, &update_path(&1, rest, fun))
  end

  defp update_path(value, [key | rest], fun) do
    Map.update!(value, key, &update_path(&1, rest, fun))
  end
end
