# Run with: mix run benchmarks/incremental_delivery.exs
# Report median initial-response and continuation times separately. These
# queries exercise shared buffers, blocked child groups, streams, fragment
# reuse, and document-size scaling.
defmodule IncrementalDeliveryBenchmark.Schema do
  use Absinthe.Schema
  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  query do
    field :rows, list_of(:row)
    field :value, :integer
  end

  object :row do
    field :id, :integer
    field :value, :integer
    field :extra, :integer
    field :nullable_value, :integer
  end
end

queries = [
  {"nullable values", "{ rows { id } ... @defer { rows { nullableValue } } }"},
  {"shared group", "{ rows { id } ... @defer { rows { value } } }"},
  {"nested groups", "{ rows { id } ... @defer { rows { value ... @defer { extra } } } }"},
  {"stream with defer", "{ rows @stream { id ... @defer { value } } }"},
  {
    "named fragment reuse",
    "{ rows { id ...Wrapper ...Wrapper } } fragment Wrapper on Row { ...Details @defer } fragment Details on Row { value extra }"
  }
]

top_level_data_fields = fn payload ->
  Enum.reduce([payload | Map.get(payload, :incremental, [])], 0, fn
    %{data: data}, count when is_map(data) -> count + map_size(data)
    _, count -> count
  end)
end

measure = fn query, root_value, options ->
  {initial, {:ok, response}} =
    :timer.tc(fn ->
      Absinthe.run_incremental(
        query,
        IncrementalDeliveryBenchmark.Schema,
        Keyword.put(options, :root_value, root_value)
      )
    end)

  {subsequent, fields} =
    :timer.tc(fn ->
      Enum.reduce(response.subsequent_results, top_level_data_fields.(response.initial_result), fn
        payload, count -> count + top_level_data_fields.(payload)
      end)
    end)

  {initial, subsequent, fields}
end

median = fn query, root_value, options ->
  measure.(query, root_value, options)
  samples = for _ <- 1..5, do: measure.(query, root_value, options)

  times =
    for index <- [0, 1] do
      samples |> Enum.map(&elem(&1, index)) |> Enum.sort() |> Enum.at(2) |> Kernel./(1_000)
    end

  {times, elem(hd(samples), 2)}
end

IO.puts("scenario | rows | initial ms | continuation ms")

for {name, query} <- queries, count <- [500, 1_000, 2_000, 4_000] do
  rows = Enum.map(1..count, &%{id: &1, value: &1, extra: &1})
  {medians, _fields} = median.(query, %{rows: rows}, [])

  IO.puts(Enum.join([name, count | Enum.map(medians, &Float.round(&1, 2))], " | "))
end

ast_query = fn field_count ->
  aliases = for index <- 1..field_count, do: "field#{index}: id"
  "{ rows { #{Enum.join(aliases, " ")} ... @defer { value } } }"
end

IO.puts("scenario | aliased fields | initial ms | continuation ms")

for field_count <- [10, 100, 500] do
  {medians, _fields} =
    median.(ast_query.(field_count), %{rows: [%{id: 1, value: 1, extra: 1}]}, [])

  IO.puts(
    Enum.join(
      ["AST aliases", field_count | Enum.map(medians, &Float.round(&1, 2))],
      " | "
    )
  )
end

sibling_query = fn group_count ->
  groups =
    for index <- 1..group_count do
      "... @defer(label: \"value#{index}\") { value#{index}: value }"
    end

  "{ #{Enum.join(groups, " ")} }"
end

IO.puts("format | scenario | groups | continuation ms | delivered top-level data fields")

for format <- [:graphql_draft, :relay], group_count <- [10, 100, 500] do
  {[_initial, continuation], fields} =
    median.(sibling_query.(group_count), %{value: 1}, incremental_format: format)

  IO.puts(
    Enum.join(
      [format, "root sibling @defer", group_count, Float.round(continuation, 2), fields],
      " | "
    )
  )
end
