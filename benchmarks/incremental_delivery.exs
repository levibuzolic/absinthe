# Run with: mix run benchmarks/incremental_delivery.exs
# Report median initial-response and continuation times separately. These
# queries exercise shared buffers, blocked child groups, streams, fragment
# reuse, and document-size scaling.
defmodule IncrementalDeliveryBenchmark.Schema do
  use Absinthe.Schema
  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  query do
    field :rows, list_of(:row)
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

measure = fn query, rows ->
  {initial, {:ok, response}} =
    :timer.tc(fn ->
      Absinthe.run_incremental(query, IncrementalDeliveryBenchmark.Schema,
        root_value: %{rows: rows}
      )
    end)

  {subsequent, _count} = :timer.tc(fn -> Enum.count(response.subsequent_results) end)
  {initial, subsequent}
end

median = fn query, rows ->
  measure.(query, rows)
  samples = for _ <- 1..5, do: measure.(query, rows)

  for index <- [0, 1] do
    samples |> Enum.map(&elem(&1, index)) |> Enum.sort() |> Enum.at(2) |> Kernel./(1_000)
  end
end

IO.puts("scenario | rows | initial ms | continuation ms")

for {name, query} <- queries, count <- [500, 1_000, 2_000, 4_000] do
  rows = Enum.map(1..count, &%{id: &1, value: &1, extra: &1})
  medians = median.(query, rows)

  IO.puts(Enum.join([name, count | Enum.map(medians, &Float.round(&1, 2))], " | "))
end

ast_query = fn field_count ->
  aliases = for index <- 1..field_count, do: "field#{index}: id"
  "{ rows { #{Enum.join(aliases, " ")} ... @defer { value } } }"
end

IO.puts("scenario | aliased fields | initial ms | continuation ms")

for field_count <- [10, 100, 500] do
  medians = median.(ast_query.(field_count), [%{id: 1, value: 1, extra: 1}])

  IO.puts(
    Enum.join(
      ["AST aliases", field_count | Enum.map(medians, &Float.round(&1, 2))],
      " | "
    )
  )
end
