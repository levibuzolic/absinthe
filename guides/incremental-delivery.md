# Incremental delivery

Absinthe can execute `@defer` fragments and `@stream` list fields incrementally.
The initial response contains the eager fields and initial list items. A lazy
enumerable resolves the remaining work as the caller requests more payloads.

This feature follows [GraphQL spec proposal #1110](https://github.com/graphql/graphql-spec/pull/1110)
at revision [`045e19363c2b55f127960bd3b5e8072a15b29aec`](https://github.com/graphql/graphql-spec/tree/045e19363c2b55f127960bd3b5e8072a15b29aec).
The proposal is a draft, and its protocol may change. Both the schema and the
execution caller must opt in.

## Enable the directives

Import the directive definitions in your schema:

```elixir
defmodule MyApp.Schema do
  use Absinthe.Schema

  import_directives Absinthe.Type.BuiltIns.IncrementalDirectives

  query do
    field :people, list_of(:person)
  end

  object :person do
    field :id, non_null(:id)
    field :name, :string
    field :biography, :string
  end
end
```

The import exposes these definitions through introspection:

```graphql
directive @defer(if: Boolean! = true, label: String)
  on FRAGMENT_SPREAD | INLINE_FRAGMENT

directive @stream(if: Boolean! = true, label: String, initialCount: Int! = 0)
  on FIELD
```

The directives are not repeatable. `@stream` applies only to list fields and
streams the outermost list when its type contains nested lists. Labels are
optional string literals; non-null labels must be unique across both directives
in the entire document. A label cannot be a variable. The `if` and `initialCount`
arguments may use variables and follow ordinary argument coercion.

Schemas without this import retain their existing behavior, including schemas
that define unrelated custom directives with the same names.

## Execute and consume results

Use `Absinthe.run_incremental/3` with the usual execution options:

```elixir
document = """
{
  people @stream(initialCount: 1, label: "morePeople") {
    id
    name
    ... @defer(label: "biography") {
      biography
    }
  }
}
"""

root_value = %{
  people: [
    %{id: 1, name: "Ada", biography: "Mathematician"},
    %{id: 2, name: "Grace", biography: "Computer scientist"}
  ]
}

deliver = &IO.inspect/1

case Absinthe.run_incremental(document, MyApp.Schema, root_value: root_value) do
  {:ok, %Absinthe.Incremental{} = response} ->
    deliver.(response.initial_result)
    Enum.each(response.subsequent_results, deliver)

  {:ok, result} ->
    deliver.(result)

  {:error, message} ->
    raise Absinthe.ExecutionError, message: message
end
```

Here `deliver` prints each payload. Replace it with a function that encodes and
sends one payload through the chosen transport. `Absinthe.run_incremental!/3` provides
the usual raising variant: pipeline errors raise, while GraphQL validation and
execution errors remain in the returned payloads.

Initial execution completes before the function returns. Deferred fields and
list-tail child fields have not executed yet. Each pull from
`subsequent_results` performs more execution in the consuming process. Consume
this enumerable once, in the request process; enumerating it again repeats the
remaining work. Halting enumeration leaves later work unexecuted. No incremental
supervisor or background coordinator is required. Existing asynchronous
middleware may create tasks while a payload is being resolved and settles those
tasks before that payload is yielded.

The list resolver still returns its list once. `@stream` defers completion of
the remaining items and their fields; it does not turn a database query into a
cursor or paginate an external data source. The remaining source items are held
until consumed or the response is discarded.

`Absinthe.run/3` retains its ordinary single-result contract and completes valid
imported directives eagerly. `run_incremental/3` also returns an ordinary map
when there is no effective incremental work, such as disabled directives, a
null parent, an empty list, or a list fully covered by `initialCount`.

## Payload protocol

For a simple deferred fragment:

```graphql
{
  person {
    id
    ... @defer(label: "details") { name }
  }
}
```

An initial result can be:

```elixir
%{
  data: %{"person" => %{"id" => "1"}},
  pending: [%{id: "0", path: ["person"], label: "details"}],
  hasNext: true
}
```

A subsequent result completes it:

```elixir
%{
  incremental: [%{id: "0", data: %{"name" => "Ada"}}],
  completed: [%{id: "0"}],
  hasNext: false
}
```

For a stream, the pending path points to the list and each incremental entry
contains `items` to append instead of `data` to merge. Items arrive in list
order. Nested work can introduce additional `pending` notices in later
payloads. Overlapping deferred selections can use `subPath` to identify a
deeper object relative to a pending notice's path.

IDs are strings unique within one response and are independent of labels.
Clients must associate updates and completion notices by ID. Paths contain
response aliases and zero-based list indices. Do not infer completion from a
particular number or order of payloads: process notices and continue until
`hasNext` is false. Optional empty lists are omitted from the payload.

## Execution, errors, and validation

`@skip` and `@include` take precedence over incremental directives. An `if: false`
directive executes eagerly. When an enabled stream completes a list, a negative
`initialCount` produces an execution error at that field. Null list values do
not start stream completion.

A response field selected eagerly and in a deferred fragment resolves once.
Its merged child selections can still be deferred. Shared fields in overlapping
deferred fragments also resolve once. The existing resolver middleware, Async,
Batch, and Dataloader plugins remain responsible for resolving fields; context
and accumulator state carry forward between incremental execution steps.
Complexity analysis includes deferred and streamed selections before execution.

The execution and result phases selected by a pipeline modifier run for initial
and subsequent execution. Preserve the inserted `Absinthe.Incremental.Start`
phase: it marks the start of the pipeline portion reused for later payloads.
Removing it raises before any resolver runs. Result phases must preserve the
GraphQL result shape so incremental delivery can associate data and errors with
response paths.
Their `extensions` maps are included on the corresponding payload; when several
buffered results are released together, later maps override duplicate keys.

Errors in initial work appear in the initial result. Nullable errors in later
work appear in the relevant incremental entry. If a non-null error reaches a
deferred object or streamed list boundary that was already delivered, that
entry is omitted and the corresponding completion notice contains the errors.
Work below a failed or nulled parent is discarded. Errors retain absolute
response paths and source locations.

Root mutation and subscription fields cannot be streamed, and root mutation or
subscription fragments cannot be deferred. Nested mutation selections support
incremental delivery while root mutations retain serial execution.

Subscriptions cannot incrementally deliver events. A subscription may contain
directives that can be disabled, such as `if: false` or `if: $variable`. Static
validation rejects unconditional usages. If a published event reaches an active
incremental directive, execution reports an error. A directive beneath a null
parent or a fragment that does not apply to that event's concrete type is not
reached. Registration does not reject those potentially unreachable branches.

## Transport integration

Absinthe returns payload maps. The proposal does not define HTTP negotiation,
multipart boundaries, SSE event names, or WebSocket envelopes. An adapter must
negotiate a compatible protocol, encode the payloads, preserve their sequence,
and stop consuming when the client disconnects. Existing transport packages do
not automatically gain incremental support by importing the directives.

The eager `Absinthe.run/3` API remains available for clients and transports that
accept only ordinary GraphQL responses. Do not pass an `Absinthe.Incremental`
struct to a JSON encoder as if it were a response map.

The repository includes a client-over-HTTP test harness in
`integration/incremental_http`, using Apollo Client's `GraphQL17Alpha9Handler`,
Relay's compiler and runtime, and a test-only local multipart adapter. Run it with
`integration/incremental_http/run`. Its README records pinned versions,
verified cases, and known client limitations; it does not add production
incremental support to Absinthe Plug.

## Relay compatibility

Relay 21.0.1 expects labeled `{data, label, path}` responses and an
`extensions.is_final` marker. It does not directly consume the default draft's
`pending`, `incremental`, and `completed` envelopes. Select its format explicitly:

```elixir
{:ok, result} =
  Absinthe.run_incremental(compiled_query, MyApp.Schema,
    variables: variables,
    incremental_format: :relay
  )
```

The return contract remains an ordinary map or an `Absinthe.Incremental` struct.
Send `initial_result`, then enumerate `subsequent_results` in the request process.
Relay responses retain `hasNext` for transport termination; `extensions.is_final`
is reserved for Relay's completion tracking. Ordinary results also get both
final markers. Result-phase extensions are preserved.

Use operations produced by the Relay compiler, including its generated labels,
IDs, `__typename`, and abstract-type discriminator selections. Configure
`deferDeduplicatedFields: true` on Relay's `Environment`. Its `Network` must
return an `Observable` that forwards every parsed response, completes when the
transport ends, and aborts the request when unsubscribed. A promise returning
only one JSON response cannot deliver these updates. Relay's
[environment documentation](https://relay.dev/docs/api-reference/relay-runtime/relay-environment/)
and [network-layer guide](https://relay.dev/docs/guides/network-layer/) describe
these integration points.

The formatter supplies accumulated snapshots for completed deferred fragments,
including parent fragments with no independent work after deduplication. This
preserves shared fields, object identity, abstract types, and deferred selections
inside eager objects and lists. Streamed objects use individual indexed patches.
Relay's `@stream_connection` compiler transform produces supported edges
`@stream` and page-info `@defer` selections, including cursor pagination.

Two compatibility behaviors differ from the default draft format:

- A failed incremental boundary becomes a terminal Relay operation error.
  Previously delivered cache data remains available, and later resolver work
  stops. Relay has no equivalent to an isolated failed `completed` notice.
  Nullable field errors remain attached to their data, with paths relative to
  each Relay patch; terminal operation errors retain absolute paths.
- Relay cannot normalize a null streamed item patch. Its slot is retained in
  the server snapshot, non-null items continue progressively at their correct
  indices, and the final response replays accumulated root data with
  `is_final: true`. This also preserves errors that caused nullable items to
  become null. Relay's development build can warn that this final replay used
  non-streaming mode. The HTTP tests verify that behavior and the resulting
  cache contents.

The formatter retains delivered data until completion, and deferred snapshots
can repeat fields already sent. That memory and wire cost is specific to Relay
compatibility. Its compiler rejects `@stream` on scalar lists; use linked-object
lists or connections. Relay's optional `use_customized_batch` compiler extension
is not part of the supported draft directives and must remain disabled.

The HTTP harness tests real Relay compiler/runtime 21.0.1 artifacts, generated
from exported Absinthe SDL during the test run. Its observable network uses the
`meros` multipart parser and forwards payloads directly to Relay. The harness's
`incrementalSpec=relay` negotiation parameter is an application-defined test
convention, not a standardized GraphQL HTTP protocol. Production transports must
explicitly negotiate and select `incremental_format: :relay`; the core option
does not configure Absinthe Plug or a client network layer automatically.

## Draft interpretation

The pinned draft contains contradictory wording in its subscription validation
pseudocode and final `hasNext` description. This implementation follows the
surrounding execution semantics and GraphQL.js behavior: subscription directives
must be disableable, active usages fail when reached during event execution,
and the final incremental payload has `hasNext: false`. List updates use `items`
at the pending list path; `subPath` applies to object updates.
