# Incremental delivery over HTTP

Run from the repository root:

```sh
integration/incremental_http/run
```

Requires Elixir/OTP, Mix/Hex/Rebar, Node.js 24+ and npm. CI pins Node **24.21.0**,
Elixir **1.19.5** and OTP **28.5**. The runner installs locked dependencies,
compiles with warnings as errors, exports Absinthe's SDL, runs the Relay compiler,
checks formatting, and runs both client suites plus parser regressions. Generated
SDL and Relay artifacts are ignored.
After setup, `npm test` in this directory runs the HTTP tests.

This is a test-only HTTP adapter. The root Absinthe library has no new transport
or client dependencies. These tests do not implement incremental HTTP support
in Absinthe Plug.

## Clients and formats

| Component | Pinned version / revision |
| --- | --- |
| Absinthe | Current checkout through a local path dependency |
| Core proposal | GraphQL spec PR #1110, `045e19363c2b55f127960bd3b5e8072a15b29aec` |
| Apollo Client | **4.3.1** |
| Apollo handler / negotiation | `GraphQL17Alpha9Handler`, `multipart/mixed;incrementalSpec=v0.2` |
| Relay compiler / runtime | **21.0.1**, source `43eaa2587adad2fb10dbac402d900d035fae2f81` |
| Multipart parser / negotiation | Local JSON multipart parser; application-defined `multipart/mixed;incrementalSpec=relay` |
| GraphQL client parser | **16.12.0** |
| Independent execution reference | `graphql-reference` alias for **17.0.0-alpha.9**, source `3283f8adf52e77a47f148ff2f30185c8d11ff0f0` |
| RxJS / JSON codec | **7.8.2** / Jason **1.4.4** |

Apollo receives Absinthe's default `incremental_format: :graphql_draft` ID-based
envelopes. Only `GraphQL17Alpha9Handler` / `incrementalSpec=v0.2` is supported;
the older `Defer20220824Handler` / `GraphQL17Alpha2Handler` format is unsupported.
Relay receives the core's
`incremental_format: :relay` format. The client network layers forward patches
without translating them or repairing client data. The GraphQL.js reference
cross-checks client regressions; it is not the Absinthe server or a claim that
its execution semantics exactly match the pinned proposal.

## Apollo limitations

Stock Apollo **4.3.1** has two limitations characterized by this harness:

- Streams introduced inside streamed items lose data because the handler does
  not initialize the inner list's next position from its delivered prefix.
- HttpLink cancellation aborts Fetch, and its multipart reader can produce an
  unhandled `AbortError`.

Two nested-stream tests verify correct wire data from Absinthe and GraphQL.js,
then assert the known stock-client data loss. An inner stream introduced by an
explicit defer is covered as a successful case. A third characterization uses
public ApolloClient/HttpLink APIs and an abortable Fetch response in a child
process: initial delivery succeeds, then cancellation must produce the known
unhandled `AbortError`. These tests run without skips or rejection handlers;
they do not establish successful Apollo cancellation. Relay and the core tests
cover successful request cancellation.

The local multipart parser is a small harness parser for JSON parts, boundaries,
UTF-8 chunks, truncation, and cancellation. It is not a general MIME parser.
Relay **21.0.1** remains unmodified.

## What the HTTP tests establish

The Node server listens on a dynamically assigned loopback port. Apollo
`watchQuery` uses its real `HttpLink`, native Fetch, multipart reader, incremental
handler and cache. Relay uses operations compiled against exported Absinthe SDL,
its real `Environment`, `Observable`, normalized store and fragment readers.
Mutations use `Environment.executeMutation`. Relay enables
`deferDeduplicatedFields: true`.

Each request has one monitored Elixir worker calling `Absinthe.run_incremental!/3`
and owning its continuation enumerable. The bridge serializes its original
payloads. Tests compare client-observed wire payloads with the emitted maps and
check pending-ID lifecycles for the draft format. There is no test data merger.

Tests grant continuation permits one at a time. Suspended enumeration prevents
the next resolver from starting before its permit. Resolver traces and explicit
resolver gates establish ordering without sleeps. Cancellation waits for both
socket closure and worker `DOWN` before asserting that later work did not run.
Shutdown checks every worker stopped and awaits BEAM exit; forced termination
fails the test. These checks do not undo completed side effects or establish
cancellation of detached resolver-owned tasks or external services.

Coverage includes:

- Aliases, explicit-null labels, variables, initial prefixes, progressive lists, normalized
  cache writes, nested defer/stream, shared fields and resolver-once behavior.
- Nested-list nullability in streamed prefixes and non-null rows.
- Disabled directives, empty/null lists and parents, eager JSON fallback,
  initial and incremental errors, non-null boundaries and cancellation.
- Malformed requests, unsupported negotiation, truncated responses, terminal
  markers, closed sockets and stopped request workers.
- Relay named fragments, deduplicated deferred ancestors, abstract types and
  discriminators, nullable streamed items and error paths.
- Deferred groups that acquire their first work only after a shared linked
  field resolves, including progressive fragment reads and resolver-once checks.
- Reused eager fragments retaining their abstract-type discriminators when
  deferred snapshots omit unrelated fields.
- Compiled `@stream_connection`, deferred page info, cursor pagination, and
  subsequent null-edge pages without duplicate nodes or cursor warnings.
- Overlapping Relay and Apollo requests to the same endpoint, each retaining
  its negotiated format and independent continuation demand.
- Compiled Relay mutations with suspended eager work before the second root;
  deferred demand; late deferred failure retaining already executed mutations
  while stopping later deferred work.

Relay's network rejects multipart EOF without both terminal markers. For an
ordinary JSON fallback, it adds `extensions.is_final: true` before forwarding
to Relay, while retaining the unchanged raw wire payload for comparison.

Relay cannot represent isolated failed completion notices: a failed boundary
terminates the operation. Null streamed items require a final accumulated
snapshot, and the tests assert the resulting data, errors and expected Relay
development warning. Scalar-list streaming is compiler-rejected; customized
batching is unsupported. See the [Relay contract](../../guides/incremental-delivery.md#relay-compatibility).

Parts include the following boundary immediately, allowing clients to observe
initial data while later work is gated. Fragmented-write tests do not control
TCP or Fetch chunk boundaries. The negotiation helper recognizes only the
tested v0.2, Relay and JSON alternatives, ignores `q=0`, and returns 406 for
unsupported-only choices. The Relay negotiation parameter is application-defined.

## Verification limits

The GitHub Actions workflow runs this harness for pull requests and pushes to
main.

This suite does not certify a production Absinthe Plug adapter, browsers,
reverse proxies, compression, HTTP/2, SSE, WebSockets or general socket
backpressure. It verifies the listed client versions and formats over the
local test transport; Apollo behavior is verified against the stock 4.3.1 client.
