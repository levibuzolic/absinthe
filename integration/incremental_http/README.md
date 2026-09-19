# Incremental delivery over HTTP

Run from the repository root:

```sh
integration/incremental_http/run
```

Requires Elixir/OTP, Mix/Hex/Rebar, Node.js 24+ and npm. CI pins Node **24.21.0**,
Elixir **1.19.5** and OTP **28.5**. The runner installs locked dependencies,
explicitly applies the local Apollo and meros fixes below, compiles with warnings
as errors, exports Absinthe's SDL, runs the Relay compiler, checks formatting,
and runs both client suites plus parser regressions. Generated SDL and Relay
artifacts are ignored.
After setup, `npm test` in this directory runs the HTTP tests.

This is a test-only HTTP adapter. The root Absinthe library has no new transport
or client dependencies. These tests do not implement incremental HTTP support
in Absinthe Plug.

## Clients and formats

| Component | Pinned version / revision |
| --- | --- |
| Absinthe | Current checkout through a local path dependency |
| Core proposal | GraphQL spec PR #1110, `045e19363c2b55f127960bd3b5e8072a15b29aec` |
| Apollo Client | **4.3.1**, with the explicit local patch described below |
| Apollo handler / negotiation | `GraphQL17Alpha9Handler`, `multipart/mixed;incrementalSpec=v0.2` |
| Relay compiler / runtime | **21.0.1**, source `43eaa2587adad2fb10dbac402d900d035fae2f81` |
| Relay parser / negotiation | `meros` **1.3.2** with the explicit local parser fix below; application-defined `multipart/mixed;incrementalSpec=relay` |
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

## Apollo fixes

Stock Apollo **4.3.1** has two defects reproduced by this harness:

- Streams introduced inside streamed items lose data because the handler does
  not initialize the inner list's next position from its delivered prefix.
- Unsubscribing aborts Fetch, and the multipart reader's unawaited `cancel()`
  promise produces an unhandled `AbortError`.

The runner applies [a version-specific runtime patch](patches/@apollo+client+4.3.1.patch)
with `npm run client:patch`. This is an **unreleased local client fix**, not a
claim that published Apollo 4.3.1 supports these cases. There is no automatic
postinstall hook. A clean `npm ci --ignore-scripts` restores the stock client;
the nested-stream and cancellation regressions then fail. Run those checks with:

```sh
node --test --test-concurrency=1 --test-timeout=30000 \
  --test-name-pattern='Apollo reconstructs nested|Apollo cancellation' client.test.mjs
```

The runtime patch updates both ESM and CommonJS, based on Apollo source revision
[`4bb0b7ad64e5a928bddab32a7f398c911f24579d`](https://github.com/apollographql/apollo-client/tree/4bb0b7ad64e5a928bddab32a7f398c911f24579d).
It initializes inner
positions from incoming data at the full response path, including nonzero
prefixes and sibling lists; cached array tails cannot shift insertion indices.
It also awaits reader cancellation so the HTTP link owns rejection handling.
Unexpected cleanup errors still propagate. Source maps remain those of the
released package.

The HTTP regressions require correct final data and normalized cache contents
for inner prefixes zero and one against both Absinthe and GraphQL.js. Cancellation
runs with `--unhandled-rejections=strict`, without a rejection handler. It checks
three Absinthe cancellations and a reference cancellation, socket closure,
worker termination, and exact resolver traces.

## meros parser fix

Stock `meros` **1.3.2** skips a boundary split across Fetch chunks when the
second chunk also contains another complete boundary. The runner explicitly
applies a [local browser parser fix](patches/meros+1.3.2.patch) that searches the
accumulated buffer in order. **Relay 21.0.1 itself is unmodified**, but its
network parser includes this unreleased fix. There is no published fixed
meros version as of 2026-09-19.

The patch updates the published browser ESM and CommonJS modules, based on
source revision
[`87ed69fe97f5a250ee6e8bec1a9ba458e16655f9`](https://github.com/maraisr/meros/tree/87ed69fe97f5a250ee6e8bec1a9ba458e16655f9).
The Node-specific parser is unused and unchanged.

Deterministic parser regressions cover every two-chunk byte split, single-byte
chunks and a coalesced body, including UTF-8, in both ESM and CommonJS. These
checks complement HTTP write fragmentation, which cannot control Fetch chunks.

To reproduce against the stock parser and then verify the fix:

```sh
npm ci --ignore-scripts
node --test multipart.test.mjs # both module-format regressions fail
npm run client:patch
node --test multipart.test.mjs # both pass
```

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
local test transport; verification includes the explicit Apollo and meros patches.
