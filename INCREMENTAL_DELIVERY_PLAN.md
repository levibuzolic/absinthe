# Incremental delivery design and verification

This implementation targets [GraphQL spec PR #1110](https://github.com/graphql/graphql-spec/pull/1110)
at revision [`045e19363c2b55f127960bd3b5e8072a15b29aec`](https://github.com/graphql/graphql-spec/tree/045e19363c2b55f127960bd3b5e8072a15b29aec).
The proposal remains a draft. The [usage guide](guides/incremental-delivery.md)
documents schema opt-in, execution APIs, response shapes, and transport integration.

## Scope

A schema imports `Absinthe.Type.BuiltIns.IncrementalDirectives`; callers use
`Absinthe.run_incremental/3` or its raising variant. Ordinary `Absinthe.run/3`
keeps its single-result contract and executes imported directives eagerly.
Validation applies to both APIs. The root library adds no dependencies,
background workers or transport packages. A test-only HTTP harness has its own
locked client and transport dependencies.

The design addresses the concerns raised in
[the earlier implementation's removal](https://github.com/absinthe-graphql/absinthe/pull/1377#issuecomment-4068271138):
explicit draft support, compatibility with existing execution, tested documented
behavior, and a bounded core API. HTTP negotiation and framing belong to transport
adapters; an `Absinthe.Incremental` struct cannot be passed to an ordinary JSON
response encoder.

## Execution design

| Component | Responsibility |
| --- | --- |
| `Absinthe.Incremental` / `Start` | Build the configured initial and continuation pipelines; return an initial result and lazy enumerable |
| `Planner` | Collect field occurrences, retain defer ownership, merge selections, and partition eager/deferred work and stream prefixes |
| `State` | Track groups, runnable jobs, buffered frames, and publication dependencies |
| `Delivery` | Resume execution, publish payloads, complete groups, and cancel unreachable work |
| `Relay` | Convert published core results into labeled snapshots, indexed items and final markers |
| Existing resolution phase | Resolve fields and resume suspended Async, Batch, Dataloader, and custom middleware |
| Incremental validation phases | Validate labels, list types, operation restrictions, and overlapping streams |

The scheduler relies on these invariants:

- **Demand controls execution.** The initial call resolves eager fields and
  initial list items. Each subsequent pull resolves queued work in the consuming
  process. Halting or discarding the enumerable prevents later incremental
  execution. Consumers enumerate once; a second enumeration repeats that work.
- **Field occurrences retain ownership.** Named fragments have context-sensitive
  visited state. Eager occurrences dominate deferred ones; ancestor defer usages
  dominate descendant usages for the same response name. Merged child selections
  retain their own ownership, so shared fields resolve once without losing data.
- **Deferred groups publish atomically.** Private values wait for their whole
  group to succeed. Shared values publish once when any owner succeeds. Child
  groups and streams wait until their containing data is published, including
  when their shared work has already finished.
- **Runnable jobs follow enqueue order.** An ordered set avoids scanning blocked
  jobs. Group membership tracks queued and running work; frame IDs track
  execution order. Group identities remain available for occurrence ancestry
  after completion notices are sent.
- **Streams complete an ordinary list incrementally.** The list resolver runs
  once, its prefix completes initially, and remaining raw items complete on
  demand. Only the outermost list streams. This does not implicitly paginate an
  external data source.
- **Middleware state carries forward.** Each execution group uses the existing
  plugin pipeline to completion. Context and accumulators persist, and field
  caches distinguish concrete parent types. Pipeline modifiers and result-phase
  options also apply to continuations.
- **Failures cancel unreachable work.** Nullable errors accompany delivered
  values. Non-null failures reaching an already-delivered boundary produce
  failed completion notices and discard private data. Failed owners do not
  cancel another owner's surviving selections. Formatter-nullified containers
  also prune descendants.
- **Every announced group completes once.** String IDs identify pending work;
  paths use response aliases and list indices. Updates precede completion and
  the final payload has `hasNext: false`.

Mutation roots and their eager subtrees finish serially across middleware
suspensions. Deferred children do not block later roots. Explicit plugin resume
options apply to that resume; each new root starts with the original execution
options. A non-null failure propagating to the root prevents later mutations.

Root mutation/subscription defer and stream are rejected. Subscription directives
must be disableable, and a published event reaching an active incremental
directive reports an error. Excluded or inapplicable fragments do not activate
their directives.

Schema introspection and SDL export preserve the directive defaults. SDL export
uses the same typed default serialization as introspection, including GraphQL
string escaping.

## Relay compatibility

`incremental_format: :relay` selects the labeled response format expected by
Relay 21.0.1. The formatter retains delivered data to normalize compiled
fragments, including shared fields and deferred ancestors with no independent
work. Compiled connections stream edges and defer page info. The client uses
`deferDeduplicatedFields: true` and an Observable network with completion checks.

Failed incremental boundaries terminate the Relay operation. Nullable streamed
items keep their positions through a final accumulated snapshot; this can
produce Relay's expected non-streaming development warning. The formatter's
retained data and repeated snapshots have memory and wire costs. Scalar-list
streaming and customized batching are not supported by this Relay contract.
See the guide for negotiation and network requirements.

## Draft interpretation

The pinned proposal's subscription-validation condition conflicts with its
disablement explanation, and its final-result prose conflicts with its
termination algorithm. This implementation follows the surrounding semantics:
subscription usages must be disableable, active directives fail when reached,
and the final response has `hasNext: false`. Scheduling policy is
implementation-defined where the draft leaves work-queue behavior unspecified.

Reference probes against GraphQL.js revision
[`ee5ce41d4b68d1852306d3b56dba2cbbb6c43fea`](https://github.com/graphql/graphql-js/tree/ee5ce41d4b68d1852306d3b56dba2cbbb6c43fea)
confirmed shared-work survival, withholding private data until an owner succeeds,
and cancellation through owning parents. Dedicated conformance tests preserve
those cases; the probes are not a claim of exhaustive equivalence.

## Test coverage

The branch adds 155 incremental test declarations and two SDL-export regressions.
The tests cover:

| Area | Cases and assertions |
| --- | --- |
| Schema and validation | Defaults, coercion, custom directives alongside built-ins, adapted names, labels, skipped selections, reused fragments, unselected operations, and all overlapping-stream pairs |
| Collection and delivery | Aliases, abstract types, nested defer/stream, shared fields, initial omission, prefix limits, resolver-once behavior, and final reconstructed data |
| Errors and lifecycle | Initial/deferred/streamed failures, nullable and non-null boundaries, shared-owner cancellation, formatter redaction, error paths/locations/extensions, and early halt |
| Middleware and options | Async, Batch, Dataloader, repeated suspension, serial mutations, context, callbacks, custom execution/result phases, and continuation errors |
| Relay | Labeled snapshots, ancestor ordering, absolute stream indices, scoped errors, null replay, extensions and early halt |
| Consumer contract | Unique pending IDs, owned updates, valid list indices, duplicate-field rejection, exactly-once completion, and terminal state |

One test checks 768 deterministic combinations of shared defer parents, nested
fragments, stream enablement/prefixes, and populated/empty/null lists. Its initial
data oracle is independent of the returned payload; its final data is compared
with eager execution. Resolver traces establish demand without timing sleeps.
These cases are a bounded product, not an exhaustive schema generator.

A separate coverage audit caught all ten representative injected faults:
incorrect conditions, prefix size and stream indices, missing completion,
bypassed pruning, stale ownership, custom-directive capture, incomplete overlap
enumeration, eager execution, and ignored inline defer. Source mutations were
restored before verification. This is not an exhaustive mutation-testing score.

The diagnostic `mix run benchmarks/incremental_delivery.exs` reports median
initial and continuation times for nullable values, shared groups, nested groups,
and streams containing deferred fields. These measurements are not CI timing
assertions. Cancellation after actual failures still scans queued work.

## Verification

Local checks on 2026-09-19:

| Check | Result |
| --- | --- |
| Clean full suite, Elixir 1.20.3 / OTP 29.0.5, compiled provider | 1,660 tests, zero failures, 3 existing exclusions |
| Clean full suite, Elixir 1.20.3 / OTP 29.0.5, persistent-term provider | 1,660 tests, zero failures, 3 existing exclusions |
| Clean full suite, Elixir 1.19.5 / OTP 28.5, compiled provider | 1,660 tests, zero failures, 3 existing exclusions |
| Clean full suite, Elixir 1.19.5 / OTP 28.5, persistent-term provider | 1,660 tests, zero failures, 3 existing exclusions |
| `mix dialyzer` | Zero errors; ignore entries unchanged |
| Formatting and `git diff --check` | Passed |
| `mix docs` | Passed with existing documentation warnings |
| Integrated HTTP harness | 49 passed: 26 Relay, 23 locally patched Apollo; no failures or skips |

Full suites use `mix test --warnings-as-errors`. None of the incremental tests
is skipped. These local runs do not cover every operating system or CI runtime
combination.

The [HTTP harness](integration/incremental_http/README.md) runs in this branch's
CI using real Apollo HTTP requests and compiled Relay operations. It covers
progressive results, normalized caches, connections and pagination, suspended
mutations, errors, truncation and cancellation. Resolver gates establish demand
and ordering; cancellation checks sockets and monitored request workers.

Apollo 4.3.1 still has defects in nested streams and reader cancellation. The
harness explicitly applies a pinned, test-only client patch and includes the
corresponding source fixes and upstream regression tests. Its tests now require
correct nested data and cancellation without unhandled rejections. Those tests
failed on the stock client before the patch. This verifies locally fixed Apollo,
not the published package. Relay 21.0.1 is unmodified.

The harness does not certify a production Absinthe Plug adapter, browsers,
proxy buffering, HTTP/2, SSE, WebSockets or cancellation of arbitrary
resolver-owned background work. Core payload support remains tied to the pinned
draft. Client versions, patch provenance and verification limits are recorded
in the harness README.
