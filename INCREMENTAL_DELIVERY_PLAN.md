# Incremental delivery implementation plan

## Target and completion criteria

Implement opt-in `@defer` and `@stream` execution in Absinthe against
[GraphQL spec PR #1110](https://github.com/graphql/graphql-spec/pull/1110), pinned
to `045e19363c2b55f127960bd3b5e8072a15b29aec`. The proposal is still a draft;
support here must identify this revision and describe any unresolved draft
ambiguities. The target is real delayed execution and ordered incremental
results, including nested and overlapping selections, middleware, and errors.

Completion requires working public execution APIs, schema opt-in, validation,
conformance tests, an executable usage guide, clean formatting and compilation,
both schema-provider test runs, and Dialyzer. All existing behavior must remain
compatible for schemas and requests that do not opt into incremental execution.
Record actual checks and remaining limitations below; a test-count claim alone
does not establish conformance.

Absinthe is the execution library. Its end-to-end boundary is a transport-neutral
sequence of spec-shaped payload maps consumed by an adapter. HTTP negotiation,
multipart encoding, SSE, and WebSocket framing belong to their respective
transport packages. Provide and test a consumer example, without claiming that
unmodified transport packages understand a new return type.

## Lessons from the previous implementation

[Absinthe PR #1377](https://github.com/absinthe-graphql/absinthe/pull/1377) was
merged and subsequently removed from main by force-push. The code was retained
on `defer-stream-wip`. The maintainer's
[removal explanation](https://github.com/absinthe-graphql/absinthe/pull/1377#issuecomment-4068271138)
cites broken CI, warnings, unrelated development changes, generated artifacts,
unimplemented or untested documented functionality, and premature acceptance of
a draft feature. This work must therefore:

- Keep schema support explicit and identify the draft protocol revision.
- Add no unrelated tools, dependencies, application configuration, supervisors,
  transport frameworks, custom job backends, or speculative extension points.
- Preserve Absinthe's existing resolver, middleware, plugin, batching, adapter,
  and error behavior rather than implementing a second resolver stack.
- Keep incremental scheduling separate from payload formatting. Preserve field
  occurrence provenance so overlapping eager/deferred fields resolve once.
- Prove that delayed fields and tail items have not run when the initial result
  becomes available, and that stopping consumption stops future work.
- Test negative and combined cases, not just happy-path response examples.
- Keep research scratch files outside the package and documentation limited to
  behavior that is implemented and demonstrated by tests.

## Design decisions

1. **Two explicit opt-ins.** A schema imports
   `Absinthe.Type.BuiltIns.IncrementalDirectives`. A caller uses a separate
   incremental execution API. Ordinary `Absinthe.run/3` retains its single-result
   contract and executes valid imported directives eagerly. Validation remains
   active in either API.
2. **Demand-driven execution.** The new API produces the initial response and a
   lazy enumerable of subsequent responses when incremental work exists;
   otherwise it produces the ordinary result. Resume work in the consuming
   process. Do not create background work merely by defining deferred selections.
   This provides natural backpressure and makes early halt predictable.
3. **One middleware engine.** Integrate with the existing resolution phase and
   its suspended-field pool. Run the existing plugin pipeline to completion for
   each execution group. Carry context, accumulator, fragments, and appropriate
   caches forward. Do not replay an entire query for each patch.
4. **Spec field collection.** Preserve each field occurrence with its enclosing
   defer usage; collect named fragments with context-sensitive visited state;
   partition fields by filtered defer-usage sets. Eager occurrences dominate
   deferred ones, and ancestor defer usages dominate descendant usages for the
   same response name. Keep all child selections for later partitioning.
5. **List completion.** Resolve a list field once, complete only its initial
   prefix, and keep remaining raw items for subsequent demand. Stream the
   outermost list only. A tail error must not invalidate the initial prefix.
6. **Default wire format.** Use string `id`s, `pending` notices with response
   paths and optional labels, `incremental` object/list records, optional
   `subPath`, `completed` notices, and a terminal `hasNext: false`. Relay
   compatibility uses an explicitly selected format, described below.
7. **Errors and lifecycle.** Nullable errors accompany their delivered data.
   Non-null failures reaching an already-delivered incremental boundary produce
   failed completion notices, discard unusable data, and cancel unreachable
   descendant work. Every announced group completes exactly once. Initial
   failures suppress pending notices for paths that were nulled before delivery.
8. **Mutation/subscription boundary.** Reject root mutation/subscription defer
   and stream directives. Allow nested mutation incremental work while keeping
   mutation root resolution serial. Subscriptions may contain directives only
   under the proposal's disablement rules; active incremental subscription
   execution is an error, not an undocumented new subscription transport.
9. **Atomic deferred groups.** Buffer completed tasks until at least one owning
   group succeeds. Publish shared values once; a failed group discards its
   private values without cancelling another group's surviving selections.
   Newly discovered streams and child groups wait until their containing data
   has been delivered.
10. **Configured pipelines and adapters.** Preserve the configured execution
    and result phases across subsequent pulls. Carry payload extensions forward.
    Recognize directive and argument identifiers through their resolved schema
    nodes, so custom external naming works without capturing unrelated custom
    directives that happen to have the same names.

## Implementation sequence

### 1. Research and baseline

- [x] Pin both PR heads and read the maintainer removal discussion.
- [x] Inspect field projection, result construction, suspended middleware,
  plugin scheduling, directive expansion, and public execution entry points.
- [x] Establish the baseline: 1,503 tests pass, 3 excluded on Elixir 1.20.3 /
  OTP 29. The dependency Dataloader emits an existing unused-require warning.
- [x] Finish independent reviews of the old implementation and draft;
  turn their concrete findings into tests and documented decisions.

### 2. Schema and validation

- [x] Define non-repeatable directives with exact locations and defaults:
  `if: Boolean! = true`, nullable `label: String`, and stream
  `initialCount: Int! = 0`.
- [x] Reuse existing argument coercion, directive placement, duplicate directive,
  variable compatibility, and required argument validation.
- [x] Validate stream list types, document-wide unique literal labels, root
  operation restrictions, subscription disablement, and overlapping streams.
- [x] Treat negative initial count as an execution error at the selected field;
  honor skip/include and disabled stream behavior.
- [x] Test introspection and both compiled and persistent-term schema providers.

### 3. Collection and execution

- [x] Add occurrence-preserving incremental collection and execution grouping.
- [x] Integrate groups with the existing resolver engine and plugin passes.
- [x] Add list-prefix/tail completion, aliases, index paths, abstract types,
  repeated fragments, and nested list support.
- [x] Preserve per-request context and avoid cache leakage across delivery
  groups and concrete list element types.
- [x] Prove resolver-once behavior for shared eager/deferred and sibling/nested
  deferred selections with different child selections.

### 4. Public API and delivery

- [x] Provide a typed, documented incremental result containing the initial
  payload and a lazy enumerable of subsequent payloads.
- [x] Add public execution and raising variants with the usual options and
  pipeline modifier support.
- [x] Implement dependency-aware announcements, completion accounting, nested
  group release, list ordering, and clean termination.
- [x] Handle nullable errors, failed boundaries, cancellation, and early halt.
- [x] Ensure ordinary execution and requests with no effective deferred work
  return ordinary results with no spurious protocol keys.

### 5. Conformance and integration tests

- [x] Directive/coercion/validation matrix, including invalid unselected
  operations, reused fragments, null labels, variable labels, false/variable
  conditions, omitted/default/null initial count, and negative counts.
- [x] Initial payload and all subsequent payload shapes, IDs, labels, aliases,
  list indices, final completion, and a consumer that reconstructs results.
- [x] Defer inside stream, stream inside defer, nested streams, nested defers,
  overlapping deferred fragments, shared fields, and abstract types.
- [x] Nullable and non-null failures before and after the initial response,
  failed parents with pending descendants, scalar/enum serialization, and error
  extensions and locations.
- [x] Async, Batch, Dataloader, custom middleware/plugin callbacks, context,
  root values, adapters, operation selection, and complexity limits.
- [x] Delayed execution and demand bounds established by resolver messages or
  counters, without timing-sensitive sleeps.
- [x] Independent adversarial review of execution, validation and client behavior.

### 6. Documentation and final checks

- [x] Write the incremental delivery guide and API docs, including the pinned
  draft, single-result fallback, demand/lifecycle behavior, and adapter boundary.
- [x] Run formatter, warnings-as-errors compilation, full tests with both schema
  providers, and Dialyzer. Resolve introduced failures; distinguish baseline and
  environment limitations with evidence.
- [x] Review the entire diff for unnecessary abstractions, duplicate logic,
  unsupported claims, debug files, and accidental unrelated changes.
- [x] Update this plan with completed checks, known draft ambiguities, and exact
  scope.

## Draft interpretation

The pinned proposal's subscription validation pseudocode says its condition
must not be false, contradicting the surrounding disablement explanation. Its
update-result prose also says `hasNext` remains true in the final result,
contradicting the termination algorithm. Follow the coherent execution and
response semantics: subscription usages must be disableable, active directives
fail when reached during event execution, and the terminal payload has
`hasNext: false`. These choices are documented and tested. The draft leaves
parts of the stream/work-queue mechanism implementation-defined; scheduling
policy must not be mistaken for normative protocol behavior.

Cross-checks used GraphQL.js revision
[`ee5ce41d4b68d1852306d3b56dba2cbbb6c43fea`](https://github.com/graphql/graphql-js/tree/ee5ce41d4b68d1852306d3b56dba2cbbb6c43fea).
Executable reference probes confirmed the handling of shared work after one
owner fails, withholding private data until a group succeeds, and cancelling
a repeated directive node with its owning parent. Those cases have dedicated
regressions in `incremental_conformance_test.exs`.

## Lifecycle and scheduling invariants

Regression tests cover unpublished streams after parent failure, completed
shared children surviving another owner's failure, and child publication after
the last resolver job. Pruning checks both the execution tree and formatted data
so custom result phases can null containers or hide errors without leaving
unreachable work queued.

Runnable jobs use an ordered set; groups track their buffered values and waiters
wake when their dependencies change. A separate ownership index preserves
ancestry even for groups with no independent work. Failed boundaries follow
those links and restrict only affected jobs. Ordinary null values and nullable
scalar errors do not trigger scans of pending work. Nulling a container still
checks queued paths against the execution tree and formatted result.
Stream overlap validation identifies occurrences by definition and selection
position, including fields with missing or shared source locations.

The diagnostic `benchmarks/incremental_delivery.exs` measures initial and
continuation work separately with five-sample medians in draft and Relay
formats, including nullable errors and failed groups. Run it with
`mix run benchmarks/incremental_delivery.exs`; results depend on the machine
and are not CI timing assertions.

## Relay compatibility

Relay compiler/runtime 21.0.1 requires `incremental_format: :relay`. The
formatter uses execution group metadata to include deduplicated ancestors that
are absent from draft wire notices, and retains data for complete deferred
snapshots. Errors are indexed by response-path prefix so each fragment reads
its own errors without rescanning earlier failures. This costs memory and can
repeat fields on the wire. Failed boundaries terminate the Relay operation;
nullable streamed items require a
final root replay. See the [compatibility guide](guides/incremental-delivery.md#relay-compatibility).

The [HTTP harness](integration/incremental_http/README.md) generates actual Relay
artifacts from exported Absinthe SDL and checks its normalized store, fragment
readers and paginated connections, rejects truncated multipart responses, and
normalizes eagerly completed fragments in JSON fallback responses. SDL export
preserves typed macro defaults and GraphQL string literals without requiring a
started schema provider. Generated artifacts stay out of Git.

## Verification record

Baseline: 1,503 passed, 3 excluded on 2026-09-18. Latest verification on
2026-09-19 uses `mix test --warnings-as-errors` for each core suite:

| Check | Result |
| --- | --- |
| Clean full core suites, Elixir 1.20.3 / OTP 29.0.5, both schema providers | 1,629 passed per run; 3 excluded |
| Clean full core suites, Elixir 1.19.5 / OTP 28.5, both schema providers | 1,629 tests per run; 0 failures, 3 excluded |
| Fresh HTTP runner, Node 24.21.0 / Elixir 1.19.5 / OTP 28.5 | 46 passed: 22 Apollo and 24 Relay |
| `mix dialyzer` | 0 errors; no new suppressions |
| Root and harness formatting, `git diff --check` | Passed |
| `mix docs` | Passed with existing documentation warnings |

The HTTP total includes explicit characterizations of the documented Apollo
client defects and Relay protocol/compiler rejections; those cases remain
unsupported. The harness README records their evidence and pinned versions.
Documentation generation retains existing MakeupGraphql deprecations and
hidden-type reference warnings.

Absinthe Plug at `a20146ead4bdd885f3c22115fbe37b86b4330217` passed all 87 tests
against core commit `d66f7e35`, using `--max-cases 1` because its parallel
subscription fixture hit an already-started registry. This checked ordinary
execution compatibility, not production incremental HTTP support.
