# Unreleased Apollo client fixes

[source.patch](source.patch) contains two TypeScript fixes, six regression cases,
and two changesets against Apollo Client **4.3.1**, source revision
[`4bb0b7ad64e5a928bddab32a7f398c911f24579d`](https://github.com/apollographql/apollo-client/tree/4bb0b7ad64e5a928bddab32a7f398c911f24579d).
These changes have not been published or submitted upstream.

In an Apollo checkout at that revision, apply the patch with `git apply`, then
install its locked dependencies. Verification commands from the Apollo root:

```sh
npm test -- --selectProjects "Core Tests" --runInBand --runTestsByPath \
  src/incremental/handlers/__tests__/graphql17Alpha9/stream.test.ts \
  src/incremental/handlers/__tests__/graphql17Alpha9/defer.test.ts \
  src/link/http/__tests__/responseIterator.ts \
  src/link/http/__tests__/readMultipartBody.test.ts
npm run build
npx tsc --project config/tsconfig.json
npx tsc --noEmit --project tsconfig.json
```

The [runtime patch](../patches/@apollo+client+4.3.1.patch) contains the matching
emitted ESM and CommonJS changes. The harness installs the exact npm package
from its lockfile and applies this patch explicitly with `npm run client:patch`.
It does not replace Apollo's parser, link or cache. Source maps remain those
of the released package; use the source build when debugging the changed code.

The stream fix initializes each newly introduced list from its delivered prefix
at the full response path, before merging cached data. Its regressions cover
zero/nonzero prefixes, parent and child items in the same or separate response,
independent parent indices, and stale cached tails.

The cancellation fix awaits `reader.cancel()`, allowing the HTTP link to handle
the promise rejection. Tests verify that unexpected cleanup failures propagate
and that canceling an already errored stream retains the original read error.
No rejection handler suppresses failures in the HTTP harness.

Verified on Node 24.21.0:

- All four stream cases failed before the fix; cancellation reproduced the
  unhandled rejection. Afterward, the four focused suites passed **92 tests**,
  with 36 pre-existing skipped cases.
- Source build, source/test type checking, formatting and diff checks passed.
- Emitted ESM and CommonJS builds reconstructed captured Absinthe and GraphQL.js
  nested streams and canceled real multipart HTTP requests without unhandled
  rejections.
- The broader HttpLink suite has an existing `whatwg stream bodies` timing
  failure at `src/link/http/__tests__/HttpLink.ts:1875`, reproduced on pristine
  source. Its producer waits 10ms per line against a 100ms assertion deadline.
- ESLint could not initialize without the generated
  `docs/public/canonical-references.json`; no lint suppressions were added.

The Absinthe HTTP suite independently tests both fixes through the actual
clients and transport. These results describe the locally patched client;
stock 4.3.1 still fails those regressions.
