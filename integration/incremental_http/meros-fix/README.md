# Unreleased meros browser parser fix

Published `meros` **1.3.2** skips a multipart boundary that spans two Fetch
chunks when the second chunk also contains a complete later boundary. Its
chunk-first search selects that later boundary, merging adjacent JSON bodies
or discarding the initial part. TCP write fragmentation cannot reliably expose
this because Fetch may coalesce the writes.

[source.patch](source.patch) removes that unsafe optimization from the browser
parser at upstream revision
[`87ed69fe97f5a250ee6e8bec1a9ba458e16655f9`](https://github.com/maraisr/meros/tree/87ed69fe97f5a250ee6e8bec1a9ba458e16655f9).
It searches the accumulated buffer in order. The corresponding
[runtime patch](../patches/meros+1.3.2.patch) applies the same change to the
published browser ESM and CommonJS modules. The Node-specific parser is unused
and unchanged. This fix has not been published or submitted upstream; npm's
latest release and upstream main still contained the defect on 2026-09-19.

The runner applies it explicitly alongside the Apollo fixes with
`npm run client:patch`. Relay **21.0.1** itself is unmodified; its network uses
this locally patched third-party parser. No payload translation is added.

The [deterministic regression](../multipart.test.mjs) feeds the actual browser
parser fixed `ReadableStream` chunks: one coalesced chunk, single-byte chunks,
and every possible two-chunk byte split across boundaries, headers, JSON and
UTF-8 characters. It checks both ESM and CommonJS against identical payloads.
The existing Relay HTTP suite exercises the patched ESM module in its network.

To reproduce against the stock package and then verify the patch:

```sh
npm ci --ignore-scripts
node --test multipart.test.mjs # both module-format regressions fail
npm run client:patch
node --test multipart.test.mjs # both pass
```
