// Run outside node:test: its unhandled-rejection hook correctly fails any
// test encountering Apollo 4.3.0's unawaited reader.cancel() rejection. This
// probe explicitly asserts that known client bug, without patching Apollo or
// suppressing unexpected rejections in the main test runner.
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { observe, data, paths } from "./client.mjs";
import { startServer, waitFor } from "./server.mjs";

const events = new EventEmitter();
const rejections = [];
process.on("unhandledRejection", (error) => {
  rejections.push(error);
  events.emit("change");
});
const server = await startServer();
let reference;
const cleanups = [];
const scope = { after: (cleanup) => cleanups.push(cleanup) };
try {
  reference = await startServer({ reference: true });
  const deferred = observe(
    server,
    scope,
    "{ person { id ... @defer { name } } }",
  );
  await deferred.initial();
  deferred.subscription.unsubscribe();
  await stopped(deferred);
  assert.deepEqual(paths(server, deferred.id), [["person"], ["person", "id"]]);
  assert.equal(deferred.raw.length, 1);

  const stream = observe(
    server,
    scope,
    "{ people @stream(initialCount: 0) { name } }",
  );
  assert.deepEqual(data(await stream.initial()), { people: [] });
  await stream.next();
  assert.deepEqual(data(stream.results.at(-1)), { people: [{ name: "Ada" }] });
  stream.subscription.unsubscribe();
  await stopped(stream);
  assert.deepEqual(paths(server, stream.id), [
    ["people"],
    ["people", 0, "name"],
  ]);
  assert.equal(stream.raw.length, 2);

  const blocked = observe(
    server,
    scope,
    "{ people @stream(initialCount: 0) { slow name } }",
  );
  await blocked.initial();
  server.next(blocked.id);
  await server.wait(
    blocked.id,
    (s) => s.events.some((e) => e.event === "blocked"),
    "resolver gate",
  );
  blocked.subscription.unsubscribe();
  await stopped(blocked);
  assert.deepEqual(paths(server, blocked.id), [
    ["people"],
    ["people", 0, "slow"],
  ]);
  assert.equal(blocked.raw.length, 1);

  const comparison = observe(
    reference,
    scope,
    "{ person { id ... @defer { name } } }",
  );
  await comparison.initial();
  comparison.subscription.unsubscribe();
  await stopped(comparison, reference);
  assert.equal(comparison.raw.length, 1);

  await waitFor(
    events,
    () => rejections.length >= 4,
    "known Apollo reader.cancel rejections",
  );
  assert.equal(rejections.length, 4);
  for (const error of rejections) {
    assert.equal(error.name, "AbortError");
    assert.match(error.stack, /@apollo\/client\/link\/http\/BaseHttpLink/);
  }
} finally {
  cleanups.forEach((cleanup) => cleanup());
  const shutdown = await Promise.allSettled([
    server.close(),
    reference?.close(),
  ]);
  for (const result of shutdown) {
    if (result.status === "rejected") throw result.reason;
  }
}
assert.equal(rejections.length, 4, "No extra rejections during cleanup");
console.log(
  JSON.stringify({
    cancelled: 3,
    referenceCancelled: 1,
    knownApolloAbortRejections: rejections.length,
  }),
);

async function stopped(operation, backend = server) {
  await backend.wait(
    operation.id,
    (s) => s.disconnected && s.stopped,
    "socket closed and worker DOWN",
  );
  const session = backend.sessions.get(operation.id);
  assert.equal(session.stopped, ":killed");
  assert.deepEqual(session.payloads, operation.raw);
}
