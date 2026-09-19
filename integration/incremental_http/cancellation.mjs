// The parent runs this probe with --unhandled-rejections=strict. Any rejected
// reader cleanup crashes the process instead of being swallowed by a handler.
import assert from "node:assert/strict";
import { observe, data } from "./client.mjs";
import { startServer } from "./server.mjs";

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
  assert.deepEqual(server.paths(deferred.id), [["person"], ["person", "id"]]);
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
  assert.deepEqual(server.paths(stream.id), [
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
  assert.deepEqual(server.paths(blocked.id), [
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
console.log(
  JSON.stringify({
    cancelled: 3,
    referenceCancelled: 1,
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
