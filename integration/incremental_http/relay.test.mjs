import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { copyFile, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { observeRelay } from "./relay-client.mjs";
import { data, observe as observeApollo } from "./client.mjs";
import { startServer } from "./server.mjs";

import AbstractQuery from "./relay/__generated__/RelayCasesAbstractQuery.graphql.js";
import AncestorPathQuery from "./relay/__generated__/RelayCasesAncestorPathQuery.graphql.js";
import AncestorQuery from "./relay/__generated__/RelayCasesAncestorQuery.graphql.js";
import ConnectionQuery from "./relay/__generated__/RelayConnectionQuery.graphql.js";
import DeferQuery from "./relay/__generated__/RelayDeferQuery.graphql.js";
import FailureQuery from "./relay/__generated__/RelayCasesFailureQuery.graphql.js";
import FatalQuery from "./relay/__generated__/RelayCasesFatalQuery.graphql.js";
import LateWorkQuery from "./relay/__generated__/RelayCasesLateWorkQuery.graphql.js";
import NestedQuery from "./relay/__generated__/RelayCasesNestedQuery.graphql.js";
import NestedStreamsQuery from "./relay/__generated__/RelayCasesNestedStreamsQuery.graphql.js";
import NullEdgeConnectionQuery from "./relay/__generated__/RelayConnectionQueryNullEdgeQuery.graphql.js";
import NullErrorQuery from "./relay/__generated__/RelayCasesNullErrorQuery.graphql.js";
import NullItemQuery from "./relay/__generated__/RelayCasesNullItemQuery.graphql.js";
import NullNestedQuery from "./relay/__generated__/RelayCasesNullNestedQuery.graphql.js";
import SharedQuery from "./relay/__generated__/RelayCasesSharedQuery.graphql.js";
import SlowQuery from "./relay/__generated__/RelayCasesSlowQuery.graphql.js";
import StreamErrorsQuery from "./relay/__generated__/RelayCasesStreamErrorsQuery.graphql.js";
import StreamFatalQuery from "./relay/__generated__/RelayCasesStreamFatalQuery.graphql.js";
import StreamQuery from "./relay/__generated__/RelayCasesStreamQuery.graphql.js";
import MutationQuery from "./relay/__generated__/RelayMutationQuery.graphql.js";
import MutationDetails from "./relay/__generated__/RelayMutationQuery_details.graphql.js";
import MutationFailureQuery from "./relay/__generated__/RelayMutationFailureQuery.graphql.js";
import MutationLater from "./relay/__generated__/RelayMutationFailureQuery_later.graphql.js";

import Age from "./relay/__generated__/RelayCases_age.graphql.js";
import Details from "./relay/__generated__/RelayDeferQuery_details.graphql.js";
import Failure from "./relay/__generated__/RelayCases_failure.graphql.js";
import Friends from "./relay/__generated__/RelayCases_friends.graphql.js";
import Inner from "./relay/__generated__/RelayCases_inner.graphql.js";
import LateInner from "./relay/__generated__/RelayCases_lateInner.graphql.js";
import LateInnerLeaf from "./relay/__generated__/RelayCases_lateInnerLeaf.graphql.js";
import LateOuter from "./relay/__generated__/RelayCases_lateOuter.graphql.js";
import LateOuterLeaf from "./relay/__generated__/RelayCases_lateOuterLeaf.graphql.js";
import Name from "./relay/__generated__/RelayCases_name.graphql.js";
import NodeA from "./relay/__generated__/RelayCases_nodeA.graphql.js";
import NodeB from "./relay/__generated__/RelayCases_nodeB.graphql.js";
import Outer from "./relay/__generated__/RelayCases_outer.graphql.js";
import OuterPath from "./relay/__generated__/RelayCases_outerPath.graphql.js";
import SharedA from "./relay/__generated__/RelayCases_a.graphql.js";
import SharedB from "./relay/__generated__/RelayCases_b.graphql.js";

let server;
before(async () => {
  server = await startServer();
});
after(async () => {
  if (server) await server.close();
});

test("Relay compiled defer reconstructs a named fragment", async (t) => {
  const client = observeRelay(server, t, DeferQuery);
  const initial = await client.initial();
  assert.equal(initial.data.hero.id, "1");
  assert.equal(client.fragment(Details, initial.data.hero).isMissingData, true);
  assert.ok(!paths(client.id).some((path) => path.at(-1) === "display"));
  const final = await client.finish();
  assert.equal(client.fragment(Details, final.data.hero).data.display, "Ada");
  for (const payload of client.raw) {
    assert.deepEqual(payload.extensions, {
      trace: "preserved",
      is_final: !payload.hasNext,
    });
  }
});

test("Relay and Apollo negotiate independent formats on the same endpoint", async (t) => {
  const options = { variables: { initial: 1 } };
  const relay = observeRelay(server, t, NestedQuery, options);
  const apollo = observeApollo(server, t, NestedQuery.params.text, {
    ...options,
    fetchPolicy: "network-only",
  });
  const [relayInitial, apolloInitial] = await Promise.all([
    relay.initial(),
    apollo.initial(),
  ]);
  assert.equal(relayInitial.data.hero.id, "1");
  assert.deepEqual(data(apolloInitial), { hero: { id: "1" } });
  assert.equal(server.sessions.get(relay.id).mode, "relay");
  assert.equal(server.sessions.get(apollo.id).mode, "draft");
  assert.ok(!("pending" in relay.raw[0]));
  assert.equal(apollo.raw[0].pending.length, 1);

  const apolloFinal = data(await apollo.finish());
  assert.deepEqual(data(apollo.cached()), apolloFinal);
  assert.deepEqual(apolloFinal, {
    hero: {
      id: "1",
      crew: [
        { id: "2", name: "Grace", age: 40 },
        { id: "3", name: "Edsger", age: 41 },
      ],
    },
  });
  assert.equal(relay.raw.length, 1);
  assert.ok(!server.paths(relay.id).some((path) => path.includes("crew")));

  const relayFinal = await relay.finish();
  const friends = relay.fragment(Friends, relayFinal.data.hero);
  assert.equal(friends.isMissingData, false);
  const people = friends.data.crew.map((person) => {
    const name = relay.fragment(Name, person);
    const age = relay.fragment(Age, name.data);
    assert.equal(name.isMissingData, false);
    assert.equal(age.isMissingData, false);
    return { id: person.id, name: name.data.name, age: age.data.age };
  });
  assert.deepEqual(people, apolloFinal.hero.crew);
  assert.equal(relay.raw.at(-1).extensions.is_final, true);
  assert.equal(apollo.raw.at(-1).hasNext, false);
});

test("Relay rejects a truncated response before deferred work completes", async (t) => {
  const client = observeRelay(server, t, DeferQuery, { truncate: true });
  await assert.rejects(
    client.finish(),
    /Relay response ended before its terminal payload/,
  );
  assert.equal(client.read().data.hero.id, "1");
  assert.equal(
    client.fragment(Details, client.read().data.hero).isMissingData,
    true,
  );
  assert.equal(server.sessions.get(client.id).stopped, ":killed");
  assert.ok(!paths(client.id).some((path) => path.at(-1) === "display"));
});

test("Relay accepts ordinary JSON fallback without incremental terminal markers", async (t) => {
  const verifyWarning = recordReplayWarning(t, DeferQuery);
  const client = observeRelay(server, t, DeferQuery, {
    accept: "application/json",
  });
  const final = await client.finish();
  assert.equal(client.fragment(Details, final.data.hero).data.display, "Ada");
  assert.equal(client.raw.length, 1);
  assert.equal(client.raw[0].hasNext, undefined);
  assert.equal(client.raw[0].extensions, undefined);
  verifyWarning();
});

test("raw modern incremental frames are not the Relay protocol", async (t) => {
  const client = observeRelay(server, t, DeferQuery, {
    accept: "multipart/mixed;incrementalSpec=v0.2",
  });
  await client.initial();
  await assert.rejects(client.finish(), /No data returned/);
});

const paths = (id) => server.paths(id);

test("Relay mutation waits for a suspended eager child before its second root", async (t) => {
  const client = observeRelay(server, t, MutationQuery);
  await server.wait(
    client.id,
    (s) => s.events.some((event) => event.event === "blocked"),
    "first mutation child suspended",
  );
  assert.deepEqual(paths(client.id), [["first"], ["first", "id"]]);
  assert.deepEqual(client.raw, []);
  server.releaseResolver(client.id);

  const initial = await client.initial();
  assert.equal(initial.data.first.id, "1");
  assert.equal(initial.data.first.delayedId, "1");
  assert.equal(initial.data.second.id, "2");
  assert.equal(
    client.fragment(MutationDetails, initial.data.first).isMissingData,
    true,
  );
  assert.deepEqual(paths(client.id), [
    ["first"],
    ["first", "id"],
    ["first", "delayedId"],
    ["second"],
    ["second", "id"],
  ]);

  const final = await client.finish();
  assert.equal(
    client.fragment(MutationDetails, final.data.first).data.name,
    "Ada",
  );
  assert.deepEqual(paths(client.id).at(-1), ["first", "name"]);
  assert.equal(client.raw.at(-1).extensions.is_final, true);
});

test("Relay mutation failure stops later deferred work after both roots have executed", async (t) => {
  const client = observeRelay(server, t, MutationFailureQuery);
  const initial = await client.initial();
  assert.equal(initial.data.first.id, "1");
  assert.equal(initial.data.second.id, "2");
  assert.equal(
    client.fragment(MutationLater, initial.data.second).isMissingData,
    true,
  );
  const eagerPaths = [["first"], ["first", "id"], ["second"], ["second", "id"]];
  assert.deepEqual(paths(client.id), eagerPaths);

  await assert.rejects(client.finish(), /required value unavailable/);
  assert.equal(client.raw.length, 2);
  assert.equal(client.raw.at(-1).hasNext, false);
  assert.equal(client.raw.at(-1).extensions.is_final, true);
  assert.deepEqual(client.raw.at(-1).errors[0].path, [
    "first",
    "requiredFailure",
  ]);
  assert.equal(client.environment.getStore().getSource().get("2").id, "2");
  assert.equal(client.read().data.second.id, "2");
  assert.equal(
    client.fragment(MutationLater, client.read().data.second).isMissingData,
    true,
  );
  assert.deepEqual(paths(client.id), eagerPaths);
  assert.equal(server.sessions.get(client.id).stopped, ":normal");
});

test("Relay streams aliased objects progressively using variable initialCount", async (t) => {
  const client = observeRelay(server, t, StreamQuery, {
    variables: { initial: 1, enabled: true },
    fragmented: true,
  });
  assert.equal((await client.initial()).data.roster.length, 1);
  assert.equal(paths(client.id).filter((p) => p.length === 1).length, 1);
  await client.next();
  assert.equal(client.read().data.roster.length, 2);
  const final = await client.finish();
  assert.deepEqual(final.data.roster, [
    { id: "1", __typename: "Person", name: "Ada" },
    { id: "2", __typename: "Person", name: "Grace" },
    { id: "3", __typename: "Person", name: "Edsger" },
  ]);
  assert.deepEqual(
    client.raw.filter((p) => p.label).map((p) => p.path),
    [
      ["roster", 1],
      ["roster", 2],
    ],
  );
  assert.equal(client.raw.at(-1).extensions.is_final, true);
});

test("disabled incremental directives produce an ordinary Relay result", async (t) => {
  const client = observeRelay(server, t, StreamQuery, {
    variables: { initial: 0, enabled: false },
  });
  assert.equal((await client.finish()).data.roster.length, 3);
  assert.equal(client.raw.length, 1);
});

test("Relay reads nested deferred fragments inside a stream inside a deferred fragment", async (t) => {
  const client = observeRelay(server, t, NestedQuery, {
    variables: { initial: 1 },
  });
  const initial = await client.initial();
  assert.equal(client.fragment(Friends, initial.data.hero).isMissingData, true);
  const final = await client.finish();
  const friends = client.fragment(Friends, final.data.hero);
  assert.equal(friends.isMissingData, false);
  const people = friends.data.crew.map((person) => {
    const name = client.fragment(Name, person);
    const age = client.fragment(Age, name.data);
    assert.equal(name.isMissingData, false);
    assert.equal(age.isMissingData, false);
    return { id: person.id, name: name.data.name, age: age.data.age };
  });
  assert.deepEqual(people, [
    { id: "2", name: "Grace", age: 40 },
    { id: "3", name: "Edsger", age: 41 },
  ]);
});

test("Relay normalizes nested streams from independent list items", async (t) => {
  const client = observeRelay(server, t, NestedStreamsQuery);
  const final = await client.finish();
  assert.deepEqual(final.data.people[0].friends, [
    { id: "2", name: "Grace" },
    { id: "3", name: "Edsger" },
  ]);
  assert.equal(final.data.people[1].friends, null);
  assert.equal(final.data.people[2].friends, null);
  assert.equal(final.isMissingData, false);
});

test("shared deferred fields populate both compiled fragments and resolve once", async (t) => {
  const client = observeRelay(server, t, SharedQuery);
  const final = await client.finish();
  assert.deepEqual(client.fragment(SharedA, final.data.person).data, {
    name: "Ada",
    age: 37,
  });
  assert.deepEqual(client.fragment(SharedB, final.data.person).data, {
    name: "Ada",
    friend: { id: "2", name: "Grace" },
  });
  assert.equal(
    paths(client.id).filter((p) => JSON.stringify(p) === '["person","name"]')
      .length,
    1,
  );
});

test("a no-work deferred ancestor still creates Relay's nested placeholder", async (t) => {
  const client = observeRelay(server, t, AncestorQuery);
  const final = await client.finish();
  const outer = client.fragment(Outer, final.data.person);
  assert.equal(outer.isMissingData, false);
  const inner = client.fragment(Inner, outer.data);
  assert.equal(inner.isMissingData, false);
  assert.equal(inner.data.name, "Ada");
});

test("a deferred group acquires its first work when an outer linked field resolves", async (t) => {
  const client = observeRelay(server, t, LateWorkQuery);
  const initial = await client.initial();
  assert.equal(initial.data.person.id, "1");
  assert.equal(
    client.fragment(LateOuter, initial.data.person).isMissingData,
    true,
  );
  assert.deepEqual(paths(client.id), [["person"], ["person", "id"]]);

  await client.next();
  const outer = client.fragment(LateOuter, client.read().data.person);
  assert.equal(outer.isMissingData, false);
  assert.equal(outer.data.friend.id, "2");
  assert.equal(client.fragment(LateInner, outer.data).isMissingData, true);
  assert.equal(
    client.fragment(LateOuterLeaf, outer.data.friend).isMissingData,
    true,
  );
  assert.deepEqual(paths(client.id), [
    ["person"],
    ["person", "id"],
    ["person", "friend"],
    ["person", "friend", "id"],
  ]);

  await client.next();
  const inner = client.fragment(LateInner, outer.data);
  assert.equal(inner.isMissingData, false);
  assert.equal(inner.data.friend.age, 40);
  assert.equal(
    client.fragment(LateInnerLeaf, inner.data.friend).isMissingData,
    true,
  );
  assert.deepEqual(paths(client.id).at(-1), ["person", "friend", "age"]);
  assert.ok(!paths(client.id).some((path) => path.at(-1) === "name"));

  const final = await client.finish();
  const finalOuter = client.fragment(LateOuter, final.data.person);
  const finalInner = client.fragment(LateInner, finalOuter.data);
  for (const [fragment, reference] of [
    [LateOuterLeaf, finalOuter.data.friend],
    [LateInnerLeaf, finalInner.data.friend],
  ]) {
    const leaf = client.fragment(fragment, reference);
    assert.equal(leaf.isMissingData, false);
    assert.deepEqual(leaf.data, { name: "Grace", id: "2" });
  }
  assert.equal(
    paths(client.id).filter((path) => path.at(-1) === "name").length,
    1,
  );
  assert.deepEqual(
    client.raw
      .filter((payload) => payload.label)
      .map((payload) => payload.label),
    [
      "RelayCasesLateWorkQuery$defer$RelayCases_lateOuter",
      "RelayCases_lateOuter$defer$RelayCases_lateInner",
      "RelayCases_lateInner$defer$RelayCases_lateInnerLeaf",
      "RelayCases_lateOuter$defer$RelayCases_lateOuterLeaf",
    ],
  );
  assert.equal(client.raw.at(-1).extensions.is_final, true);
});

test("nullable deferred field errors keep data and use patch-relative error paths", async (t) => {
  const client = observeRelay(server, t, FailureQuery);
  const final = await client.finish();
  assert.deepEqual(client.fragment(Failure, final.data.person).data, {
    name: "Ada",
    failure: null,
  });
  const errorPayload = client.raw.find((p) => p.errors?.length);
  assert.deepEqual(errorPayload.errors[0].path, ["failure"]);
  assert.equal(errorPayload.errors[0].message, "unavailable");
});

test("a failed deferred group terminates Relay while retaining normalized initial data", async (t) => {
  const client = observeRelay(server, t, FatalQuery);
  await client.initial();
  await assert.rejects(client.finish(), /required value unavailable/);
  assert.equal(client.environment.getStore().getSource().get("1").id, "1");
  assert.ok(!paths(client.id).some((p) => p.at(-1) === "slow"));
});

test("Relay unsubscribe cancels queued deferred work", async (t) => {
  const client = observeRelay(server, t, SlowQuery);
  await client.initial();
  client.subscription.unsubscribe();
  await server.wait(
    client.id,
    (s) => s.closed && s.stopped === ":killed",
    "Relay cancellation",
  );
  assert.ok(!paths(client.id).some((p) => p.at(-1) === "slow"));
});

test("Relay unsubscribe cancels an executing deferred resolver", async (t) => {
  const client = observeRelay(server, t, SlowQuery);
  await client.initial();
  server.next(client.id);
  await server.wait(
    client.id,
    (s) => s.events.some((e) => e.event === "blocked"),
    "resolver blocked",
  );
  client.subscription.unsubscribe();
  await server.wait(
    client.id,
    (s) => s.closed && s.stopped === ":killed",
    "running resolver cancelled",
  );
});

test("compiled stream_connection populates Relay's connection and merges the next page", async (t) => {
  const first = observeRelay(server, t, ConnectionQuery, {
    variables: { count: 2, cursor: null, initial: 1 },
  });
  const initial = await first.initial();
  assert.deepEqual(
    initial.data.peopleConnection.edges.map((edge) => edge.node.name),
    ["Ada"],
  );
  const firstFinal = await first.finish();
  assert.deepEqual(
    firstFinal.data.peopleConnection.edges.map((e) => e.node.name),
    ["Ada", "Grace"],
  );
  assert.deepEqual(firstFinal.data.peopleConnection.pageInfo, {
    hasNextPage: true,
    hasPreviousPage: false,
    startCursor: "1",
    endCursor: "2",
  });
  assert.ok(first.raw.some((p) => p.label?.endsWith("$pageInfo")));
  const second = observeRelay(server, t, ConnectionQuery, {
    variables: { count: 1, cursor: "2", initial: 0 },
    store: first.environment.getStore(),
  });
  const secondFinal = await second.finish();
  assert.deepEqual(
    secondFinal.data.peopleConnection.edges.map((e) => e.node.name),
    ["Ada", "Grace", "Edsger"],
  );
  assert.equal(secondFinal.data.peopleConnection.pageInfo.hasNextPage, false);
  assert.equal(secondFinal.data.peopleConnection.pageInfo.endCursor, "3");
});

test("a no-work ancestor at another response path releases each nested fragment", async (t) => {
  const client = observeRelay(server, t, AncestorPathQuery);
  const final = await client.finish();
  const outer = client.fragment(OuterPath, final.data.person);
  assert.equal(outer.isMissingData, false);
  const names = outer.data.friends.map((person) => {
    const inner = client.fragment(Inner, person);
    assert.equal(inner.isMissingData, false);
    return inner.data.name;
  });
  assert.deepEqual(names, ["Grace", "Edsger"]);
});

test("null streamed objects retain their slots through a final Relay snapshot", async (t) => {
  const verifyWarning = recordReplayWarning(t, NullItemQuery);
  const client = observeRelay(server, t, NullItemQuery);
  await client.initial();
  await client.next();
  assert.equal(client.read().data.nullablePeople[0].name, "Ada");
  const final = await client.finish();
  assert.deepEqual(final.data.nullablePeople, [
    { id: "1", name: "Ada" },
    null,
    { id: "2", name: "Grace" },
  ]);
  assert.equal(final.isMissingData, false);
  verifyWarning();
});

test("the pinned Relay compiler rejects scalar-list streaming", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "absinthe-relay-compiler-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await copyFile(
    new URL("./relay/schema.graphql", import.meta.url),
    join(directory, "schema.graphql"),
  );
  await writeFile(
    join(directory, "relay.config.json"),
    JSON.stringify({
      src: ".",
      schema: "schema.graphql",
      language: "javascript",
    }),
  );
  await writeFile(
    join(directory, "ScalarQuery.js"),
    'import { graphql } from "relay-runtime"; export const query = graphql`query ScalarQuery { numbers @stream(initialCount: 0, if: true) }`;',
  );
  await assert.rejects(
    promisify(execFile)(
      fileURLToPath(
        new URL("./node_modules/.bin/relay-compiler", import.meta.url),
      ),
      ["--noWatchman"],
      { cwd: directory },
    ),
    (error) =>
      /Invalid use of @stream on scalar field 'numbers'/.test(
        error.stdout + error.stderr,
      ),
  );
});

test("nullable streamed field errors normalize each object with relative error paths", async (t) => {
  const client = observeRelay(server, t, StreamErrorsQuery);
  const final = await client.finish();
  assert.deepEqual(final.data.people, [
    { id: "1", failure: null },
    { id: "2", failure: null },
    { id: "3", failure: null },
  ]);
  assert.equal(final.isMissingData, false);
  const packets = client.raw.filter((payload) => payload.errors?.length);
  assert.equal(packets.length, 3);
  assert.deepEqual(
    packets.map((payload) => payload.path),
    [
      ["people", 0],
      ["people", 1],
      ["people", 2],
    ],
  );
  for (const packet of packets) {
    assert.deepEqual(packet.errors[0].path, ["failure"]);
    assert.equal(packet.errors[0].message, "unavailable");
    assert.equal(packet.errors[0].code, "OFFLINE");
    assert.ok(packet.errors[0].locations.length > 0);
  }
});

test("a non-null stream item failure terminates Relay and retains the delivered prefix", async (t) => {
  const client = observeRelay(server, t, StreamFatalQuery);
  assert.deepEqual((await client.initial()).data.requiredPeople, [
    { id: "1", name: "Ada" },
  ]);
  await assert.rejects(
    client.finish(),
    /Cannot return null for non-nullable field/,
  );
  assert.deepEqual(client.read().data.requiredPeople, [
    { id: "1", name: "Ada" },
  ]);
  assert.deepEqual(client.raw.at(-1).errors[0].path, ["requiredPeople", 1]);
  assert.equal(client.raw.at(-1).extensions.is_final, true);
  assert.ok(
    !paths(client.id).some(
      (path) => path[0] === "requiredPeople" && path[1] === 2,
    ),
  );
});

// This documents Relay's development diagnostic without hiding other warnings.
function recordReplayWarning(t, query, count = 1) {
  const error = t.mock.method(console, "error");
  const message = `Warning: RelayModernEnvironment: Operation \`${query.params.name}\` contains @defer/@stream directives but was executed in non-streaming mode. See https://fburl.com/relay-incremental-delivery-non-streaming-warning.`;
  return () =>
    assert.deepEqual(
      error.mock.calls.map((call) => call.arguments),
      Array.from(
        { length: process.env.NODE_ENV === "production" ? 0 : count },
        () => [message],
      ),
    );
}

test("nullable streamed objects with non-null child errors replay null slots and absolute errors", async (t) => {
  const verifyWarning = recordReplayWarning(t, NullErrorQuery);
  const client = observeRelay(server, t, NullErrorQuery);
  const final = await client.finish();
  assert.deepEqual(final.data.people, [null, null, null]);
  assert.equal(final.isMissingData, false);
  assert.deepEqual(
    client.raw.at(-1).errors.map((error) => error.path),
    [
      ["people", 0, "requiredFailure"],
      ["people", 1, "requiredFailure"],
      ["people", 2, "requiredFailure"],
    ],
  );
  assert.equal(
    client.raw
      .at(-1)
      .errors.every((error) => error.message === "required value unavailable"),
    true,
  );
  verifyWarning();
});

test("final null-slot replay retains nested deferred fragments on the other items", async (t) => {
  const verifyWarning = recordReplayWarning(t, NullNestedQuery, 3);
  const client = observeRelay(server, t, NullNestedQuery);
  const final = await client.finish();
  assert.equal(final.data.nullablePeople[1], null);
  for (const [index, expectedName, expectedAge] of [
    [0, "Ada", 37],
    [2, "Grace", 40],
  ]) {
    const name = client.fragment(Name, final.data.nullablePeople[index]);
    const age = client.fragment(Age, name.data);
    assert.equal(name.isMissingData, false);
    assert.equal(age.isMissingData, false);
    assert.equal(name.data.name, expectedName);
    assert.equal(age.data.age, expectedAge);
  }
  verifyWarning();
});

test("shared abstract fragments retain Relay type discriminators and eager record identity", async (t) => {
  const client = observeRelay(server, t, AbstractQuery);
  const initial = await client.initial();
  assert.equal(initial.data.entity.id, "1");
  assert.equal(initial.data.entity.__typename, "Person");
  const final = await client.finish();
  const a = client.fragment(NodeA, final.data.entity);
  const b = client.fragment(NodeB, final.data.entity);
  assert.equal(a.isMissingData, false);
  assert.equal(b.isMissingData, false);
  assert.deepEqual(a.data, { id: "1", name: "Ada" });
  assert.deepEqual(b.data, { id: "1", name: "Ada", age: 37 });
  assert.equal(
    client.environment.getStore().getSource().get("1").__typename,
    "Person",
  );
  for (const packet of client.raw.filter((payload) => payload.label)) {
    assert.equal(packet.data.__isNode, "Person");
    assert.deepEqual(packet.path, ["entity"]);
  }
  assert.equal(
    paths(client.id).filter(
      (path) => JSON.stringify(path) === '["entity","name"]',
    ).length,
    1,
  );
});

test("a replayed null connection edge preserves cursors and subsequent page merging", async (t) => {
  const verifyWarning = recordReplayWarning(t, NullEdgeConnectionQuery);
  const seed = observeRelay(server, t, NullEdgeConnectionQuery, {
    variables: { count: 1, cursor: null, initial: 1 },
  });
  assert.deepEqual(
    (await seed.finish()).data.nullablePeopleConnection.edges.map(
      (edge) => edge.node.name,
    ),
    ["Ada"],
  );
  const store = seed.environment.getStore();
  const nullPage = observeRelay(server, t, NullEdgeConnectionQuery, {
    variables: { count: 1, cursor: "1", initial: 0 },
    store,
  });
  const nullFinal = await nullPage.finish();
  assert.equal(
    nullPage.raw.at(-1).data.nullablePeopleConnection.edges[0],
    null,
  );
  assert.deepEqual(
    nullFinal.data.nullablePeopleConnection.edges
      .filter(Boolean)
      .map((edge) => edge.node.name),
    ["Ada"],
  );
  assert.deepEqual(nullFinal.data.nullablePeopleConnection.pageInfo, {
    hasNextPage: true,
    hasPreviousPage: false,
    startCursor: "1",
    endCursor: "2",
  });
  const followup = observeRelay(server, t, NullEdgeConnectionQuery, {
    variables: { count: 1, cursor: "2", initial: 0 },
    store,
  });
  const final = await followup.finish();
  assert.deepEqual(
    final.data.nullablePeopleConnection.edges
      .filter(Boolean)
      .map((edge) => edge.node.name),
    ["Ada", "Edsger"],
  );
  assert.deepEqual(final.data.nullablePeopleConnection.pageInfo, {
    hasNextPage: false,
    hasPreviousPage: false,
    startCursor: "1",
    endCursor: "3",
  });
  assert.equal(final.isMissingData, false);
  verifyWarning();
});
