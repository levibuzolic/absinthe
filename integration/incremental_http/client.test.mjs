import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { startServer } from "./server.mjs";
import { data, observe as observeClient } from "./client.mjs";

let server;
before(async () => {
  server = await startServer();
});
after(async () => {
  if (server) await server.close();
});
const observe = (...args) => observeClient(server, ...args);
const paths = (id) => server.paths(id);

test("Apollo receives aliased, labelled defer before delayed resolution", async (t) => {
  const operation = observe(
    t,
    '{ hero: person { id ... @defer(label: "details") { display: name } } }',
  );
  const initial = await operation.initial();
  assert.deepEqual(data(initial), { hero: { id: "1" } });
  assert.equal(initial.dataState, "streaming");
  assert.deepEqual(paths(operation.id), [["hero"], ["hero", "id"]]);
  assert.deepEqual(operation.raw[0].pending, [
    { id: "0", path: ["hero"], label: "details" },
  ]);
  assert.match(
    server.sessions.get(operation.id).accept,
    /multipart\/mixed;incrementalSpec=v0.2/,
  );
  const final = await operation.finish();
  assert.deepEqual(data(final), { hero: { id: "1", display: "Ada" } });
  assert.equal(final.dataState, "complete");
  assert.deepEqual(paths(operation.id), [
    ["hero"],
    ["hero", "id"],
    ["hero", "display"],
  ]);
});

test("Apollo writes deferred data and streamed items into its normalized cache", async (t) => {
  const operation = observe(
    t,
    "{ people @stream(initialCount: 1) { id ... @defer { name } } }",
    {
      fetchPolicy: "network-only",
    },
  );
  assert.deepEqual(data(await operation.initial()), { people: [{ id: "1" }] });
  const final = await operation.finish();
  assert.deepEqual(data(final), {
    people: [
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace" },
      { id: "3", name: "Edsger" },
    ],
  });
  assert.deepEqual(data(operation.cached()), data(final));
});

test("Apollo streams abstract items whose type depends on field arguments", async (t) => {
  const operation = observe(
    t,
    "{ people: namedPeople(person: true) @stream(initialCount: 1) { ... on Person { id name } } }",
    { fetchPolicy: "network-only" },
  );
  assert.deepEqual(data(await operation.initial()), {
    people: [{ id: "1", name: "Ada" }],
  });
  const expected = {
    people: [
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace" },
      { id: "3", name: "Edsger" },
    ],
  };
  assert.deepEqual(data(await operation.finish()), expected);
  assert.deepEqual(data(operation.cached()), expected);
});

test("Apollo preserves explicit null labels on initial and nested pending notices", async (t) => {
  const operation = observe(
    t,
    "{ people @stream(label: null, initialCount: 0) { id friends @stream(label: null, initialCount: 0) { name } } }",
    { fetchPolicy: "network-only" },
  );
  assert.deepEqual(data(await operation.initial()), { people: [] });
  const final = await operation.finish();
  assert.deepEqual(data(final), {
    people: [
      { id: "1", friends: [{ name: "Grace" }, { name: "Edsger" }] },
      { id: "2", friends: null },
      { id: "3", friends: null },
    ],
  });
  assert.deepEqual(data(operation.cached()), data(final));
  const notices = operation.raw.flatMap((payload) => payload.pending ?? []);
  assert.equal(notices.length, 2);
  assert.equal(operation.raw[0].pending.length, 1);
  for (const notice of notices) {
    assert.ok(Object.hasOwn(notice, "label"));
    assert.equal(notice.label, null);
  }
});

test("Apollo retains a nullable failed row in a streamed nested-list prefix", async (t) => {
  const operation = observe(t, "{ matrix @stream(initialCount: 2) }", {
    fetchPolicy: "network-only",
  });
  assert.deepEqual(data(await operation.initial()), { matrix: [[1], null] });
  assert.deepEqual(operation.raw[0].errors[0].path, ["matrix", 1, 0]);
  assert.deepEqual(data(await operation.finish({ expectErrors: true })), {
    matrix: [[1], null, [3]],
  });
  assert.deepEqual(data(operation.cached()), { matrix: [[1], null, [3]] });
  assert.deepEqual(
    operation.raw
      .flatMap((payload) => payload.incremental ?? [])
      .flatMap((patch) => patch.items),
    [[3]],
  );

  const required = observe(t, "{ requiredRows @stream(initialCount: 2) }");
  assert.deepEqual(data(await required.finish({ expectErrors: true })), {
    requiredRows: null,
  });
  assert.equal(required.raw.length, 1);
  assert.deepEqual(required.raw[0].errors[0].path, ["requiredRows", 1, 0]);
  assert.ok(!("pending" in required.raw[0]));
});

test("stream variables and initialCount yield progressive client arrays and resolve the list once", async (t) => {
  const operation = observe(
    t,
    `query People($count: Int!, $enabled: Boolean!) {
    roster: people @stream(initialCount: $count, if: $enabled, label: "röster 👋") { id name }
  }`,
    { variables: { count: 1, enabled: true }, fragmented: true },
  );
  assert.deepEqual(data(await operation.initial()), {
    roster: [{ id: "1", name: "Ada" }],
  });
  assert.ok(!paths(operation.id).some((path) => path[1] === 1));
  await operation.next();
  assert.deepEqual(data(operation.results.at(-1)), {
    roster: [
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace" },
    ],
  });
  const final = await operation.finish();
  assert.deepEqual(data(final), {
    roster: [
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace" },
      { id: "3", name: "Edsger" },
    ],
  });
  assert.equal(
    paths(operation.id).filter((path) => path.length === 1).length,
    1,
  );
  assert.deepEqual(
    operation.raw[0].pending.map(({ path, label }) => ({ path, label })),
    [{ path: ["roster"], label: "röster 👋" }],
  );
});

test("nested defer, stream inside defer, and defer inside streamed items reconstruct in Apollo", async (t) => {
  const operation = observe(
    t,
    `{
    hero: person { id ... @defer(label: "friends") {
      crew: friends @stream(initialCount: 1, label: "crew") {
        id ... @defer(label: "name") { name ... @defer(label: "age") { age } }
      }
    } }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), { hero: { id: "1" } });
  await operation.next();
  assert.deepEqual(data(operation.results.at(-1)), {
    hero: { id: "1", crew: [{ id: "2" }] },
  });
  assert.deepEqual(data(await operation.finish()), {
    hero: {
      id: "1",
      crew: [
        { id: "2", name: "Grace", age: 40 },
        { id: "3", name: "Edsger", age: 41 },
      ],
    },
  });
  const notices = operation.raw.flatMap((p) => p.pending ?? []);
  assert.ok(
    notices.some(
      (p) =>
        p.label === "name" && JSON.stringify(p.path) === '["hero","crew",1]',
    ),
  );
  assert.ok(
    notices.some(
      (p) =>
        p.label === "age" && JSON.stringify(p.path) === '["hero","crew",0]',
    ),
  );
});

for (const initialCount of [0, 1]) {
  test(`Apollo reconstructs nested streams with initialCount ${initialCount} from both engines`, async (t) => {
    const reference = await startServer({ reference: true });
    t.after(() => reference.close());
    const comparison = [];
    for (const backend of [server, reference]) {
      const operation = observeClient(
        backend,
        t,
        `query Nested($count: Int!) {
          people @stream(initialCount: 0) {
            friends @stream(initialCount: $count) { name }
          }
        }`,
        { variables: { count: initialCount }, fetchPolicy: "network-only" },
      );
      assert.deepEqual(data(await operation.initial()), { people: [] });
      const final = await operation.finish();
      comparison.push(operation.raw);
      const nested = operation.raw
        .flatMap((p) => p.pending ?? [])
        .find((p) => p.path.length === 3);
      assert.deepEqual(nested.path, ["people", 0, "friends"]);
      const delivered = operation.raw
        .flatMap((p) => p.incremental ?? [])
        .filter((patch) => patch.id === nested.id)
        .flatMap((patch) => patch.items);
      assert.deepEqual(
        delivered.map((person) => person.name),
        ["Grace", "Edsger"].slice(initialCount),
      );
      assert.deepEqual(data(final), {
        people: [
          { friends: [{ name: "Grace" }, { name: "Edsger" }] },
          { friends: null },
          { friends: null },
        ],
      });
      assert.deepEqual(data(operation.cached()), data(final));
    }
    assert.deepEqual(
      comparison[0][0],
      comparison[1][0],
      "Identical initial payloads",
    );
  });
}

test("overlapping eager and deferred selections share resolvers and merge subPath patches", async (t) => {
  const operation = observe(
    t,
    `{
    person { id friend { id } }
    ... @defer(label: "a") { person { name friend { name } } }
    ... @defer(label: "b") { person { name age friend { name age } } }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), {
    person: { id: "1", friend: { id: "2" } },
  });
  assert.deepEqual(data(await operation.finish()), {
    person: {
      id: "1",
      name: "Ada",
      age: 37,
      friend: { id: "2", name: "Grace", age: 40 },
    },
  });
  const resolved = paths(operation.id).map(JSON.stringify);
  assert.equal(
    new Set(resolved).size,
    resolved.length,
    "Each response path resolves once",
  );
  assert.ok(
    operation.raw
      .flatMap((p) => p.incremental ?? [])
      .some((p) => p.subPath?.length),
  );
});

test("disabled directives and requests without incremental work return ordinary JSON", async (t) => {
  const cases = [
    [
      "query($on: Boolean!) { person { id ... @defer(if: $on) { name } } numbers @stream(if: $on) }",
      { person: { id: "1", name: "Ada" }, numbers: [1, 2, 3] },
      { on: false },
    ],
    [
      "{ empty @stream { id } absent { ... @defer { name } } }",
      { empty: [], absent: null },
    ],
    ["{ numbers @stream(initialCount: 3) }", { numbers: [1, 2, 3] }],
    ["{ person { name } }", { person: { name: "Ada" } }],
  ];
  for (const [query, expected, variables] of cases) {
    const operation = observe(t, query, { variables });
    assert.deepEqual(data(await operation.finish()), expected);
    assert.equal(operation.raw.length, 1);
    assert.equal(
      operation.responses[0].type,
      "application/graphql-response+json",
    );
  }
});

test("JSON-only clients receive eager fallback for active directives", async (t) => {
  const operation = observe(
    t,
    "{ person { id ... @defer { name } } numbers @stream(initialCount: 1) }",
    {
      headers: { accept: "application/json" },
    },
  );
  assert.deepEqual(data(await operation.finish()), {
    person: { id: "1", name: "Ada" },
    numbers: [1, 2, 3],
  });
  assert.equal(operation.raw.length, 1);
});

test("nullable deferred errors retain data, absolute alias paths and locations in Apollo", async (t) => {
  const operation = observe(
    t,
    "{ hero: person { id ... @defer { broken: failure name } } }",
  );
  assert.deepEqual(data(await operation.initial()), { hero: { id: "1" } });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), {
    hero: { id: "1", broken: null, name: "Ada" },
  });
  assert.equal(final.error.errors[0].message, "unavailable");
  assert.deepEqual(final.error.errors[0].path, ["hero", "broken"]);
  assert.ok(final.error.errors[0].locations[0].column > 0);
  assert.equal(final.error.errors[0].code, "OFFLINE");
  assert.equal(
    operation.raw[1].incremental[0].errors[0].message,
    "unavailable",
  );
});

test("non-null deferred failures complete the ID with errors and suppress descendant work", async (t) => {
  const operation = observe(
    t,
    `{
    person { id ... @defer(label: "failed") {
      requiredFailure ... @defer(label: "unreachable") { name }
    } }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), { person: { id: "1" } });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), { person: { id: "1" } });
  assert.deepEqual(final.error.errors[0].path, ["person", "requiredFailure"]);
  assert.equal(
    operation.raw.at(-1).completed[0].errors[0].message,
    "required value unavailable",
  );
  assert.ok(!paths(operation.id).some((p) => p.at(-1) === "name"));
  assert.ok(
    !operation.raw
      .flatMap((p) => p.pending ?? [])
      .some((p) => p.label === "unreachable"),
  );
});

test("a deferred group spanning list items publishes only after all private tasks succeed", async (t) => {
  const operation = observe(
    t,
    `{
    people { id }
    ... @defer(label: "allPeople") { people { slow } }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), {
    people: [{ id: "1" }, { id: "2" }, { id: "3" }],
  });
  server.next(operation.id);
  for (let index = 0; index < 3; index++) {
    await server.wait(
      operation.id,
      (s) => s.events.filter((e) => e.event === "blocked").length === index + 1,
      `list item ${index} blocked`,
    );
    assert.equal(
      operation.raw.length,
      1,
      "No private data published while any task is unfinished",
    );
    server.releaseResolver(operation.id);
  }
  await server.wait(operation.id, (s) => s.stopped, "deferred group finished");
  assert.deepEqual(data(await operation.finish()), {
    people: [
      { id: "1", slow: "released" },
      { id: "2", slow: "released" },
      { id: "3", slow: "released" },
    ],
  });
});

test("a later failed task cancels a stream whose containing deferred data was never published", async (t) => {
  const operation = observe(
    t,
    `{
    person {
      friend { id }
      ... @defer(label: "failedParent") {
        friends @stream(initialCount: 0, label: "unpublished") { name }
        friend { requiredFailure }
      }
    }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), {
    person: { friend: { id: "2" } },
  });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), { person: { friend: { id: "2" } } });
  assert.deepEqual(final.error.errors[0].path, [
    "person",
    "friend",
    "requiredFailure",
  ]);
  assert.ok(
    !operation.raw
      .flatMap((p) => p.pending ?? [])
      .some((p) => p.label === "unpublished"),
  );
  assert.ok(!paths(operation.id).some((p) => p.at(-1) === "name"));
});

test("a non-null stream tail error preserves the delivered prefix and completes with errors", async (t) => {
  const operation = observe(
    t,
    "{ values: requiredNumbers @stream(initialCount: 1) }",
  );
  assert.deepEqual(data(await operation.initial()), { values: [1] });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), { values: [1] });
  assert.deepEqual(final.error.errors[0].path, ["values", 1]);
  assert.ok(operation.raw.at(-1).completed[0].errors.length);
});

test("nullable streamed items contain non-null field failures and the stream still completes", async (t) => {
  const operation = observe(
    t,
    "{ people @stream(initialCount: 0) { id requiredFailure } }",
  );
  assert.deepEqual(data(await operation.initial()), { people: [] });
  await operation.next();
  assert.deepEqual(data(operation.results.at(-1)), { people: [null] });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), { people: [null, null, null] });
  assert.deepEqual(
    final.error.errors.map((e) => e.path),
    [
      ["people", 0, "requiredFailure"],
      ["people", 1, "requiredFailure"],
      ["people", 2, "requiredFailure"],
    ],
  );
  assert.ok(
    !operation.raw.flatMap((p) => p.completed ?? []).some((c) => c.errors),
  );
});

test("a completed nested child retains shared data when a later sibling defer fails", async (t) => {
  const operation = observe(
    t,
    `{
    person {
      ... @defer(label: "a") { ... @defer(label: "child") { name } age }
      ... @defer(label: "b") { name requiredFailure }
    }
  }`,
  );
  assert.deepEqual(data(await operation.initial()), { person: {} });
  const final = await operation.finish({ expectErrors: true });
  assert.deepEqual(data(final), { person: { age: 37, name: "Ada" } });
  assert.deepEqual(final.error.errors[0].path, ["person", "requiredFailure"]);
  const notices = operation.raw.flatMap((p) => p.pending ?? []);
  assert.ok(notices.some((p) => p.label === "child"));
  assert.equal(
    paths(operation.id).filter((p) => p.at(-1) === "name").length,
    1,
  );
});

test("initial non-null errors and invalid variables return ordinary GraphQL errors", async (t) => {
  const failed = observe(
    t,
    "{ person { requiredFailure ... @defer { name } } }",
  );
  const result = await failed.finish({ expectErrors: true });
  assert.deepEqual(data(result), { person: null });
  assert.deepEqual(result.error.errors[0].path, ["person", "requiredFailure"]);
  const invalid = observe(
    t,
    "query($count: Int!) { numbers @stream(initialCount: $count) }",
    {
      variables: { count: "wrong" },
    },
  );
  const invalidResult = await invalid.finish({ expectErrors: true });
  assert.equal(
    invalidResult.error.errors[0].message,
    'Argument "initialCount" has invalid value $count.',
  );
  assert.equal(invalid.raw.length, 1);
  assert.ok(!("hasNext" in invalid.raw[0]));
  assert.deepEqual(paths(invalid.id), []);
});

test("Apollo cancellation closes sockets and workers without unhandled rejections", async () => {
  const { stdout } = await promisify(execFile)(
    process.execPath,
    ["--unhandled-rejections=strict", "cancellation.mjs"],
    {
      cwd: new URL(".", import.meta.url),
      timeout: 20_000,
    },
  );
  assert.deepEqual(JSON.parse(stdout), {
    cancelled: 4,
    referenceCancelled: 1,
  });
});

for (const { selection, field, delivered, children } of [
  { selection: "slow", field: "slow", delivered: "released", children: [] },
  {
    selection: "friend(wait: true) { id name }",
    field: "friend",
    delivered: { id: "2", name: "Grace" },
    children: ["id", "name"],
  },
]) {
  test(`initial data reaches Apollo while deferred ${field} is blocked`, async (t) => {
    const operation = observe(
      t,
      `{ person { id ... @defer { ${selection} } } }`,
    );
    await server.wait(
      operation.id,
      (s) => s.payloads.length === 1,
      "server wrote initial part",
    );
    server.next(operation.id);
    await server.wait(
      operation.id,
      (s) => s.events.some((e) => e.event === "blocked"),
      "delayed resolver gate",
    );
    assert.deepEqual(data(await operation.initial()), { person: { id: "1" } });
    assert.equal(operation.raw.length, 1);
    assert.equal(operation.results.at(-1).dataState, "streaming");
    const beforeRelease = [["person"], ["person", "id"], ["person", field]];
    assert.deepEqual(paths(operation.id), beforeRelease);
    server.releaseResolver(operation.id);
    // The continuation is already running. Wait for its payload before finish
    // so no second permit is submitted to the same suspended continuation.
    await server.wait(
      operation.id,
      (s) => s.stopped,
      "released resolver completes",
    );
    assert.deepEqual(data(await operation.finish()), {
      person: { id: "1", [field]: delivered },
    });
    assert.deepEqual(paths(operation.id), [
      ...beforeRelease,
      ...children.map((child) => ["person", field, child]),
    ]);
  });
}

test("negotiation rejects unsupported protocols and malformed HTTP requests before execution", async () => {
  const cases = [
    [{ accept: "multipart/mixed;deferSpec=20220824" }, 406],
    [{ accept: "multipart/mixed;incrementalSpec=v9" }, 406],
    [{ accept: "multipart/mixed" }, 406],
    [
      {
        accept: "multipart/mixed;incrementalSpec=v0.2;q=0,application/json;q=0",
      },
      406,
    ],
    [{ accept: "text/html" }, 406],
    [{ "content-type": "text/plain" }, 415],
  ];
  const before = server.sessions.size;
  for (const [headers, status] of cases) {
    const response = await fetch(server.url, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify({ query: "{ numbers @stream }" }),
    });
    assert.equal(response.status, status);
    assert.ok((await response.json()).errors[0].message);
  }
  for (const body of ["{", JSON.stringify({ variables: {} })]) {
    const response = await fetch(server.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    });
    assert.equal(response.status, 400);
    await response.json();
  }
  const get = await fetch(server.url);
  assert.equal(get.status, 405);
  await get.json();
  assert.equal(
    server.sessions.size,
    before,
    "Rejected requests never reach Absinthe",
  );
});

test("Apollo reports HTTP protocol negotiation failure", async (t) => {
  const operation = observe(t, "{ numbers @stream }", {
    headers: { accept: "multipart/mixed;deferSpec=20220824" },
  });
  const result = await operation.settled();
  assert.equal(result.error.statusCode, 406);
  assert.equal(operation.raw.length, 0);
  assert.ok(!server.sessions.has(operation.id));
});

test("Apollo detects a truncated multipart body and the adapter stops its worker", async (t) => {
  const operation = observe(t, "{ person { id ... @defer { name } } }", {
    headers: { "x-test-truncate": "true" },
  });
  const result = await operation.settled();
  assert.match(result.error.message, /premature end of multipart body/);
  await server.wait(
    operation.id,
    (s) => s.stopped && s.closed,
    "truncated response worker stopped",
  );
  assert.equal(server.sessions.get(operation.id).stopped, ":killed");
  assert.equal(operation.raw.length, 1);
  assert.equal(operation.raw[0].hasNext, true);
  assert.deepEqual(paths(operation.id), [["person"], ["person", "id"]]);
});

test("unsupported multipart versions may fall back to an explicitly accepted JSON response", async (t) => {
  for (const accept of [
    "multipart/mixed;deferSpec=20220824, application/json",
    "multipart/mixed;incrementalSpec=v0.2;q=0, application/json",
  ]) {
    const operation = observe(t, "{ numbers @stream(initialCount: 1) }", {
      headers: { accept },
    });
    assert.deepEqual(data(await operation.finish()), { numbers: [1, 2, 3] });
    assert.equal(operation.responses[0].type, "application/json");
  }
});
