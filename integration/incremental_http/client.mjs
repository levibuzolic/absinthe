import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  ApolloClient,
  ApolloLink,
  HttpLink,
  InMemoryCache,
  gql,
} from "@apollo/client/core";
import { GraphQL17Alpha9Handler } from "@apollo/client/incremental";
import { tap } from "rxjs";
import { waitFor } from "./server.mjs";

let serial = 0;
export function data(result) {
  return JSON.parse(
    JSON.stringify(result.data, (key, value) =>
      key === "__typename" ? undefined : value,
    ),
  );
}

export function observe(
  server,
  t,
  query,
  {
    variables,
    headers = {},
    fragmented = false,
    fetchPolicy = "no-cache",
  } = {},
) {
  const id = `request-${++serial}`;
  const events = new EventEmitter();
  const raw = [];
  const results = [];
  const responses = [];
  let error;
  const client = new ApolloClient({
    cache: new InMemoryCache(),
    incrementalHandler: new GraphQL17Alpha9Handler(),
    link: ApolloLink.from([
      new ApolloLink((operation, forward) => {
        if (headers.accept)
          operation.setContext(({ http }) => ({
            http: { ...http, accept: [] },
          }));
        return forward(operation).pipe(
          tap((payload) => {
            raw.push(structuredClone(payload));
            events.emit("change");
          }),
        );
      }),
      new HttpLink({
        uri: server.url,
        headers: {
          "x-test-id": id,
          "x-test-fragmented": String(fragmented),
          ...headers,
        },
        fetch: async (...args) => {
          const response = await fetch(...args);
          responses.push({
            status: response.status,
            type: response.headers.get("content-type"),
          });
          return response;
        },
      }),
    ]),
  });
  const subscription = client
    .watchQuery({
      query: gql(query),
      variables,
      fetchPolicy,
      errorPolicy: "all",
    })
    .subscribe({
      next(result) {
        results.push(result);
        events.emit("change");
      },
      error(value) {
        error = value;
        events.emit("change");
      },
    });
  t.after(() => {
    subscription.unsubscribe();
    client.stop();
  });
  const wait = (predicate, description) =>
    waitFor(
      events,
      () => {
        if (error) throw error;
        return predicate();
      },
      `${id}: ${description}`,
    );
  return {
    id,
    raw,
    results,
    responses,
    subscription,
    settled: () =>
      wait(() => results.findLast((r) => !r.loading), "settled client result"),
    cached: () => ({
      data: client.readQuery({ query: gql(query), variables }),
    }),
    initial: () =>
      wait(
        () => results.find((r) => r.data !== undefined),
        "initial client result",
      ),
    async next() {
      const count = raw.length;
      server.next(id);
      await wait(() => raw.length > count, "next parsed multipart payload");
    },
    async finish() {
      await wait(() => raw.length > 0, "first parsed response");
      while (raw.at(-1).hasNext === true) await this.next();
      await wait(
        () => results.findLast((r) => !r.loading),
        "final client result",
      );
      await server.wait(
        id,
        (s) => s.stopped && s.closed,
        "worker and HTTP response complete",
      );
      const session = server.sessions.get(id);
      assert.equal(session.stopped, ":normal");
      assert.deepEqual(
        raw,
        session.payloads,
        "Apollo parsed the unmodified Absinthe payloads over HTTP",
      );
      assertLifecycle(raw);
      return results.at(-1);
    },
  };
}

// Validate wire accounting only; reconstruction belongs entirely to Apollo.
function assertLifecycle(payloads) {
  const pending = new Set();
  const announced = new Set();
  for (const [index, payload] of payloads.entries()) {
    for (const notice of payload.pending ?? []) {
      assert.equal(typeof notice.id, "string");
      assert.ok(!announced.has(notice.id), `Reused ID ${notice.id}`);
      announced.add(notice.id);
      pending.add(notice.id);
    }
    for (const patch of payload.incremental ?? [])
      assert.ok(pending.has(patch.id));
    for (const completion of payload.completed ?? [])
      assert.ok(pending.delete(completion.id));
    if (payloads.length > 1)
      assert.equal(payload.hasNext, index < payloads.length - 1);
    else assert.ok(!("hasNext" in payload));
  }
  assert.equal(pending.size, 0, "No unresolved pending IDs at completion");
}

export const paths = (server, id) =>
  server.sessions
    .get(id)
    .events.filter((e) => e.event === "resolved")
    .map((e) => e.path);
