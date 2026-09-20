import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import Relay from "relay-runtime";
import { readMultipart } from "./multipart.mjs";
import { waitFor } from "./server.mjs";

const {
  Environment,
  Network,
  Observable,
  RecordSource,
  Store,
  createOperationDescriptor,
  getSelector,
} = Relay;
let serial = 0;

export function observeRelay(
  server,
  t,
  query,
  {
    variables = {},
    accept = "multipart/mixed;incrementalSpec=relay",
    fragmented = false,
    truncate = false,
    store = new Store(new RecordSource()),
  } = {},
) {
  const id = `relay-${++serial}`;
  const events = new EventEmitter();
  const raw = [];
  let initialSnapshot;
  let error;
  let completed = false;
  const operation = createOperationDescriptor(query, variables);
  const environment = new Environment({
    deferDeduplicatedFields: true,
    store,
    network: Network.create((request, vars) =>
      Observable.create((sink) => {
        const controller = new AbortController();
        (async () => {
          const response = await fetch(server.url, {
            method: "POST",
            headers: {
              "content-type": "application/json",
              accept,
              "x-test-id": id,
              "x-test-fragmented": String(fragmented),
              "x-test-truncate": String(truncate),
            },
            body: JSON.stringify({
              query: request.text,
              variables: vars,
              operationName: request.name,
            }),
            signal: controller.signal,
          });
          assert.equal(response.status, 200);
          const isMultipart = /^multipart\/mixed\b/i.test(
            response.headers.get("content-type") ?? "",
          );
          const emit = (payload) => {
            raw.push(structuredClone(payload));
            sink.next(
              isMultipart
                ? payload
                : {
                    ...payload,
                    extensions: { ...payload.extensions, is_final: true },
                  },
            );
            events.emit("change");
          };
          if (isMultipart) {
            for await (const payload of readMultipart(response)) emit(payload);
            const terminal = raw.at(-1);
            if (
              terminal?.hasNext !== false ||
              terminal?.extensions?.is_final !== true
            ) {
              throw new Error(
                "Relay response ended before its terminal payload",
              );
            }
          } else emit(await response.json());
          sink.complete();
        })().catch((reason) => {
          if (!controller.signal.aborted) sink.error(reason);
        });
        return () => controller.abort();
      }),
    ),
  });
  const execution =
    query.params.operationKind === "mutation"
      ? environment.executeMutation({ operation })
      : environment.execute({ operation });
  const subscription = execution.subscribe({
    next() {
      initialSnapshot ??= environment.lookup(operation.fragment);
      events.emit("change");
    },
    error(value) {
      error = value;
      events.emit("change");
    },
    complete() {
      completed = true;
      events.emit("change");
    },
  });
  t.after(() => subscription.unsubscribe());
  const wait = (predicate, description) =>
    waitFor(events, predicate, `${id}: ${description}`);
  return {
    id,
    environment,
    raw,
    subscription,
    read: () => environment.lookup(operation.fragment),
    fragment: (fragment, reference) =>
      environment.lookup(getSelector(fragment, reference)),
    initial: () =>
      wait(() => initialSnapshot || error, "initial Relay snapshot"),
    async next() {
      const count = raw.length;
      server.next(id);
      await wait(() => raw.length > count || error, "next Relay payload");
    },
    async finish() {
      await this.initial();
      while (!error && raw.at(-1)?.hasNext === true) await this.next();
      await wait(() => completed || error, "Relay completion");
      await server.wait(
        id,
        (s) => s.stopped && s.closed,
        "worker and HTTP close",
      );
      if (error) throw error;
      const session = server.sessions.get(id);
      assert.equal(session.stopped, ":normal");
      assert.deepEqual(raw, session.payloads);
      return this.read();
    },
  };
}
