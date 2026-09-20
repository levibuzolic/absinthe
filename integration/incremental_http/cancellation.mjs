// Run with --unhandled-rejections=strict: stock HttpLink leaves the rejected
// reader.cancel() promise unobserved after an intentional multipart abort.
import assert from "node:assert/strict";
import {
  ApolloClient,
  HttpLink,
  InMemoryCache,
  gql,
} from "@apollo/client/core";
import { GraphQL17Alpha9Handler } from "@apollo/client/incremental";

const payload = {
  data: { person: { id: "1" } },
  pending: [{ id: "0", path: ["person"], label: "details" }],
  hasNext: true,
};
const client = new ApolloClient({
  cache: new InMemoryCache(),
  incrementalHandler: new GraphQL17Alpha9Handler(),
  link: new HttpLink({
    uri: "http://apollo-probe.invalid/graphql",
    fetch: async (_, { signal }) =>
      new Response(
        new ReadableStream({
          start(controller) {
            signal.addEventListener(
              "abort",
              () =>
                controller.error(
                  new DOMException(
                    "Stock Apollo multipart cancellation",
                    "AbortError",
                  ),
                ),
              { once: true },
            );
            controller.enqueue(
              new TextEncoder().encode(
                `--graphql\r\nContent-Type: application/json\r\n\r\n${JSON.stringify(payload)}\r\n--graphql\r\n`,
              ),
            );
          },
        }),
        { headers: { "content-type": "multipart/mixed;boundary=graphql" } },
      ),
  }),
});
const initial = Promise.withResolvers();
const subscription = client
  .watchQuery({
    query: gql`
      {
        person {
          id
          ... @defer(label: "details") {
            name
          }
        }
      }
    `,
    fetchPolicy: "no-cache",
  })
  .subscribe({
    next(result) {
      if (result.data !== undefined) initial.resolve(result);
    },
    error: initial.reject,
  });
const result = await initial.promise;
assert.deepEqual(result.data, payload.data);
assert.equal(result.error, undefined);
console.log("INITIAL_PAYLOAD_OBSERVED");
subscription.unsubscribe();
client.stop();
