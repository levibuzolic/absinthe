// Independent execution-engine probe for the client limitations. This uses
// the same JSON-lines control channel and HTTP encoder, but every GraphQL
// payload is produced unchanged by graphql@17.0.0-alpha.9.
import { createInterface } from "node:readline";
import {
  buildSchema,
  parse,
  experimentalExecuteIncrementally,
} from "graphql-reference";

const schema = buildSchema(`
  directive @defer(if: Boolean! = true, label: String) on FRAGMENT_SPREAD | INLINE_FRAGMENT
  directive @stream(if: Boolean! = true, label: String, initialCount: Int! = 0) on FIELD
  type Person { id: ID, name: String, friends: [Person] }
  type Query { person: Person, people: [Person] }
`);
const grace = { id: 2, name: "Grace" };
const edsger = { id: 3, name: "Edsger" };
const ada = { id: 1, name: "Ada", friends: [grace, edsger] };
const rootValue = { person: ada, people: [ada, grace, edsger] };
const workers = new Map();
const emit = (id, event, extra = {}) =>
  console.log(JSON.stringify({ id, event, ...extra }));
emit(null, "ready");
for await (const line of createInterface({ input: process.stdin })) {
  const request = JSON.parse(line);
  const { id, command } = request;
  if (command === "start") {
    const result = await experimentalExecuteIncrementally({
      schema,
      document: parse(request.query),
      rootValue,
      variableValues: request.variables,
      operationName: request.operationName,
    });
    if (result.initialResult) {
      workers.set(id, result.subsequentResults);
      emit(id, "payload", { payload: result.initialResult });
    } else {
      emit(id, "payload", { payload: result });
      emit(id, "stopped", { reason: ":normal" });
    }
  } else if (command === "next" && workers.has(id)) {
    const { value, done } = await workers.get(id).next();
    if (!done) emit(id, "payload", { payload: value });
    if (done || value.hasNext === false) await stop(id, ":normal");
  } else if (command === "cancel" && workers.has(id)) {
    await stop(id, ":killed");
  }
}
for (const id of workers.keys()) await stop(id, ":killed");

async function stop(id, reason) {
  await workers.get(id).return();
  workers.delete(id);
  emit(id, "stopped", { reason });
}
