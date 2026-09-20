import assert from "node:assert/strict";
import { test } from "node:test";
import { readMultipart } from "./multipart.mjs";

const payloads = [
  { data: { hero: { id: "1" } }, hasNext: true },
  {
    label: "details",
    path: ["hero"],
    data: { display: "Adä 👋" },
    hasNext: false,
    extensions: { is_final: true },
  },
];
const boundary = "\r\n--absinthe-e2e";
const wire = Buffer.from(
  boundary +
    payloads
      .map(
        (payload) =>
          `\r\nContent-Type: application/json\r\n\r\n${JSON.stringify(payload)}${boundary}`,
      )
      .join("") +
    "--\r\n",
);

function response(
  chunks,
  contentType = 'multipart/mixed; boundary="absinthe-e2e"',
) {
  return new Response(
    new ReadableStream({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk);
        controller.close();
      },
    }),
    { headers: { "content-type": contentType } },
  );
}

async function collect(response) {
  const values = [];
  for await (const value of readMultipart(response)) values.push(value);
  return values;
}

test("preserves JSON parts across fragmented and coalesced Fetch chunks", async () => {
  const partitions = [
    [wire],
    Array.from(wire, (_, index) => wire.subarray(index, index + 1)),
    ...Array.from({ length: wire.length - 1 }, (_, index) => [
      wire.subarray(0, index + 1),
      wire.subarray(index + 1),
    ]),
  ];
  for (const chunks of partitions) {
    assert.deepEqual(
      await collect(response(chunks)),
      payloads,
      `Chunk lengths: ${chunks.map((chunk) => chunk.length)}`,
    );
  }
  assert.deepEqual(
    await collect(response([wire], "multipart/mixed; boundary=absinthe-e2e")),
    payloads,
  );
});

test("rejects truncated headers, JSON and closing boundaries", async () => {
  for (const length of [0, 25, 75, wire.length - 5, wire.length - 3]) {
    await assert.rejects(
      collect(response([wire.subarray(0, length)])),
      /Premature end of multipart body/,
    );
  }
});

test("rejects missing boundaries, non-JSON parts and invalid JSON", async () => {
  await assert.rejects(
    collect(response([wire], "multipart/mixed")),
    /Expected multipart\/mixed with a boundary/,
  );
  await assert.rejects(
    collect(
      response([
        Buffer.from(wire.toString().replace("application/json", "text/plain")),
      ]),
    ),
    /Expected a JSON multipart part/,
  );
  await assert.rejects(
    collect(response([Buffer.from(wire.toString().replace('"hero"', "hero"))])),
    SyntaxError,
  );
});

test("delivers a part before the next boundary suffix and cancels on early return", async () => {
  let cancelled = false;
  const body = new ReadableStream({
    start(controller) {
      const nextBoundary = wire.indexOf(boundary, boundary.length);
      controller.enqueue(wire.subarray(0, nextBoundary + boundary.length));
    },
    async cancel() {
      await Promise.resolve();
      cancelled = true;
    },
  });
  const parts = readMultipart(
    new Response(body, {
      headers: { "content-type": 'multipart/mixed; boundary="absinthe-e2e"' },
    }),
  );
  assert.deepEqual(await parts.next(), { done: false, value: payloads[0] });
  await parts.return();
  assert.equal(cancelled, true);
  assert.equal(body.locked, false);
});
