import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";
import { meros } from "meros/browser";

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

for (const [format, parse] of [
  ["ESM", meros],
  ["CommonJS", createRequire(import.meta.url)("meros/browser").meros],
]) {
  test(`meros ${format} preserves parts across fragmented and coalesced Fetch chunks`, async () => {
    const partitions = [
      [wire],
      Array.from(wire, (_, index) => wire.subarray(index, index + 1)),
      ...Array.from({ length: wire.length - 1 }, (_, index) => [
        wire.subarray(0, index + 1),
        wire.subarray(index + 1),
      ]),
    ];
    for (const chunks of partitions) {
      const description = `Chunk lengths: ${chunks.map((chunk) => chunk.length)}`;
      const response = new Response(
        new ReadableStream({
          start(controller) {
            for (const chunk of chunks) controller.enqueue(chunk);
            controller.close();
          },
        }),
        {
          headers: {
            "content-type": 'multipart/mixed; boundary="absinthe-e2e"',
          },
        },
      );
      const actual = [];
      for await (const part of await parse(response)) {
        assert.equal(part.json, true, description);
        actual.push(part.body);
      }
      assert.deepEqual(actual, payloads, description);
    }
  });
}
