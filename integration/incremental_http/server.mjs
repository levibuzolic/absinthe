import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { EventEmitter, once } from "node:events";
import http from "node:http";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

export const multipart =
  'multipart/mixed; boundary="absinthe-e2e"; incrementalSpec=v0.2';
const boundary = "\r\n--absinthe-e2e";

// Deadlines detect hangs. They never release work or establish event ordering.
export function waitFor(emitter, predicate, description) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => finish(new Error(`Timed out: ${description}`)),
      10_000,
    );
    const check = () => {
      try {
        const value = predicate();
        if (value) finish(null, value);
      } catch (error) {
        finish(error);
      }
    };
    function finish(error, value) {
      clearTimeout(timeout);
      emitter.off("change", check);
      error ? reject(error) : resolve(value);
    }
    emitter.on("change", check);
    check();
  });
}

function negotiate(accept = "*/*") {
  const types = accept.split(",").map((entry) => {
    const [type, ...parameters] = entry
      .trim()
      .toLowerCase()
      .split(/\s*;\s*/);
    const params = Object.fromEntries(
      parameters.map((p) => p.split("=").map((s) => s.replace(/^"|"$/g, ""))),
    );
    return {
      type,
      params,
      quality: params.q === undefined ? 1 : Number(params.q),
    };
  });
  const incremental = types.some(
    ({ type, params, quality }) =>
      quality > 0 &&
      type === "multipart/mixed" &&
      params.incrementalspec === "v0.2",
  );
  const json = types.find(
    ({ type, quality }) =>
      quality > 0 &&
      [
        "application/json",
        "application/graphql-response+json",
        "application/*",
        "*/*",
      ].includes(type),
  );
  return {
    incremental,
    json:
      json?.type === "application/graphql-response+json"
        ? "application/graphql-response+json"
        : json
          ? "application/json"
          : null,
  };
}

export async function startServer({ reference = false } = {}) {
  const events = new EventEmitter();
  const sessions = new Map();
  const child = spawn(
    reference ? process.execPath : "mix",
    reference
      ? ["reference.mjs"]
      : ["run", "--no-compile", "-e", "IncrementalHTTP.Bridge.run()"],
    {
      cwd: fileURLToPath(new URL(".", import.meta.url)),
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, MIX_ENV: "test" },
    },
  );
  let stderr = "";
  const exited = new Promise((resolve) =>
    child.once("close", (code, signal) => resolve([code, signal])),
  );
  let ready = false;
  let failure;
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  child.on("error", (error) => {
    failure = error;
    events.emit("change");
  });
  child.on("exit", (code, signal) => {
    failure = new Error(`Bridge exited (${code ?? signal})\n${stderr}`);
    events.emit("change");
  });
  const send = (command, id, extra = {}) => {
    if (failure) throw failure;
    child.stdin.write(JSON.stringify({ command, id, ...extra }) + "\n");
  };
  const lines = createInterface({ input: child.stdout });
  lines.on("line", (line) => {
    try {
      const message = JSON.parse(line);
      if (message.event === "ready") ready = true;
      else {
        const session = sessions.get(message.id);
        assert.ok(session, `Unknown bridge request: ${message.id}`);
        session.events.push(message);
        if (message.event === "payload") deliver(session, message.payload);
        if (message.event === "stopped") {
          session.stopped = message.reason;
          if (message.reason !== ":normal" && !session.disconnected) {
            session.response.destroy(
              new Error(`Worker failed: ${message.reason}`),
            );
          }
        }
      }
    } catch (error) {
      failure = error;
    }
    events.emit("change");
  });

  function deliver(session, payload) {
    session.payloads.push(payload);
    if (session.disconnected) return;
    const response = session.response;
    const isMultipart = session.payloads[0].hasNext === true;
    if (session.payloads.length === 1) {
      response.writeHead(200, {
        "content-type": isMultipart ? multipart : session.jsonType,
        "cache-control": "no-store",
      });
      if (isMultipart) response.write(boundary);
    }
    if (isMultipart) {
      // End each part with the NEXT boundary immediately. Waiting for another
      // payload before sending this delimiter would buffer the initial result
      // inside real multipart parsers and deadlock the coordinated tests.
      const part = `\r\nContent-Type: application/json\r\n\r\n${JSON.stringify(payload)}${boundary}`;
      if (session.fragmented) {
        // Separate writes deliberately cut headers, JSON and UTF-8 codepoints.
        // TCP may coalesce them; tests do not assume packet boundaries.
        const bytes = Buffer.from(part);
        for (let i = 0; i < bytes.length; i += 7)
          response.write(bytes.subarray(i, i + 7));
      } else response.write(part);
      if (session.truncate) response.end();
      else if (payload.hasNext === false) response.end("--\r\n");
    } else response.end(JSON.stringify(payload));
  }

  const server = http.createServer(async (request, response) => {
    const reply = (status, message) => {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify({ errors: [{ message }] }));
    };
    if (request.url !== "/graphql") return reply(404, "Not found");
    if (request.method !== "POST") return reply(405, "POST required");
    if (!request.headers["content-type"]?.startsWith("application/json"))
      return reply(415, "JSON required");
    const negotiated = negotiate(request.headers.accept);
    if (!negotiated.incremental && !negotiated.json)
      return reply(
        406,
        "Unsupported incremental protocol or response media type",
      );
    try {
      let body = "";
      for await (const chunk of request) body += chunk;
      const operation = JSON.parse(body);
      if (typeof operation.query !== "string")
        return reply(400, "A query string is required");
      const id = request.headers["x-test-id"];
      if (!id || sessions.has(id))
        return reply(400, "A unique test request ID is required");
      const session = {
        id,
        response,
        events: [],
        payloads: [],
        accept: request.headers.accept,
        jsonType: negotiated.json || "application/json",
        fragmented: request.headers["x-test-fragmented"] === "true",
        truncate: request.headers["x-test-truncate"] === "true",
      };
      sessions.set(id, session);
      response.on("close", () => {
        if (
          !response.writableFinished ||
          session.payloads.at(-1)?.hasNext === true
        ) {
          session.disconnected = true;
          if (!failure) send("cancel", id);
        }
        session.closed = true;
        events.emit("change");
      });
      send("start", id, {
        query: operation.query,
        variables: operation.variables,
        operationName: operation.operationName,
        incremental: negotiated.incremental,
      });
      events.emit("change");
    } catch (error) {
      reply(400, error.message);
    }
  });

  try {
    await waitFor(
      events,
      () => {
        if (failure) throw failure;
        return ready;
      },
      "bridge ready",
    );
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
  } catch (error) {
    child.kill("SIGKILL");
    await exited;
    throw error;
  }

  return {
    url: `http://127.0.0.1:${server.address().port}/graphql`,
    sessions,
    events,
    next: (id) => send("next", id),
    releaseResolver: (id) => send("release_resolver", id),
    wait: (id, predicate, description) =>
      waitFor(
        events,
        () => {
          if (failure) throw failure;
          const session = sessions.get(id);
          return session && predicate(session);
        },
        `${id}: ${description}`,
      ),
    async close() {
      // Close sockets first so their close handlers cancel every active worker.
      const closed = new Promise((resolve) => server.close(resolve));
      server.closeAllConnections();
      await closed;
      if (!child.stdin.destroyed) child.stdin.end();
      const deadline = setTimeout(() => child.kill("SIGKILL"), 5000);
      const [code, signal] = await exited;
      clearTimeout(deadline);
      lines.close();
      assert.equal(
        signal,
        null,
        `Bridge required forced termination: ${stderr}`,
      );
      assert.equal(code, 0, stderr);
      assert.equal(server.listening, false);
      for (const session of sessions.values()) {
        assert.ok(session.stopped, `Leaked request worker: ${session.id}`);
      }
    },
  };
}
