// Reads the JSON multipart responses used by this harness, including boundaries
// and UTF-8 characters split across Fetch chunks. This is not a general MIME parser.
export async function* readMultipart(response) {
  const contentType = response.headers.get("content-type") ?? "";
  const boundary = /;\s*boundary\s*=\s*(?:"([^"]+)"|([^;\s]+))/i.exec(
    contentType,
  );
  if (!/^multipart\/mixed\b/i.test(contentType) || !boundary) {
    throw new Error("Expected multipart/mixed with a boundary");
  }

  const delimiter = `--${boundary[1] ?? boundary[2]}`;
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = "";
  let started = false;

  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });

      while (true) {
        if (!started) {
          const opening = buffer.indexOf(delimiter);
          if (opening < 0) break;
          buffer = buffer.slice(opening + delimiter.length);
          started = true;
        }
        if (buffer.startsWith("--")) return;
        if (buffer.length < 2) break;
        if (!buffer.startsWith("\r\n")) {
          throw new Error("Invalid multipart boundary");
        }

        const end = buffer.indexOf(`\r\n${delimiter}`, 2);
        if (end < 0) break;
        const part = buffer.slice(2, end);
        buffer = buffer.slice(end + 2 + delimiter.length);
        const headersEnd = part.indexOf("\r\n\r\n");
        if (
          headersEnd < 0 ||
          !/^content-type:\s*application\/json(?:;[^\r\n]*)?$/im.test(
            part.slice(0, headersEnd),
          )
        ) {
          throw new Error("Expected a JSON multipart part");
        }
        yield JSON.parse(part.slice(headersEnd + 4));
      }
      if (done) throw new Error("Premature end of multipart body");
    }
  } finally {
    try {
      await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
}
