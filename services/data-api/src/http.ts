import type { IncomingMessage, ServerResponse } from "node:http";

export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly retryAfterSeconds?: number,
  ) {
    super(message);
  }
}

export function send(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

export function envelope(err: HttpError, requestId: string) {
  return {
    code: err.code,
    message: err.message,
    requestId,
    ...(err.retryAfterSeconds === undefined
      ? {}
      : { retryAfterSeconds: err.retryAfterSeconds }),
  };
}

/** The body's JSON, refusing anything over `maxBytes` or slower than `deadlineMs`. */
export async function readJson(
  req: IncomingMessage,
  maxBytes: number,
  deadlineMs: number,
): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  const timer = setTimeout(
    () =>
      req.destroy(
        new HttpError(408, "timeout", "The body did not arrive in time."),
      ),
    deadlineMs,
  );
  try {
    for await (const chunk of req) {
      size += chunk.length;
      if (size > maxBytes) {
        throw new HttpError(
          413,
          "payload_too_large",
          `Body over ${maxBytes} bytes.`,
        );
      }
      chunks.push(chunk);
    }
  } catch (err) {
    if (err instanceof HttpError) throw err;
    // The client went away mid-body; nobody reads this answer.
    throw new HttpError(400, "invalid_request", "The body could not be read.");
  } finally {
    clearTimeout(timer);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new HttpError(400, "invalid_request", "Body must be valid JSON.");
  }
}
