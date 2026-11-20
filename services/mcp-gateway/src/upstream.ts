import { request as httpRequest, type IncomingHttpHeaders } from "node:http";
import { request as httpsRequest } from "node:https";
import {
  checkEndpoint,
  type EgressPolicy,
  guardedLookup,
  RegistryError,
  type Resolver,
} from "@bae6/context-mcp";

export type UpstreamFailure =
  | "blocked"
  | "unreachable"
  | "timeout"
  | "too_large"
  | "bad_response"
  | "rejected";

export class UpstreamError extends Error {
  constructor(readonly reason: UpstreamFailure) {
    super(reason);
  }
}

export interface UpstreamOptions {
  policy: EgressPolicy;
  resolve?: Resolver;
  timeoutMs: number;
  maxResponseBytes: number;
}

export interface ToolCall {
  endpoint: string;
  protocolVersion: string;
  tool: string;
  arguments: Record<string, unknown>;
}

interface Reply {
  status: number;
  headers: IncomingHttpHeaders;
  body: string;
}

const SESSION_ID = /^[\x21-\x7e]{1,128}$/;
const CLOSE_TIMEOUT_MS = 2000;

export function createUpstream(o: UpstreamOptions) {
  const lookup = guardedLookup(o.policy, o.resolve);

  function send(
    url: URL,
    method: "POST" | "DELETE",
    headers: Record<string, string>,
    payload: object | undefined,
    signal: AbortSignal,
  ): Promise<Reply> {
    return new Promise((done, fail) => {
      const body = payload === undefined ? undefined : JSON.stringify(payload);
      const request = url.protocol === "https:" ? httpsRequest : httpRequest;
      const req = request(
        url,
        {
          method,
          agent: false,
          lookup,
          signal,
          headers: {
            ...headers,
            "user-agent": "bae6-mcp-gateway",
            ...(body === undefined
              ? {}
              : {
                  "content-type": "application/json",
                  "content-length": String(Buffer.byteLength(body)),
                }),
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          let size = 0;
          res.on("data", (chunk: Buffer) => {
            size += chunk.length;
            if (size > o.maxResponseBytes) {
              res.destroy();
              fail(new UpstreamError("too_large"));
            } else {
              chunks.push(chunk);
            }
          });
          res.on("end", () =>
            done({
              status: res.statusCode ?? 0,
              headers: res.headers,
              body: Buffer.concat(chunks).toString("utf8"),
            }),
          );
          res.on("error", fail);
        },
      );
      req.on("error", fail);
      req.end(body);
    });
  }

  const bad = () => new UpstreamError("bad_response");

  function events(body: string): string[] {
    return body
      .split(/\r?\n\r?\n/)
      .map((block) =>
        block
          .split(/\r?\n/)
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).replace(/^ /, ""))
          .join("\n"),
      )
      .filter((data) => data !== "");
  }

  function messageFor(reply: Reply, id: number): unknown {
    if (reply.status !== 200) throw bad();
    const type = String(reply.headers["content-type"] ?? "");
    let messages: unknown[];
    try {
      if (type.startsWith("application/json")) {
        messages = [JSON.parse(reply.body)].flat();
      } else if (type.startsWith("text/event-stream")) {
        messages = events(reply.body).flatMap((data) => [JSON.parse(data)]);
      } else {
        throw bad();
      }
    } catch (err) {
      throw err instanceof UpstreamError ? err : bad();
    }
    let found: Record<string, unknown> | undefined;
    for (const m of messages) {
      if (typeof m !== "object" || m === null) throw bad();
      const message = m as Record<string, unknown>;
      // A request from the server (sampling, elicitation) cannot be answered.
      if ("method" in message && "id" in message) throw bad();
      if (message.id === id) found = message;
    }
    if (!found) throw bad();
    if ("error" in found) throw new UpstreamError("rejected");
    if (!("result" in found)) throw bad();
    return found.result;
  }

  return async (call: ToolCall): Promise<unknown> => {
    const signal = AbortSignal.timeout(o.timeoutMs);
    let url: URL | undefined;
    let session: string | undefined;
    const headers: Record<string, string> = {
      accept: "application/json, text/event-stream",
    };
    try {
      url = new URL(checkEndpoint(call.endpoint, o.policy));
      const target = url;
      const rpc = (id: number, method: string, params: object) =>
        send(
          target,
          "POST",
          headers,
          { jsonrpc: "2.0", id, method, params },
          signal,
        );

      const init = await rpc(1, "initialize", {
        protocolVersion: call.protocolVersion,
        capabilities: {},
        clientInfo: { name: "bae6-mcp-gateway", version: "0.0.0" },
      });
      const negotiated = messageFor(init, 1) as {
        protocolVersion?: unknown;
      } | null;
      if (negotiated?.protocolVersion !== call.protocolVersion) throw bad();
      const id = init.headers["mcp-session-id"];
      if (id !== undefined) {
        if (typeof id !== "string" || !SESSION_ID.test(id)) throw bad();
        session = id;
        headers["mcp-session-id"] = id;
      }
      headers["mcp-protocol-version"] = call.protocolVersion;

      const ack = await send(
        target,
        "POST",
        headers,
        { jsonrpc: "2.0", method: "notifications/initialized" },
        signal,
      );
      if (ack.status < 200 || ack.status >= 300) throw bad();

      return messageFor(
        await rpc(2, "tools/call", {
          name: call.tool,
          arguments: call.arguments,
        }),
        2,
      );
    } catch (err) {
      throw classify(err);
    } finally {
      if (url && session) {
        await send(
          url,
          "DELETE",
          headers,
          undefined,
          AbortSignal.timeout(CLOSE_TIMEOUT_MS),
        ).catch(() => undefined);
      }
    }
  };
}

function classify(err: unknown): UpstreamError {
  if (err instanceof UpstreamError) return err;
  if (err instanceof RegistryError) return new UpstreamError("blocked");
  const e = err as { name?: string; code?: string };
  if (e.code === "EBLOCKED") return new UpstreamError("blocked");
  if (e.name === "AbortError" || e.name === "TimeoutError") {
    return new UpstreamError("timeout");
  }
  return new UpstreamError("unreachable");
}
