import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import {
  checkEndpoint,
  type EgressPolicy,
  guardedLookup,
  type Resolver,
} from "./endpoint.js";
import type { Probe } from "./registry.js";
import { type Health, type HealthReason, RegistryError } from "./types.js";

const TIMEOUT_MS = 5000;
const MAX_METADATA_BYTES = 64 * 1024;

class Failure extends Error {
  constructor(readonly reason: HealthReason) {
    super(reason);
  }
}

interface Reply {
  status: number;
  body: string;
}

interface Net {
  policy: EgressPolicy;
  resolve?: Resolver | undefined;
  timeoutMs: number;
}

function get(url: URL, net: Net, readBody: boolean): Promise<Reply> {
  return new Promise((done, fail) => {
    const request = url.protocol === "https:" ? httpsRequest : httpRequest;
    const req = request(
      url,
      {
        method: "GET",
        agent: false,
        lookup: guardedLookup(net.policy, net.resolve),
        headers: {
          accept: "application/json",
          "user-agent": "bae6-mcp-registry",
        },
        signal: AbortSignal.timeout(net.timeoutMs),
      },
      (res) => {
        const status = res.statusCode ?? 0;
        if (!readBody || status !== 200) {
          res.destroy();
          return done({ status, body: "" });
        }
        const chunks: Buffer[] = [];
        let size = 0;
        res.on("data", (chunk: Buffer) => {
          size += chunk.length;
          if (size > MAX_METADATA_BYTES) {
            res.destroy();
            fail(new Failure("invalid_metadata"));
          } else {
            chunks.push(chunk);
          }
        });
        res.on("end", () =>
          done({ status, body: Buffer.concat(chunks).toString("utf8") }),
        );
        res.on("error", fail);
      },
    );
    req.on("error", fail);
    req.end();
  });
}

function reasonOf(err: unknown): HealthReason {
  if (err instanceof Failure) return err.reason;
  const e = err as { name?: string; code?: string };
  if (e.code === "EBLOCKED") return "endpoint_blocked";
  if (e.name === "AbortError" || e.name === "TimeoutError") return "timeout";
  return "unreachable";
}

function checked(reply: Reply) {
  if (reply.status >= 300 && reply.status < 400)
    throw new Failure("redirected");
  if (reply.status >= 500) throw new Failure("bad_status");
}

/** RFC 9728: the well-known path is inserted between the host and the resource's path. */
function metadataUrl(resource: URL) {
  const path = resource.pathname === "/" ? "" : resource.pathname;
  return new URL(
    `/.well-known/oauth-protected-resource${path}`,
    resource.origin,
  );
}

/**
 * Checks that an endpoint answers and, for `oauth2`, that it names the declared resource.
 * Sends no credentials and follows no redirects. The result carries a fixed reason code,
 * never the server's own text.
 */
export function createProbe(
  policy: EgressPolicy = {},
  opts: { resolve?: Resolver; now?: () => Date; timeoutMs?: number } = {},
): Probe {
  const { resolve, now = () => new Date(), timeoutMs = TIMEOUT_MS } = opts;
  const net: Net = { policy, resolve, timeoutMs };
  return async (version) => {
    const checkedAt = now().toISOString();
    const result = (reason?: HealthReason): Health => ({
      version: version.version,
      status: reason ? "unhealthy" : "healthy",
      checkedAt,
      ...(reason ? { reason } : {}),
    });
    try {
      // Policy may have tightened since registration.
      const endpoint = new URL(checkEndpoint(version.endpoint, policy));
      checked(await get(endpoint, net, false));
      const { method, resource } = version.authentication;
      if (method === "oauth2") {
        const reply = await get(
          metadataUrl(new URL(resource as string)),
          net,
          true,
        );
        checked(reply);
        if (reply.status !== 200) throw new Failure("bad_status");
        let declared: unknown;
        try {
          declared = (JSON.parse(reply.body) as { resource?: unknown })
            .resource;
          declared = new URL(declared as string).href;
        } catch {
          throw new Failure("invalid_metadata");
        }
        if (declared !== resource) throw new Failure("resource_mismatch");
      }
      return result();
    } catch (err) {
      return result(
        err instanceof RegistryError ? "endpoint_blocked" : reasonOf(err),
      );
    }
  };
}
