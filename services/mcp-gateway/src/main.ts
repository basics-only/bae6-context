import type { EgressPolicy } from "@bae6/context-mcp";
import { createEntries } from "./entries.js";
import { createVerifier, type Issuer } from "./identity.js";
import { createGateway } from "./server.js";
import { createUpstream } from "./upstream.js";

const env = process.env;
const log = (e: Record<string, unknown>) => console.log(JSON.stringify(e));

function int(name: string, fallback: number, min: number, max: number) {
  const v = Number(env[name] ?? fallback);
  if (!Number.isInteger(v) || v < min || v > max) {
    throw new Error(`${name} must be an integer from ${min} to ${max}`);
  }
  return v;
}

// Fails closed: an unset NODE_ENV is not development.
const dev = (name: string) => {
  const on = env[name] === "true";
  if (on && env.NODE_ENV !== "development") {
    throw new Error(`${name} needs NODE_ENV=development`);
  }
  if (on) log({ msg: "insecure_dev_flag", flag: name });
  return on;
};
const allowHttpJwks = dev("BAE6_ALLOW_HTTP_JWKS");
const allowInsecure = dev("BAE6_MCP_ALLOW_INSECURE_ENDPOINTS");

function json(name: string): unknown {
  try {
    return JSON.parse(env[name] ?? "null");
  } catch {
    throw new Error(`${name} must be valid JSON`);
  }
}

const issuers = json("BAE6_MCP_ISSUERS") as Issuer[] | null;
if (!Array.isArray(issuers)) {
  throw new Error(
    "BAE6_MCP_ISSUERS must be a JSON array of {issuer,audience,jwksUri,organization,application}",
  );
}
const registryUrl = env.BAE6_MCP_REGISTRY_URL;
if (!registryUrl || !/^https?:\/\//.test(registryUrl)) {
  throw new Error("BAE6_MCP_REGISTRY_URL must be an http(s) URL");
}
const registryTokens = json("BAE6_MCP_REGISTRY_TOKENS") as Record<
  string,
  string
> | null;
if (
  typeof registryTokens !== "object" ||
  registryTokens === null ||
  Array.isArray(registryTokens) ||
  !Object.values(registryTokens).every((t) => typeof t === "string" && t !== "")
) {
  throw new Error(
    "BAE6_MCP_REGISTRY_TOKENS must be a JSON object of organization to registry token",
  );
}
const suffixes = (env.BAE6_MCP_ALLOWED_HOST_SUFFIXES ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (suffixes.length === 0 && !allowInsecure) {
  throw new Error("BAE6_MCP_ALLOWED_HOST_SUFFIXES is required");
}
const policy: EgressPolicy = {
  ...(suffixes.length > 0 ? { allowedHostSuffixes: suffixes } : {}),
  ...(allowInsecure ? { allowInsecure } : {}),
};

const port = int("PORT", 8080, 1, 65535);
const maxInflight = int("BAE6_MAX_INFLIGHT", 32, 1, 10_000);
const maxInflightPerOrganization = int(
  "BAE6_MAX_INFLIGHT_PER_ORGANIZATION",
  Math.max(1, Math.floor(maxInflight / 2)),
  1,
  maxInflight,
);
const shutdownDelayMs = int("BAE6_SHUTDOWN_DELAY_MS", 0, 0, 15_000);
const callTimeoutMs = int("BAE6_MCP_CALL_TIMEOUT_MS", 20_000, 1000, 120_000);
const requestTimeoutMs = int(
  "BAE6_REQUEST_TIMEOUT_MS",
  callTimeoutMs + 10_000,
  callTimeoutMs,
  600_000,
);

const server = createGateway({
  verify: createVerifier(issuers, { allowHttpJwks }),
  entries: createEntries({
    registryUrl,
    token: async (organization) =>
      Object.hasOwn(registryTokens, organization)
        ? registryTokens[organization]
        : undefined,
    ttlMs: int("BAE6_MCP_CACHE_TTL_MS", 30_000, 1000, 300_000),
    timeoutMs: 5000,
    maxBytes: 1024 * 1024,
    maxEntries: 1000,
  }),
  upstream: createUpstream({
    policy,
    timeoutMs: callTimeoutMs,
    maxResponseBytes: 1024 * 1024,
  }),
  maxInflight,
  maxInflightPerOrganization,
  requestTimeoutMs,
  log,
});
server.listen(port, "0.0.0.0", () => log({ msg: "listening", port }));

process.once("SIGTERM", () => {
  server.drain();
  setTimeout(() => {
    server.close(() => process.exit(0));
    server.closeIdleConnections();
    setTimeout(() => process.exit(1), requestTimeoutMs + 5000).unref();
  }, shutdownDelayMs).unref();
});
