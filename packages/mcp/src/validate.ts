import { checkEndpoint, type EgressPolicy } from "./endpoint.js";
import {
  type Authentication,
  type Capability,
  type NewServer,
  RegistryError,
  type ScanStatus,
  SERVER_ID,
  SLUG,
  VERSION_NAME,
  type VersionInput,
} from "./types.js";

const MAX_CAPABILITIES = 200;
const MAX_SCOPES = 50;
const MAX_ZONES = 20;
const SCOPE_TOKEN = /^[\x21\x23-\x5B\x5D-\x7E]{1,128}$/;
const PROTOCOL_VERSION = /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/;
const hasControl = (s: string) =>
  [...s].some((c) => (c.codePointAt(0) as number) < 32 || c === "\u007f");

const bad = (message: string): never => {
  throw new RegistryError("invalid_request", message);
};

function object(where: string, v: unknown, keys: string[]) {
  if (typeof v !== "object" || v === null || Array.isArray(v)) {
    return bad(`${where} must be an object`);
  }
  const extra = Object.keys(v).find((k) => !keys.includes(k));
  if (extra !== undefined) bad(`${where}: unknown field ${extra}`);
  return v as Record<string, unknown>;
}

function text(where: string, v: unknown, max: number, min = 1): string {
  if (
    typeof v !== "string" ||
    v.length < min ||
    v.length > max ||
    hasControl(v)
  ) {
    return bad(
      `${where} must be ${min === 0 ? "at most" : "1 to"} ${max} characters`,
    );
  }
  return v;
}

function list<T>(
  where: string,
  v: unknown,
  max: number,
  item: (x: unknown, i: number) => T,
  key: (x: T) => string,
): T[] {
  if (v === undefined) return [];
  if (!Array.isArray(v) || v.length > max) {
    return bad(`${where} must be an array of at most ${max}`);
  }
  const out = v.map(item);
  if (new Set(out.map(key)).size !== out.length) bad(`${where} has duplicates`);
  return out;
}

function capabilities(where: string, v: unknown): Capability[] {
  return list(
    where,
    v,
    MAX_CAPABILITIES,
    (x, i): Capability => {
      const o = object(`${where}[${i}]`, x, ["name", "description"]);
      return {
        name: text(`${where}[${i}].name`, o.name, 128),
        ...(o.description === undefined
          ? {}
          : {
              description: text(
                `${where}[${i}].description`,
                o.description,
                1024,
                0,
              ),
            }),
      };
    },
    (c) => c.name,
  );
}

function authentication(
  v: unknown,
  endpoint: string,
  policy: EgressPolicy,
): Authentication {
  const o = object("authentication", v, [
    "method",
    "resource",
    "requiredScopes",
  ]);
  if (o.method === "none") {
    if (o.resource !== undefined || o.requiredScopes !== undefined) {
      bad("authentication none takes no resource or scopes");
    }
    return { method: "none" };
  }
  if (o.method !== "oauth2")
    bad("authentication.method must be oauth2 or none");
  const resource = checkEndpoint(o.resource, policy);
  // A token bound to another origin could be replayed against the server, or the reverse.
  if (new URL(resource).origin !== new URL(endpoint).origin) {
    bad("authentication.resource must share the endpoint's origin");
  }
  return {
    method: "oauth2",
    resource,
    requiredScopes: list(
      "authentication.requiredScopes",
      o.requiredScopes,
      MAX_SCOPES,
      (s) => {
        if (typeof s !== "string" || !SCOPE_TOKEN.test(s)) {
          bad("authentication.requiredScopes entries must be scope tokens");
        }
        return s as string;
      },
      (s) => s,
    ),
  };
}

export function parseVersion(raw: unknown, policy: EgressPolicy): VersionInput {
  const o = object("version", raw, [
    "version",
    "endpoint",
    "protocolVersion",
    "transport",
    "authentication",
    "tools",
    "resources",
    "prompts",
    "allowedNetworkZones",
  ]);
  if (typeof o.version !== "string" || !VERSION_NAME.test(o.version)) {
    bad(`version must match ${VERSION_NAME}`);
  }
  if (
    typeof o.protocolVersion !== "string" ||
    !PROTOCOL_VERSION.test(o.protocolVersion)
  ) {
    bad("protocolVersion must be a date like 2025-06-18");
  }
  if (o.transport !== "streamable-http") {
    bad("transport must be streamable-http");
  }
  const endpoint = checkEndpoint(o.endpoint, policy);
  return {
    version: o.version as string,
    endpoint,
    protocolVersion: o.protocolVersion as string,
    transport: "streamable-http",
    authentication: authentication(o.authentication, endpoint, policy),
    tools: capabilities("tools", o.tools),
    resources: capabilities("resources", o.resources),
    prompts: capabilities("prompts", o.prompts),
    allowedNetworkZones: list(
      "allowedNetworkZones",
      o.allowedNetworkZones,
      MAX_ZONES,
      (z) => {
        if (typeof z !== "string" || !SLUG.test(z)) {
          bad(`allowedNetworkZones entries must match ${SLUG}`);
        }
        return z as string;
      },
      (z) => z,
    ),
  };
}

export function parseNewServer(raw: unknown, policy: EgressPolicy): NewServer {
  const o = object("body", raw, [
    "id",
    "team",
    "name",
    "description",
    "version",
  ]);
  if (typeof o.id !== "string" || !SERVER_ID.test(o.id)) {
    bad(`id must match ${SERVER_ID}`);
  }
  if (typeof o.team !== "string" || !SLUG.test(o.team)) {
    bad(`team must match ${SLUG}`);
  }
  return {
    id: o.id as string,
    team: o.team as string,
    name: text("name", o.name, 128),
    ...(o.description === undefined
      ? {}
      : { description: text("description", o.description, 1024, 0) }),
    version: parseVersion(o.version, policy),
  };
}

function versionName(v: unknown): string {
  if (typeof v !== "string" || !VERSION_NAME.test(v)) {
    return bad(`version must match ${VERSION_NAME}`);
  }
  return v;
}

export function parseApprove(raw: unknown): {
  version: string;
  scanStatus: ScanStatus;
} {
  const o = object("body", raw, ["version", "scanStatus"]);
  const scan = o.scanStatus ?? "unscanned";
  if (scan !== "unscanned" && scan !== "clean") {
    bad("scanStatus must be unscanned or clean");
  }
  return { version: versionName(o.version), scanStatus: scan as ScanStatus };
}

export function parseRevoke(raw: unknown): { version?: string } {
  const o = object("body", raw, ["version"]);
  return o.version === undefined ? {} : { version: versionName(o.version) };
}

export { versionName };
