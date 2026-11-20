import { SERVER_ID, type Server } from "@bae6/context-mcp";

export interface EntriesOptions {
  registryUrl: string;
  /** The registry token for the organization, or undefined when it has none. */
  token(organization: string): Promise<string | undefined>;
  ttlMs: number;
  timeoutMs: number;
  maxBytes: number;
  maxEntries: number;
  now?: () => number;
}

export class RegistryUnavailable extends Error {}

/** The registry's view of one server for one organization, cached for `ttlMs`. */
export type Entries = (
  organization: string,
  serverId: string,
) => Promise<Server | null>;

export function createEntries(o: EntriesOptions): Entries {
  const now = o.now ?? Date.now;
  const base = o.registryUrl.replace(/\/+$/, "");
  const cache = new Map<string, { server: Server; expires: number }>();

  async function fetchServer(
    organization: string,
    serverId: string,
  ): Promise<Server | null> {
    const token = await o.token(organization);
    if (token === undefined) return null;
    let res: Response;
    try {
      res = await fetch(`${base}/v1/servers/${encodeURIComponent(serverId)}`, {
        headers: { authorization: `Bearer ${token}` },
        redirect: "error",
        signal: AbortSignal.timeout(o.timeoutMs),
      });
    } catch {
      throw new RegistryUnavailable("registry unreachable");
    }
    if (res.status === 404) {
      await res.body?.cancel();
      return null;
    }
    if (res.status !== 200) {
      await res.body?.cancel();
      throw new RegistryUnavailable(`registry answered ${res.status}`);
    }
    return parse(await readBounded(res, o.maxBytes), organization, serverId);
  }

  return async (organization, serverId) => {
    const key = `${organization}/${serverId}`;
    const hit = cache.get(key);
    if (hit && hit.expires > now()) return hit.server;
    cache.delete(key);
    const server = await fetchServer(organization, serverId);
    if (!server) return null;
    if (cache.size >= o.maxEntries) {
      const oldest = cache.keys().next().value;
      if (oldest !== undefined) cache.delete(oldest);
    }
    cache.set(key, { server, expires: now() + o.ttlMs });
    return server;
  };
}

async function readBounded(res: Response, maxBytes: number): Promise<string> {
  const chunks: Uint8Array[] = [];
  let size = 0;
  for await (const chunk of res.body ?? []) {
    size += chunk.length;
    if (size > maxBytes) {
      throw new RegistryUnavailable("registry reply too large");
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function parse(text: string, organization: string, serverId: string): Server {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    throw new RegistryUnavailable("registry reply is not JSON");
  }
  const s = raw as Partial<Server> | null;
  if (
    typeof s !== "object" ||
    s === null ||
    s.id !== serverId ||
    !SERVER_ID.test(s.id) ||
    s.organization !== organization ||
    !Array.isArray(s.versions)
  ) {
    throw new RegistryUnavailable("registry reply is not the requested server");
  }
  return s as Server;
}
