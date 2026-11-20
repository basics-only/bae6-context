import { RegistryUnavailable, readBounded } from "./entries.js";

export interface ClientCredentials {
  tokenUrl: string;
  clientId: string;
  clientSecret: string;
  scope?: string;
}

export interface TokenSourceOptions {
  credentials: Record<string, ClientCredentials>;
  timeoutMs: number;
  maxBytes: number;
  now?: () => number;
}

const SKEW_MS = 60_000;

/** Registry tokens by organization, fetched by client credentials and renewed before they expire. */
export function createTokenSource(o: TokenSourceOptions) {
  const now = o.now ?? Date.now;
  const held = new Map<string, { token: string; expires: number }>();
  const pending = new Map<string, Promise<string>>();

  async function fetchToken(c: ClientCredentials, organization: string) {
    let res: Response;
    try {
      res = await fetch(c.tokenUrl, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "client_credentials",
          client_id: c.clientId,
          client_secret: c.clientSecret,
          ...(c.scope ? { scope: c.scope } : {}),
        }),
        redirect: "error",
        signal: AbortSignal.timeout(o.timeoutMs),
      });
    } catch {
      throw new RegistryUnavailable("token endpoint unreachable");
    }
    if (res.status !== 200) {
      await res.body?.cancel();
      throw new RegistryUnavailable(`token endpoint answered ${res.status}`);
    }
    let body: {
      access_token?: unknown;
      token_type?: unknown;
      expires_in?: unknown;
    } | null;
    try {
      body = JSON.parse(await readBounded(res, o.maxBytes));
    } catch (e) {
      if (e instanceof RegistryUnavailable) throw e;
      throw new RegistryUnavailable("token reply is not JSON");
    }
    if (
      typeof body !== "object" ||
      body === null ||
      typeof body.access_token !== "string" ||
      body.access_token === "" ||
      typeof body.token_type !== "string" ||
      body.token_type.toLowerCase() !== "bearer" ||
      typeof body.expires_in !== "number" ||
      !Number.isFinite(body.expires_in) ||
      body.expires_in <= 0
    ) {
      throw new RegistryUnavailable("token reply is not a bearer token");
    }
    held.set(organization, {
      token: body.access_token,
      expires: now() + body.expires_in * 1000 - SKEW_MS,
    });
    return body.access_token;
  }

  return async (organization: string): Promise<string | undefined> => {
    if (!Object.hasOwn(o.credentials, organization)) return undefined;
    const hit = held.get(organization);
    if (hit && hit.expires > now()) return hit.token;
    held.delete(organization);
    let inflight = pending.get(organization);
    if (!inflight) {
      inflight = fetchToken(
        o.credentials[organization] as ClientCredentials,
        organization,
      ).finally(() => pending.delete(organization));
      pending.set(organization, inflight);
    }
    return inflight;
  };
}
