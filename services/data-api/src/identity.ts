import { createRemoteJWKSet, decodeJwt, jwtVerify } from "jose";

export interface Issuer {
  issuer: string;
  audience: string;
  jwksUri: string;
  /** The tenant every token from this issuer acts in (ADR-0002); never read from claims. */
  organization: string;
  application: string;
}

export interface Identity {
  subjectId: string;
  subjectType: "workload" | "user";
  organizationId: string;
  applicationId: string;
  scopes: string[];
}

/** null = invalid token; throws = validity could not be determined (fail closed). */
export type Verifier = (token: string | undefined) => Promise<Identity | null>;

export const SLUG = /^[a-z0-9][a-z0-9-]{0,62}$/;
const ALGORITHMS = ["RS256", "PS256", "ES256", "EdDSA"];
const TOKEN = /^[A-Za-z0-9._~+/-]+=*$/;
const INVALID = new Set([
  "ERR_JWT_EXPIRED",
  "ERR_JWT_CLAIM_VALIDATION_FAILED",
  "ERR_JWT_INVALID",
  "ERR_JWS_INVALID",
  "ERR_JWS_SIGNATURE_VERIFICATION_FAILED",
  "ERR_JOSE_ALG_NOT_ALLOWED",
  "ERR_JOSE_NOT_SUPPORTED",
  "ERR_JWKS_NO_MATCHING_KEY",
  "ERR_JWKS_MULTIPLE_MATCHING_KEYS",
]);

export function createVerifier(
  issuers: readonly Issuer[],
  opts: { allowHttpJwks?: boolean } = {},
): Verifier {
  if (issuers.length === 0) throw new Error("at least one issuer is required");
  const byIssuer = new Map<
    string,
    Issuer & { keys: ReturnType<typeof createRemoteJWKSet> }
  >();
  for (const i of issuers) {
    if (
      typeof i.organization !== "string" ||
      !SLUG.test(i.organization) ||
      typeof i.application !== "string" ||
      !SLUG.test(i.application)
    ) {
      throw new Error(
        `issuer ${i.issuer}: organization and application must match ${SLUG}`,
      );
    }
    const jwks = new URL(i.jwksUri);
    if (
      jwks.protocol !== "https:" &&
      !(jwks.protocol === "http:" && opts.allowHttpJwks)
    ) {
      throw new Error(`issuer ${i.issuer}: jwksUri must be https`);
    }
    if (byIssuer.has(i.issuer)) throw new Error(`duplicate issuer ${i.issuer}`);
    byIssuer.set(i.issuer, {
      ...i,
      keys: createRemoteJWKSet(jwks, {
        cooldownDuration: 30_000,
        cacheMaxAge: 600_000,
        timeoutDuration: 5_000,
      }),
    });
  }

  return async (token) => {
    if (!token || token.length > 8192 || !TOKEN.test(token)) return null;
    let iss: unknown;
    try {
      // Unverified; only selects the issuer's keys. jwtVerify re-checks it.
      iss = decodeJwt(token).iss;
    } catch {
      return null;
    }
    const cfg = typeof iss === "string" ? byIssuer.get(iss) : undefined;
    if (!cfg) return null;
    let payload: Record<string, unknown>;
    try {
      ({ payload } = await jwtVerify(token, cfg.keys, {
        issuer: cfg.issuer,
        audience: cfg.audience,
        algorithms: ALGORITHMS,
        clockTolerance: 60,
        requiredClaims: ["exp", "sub"],
      }));
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (typeof code === "string" && INVALID.has(code)) return null;
      throw err;
    }
    const sub = payload.sub;
    if (typeof sub !== "string" || sub === "" || sub.length > 256) return null;
    if (
      typeof payload.iat === "number" &&
      payload.iat > Date.now() / 1000 + 60
    ) {
      return null;
    }
    const scopes =
      typeof payload.scope === "string"
        ? payload.scope.split(" ").filter(Boolean)
        : Array.isArray(payload.scp)
          ? payload.scp.filter((s): s is string => typeof s === "string")
          : [];
    return {
      subjectId: sub,
      // Client-credentials tokens name the client as their own subject.
      subjectType:
        sub === (payload.client_id ?? payload.azp) ? "workload" : "user",
      organizationId: cfg.organization,
      applicationId: cfg.application,
      scopes: [...new Set(scopes)].slice(0, 100),
    };
  };
}

/**
 * The caller's identity. With `Bae6-End-User-Token` the user is the subject, and both
 * tokens must map to the same organization and application (ADR-0003), else null.
 */
export async function identify(
  verify: Verifier,
  authorization: string | undefined,
  endUserToken: string | undefined,
): Promise<(Identity & { workloadId?: string }) | null> {
  const bearer = /^Bearer (\S+)$/.exec(authorization ?? "")?.[1];
  const workload = await verify(bearer);
  if (!workload) return null;
  if (endUserToken === undefined) return workload;
  const user = await verify(endUserToken);
  if (
    !user ||
    user.organizationId !== workload.organizationId ||
    user.applicationId !== workload.applicationId
  ) {
    return null;
  }
  return { ...user, scopes: workload.scopes, workloadId: workload.subjectId };
}
