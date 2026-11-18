import { SLUG } from "./identity.js";

export const SOURCE_ID = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const ENV_NAME = /^[A-Z][A-Z0-9_]{0,127}$/;

export type SourceConfig = {
  id: string;
  organization: string;
  /** Concurrent calls this replica sends the source; more are refused with 429. */
  maxConcurrent: number;
  limits?: Record<string, number>;
} & (
  | {
      kind: "postgres";
      /** Read from the environment variable `connectionStringEnv`. */
      connectionString: string;
      poolMax: number;
    }
  | {
      kind: "databricks";
      host: string;
      warehouseId: string;
      auth:
        | { mode: "service_principal"; clientId: string; clientSecret: string }
        | { mode: "delegated" };
    }
  | {
      kind: "s3";
      region: string;
      bucket: string;
      prefix: string;
      endpoint?: string;
    }
);

const KEYS: Record<SourceConfig["kind"], string[]> = {
  postgres: ["connectionStringEnv", "poolMax"],
  databricks: ["host", "warehouseId", "auth"],
  s3: ["region", "bucket", "prefix", "endpoint"],
};
const LIMITS: Record<SourceConfig["kind"], string[]> = {
  postgres: ["maxRows", "maxBytes", "maxTimeoutMs", "maxStatementLength"],
  databricks: ["maxRows", "maxBytes", "maxTimeoutMs", "maxStatementLength"],
  s3: ["maxObjectBytes", "maxListKeys", "maxTimeoutMs"],
};

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function only(where: string, o: Record<string, unknown>, keys: string[]) {
  const extra = Object.keys(o).filter((k) => !keys.includes(k));
  if (extra.length > 0)
    throw new Error(`${where}: unknown ${extra.join(", ")}`);
}

function str(where: string, v: unknown): string {
  if (typeof v !== "string" || v === "" || v.length > 2048) {
    throw new Error(`${where} must be a non-empty string`);
  }
  return v;
}

function int(where: string, v: unknown, max: number): number {
  if (!Number.isInteger(v) || (v as number) < 1 || (v as number) > max) {
    throw new Error(`${where} must be an integer from 1 to ${max}`);
  }
  return v as number;
}

/** A secret from the environment, named in config so config itself holds none. */
function secret(where: string, name: unknown, env: NodeJS.ProcessEnv) {
  if (typeof name !== "string" || !ENV_NAME.test(name)) {
    throw new Error(`${where} must name an environment variable`);
  }
  const v = env[name];
  if (!v) throw new Error(`${where}: ${name} is not set`);
  return v;
}

export function parseConfig(
  raw: unknown,
  env: NodeJS.ProcessEnv,
): SourceConfig[] {
  if (!isObject(raw) || !Array.isArray(raw.sources)) {
    throw new Error("config must be {sources: [...]}");
  }
  only("config", raw, ["sources"]);
  if (raw.sources.length > 1000) throw new Error("at most 1000 sources");
  const seen = new Set<string>();
  return raw.sources.map((s: unknown, i): SourceConfig => {
    if (!isObject(s)) throw new Error(`sources[${i}] must be an object`);
    const { id, organization, kind } = s;
    if (typeof id !== "string" || !SOURCE_ID.test(id)) {
      throw new Error(`sources[${i}].id must match ${SOURCE_ID}`);
    }
    const where = `source ${id}`;
    if (typeof organization !== "string" || !SLUG.test(organization)) {
      throw new Error(`${where}: organization must match ${SLUG}`);
    }
    if (kind !== "postgres" && kind !== "databricks" && kind !== "s3") {
      throw new Error(`${where}: kind must be postgres, databricks or s3`);
    }
    if (seen.has(`${organization}/${id}`)) {
      throw new Error(`${where}: duplicate in ${organization}`);
    }
    seen.add(`${organization}/${id}`);
    only(where, s, [
      "id",
      "organization",
      "kind",
      "maxConcurrent",
      "limits",
      ...KEYS[kind],
    ]);
    let limits: Record<string, number> | undefined;
    if (s.limits !== undefined) {
      if (!isObject(s.limits))
        throw new Error(`${where}: limits must be an object`);
      only(`${where} limits`, s.limits, LIMITS[kind]);
      limits = Object.fromEntries(
        Object.entries(s.limits).map(([k, v]) => [
          k,
          int(`${where} limits.${k}`, v, 2 ** 31),
        ]),
      );
    }
    const common = {
      id,
      organization,
      maxConcurrent:
        s.maxConcurrent === undefined
          ? 16
          : int(`${where} maxConcurrent`, s.maxConcurrent, 1000),
      ...(limits ? { limits } : {}),
    };
    if (kind === "postgres") {
      return {
        ...common,
        kind,
        connectionString: secret(
          `${where} connectionStringEnv`,
          s.connectionStringEnv,
          env,
        ),
        poolMax: int(`${where} poolMax`, s.poolMax, 100),
      };
    }
    if (kind === "databricks") {
      const auth = s.auth;
      if (!isObject(auth)) throw new Error(`${where}: auth must be an object`);
      if (auth.mode === "delegated") {
        only(`${where} auth`, auth, ["mode"]);
      } else if (auth.mode === "service_principal") {
        only(`${where} auth`, auth, ["mode", "clientId", "clientSecretEnv"]);
      } else {
        throw new Error(
          `${where}: auth.mode must be service_principal or delegated`,
        );
      }
      return {
        ...common,
        kind,
        host: str(`${where} host`, s.host),
        warehouseId: str(`${where} warehouseId`, s.warehouseId),
        auth:
          auth.mode === "delegated"
            ? { mode: "delegated" }
            : {
                mode: "service_principal",
                clientId: str(`${where} auth.clientId`, auth.clientId),
                clientSecret: secret(
                  `${where} auth.clientSecretEnv`,
                  auth.clientSecretEnv,
                  env,
                ),
              },
      };
    }
    if (typeof s.prefix !== "string") {
      throw new Error(
        `${where}: prefix must be a string ("" for the whole bucket)`,
      );
    }
    return {
      ...common,
      kind,
      region: str(`${where} region`, s.region),
      bucket: str(`${where} bucket`, s.bucket),
      prefix: s.prefix,
      ...(s.endpoint === undefined
        ? {}
        : { endpoint: str(`${where} endpoint`, s.endpoint) }),
    };
  });
}

/** Opens on first use and shares the result; a failed open is retried on the next call. */
export function lazy<T>(open: () => Promise<T>): () => Promise<T> {
  let opened: Promise<T> | undefined;
  return () => {
    opened ??= open().catch((err) => {
      opened = undefined;
      throw err;
    });
    return opened;
  };
}
