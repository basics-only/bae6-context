import {
  abortError,
  type CapabilityDescriptor,
  DataError,
  type ListRequest,
  type ObjectInfo,
  type ObjectSource,
  type ReadRequest,
  type StatRequest,
} from "@bae6/context-data";
import { type AwsCredentials, encode, sha256, signS3 } from "./sigv4.js";

export { awsCredentials } from "./credentials.js";
export type { AwsCredentials } from "./sigv4.js";

export interface S3Limits {
  maxObjectBytes: number;
  maxListKeys: number;
  maxTimeoutMs: number;
}

export interface S3Config {
  id: string;
  region: string;
  bucket: string;
  /** Every key read or listed must start with this; "" opens the whole bucket. */
  prefix: string;
  /** Defaults to https://s3.<region>.amazonaws.com. */
  endpoint?: string;
  credentials: () => Promise<AwsCredentials>;
  limits?: Partial<S3Limits>;
  /** Tests only. */
  allowHttp?: boolean;
}

const ID = /^[a-z0-9][a-z0-9._:/-]{0,127}$/;
const REGION = /^[a-z]{2}(-[a-z]+)+-\d{1,2}$/;
const BUCKET = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const MAX_LIST_RESPONSE_BYTES = 4 * 1024 * 1024;
const EMPTY = sha256("");
const DEFAULT_LIMITS: S3Limits = {
  maxObjectBytes: 16 * 1024 * 1024,
  maxListKeys: 1000,
  maxTimeoutMs: 30_000,
};

/** Keys and prefixes: no control characters, lone surrogates, or . / .. segments. */
const safePath = (p: unknown): p is string =>
  typeof p === "string" &&
  Buffer.byteLength(p) <= 1024 &&
  // biome-ignore lint/suspicious/noControlCharactersInRegex: refusing control characters
  !/[\u0000-\u001f\u007f]|\p{Cs}/u.test(p) &&
  // URL parsing (or a filesystem-backed store) would resolve these.
  !p.split("/").some((s) => s === "." || s === "..");

function checkKey(key: unknown, prefix: string): string {
  if (!safePath(key) || key.length === 0) {
    throw new DataError(
      "invalid_request",
      "key must be 1 to 1024 bytes with no control characters or . / .. segments.",
    );
  }
  if (!key.startsWith(prefix)) {
    throw new DataError("forbidden", "The key is outside this source.");
  }
  return key;
}

const malformed = () =>
  new DataError("unavailable", "The object store sent a malformed response.");
const iso = (v: string | null | undefined) => {
  const t = Date.parse(v ?? "");
  if (Number.isNaN(t)) throw malformed();
  return new Date(t).toISOString();
};

const ENTITIES: Record<string, string> = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
};
const unxml = (s: string) => {
  try {
    // Any & is an entity in well-formed XML; one this cannot decode means a malformed body.
    return s.replace(/&([^;&]{0,10})(;?)/g, (_, e: string, end: string) => {
      if (!end) throw malformed();
      if (/^#x[0-9a-fA-F]{1,6}$/.test(e)) {
        return String.fromCodePoint(Number.parseInt(e.slice(2), 16));
      }
      if (/^#\d{1,7}$/.test(e)) return String.fromCodePoint(Number(e.slice(1)));
      const named = ENTITIES[e];
      if (named === undefined) throw malformed();
      return named;
    });
  } catch {
    throw malformed();
  }
};
const tag = (xml: string, name: string) => {
  const m = xml.match(new RegExp(`<${name}>([^<]*)</${name}>`));
  return m ? unxml(m[1] as string) : undefined;
};
const unquote = (etag: string) => etag.replace(/^"|"$/g, "");

async function readCapped(
  res: Response,
  max: number,
  signal: AbortSignal,
  caller: AbortSignal | undefined,
): Promise<Uint8Array> {
  const reader = res.body?.getReader();
  if (!reader) return new Uint8Array();
  const parts: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    let r: ReadableStreamReadResult<Uint8Array>;
    try {
      r = await reader.read();
    } catch {
      throw signal.aborted
        ? abortError(caller)
        : new DataError("unavailable", "The object store connection failed.");
    }
    if (r.done) break;
    size += r.value.byteLength;
    if (size > max) {
      await reader.cancel();
      throw new DataError("too_large", `The object is over ${max} bytes.`);
    }
    parts.push(r.value);
  }
  return Buffer.concat(parts);
}

/** A read-only S3 object source restricted to one bucket and key prefix. */
export function createS3Source(cfg: S3Config): ObjectSource {
  if (!ID.test(cfg.id)) throw new Error(`id must match ${ID}`);
  if (!REGION.test(cfg.region)) throw new Error(`${cfg.id}: bad region`);
  if (!BUCKET.test(cfg.bucket)) throw new Error(`${cfg.id}: bad bucket name`);
  // A prefix like "tenant1" would also match "tenant10/…".
  if (
    !safePath(cfg.prefix) ||
    Buffer.byteLength(cfg.prefix) > 512 ||
    !(cfg.prefix === "" || cfg.prefix.endsWith("/"))
  ) {
    throw new Error(
      `${cfg.id}: prefix must be "" or end in "/", at most 512 bytes`,
    );
  }
  const endpoint = new URL(
    cfg.endpoint ?? `https://s3.${cfg.region}.amazonaws.com`,
  );
  if (
    !(
      endpoint.protocol === "https:" ||
      (endpoint.protocol === "http:" && cfg.allowHttp)
    ) ||
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== "/" ||
    endpoint.search
  ) {
    throw new Error(`${cfg.id}: endpoint must be a bare https:// URL`);
  }
  // Requests are signed with real credentials, so only AWS hosts get them outside development.
  if (!cfg.allowHttp && !endpoint.hostname.endsWith(".amazonaws.com")) {
    throw new Error(`${cfg.id}: endpoint is not an AWS host`);
  }
  const limits = { ...DEFAULT_LIMITS, ...cfg.limits };
  const signal = (caller?: AbortSignal) => {
    const t = AbortSignal.timeout(limits.maxTimeoutMs);
    return caller ? AbortSignal.any([caller, t]) : t;
  };

  async function send(
    method: "GET" | "HEAD",
    key: string | undefined,
    query: Record<string, string | undefined>,
    s: AbortSignal,
    caller: AbortSignal | undefined,
    extra: Record<string, string> = {},
  ): Promise<Response> {
    // shortcut: path-style addressing only; switch to virtual-hosted for buckets with dots over TLS if AWS ends path-style.
    const url = new URL(
      `/${cfg.bucket}${key === undefined ? "" : `/${key.split("/").map(encode).join("/")}`}`,
      endpoint,
    );
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined) url.searchParams.set(k, v);
    }
    let res: Response;
    try {
      const credentials = await cfg.credentials();
      res = await fetch(url, {
        method,
        headers: {
          ...signS3({
            method,
            url,
            payloadHash: EMPTY,
            region: cfg.region,
            credentials,
          }),
          ...extra,
        },
        redirect: "manual",
        signal: s,
      });
    } catch {
      throw s.aborted
        ? abortError(caller)
        : new DataError("unavailable", "The object store is unreachable.");
    }
    if (res.ok) return res;
    await res.body?.cancel();
    if (res.status === 403) {
      throw new DataError("forbidden", "The object store refused access.");
    }
    if (res.status === 404) {
      throw new DataError("not_found", "No such object or version.");
    }
    if (res.status === 400 || res.status === 416) {
      throw new DataError(
        "invalid_request",
        "The object store rejected the request.",
      );
    }
    throw new DataError(
      "unavailable",
      `The object store answered ${res.status}.`,
    );
  }

  const info = (key: string, res: Response, size: number): ObjectInfo => {
    const versionId = res.headers.get("x-amz-version-id");
    return {
      key,
      size,
      etag: unquote(res.headers.get("etag") ?? ""),
      ...(versionId && versionId !== "null" ? { versionId } : {}),
      lastModified: iso(res.headers.get("last-modified")),
    };
  };

  const checkVersion = (v: unknown) => {
    if (
      v !== undefined &&
      (typeof v !== "string" || !/^[\x21-\x7e]{1,1024}$/.test(v))
    ) {
      throw new DataError("invalid_request", "versionId is malformed.");
    }
    return v as string | undefined;
  };

  return {
    capabilities: (): CapabilityDescriptor => ({
      kind: "data_source",
      id: cfg.id,
      operations: ["list", "read", "stat"],
      limits: { ...limits },
      attributes: {
        dialect: "s3",
        access: "service_principal",
        readOnly: true,
        bucket: cfg.bucket,
        prefix: cfg.prefix,
      },
    }),

    async list(req: ListRequest) {
      const prefix = req.prefix ?? cfg.prefix;
      if (!safePath(prefix)) {
        throw new DataError(
          "invalid_request",
          "prefix must be at most 1024 bytes with no control characters or . / .. segments.",
        );
      }
      if (!prefix.startsWith(cfg.prefix)) {
        throw new DataError("forbidden", "The prefix is outside this source.");
      }
      const limit = req.limit ?? limits.maxListKeys;
      if (!Number.isInteger(limit) || limit < 1 || limit > limits.maxListKeys) {
        throw new DataError(
          "invalid_request",
          `limit must be 1 to ${limits.maxListKeys}.`,
        );
      }
      if (
        req.cursor !== undefined &&
        (typeof req.cursor !== "string" ||
          !/^[\x21-\x7e]{1,2048}$/.test(req.cursor))
      ) {
        throw new DataError("invalid_request", "cursor is malformed.");
      }
      const s = signal(req.signal);
      const res = await send(
        "GET",
        undefined,
        {
          "list-type": "2",
          prefix,
          "max-keys": String(limit),
          "continuation-token": req.cursor,
        },
        s,
        req.signal,
      );
      const xml = Buffer.from(
        await readCapped(res, MAX_LIST_RESPONSE_BYTES, s, req.signal),
      ).toString("utf8");
      const items: ObjectInfo[] = [];
      for (const m of xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)) {
        const c = m[1] as string;
        const key = tag(c, "Key");
        const size = Number(tag(c, "Size"));
        // A store that ignores the prefix or the limit must fail, never widen or silently drop.
        if (
          !safePath(key) ||
          !key.startsWith(prefix) ||
          !Number.isSafeInteger(size) ||
          items.length === limit
        ) {
          throw malformed();
        }
        items.push({
          key,
          size,
          etag: unquote(tag(c, "ETag") ?? ""),
          lastModified: iso(tag(c, "LastModified")),
        });
      }
      const next =
        tag(xml, "IsTruncated") === "true"
          ? tag(xml, "NextContinuationToken")
          : undefined;
      return next ? { items, nextCursor: next } : { items };
    },

    async stat(req: StatRequest) {
      const key = checkKey(req.key, cfg.prefix);
      const res = await send(
        "HEAD",
        key,
        { versionId: checkVersion(req.versionId) },
        signal(req.signal),
        req.signal,
      );
      return info(key, res, Number(res.headers.get("content-length") ?? 0));
    },

    async read(req: ReadRequest) {
      const key = checkKey(req.key, cfg.prefix);
      const versionId = checkVersion(req.versionId);
      const extra: Record<string, string> = {};
      if (req.range !== undefined) {
        const { start, end } = req.range;
        if (
          !Number.isSafeInteger(start) ||
          !Number.isSafeInteger(end) ||
          start < 0 ||
          end < start
        ) {
          throw new DataError(
            "invalid_request",
            "range must be 0 <= start <= end.",
          );
        }
        if (end - start + 1 > limits.maxObjectBytes) {
          throw new DataError(
            "too_large",
            `A range is at most ${limits.maxObjectBytes} bytes.`,
          );
        }
        extra.range = `bytes=${start}-${end}`;
      }
      const s = signal(req.signal);
      const res = await send("GET", key, { versionId }, s, req.signal, extra);
      // A store that ignores Range would hand back the whole object as if it were the range.
      if (extra.range && res.status !== 206) {
        await res.body?.cancel();
        throw malformed();
      }
      const length = Number(res.headers.get("content-length") ?? Number.NaN);
      if (length > limits.maxObjectBytes) {
        await res.body?.cancel();
        throw new DataError(
          "too_large",
          `The object is over ${limits.maxObjectBytes} bytes.`,
        );
      }
      const total = res.headers.get("content-range")?.match(/\/(\d+)$/)?.[1];
      const body = await readCapped(res, limits.maxObjectBytes, s, req.signal);
      return {
        info: info(key, res, total ? Number(total) : body.byteLength),
        body,
      };
    },

    close: async () => {},
  };
}
