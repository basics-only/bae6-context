import { checkPrincipals } from "@bae6/context-embed";
import { SLUG } from "./identity.js";

export const COLLECTION_ID = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const MAX_COLLECTIONS = 1000;
const MAX_ACL_RULES = 100;
const MAX_DIMENSIONS = 2000;

export interface CollectionConfig {
  id: string;
  organization: string;
  model: { id: string; dimensions: number };
  source: { region: string; bucket: string; prefix: string; endpoint?: string };
  /** Longest prefix wins; a key matching none is not indexed. */
  acl: { prefix: string; principals: string[] }[];
}

const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

function only(where: string, o: Record<string, unknown>, keys: string[]) {
  const extra = Object.keys(o).filter((k) => !keys.includes(k));
  if (extra.length > 0)
    throw new Error(`${where}: unknown ${extra.join(", ")}`);
}

function str(where: string, v: unknown, allowEmpty = false): string {
  if (typeof v !== "string" || (!allowEmpty && v === "") || v.length > 2048) {
    throw new Error(
      `${where} must be a ${allowEmpty ? "" : "non-empty "}string`,
    );
  }
  return v;
}

function obj(where: string, v: unknown, keys: string[]) {
  if (!isObject(v)) throw new Error(`${where} must be an object`);
  only(where, v, keys);
  return v;
}

export function parseConfig(raw: unknown): CollectionConfig[] {
  const top = obj("config", raw, ["collections"]);
  if (!Array.isArray(top.collections)) {
    throw new Error("config must be {collections: [...]}");
  }
  if (top.collections.length > MAX_COLLECTIONS) {
    throw new Error(`at most ${MAX_COLLECTIONS} collections`);
  }
  const seen = new Set<string>();
  return top.collections.map((c: unknown, i): CollectionConfig => {
    const o = obj(`collections[${i}]`, c, [
      "id",
      "organization",
      "model",
      "source",
      "acl",
    ]);
    if (typeof o.id !== "string" || !COLLECTION_ID.test(o.id)) {
      throw new Error(`collections[${i}].id must match ${COLLECTION_ID}`);
    }
    const where = `collection ${o.id}`;
    if (typeof o.organization !== "string" || !SLUG.test(o.organization)) {
      throw new Error(`${where}: organization must match ${SLUG}`);
    }
    if (seen.has(`${o.organization}/${o.id}`)) {
      throw new Error(`${where}: duplicate in ${o.organization}`);
    }
    seen.add(`${o.organization}/${o.id}`);
    const model = obj(`${where} model`, o.model, ["id", "dimensions"]);
    if (
      !Number.isInteger(model.dimensions) ||
      (model.dimensions as number) < 1 ||
      (model.dimensions as number) > MAX_DIMENSIONS
    ) {
      throw new Error(
        `${where}: model.dimensions must be an integer from 1 to ${MAX_DIMENSIONS}`,
      );
    }
    const source = obj(`${where} source`, o.source, [
      "region",
      "bucket",
      "prefix",
      "endpoint",
    ]);
    if (!Array.isArray(o.acl) || o.acl.length > MAX_ACL_RULES) {
      throw new Error(
        `${where}: acl must be an array of at most ${MAX_ACL_RULES}`,
      );
    }
    return {
      id: o.id,
      organization: o.organization,
      model: {
        id: str(`${where} model.id`, model.id),
        dimensions: model.dimensions as number,
      },
      source: {
        region: str(`${where} source.region`, source.region),
        bucket: str(`${where} source.bucket`, source.bucket),
        prefix: str(`${where} source.prefix`, source.prefix, true),
        ...(source.endpoint === undefined
          ? {}
          : { endpoint: str(`${where} source.endpoint`, source.endpoint) }),
      },
      acl: o.acl.map((r: unknown, j) => {
        const rule = obj(`${where} acl[${j}]`, r, ["prefix", "principals"]);
        return {
          prefix: str(`${where} acl[${j}].prefix`, rule.prefix, true),
          principals: checkPrincipals(rule.principals as string[]),
        };
      }),
    };
  });
}
