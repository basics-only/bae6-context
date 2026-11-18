import type { ObjectInfo, ObjectSource } from "@bae6/context-data";
import { type ChunkOptions, chunkText } from "./chunk.js";
import {
  checkPrincipals,
  type EmbeddingModel,
  EmbedError,
  type IndexedChunk,
  type Scope,
  type VectorIndex,
} from "./index.js";

/** Who may read an object, as principal keys. An empty answer means nobody: it is not indexed. */
export type AclResolver = (
  info: ObjectInfo,
  signal?: AbortSignal,
) => Promise<string[]>;

/** The first matching rule wins when ordered longest prefix first; no match means no access. */
export function prefixAcl(
  rules: { prefix: string; principals: string[] }[],
): AclResolver {
  const sorted = rules
    .map((r) => ({
      prefix: r.prefix,
      principals: checkPrincipals(r.principals),
    }))
    .sort((a, b) => b.prefix.length - a.prefix.length);
  return async (info) =>
    sorted.find((r) => info.key.startsWith(r.prefix))?.principals ?? [];
}

interface Deps {
  source: ObjectSource;
  model: EmbeddingModel;
  index: VectorIndex;
  acl: AclResolver;
  chunking?: ChunkOptions;
}

const TEXT_KEY = /\.(txt|md|markdown|csv|json)$/i;
const EMBED_BATCH = 32;

const revisionOf = (info: ObjectInfo) => info.versionId ?? info.etag;

export async function ingestObject(
  deps: Deps,
  scope: Scope,
  info: ObjectInfo,
  principals: string[],
  signal?: AbortSignal,
) {
  const acl = checkPrincipals(principals);
  if (acl.length === 0 || !TEXT_KEY.test(info.key)) {
    throw new EmbedError("invalid_request", "the object cannot be indexed.");
  }
  const { info: read, body } = await deps.source.read({
    key: info.key,
    ...(info.versionId ? { versionId: info.versionId } : {}),
    ...(signal ? { signal } : {}),
  });
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(body);
  } catch {
    throw new EmbedError("invalid_request", "the object is not UTF-8 text.");
  }
  const chunks = chunkText(info.key, text, deps.chunking);
  const indexed: IndexedChunk[] = [];
  for (let i = 0; i < chunks.length; i += EMBED_BATCH) {
    const batch = chunks.slice(i, i + EMBED_BATCH);
    const vectors = await deps.model.embed(
      batch.map((c) => c.text),
      signal,
    );
    if (
      vectors.length !== batch.length ||
      !vectors.every(
        (v) => v.length === deps.model.dimensions && v.every(Number.isFinite),
      )
    ) {
      throw new EmbedError(
        "model_error",
        "the model returned unusable vectors.",
      );
    }
    for (const [j, c] of batch.entries()) {
      indexed.push({ ...c, vector: vectors[j] as number[] });
    }
  }
  await deps.index.upsert(
    scope,
    {
      key: info.key,
      revision: revisionOf(read),
      principals: acl,
      chunks: indexed,
    },
    signal,
  );
}

export interface SyncResult {
  ingested: number;
  refreshed: number;
  removed: number;
  /** Objects that could not be indexed this round; they stay out of search once their ACL is stale. */
  failed: number;
}

/**
 * Brings the index in line with the source. A listing that cannot be completed within
 * `maxObjects` is an error: nothing is removed from a partial view.
 */
export async function syncCollection(
  deps: Deps,
  scope: Scope,
  {
    prefix,
    maxObjects = 10_000,
    signal,
  }: {
    prefix?: string;
    maxObjects?: number;
    signal?: AbortSignal;
  } = {},
): Promise<SyncResult> {
  const listed: ObjectInfo[] = [];
  let cursor: string | undefined;
  do {
    const page = await deps.source.list({
      ...(prefix !== undefined ? { prefix } : {}),
      ...(cursor ? { cursor } : {}),
      ...(signal ? { signal } : {}),
    });
    listed.push(...page.items);
    if (listed.length > maxObjects) {
      throw new EmbedError(
        "too_large",
        `a sync covers at most ${maxObjects} objects.`,
      );
    }
    cursor = page.nextCursor;
  } while (cursor);

  const known = await deps.index.revisions(scope, maxObjects, signal);
  const result: SyncResult = {
    ingested: 0,
    refreshed: 0,
    removed: 0,
    failed: 0,
  };
  const seen = new Set<string>();
  for (const info of listed) {
    if (signal?.aborted)
      throw new EmbedError("cancelled", "the sync was cancelled.");
    try {
      const principals = TEXT_KEY.test(info.key)
        ? await deps.acl(info, signal)
        : [];
      if (principals.length === 0) continue;
      seen.add(info.key);
      if (known.get(info.key) === revisionOf(info)) {
        await deps.index.refreshAcl(
          scope,
          info.key,
          checkPrincipals(principals),
          signal,
        );
        result.refreshed++;
      } else {
        await ingestObject(deps, scope, info, principals, signal);
        result.ingested++;
      }
    } catch (err) {
      if (signal?.aborted) throw err;
      result.failed++;
    }
  }
  for (const key of known.keys()) {
    if (seen.has(key)) continue;
    await deps.index.remove(scope, key, signal);
    result.removed++;
  }
  return result;
}
