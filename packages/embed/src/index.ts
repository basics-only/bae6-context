export { type Chunk, type ChunkOptions, chunkText } from "./chunk.js";
export {
  type AclResolver,
  ingestObject,
  prefixAcl,
  type SyncResult,
  syncCollection,
} from "./pipeline.js";

export interface EmbeddingModel {
  readonly id: string;
  readonly dimensions: number;
  embed(texts: string[], signal?: AbortSignal): Promise<number[][]>;
}

/** Where a collection lives and what was indexed from it: one tenant, one source, one model. */
export interface Scope {
  organizationId: string;
  collectionId: string;
}

export interface IndexedChunk {
  id: string;
  ordinal: number;
  text: string;
  /** Character offsets into the decoded document: the cited span. */
  start: number;
  end: number;
  vector: number[];
}

export interface IndexedDocument {
  key: string;
  /** The source's version of the object (version ID, else ETag). */
  revision: string;
  /** Principal keys allowed to read it (ADR-0005), captured at sync time. */
  principals: string[];
  chunks: IndexedChunk[];
}

export interface Hit {
  chunkId: string;
  key: string;
  revision: string;
  start: number;
  end: number;
  text: string;
  score: number;
}

export interface SearchRequest {
  vector: number[];
  limit: number;
  /** The caller's own principals, never taken from the request body. */
  principals: string[];
}

/** An index replaces and removes whole documents, so a partly written revision is never visible. */
export interface VectorIndex {
  /** Atomic: the document's earlier chunks are replaced by `doc.chunks`. */
  upsert(
    scope: Scope,
    doc: IndexedDocument,
    signal?: AbortSignal,
  ): Promise<void>;
  /** Re-captures principals of an unchanged document and marks its ACL fresh. */
  refreshAcl(
    scope: Scope,
    key: string,
    principals: string[],
    signal?: AbortSignal,
  ): Promise<void>;
  /** Tombstones: the document is excluded from search at once. */
  remove(scope: Scope, key: string, signal?: AbortSignal): Promise<void>;
  /** Live documents and their revisions, at most `limit`; more than that is an error. */
  revisions(
    scope: Scope,
    limit: number,
    signal?: AbortSignal,
  ): Promise<Map<string, string>>;
  search(
    scope: Scope,
    req: SearchRequest,
    signal?: AbortSignal,
  ): Promise<Hit[]>;
}

export type EmbedErrorCode =
  | "invalid_request"
  | "too_large"
  | "model_error"
  | "cancelled";

/** Messages never carry document text. */
export class EmbedError extends Error {
  constructor(
    readonly code: EmbedErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "EmbedError";
  }
}

export const MAX_PRINCIPALS = 64;
const PRINCIPAL = /^(user|group):[A-Za-z0-9._@:+-]{1,256}$/;

export function checkPrincipals(principals: readonly string[]) {
  if (
    !Array.isArray(principals) ||
    principals.length > MAX_PRINCIPALS ||
    !principals.every((p) => typeof p === "string" && PRINCIPAL.test(p))
  ) {
    throw new EmbedError(
      "invalid_request",
      `principals must be at most ${MAX_PRINCIPALS} keys like user:<id> or group:<id>.`,
    );
  }
  return [...new Set(principals)].sort();
}
