import { createHash } from "node:crypto";
import { EmbedError } from "./index.js";

export interface Chunk {
  /** Stable: the same text at the same position in the same document always has this ID. */
  id: string;
  ordinal: number;
  text: string;
  start: number;
  end: number;
}

export interface ChunkOptions {
  size?: number;
  overlap?: number;
  maxChunks?: number;
}

/** Fixed windows with overlap, cut at the last whitespace in the window when there is one. */
export function chunkText(
  key: string,
  text: string,
  { size = 1000, overlap = 100, maxChunks = 500 }: ChunkOptions = {},
): Chunk[] {
  if (
    !Number.isInteger(size) ||
    !Number.isInteger(overlap) ||
    size < 100 ||
    size > 8000 ||
    overlap < 0 ||
    overlap >= size / 2
  ) {
    throw new EmbedError(
      "invalid_request",
      "size must be 100 to 8000 and overlap under half of it.",
    );
  }
  const chunks: Chunk[] = [];
  let start = 0;
  while (start < text.length) {
    let end = Math.min(start + size, text.length);
    if (end < text.length) {
      const cut = text.slice(start + size / 2, end).search(/\s\S*$/);
      if (cut >= 0) end = start + size / 2 + cut + 1;
    }
    if (chunks.length === maxChunks) {
      throw new EmbedError(
        "too_large",
        `a document has at most ${maxChunks} chunks.`,
      );
    }
    const slice = text.slice(start, end);
    if (slice.trim() !== "") {
      const id = createHash("sha256")
        .update(`${key}\0${start}\0${slice}`)
        .digest("hex")
        .slice(0, 32);
      chunks.push({ id, ordinal: chunks.length, text: slice, start, end });
    }
    if (end === text.length) break;
    start = end - overlap;
  }
  return chunks;
}
