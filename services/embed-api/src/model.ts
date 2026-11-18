import { type EmbeddingModel, EmbedError } from "@bae6/context-embed";

const AI_BATCH = 16;
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

export interface AiModelOptions {
  /** Base URL of bae6-ai. */
  url: string;
  model: string;
  dimensions: number;
  /** The service's own token for bae6-ai, never a caller's (ADR-0003). */
  token: () => Promise<string>;
  timeoutMs?: number;
  fetch?: typeof fetch;
}

const fail = (message: string) => new EmbedError("model_error", message);

async function bounded(res: Response, max: number) {
  const chunks: Uint8Array[] = [];
  let size = 0;
  for await (const chunk of res.body ?? []) {
    size += chunk.length;
    if (size > max) throw fail("the embeddings response is too large.");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

export function createAiEmbeddingModel(opts: AiModelOptions): EmbeddingModel {
  const { timeoutMs = 30_000, fetch: send = fetch } = opts;
  const endpoint = new URL("/v1/embeddings", opts.url);

  async function batch(texts: string[], signal?: AbortSignal) {
    const deadline = AbortSignal.timeout(timeoutMs);
    let res: Response;
    let text: string;
    try {
      res = await send(endpoint, {
        method: "POST",
        headers: {
          authorization: `Bearer ${await opts.token()}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: opts.model,
          input: texts,
          encoding_format: "float",
        }),
        signal: signal ? AbortSignal.any([signal, deadline]) : deadline,
      });
      if (!res.ok) throw fail(`the embeddings call returned ${res.status}.`);
      text = await bounded(res, MAX_RESPONSE_BYTES);
    } catch (err) {
      if (signal?.aborted) throw err;
      if (err instanceof EmbedError) throw err;
      throw fail("the embeddings call failed.");
    }
    let data: unknown;
    try {
      data = (JSON.parse(text) as { data?: unknown }).data;
    } catch {
      throw fail("the embeddings response is not JSON.");
    }
    if (!Array.isArray(data) || data.length !== texts.length) {
      throw fail("the embeddings response does not match the input.");
    }
    const vectors: number[][] = new Array(texts.length);
    for (const item of data as { index?: unknown; embedding?: unknown }[]) {
      const { index, embedding } = item ?? {};
      if (
        !Number.isInteger(index) ||
        (index as number) < 0 ||
        (index as number) >= texts.length ||
        vectors[index as number] !== undefined ||
        !Array.isArray(embedding) ||
        embedding.length !== opts.dimensions ||
        !embedding.every(Number.isFinite)
      ) {
        throw fail("the embeddings response is unusable.");
      }
      vectors[index as number] = embedding as number[];
    }
    return vectors;
  }

  return {
    id: opts.model,
    dimensions: opts.dimensions,
    async embed(texts, signal) {
      const out: number[][] = [];
      for (let i = 0; i < texts.length; i += AI_BATCH) {
        out.push(...(await batch(texts.slice(i, i + AI_BATCH), signal)));
      }
      return out;
    },
  };
}
