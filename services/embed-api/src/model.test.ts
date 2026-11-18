import assert from "node:assert/strict";
import { test } from "node:test";
import { createAiEmbeddingModel } from "./model.js";

type Call = {
  url: string;
  auth: string | null;
  body: { input: string[]; model: string };
};

function stub(answer: (call: Call, n: number) => Response | Promise<Response>) {
  const calls: Call[] = [];
  const fetchStub = (async (url: URL, init: RequestInit) => {
    const call = {
      url: String(url),
      auth: new Headers(init.headers).get("authorization"),
      body: JSON.parse(init.body as string),
    };
    calls.push(call);
    const signal = init.signal as AbortSignal;
    return Promise.race([
      answer(call, calls.length),
      new Promise<never>((_, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        }),
      ),
    ]);
  }) as unknown as typeof fetch;
  const model = createAiEmbeddingModel({
    url: "http://ai.test",
    model: "m1",
    dimensions: 2,
    token: async () => "svc-token",
    fetch: fetchStub,
  });
  return { model, calls };
}

const reply = (inputs: string[], reverse = false) => {
  const data = inputs.map((t, index) => ({
    object: "embedding",
    index,
    embedding: [t.length, 1],
  }));
  return Response.json({ data: reverse ? data.reverse() : data });
};
const texts = (n: number) =>
  Array.from({ length: n }, (_, i) => "x".repeat(i + 1));

test("splits into batches the AI service accepts and keeps input order", async () => {
  const { model, calls } = stub((c) => reply(c.body.input, true));
  const vectors = await model.embed(texts(40));
  assert.deepEqual(
    calls.map((c) => c.body.input.length),
    [16, 16, 8],
  );
  assert.deepEqual(
    vectors.map((v) => v[0]),
    texts(40).map((t) => t.length),
  );
  assert.equal(calls[0]?.auth, "Bearer svc-token");
  assert.equal(calls[0]?.url, "http://ai.test/v1/embeddings");
  assert.equal(calls[0]?.body.model, "m1");
});

test("an unusable answer is a model error that does not repeat the response", async () => {
  const bad: Response[] = [
    new Response("upstream secret", { status: 500 }),
    Response.json({ data: [] }),
    Response.json({ data: [{ index: 0, embedding: [1] }] }),
    Response.json({ data: [{ index: 0, embedding: [1, "x"] }] }),
    Response.json({ data: [{ index: 3, embedding: [1, 1] }] }),
    new Response("not json"),
  ];
  for (const res of bad) {
    const { model } = stub(() => res);
    await assert.rejects(model.embed(["a"]), (err: Error) => {
      assert.equal((err as { code?: string }).code, "model_error");
      assert.doesNotMatch(err.message, /secret/);
      return true;
    });
  }
});

test("a response that repeats an index is refused", async () => {
  const { model } = stub(() =>
    Response.json({
      data: [
        { index: 0, embedding: [1, 1] },
        { index: 0, embedding: [2, 2] },
      ],
    }),
  );
  await assert.rejects(model.embed(["a", "b"]), { code: "model_error" });
});

test("a caller that gives up is not reported as a model failure", async () => {
  const stop = new AbortController();
  const { model } = stub(() => new Promise<Response>(() => {}));
  const pending = model.embed(["a"], stop.signal);
  setTimeout(() => stop.abort(), 10);
  await assert.rejects(pending, (err: Error) => {
    assert.notEqual((err as { code?: string }).code, "model_error");
    return true;
  });
});
