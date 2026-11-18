import assert from "node:assert/strict";
import { test } from "node:test";
import { checkQuery, DataError } from "./index.js";

const limits = {
  maxRows: 100,
  maxBytes: 1000,
  maxTimeoutMs: 5000,
  maxStatementLength: 50,
};

const code = (fn: () => unknown) => {
  try {
    fn();
  } catch (err) {
    assert.ok(err instanceof DataError);
    return err.code;
  }
  assert.fail("expected a DataError");
};

test("defaults to the adapter's limits", () => {
  const q = checkQuery({ statement: "SELECT 1" }, limits);
  assert.equal(q.maxRows, 100);
  assert.equal(q.timeoutMs, 5000);
  assert.equal(q.signal.aborted, false);
});

test("refuses requests over the limits instead of clamping them", () => {
  for (const req of [
    { statement: "" },
    { statement: "x".repeat(51) },
    { statement: "SELECT 1", maxRows: 101 },
    { statement: "SELECT 1", maxRows: 0 },
    { statement: "SELECT 1", timeoutMs: 5001 },
    { statement: "SELECT 1", parameters: [Number.NaN] },
    { statement: "SELECT 1", parameters: { a: {} as unknown as string } },
    { statement: "SELECT 1", parameters: Array(257).fill(1) },
  ]) {
    assert.equal(
      code(() => checkQuery(req, limits)),
      "invalid_request",
    );
  }
});

test("the caller's signal and the timeout both abort", async () => {
  const caller = new AbortController();
  const q = checkQuery(
    { statement: "SELECT 1", signal: caller.signal },
    limits,
  );
  caller.abort();
  assert.equal(q.signal.aborted, true);
  const t = checkQuery({ statement: "SELECT 1", timeoutMs: 1 }, limits);
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(t.signal.aborted, true);
});
