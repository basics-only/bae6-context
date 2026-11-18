import assert from "node:assert/strict";
import { test } from "node:test";
import { parseConfig } from "./config.js";

const collection = (over: Record<string, unknown> = {}) => ({
  id: "kb",
  organization: "acme",
  model: { id: "m1", dimensions: 8 },
  source: { region: "us-east-1", bucket: "b", prefix: "" },
  acl: [{ prefix: "", principals: ["user:alice"] }],
  ...over,
});

test("accepts a complete collection", () => {
  const [c] = parseConfig({ collections: [collection()] });
  assert.equal(c?.id, "kb");
  assert.deepEqual(c?.acl, [{ prefix: "", principals: ["user:alice"] }]);
});

test("refuses malformed or unknown configuration", () => {
  const bad: unknown[] = [
    null,
    [],
    {},
    { collections: [], extra: 1 },
    { collections: [collection({ id: "Bad Id" })] },
    { collections: [collection({ organization: "A B" })] },
    { collections: [collection({ extra: 1 })] },
    { collections: [collection({ model: { id: "m", dimensions: 0 } })] },
    { collections: [collection({ model: { id: "m", dimensions: 2001 } })] },
    { collections: [collection({ source: { region: "r", bucket: "b" } })] },
    { collections: [collection({ acl: "all" })] },
    { collections: [collection({ acl: [{ prefix: "", principals: ["*"] }] })] },
  ];
  for (const raw of bad) {
    assert.throws(() => parseConfig(raw), JSON.stringify(raw));
  }
});

test("refuses the same collection twice in one organization but allows it in another", () => {
  assert.throws(() =>
    parseConfig({ collections: [collection(), collection()] }),
  );
  assert.equal(
    parseConfig({
      collections: [collection(), collection({ organization: "globex" })],
    }).length,
    2,
  );
});
