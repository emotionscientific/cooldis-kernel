import { expect, test } from "bun:test";
import {
  agentRecordRef,
  cloneRecord,
  ensureArray,
  ensureArrayObject,
  ensureObject,
  manifestHash,
  operationRef,
  sanitizeRecordName,
  setOptional,
  splitList,
} from "./agentManifestDraft";

test("normalizes agent record refs and hashes from wire shapes", () => {
  expect(agentRecordRef({ name: "demo", version: "0.1.0", ref_uri: "agent://demo@0.1.0" })).toBe(
    "agent://demo@0.1.0",
  );
  expect(agentRecordRef({ name: "demo", version: "0.1.1" })).toBe("agent://demo@0.1.1");
  expect(manifestHash({ name: "demo", version: "0.1.0", manifestHash: "sha256:new" })).toBe("sha256:new");
});

test("mutates manifest draft records through stable helpers", () => {
  const draft = cloneRecord({ identity: { name: "demo" } })!;
  const identity = ensureObject(draft, "identity");
  setOptional(identity, "description", "hello");
  setOptional(identity, "labels", []);
  const tools = ensureArray(draft, "tools");
  const firstTool = ensureArrayObject(tools, 0);
  firstTool.type = "bash_tool";

  expect(draft).toEqual({
    identity: { name: "demo", description: "hello" },
    tools: [{ type: "bash_tool" }],
  });
});

test("builds operation refs and simple manifest strings", () => {
  expect(operationRef({ id: "search", name: "Search", version: "abc", artifactHash: "sha256:123", summary: "", status: "published", power: "operation", calls: 0, source: "search", inputs: [] })).toBe(
    "op://search@sha256:123",
  );
  expect(sanitizeRecordName("search web!")).toBe("search-web");
  expect(splitList("fs:read, net\nprocess")).toEqual(["fs:read", "net", "process"]);
});
