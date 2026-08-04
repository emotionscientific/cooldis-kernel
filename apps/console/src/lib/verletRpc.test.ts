import { expect, test } from "bun:test";
import { buildAgentDraftParams, parseAgentPublishResponse, parseToolCallItem } from "./verletRpc";

test("parses dynamic tool call item state", () => {
  const item = {
    type: "dynamicToolCall",
    id: "call_1",
    tool: "bash",
    arguments: { command: "pwd" },
    status: "completed",
    success: true,
    durationMs: 42,
    contentItems: [{ type: "inputText", text: "/workspace\n" }],
  };

  expect(parseToolCallItem(item)).toEqual({
    id: "call_1",
    tool: "bash",
    arguments: { command: "pwd" },
    status: "completed",
    success: true,
    durationMs: 42,
    output: "/workspace\n",
  });
});

test("builds agent draft payload with stale-base fields", () => {
  expect(
    buildAgentDraftParams({
      source: "[agent]\nname = \"demo\"\n",
      baseRef: "agent://demo@latest",
      baseManifestHash: "sha256:old",
      expectedLatestVersion: "0.1.0",
    }),
  ).toEqual({
    source: "[agent]\nname = \"demo\"\n",
    baseRef: "agent://demo@latest",
    baseManifestHash: "sha256:old",
    expectedLatestVersion: "0.1.0",
  });
});

test("parses agent publish response", () => {
  const response = parseAgentPublishResponse({
    record: {
      name: "demo",
      version: "0.1.1",
      ref_uri: "agent://demo@0.1.1",
    },
    manifest: { identity: { name: "demo" } },
    source: "[agent]\nname = \"demo\"\n",
    latestAlias: {
      alias: "latest",
      version: "0.1.1",
    },
  });

  expect(response.record.version).toBe("0.1.1");
  expect(response.source).toContain("demo");
  expect(response.latestAlias?.version).toBe("0.1.1");
});
