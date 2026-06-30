import { expect, test } from "bun:test";
import { mapThreadStatus, normalizeProvider, reboundThreadFromResponse } from "./threadProjection";

test("normalizes thread provider and status labels", () => {
  expect(normalizeProvider("gpt-test", "gpt-test", "openai")).toBe("openai");
  expect(normalizeProvider("wafer", "glm-5", "openai")).toBe("wafer");
  expect(mapThreadStatus("active")).toBe("running");
  expect(mapThreadStatus("failed")).toBe("error");
  expect(mapThreadStatus(undefined)).toBe("idle");
});

test("projects thread/rebindFork response into a console thread", () => {
  const thread = reboundThreadFromResponse(
    {
      thread: {
        id: "child",
        parentThreadId: "parent",
        model: "echo",
        modelProvider: "local_offline",
        status: "idle",
      },
    },
    {
      agentRef: "agent://demo@0.1.1",
      manifest: {
        id: "agent://demo@0.1.1",
        name: "Demo",
        version: "0.1.1",
        summary: "",
        status: "published",
        model: "model://local_offline/echo",
        tools: [],
        source: "agent://demo@0.1.1",
      },
      runtimeModel: "fallback-model",
      runtimeProvider: "fallback-provider",
      fallbackId: "fallback-id",
      nowMs: 12,
    },
  );

  expect(thread).toMatchObject({
    id: "child",
    parentId: "parent",
    agentName: "Demo",
    status: "idle",
    updatedAt: 12,
  });
});
