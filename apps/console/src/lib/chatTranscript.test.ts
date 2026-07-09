import { expect, test } from "bun:test";
import { mergeTranscriptMessages } from "./chatTranscript";
import type { TranscriptItem } from "./cooldisRpc";
import type { ChatMessage } from "./schema";

test("keeps streamed assistant/tool ordering when history has one full assistant item", () => {
  const history: TranscriptItem[] = [
    { id: "turn_1:agent-message", role: "assistant", kind: "message", text: "before after", turnId: "turn_1" },
    {
      id: "call_1",
      role: "assistant",
      kind: "tool",
      text: "",
      turnId: "turn_1",
      toolCall: { id: "call_1", tool: "bash", status: "completed", output: "ok" },
    },
  ];
  const streamed: ChatMessage[] = [
    { id: "seg_1", kind: "text", role: "assistant", text: "before", turnId: "turn_1" },
    {
      id: "tool_call_1",
      kind: "tool",
      role: "assistant",
      text: "",
      turnId: "turn_1",
      toolCall: { id: "call_1", tool: "bash", status: "inProgress" },
    },
    { id: "seg_2", kind: "text", role: "assistant", text: "after", turnId: "turn_1" },
  ];

  const merged = mergeTranscriptMessages(history, streamed);

  expect(merged.map((message) => message.id)).toEqual(["seg_1", "tool_call_1", "seg_2"]);
  expect(merged[1].toolCall?.status).toBe("completed");
  expect(merged[1].toolCall?.output).toBe("ok");
});
