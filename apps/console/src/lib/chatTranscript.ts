import type { TranscriptItem } from "./cooldisRpc";
import type { ChatMessage } from "./schema";

export function mergeTranscriptMessages(items: TranscriptItem[], streamed: ChatMessage[]): ChatMessage[] {
  const streamedByKey = new Map(
    streamed
      .map((message) => [messageKey(message), message] as const)
      .filter((entry): entry is readonly [string, ChatMessage] => Boolean(entry[0])),
  );
  const streamedAssistantTextTurnIds = new Set(
    streamed
      .filter((message) => message.role === "assistant" && message.kind !== "tool" && message.turnId)
      .map((message) => message.turnId as string),
  );
  const thinkingByTurn = new Map<string, string>();
  for (const item of items) {
    if (item.kind !== "thinking" || !item.turnId) continue;
    const previous = thinkingByTurn.get(item.turnId);
    thinkingByTurn.set(item.turnId, previous ? `${previous}\n${item.text}` : item.text);
  }
  const history: ChatMessage[] = [];
  for (const item of items) {
    if (item.kind === "thinking") continue;
    if (item.kind === "message" && item.role === "assistant" && item.turnId && streamedAssistantTextTurnIds.has(item.turnId)) {
      continue;
    }
    if (item.kind === "tool" && item.toolCall) {
      history.push({
        id: item.id,
        kind: "tool",
        role: "assistant",
        text: "",
        toolCall: item.toolCall,
        turnId: item.turnId,
        live: item.toolCall.status === "inProgress",
      });
      continue;
    }
    const message: ChatMessage = { id: item.id, role: item.role, text: item.text, turnId: item.turnId };
    if (message.role === "assistant" && message.turnId) {
      mergeThinkingText(message, thinkingByTurn.get(message.turnId));
    }
    history.push(message);
  }
  // Thinking rides on its turn's assistant message rather than being a message
  // (the agentThinking item precedes the agentMessage item in turn order).
  const deduped = history.filter((message) => {
    const key = messageKey(message);
    const streamedMessage = key ? streamedByKey.get(key) : undefined;
    if (!streamedMessage) return true;
    if (message.kind === "tool" && streamedMessage.kind === "tool") mergeToolCall(streamedMessage, message);
    if (message.role === "assistant") mergeThinkingText(streamedMessage, message.thinking);
    return false;
  });
  for (const message of streamed) {
    if (message.role === "assistant" && message.turnId) mergeThinkingText(message, thinkingByTurn.get(message.turnId));
  }
  return [...deduped, ...streamed];
}

function mergeToolCall(target: ChatMessage, source: ChatMessage) {
  if (!target.toolCall || !source.toolCall) return;
  target.toolCall = {
    ...target.toolCall,
    ...source.toolCall,
    output: source.toolCall.output ?? target.toolCall.output,
  };
  target.live = source.toolCall.status === "inProgress";
}

function mergeThinkingText(message: ChatMessage, thinking: string | undefined) {
  if (!thinking) return;
  if (!message.thinking) {
    message.thinking = thinking;
    return;
  }
  if (message.thinking === thinking || message.thinking.startsWith(thinking)) return;
  message.thinking = thinking.startsWith(message.thinking) ? thinking : `${message.thinking}\n${thinking}`;
}

function messageKey(message: Pick<ChatMessage, "kind" | "role" | "toolCall" | "turnId">) {
  if (!message.turnId) return undefined;
  if (message.kind === "tool") return message.toolCall?.id ? `${message.turnId}:tool:${message.toolCall.id}` : undefined;
  return `${message.turnId}:${message.kind ?? "text"}:${message.role}`;
}
