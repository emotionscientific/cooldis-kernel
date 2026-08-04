import { getString, type JsonValue } from "./verletRpc";
import type { ManifestDef, Thread, ThreadRebindForkResponse } from "./schema";

export function normalizeProvider(provider: string | undefined, model: string | undefined, fallback: string) {
  if (!provider || provider === model) return fallback;
  return provider;
}

export function mapThreadStatus(status: string | undefined): Thread["status"] {
  switch (status) {
    case "running":
    case "active":
      return "running";
    case "error":
    case "failed":
      return "error";
    default:
      return "idle";
  }
}

export function reboundThreadFromResponse(
  response: ThreadRebindForkResponse,
  options: {
    agentRef: string;
    manifest?: ManifestDef;
    runtimeModel: string;
    runtimeProvider: string;
    fallbackId: string;
    nowMs: number;
  },
): Thread {
  const threadValue = response.thread as JsonValue;
  const model = getString(threadValue, "model") ?? options.manifest?.model ?? options.runtimeModel;
  return {
    id: getString(threadValue, "id") ?? options.fallbackId,
    agentRef: options.agentRef,
    agentName: options.manifest?.name ?? options.agentRef,
    parentId: getString(threadValue, "parentThreadId") ?? getString(threadValue, "forkedFromId"),
    title: getString(threadValue, "name") ?? "Rebound thread",
    preview: getString(threadValue, "preview") ?? "",
    model,
    provider: normalizeProvider(getString(threadValue, "modelProvider"), model, options.runtimeProvider),
    status: mapThreadStatus(getString(threadValue, "status")),
    turns: 0,
    updatedAt: options.nowMs,
  };
}
