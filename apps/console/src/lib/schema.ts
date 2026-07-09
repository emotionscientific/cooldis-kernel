export type ModeId = "chat" | "registry" | "workspace" | "activity";

export interface NavMode {
  id: ModeId;
  label: string;
  icon: string;
  key?: string;
}

export const MODES: NavMode[] = [
  { id: "chat", label: "Chat", icon: "MessagesSquare", key: "1" },
  { id: "registry", label: "Registry", icon: "Library", key: "2" },
  { id: "workspace", label: "Workspace", icon: "FolderTree", key: "3" },
  { id: "activity", label: "Activity", icon: "Activity", key: "4" },
];

export interface Thread {
  id: string;
  title: string;
  preview: string;
  model: string;
  provider: string;
  status: "running" | "idle" | "error" | "done";
  turns: number;
  updatedAt: number;
  /** Thread-level thinking config as a display label (e.g. "effort: high"). */
  thinking?: string;
  parentId?: string;
  // present when the thread was started from a published agent manifest
  agentRef?: string;
  agentName?: string;
}

export interface ModelInfo {
  id: string;
  name: string;
  provider: string;
  context: string;
  kind: "chat" | "echo";
}

export interface ModelListEntry {
  id: string;
  model?: string;
  providerId?: string;
  providerRef?: string;
  modelRef?: string;
  displayName?: string;
  name?: string;
  description?: string;
  contextWindowTokens?: number;
  maxOutputTokens?: number;
  metadata?: Record<string, unknown>;
  isDefault?: boolean;
}

export interface ModelListResponse {
  data: ModelListEntry[];
  nextCursor: string | null;
}

export interface ModelProviderAuthStatus {
  providerId: string;
  displayName: string | null;
  configured: boolean;
  source: string | null;
  label: string | null;
  authHeader: boolean;
}

export interface ModelProviderAuthListResponse {
  auth: ModelProviderAuthStatus | null;
  data: ModelProviderAuthStatus[];
  nextCursor: string | null;
}

export interface AgentAlias {
  alias: string;
  version: string;
}

export interface AgentModelProfile {
  id: string;
  providerRef: string;
  modelRef: string;
}

export interface AgentListEntry {
  name: string;
  version: string;
  refUri: string;
  manifestHash: string;
  title?: string;
  summary?: string;
  defaultModelProfile: AgentModelProfile | null;
  toolIds: string[];
  aliases: AgentAlias[];
  publishedAtMs: number;
}

export interface AgentListResponse {
  data: AgentListEntry[];
  cursor: string | null;
}

export interface PublishedAgentRecord {
  name: string;
  version: string;
  ref_uri?: string;
  refUri?: string;
  manifest_hash?: string;
  manifestHash?: string;
  resolved_manifest?: unknown;
  resolvedManifest?: unknown;
  aliasResolutionReceipt?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface AgentDraftRequest {
  source?: string;
  manifest?: unknown;
  baseRef?: string;
  baseManifestHash?: string;
  expectedLatestVersion?: string;
}

export interface AgentPlanDiagnostic {
  code: string;
  severity?: string;
  message: string;
  ref?: string;
}

export interface AgentPlanResponse {
  plan: Record<string, unknown>;
  manifest: unknown;
  source: string;
  diagnostics: AgentPlanDiagnostic[];
  suggestedNextVersion: string;
  base: Record<string, unknown> | null;
}

export interface AgentPublishResponse {
  record: PublishedAgentRecord;
  manifest: unknown;
  source: string;
  latestAlias?: Record<string, unknown>;
}

export interface ThreadRebindForkResponse {
  thread: Record<string, unknown>;
  fork?: Record<string, unknown>;
}

export interface OperationListEntry {
  name: string;
  activeArtifactHash: string;
  summary?: string | null;
  manifest?: unknown;
  projections?: unknown;
  interface?: unknown;
  capabilityGrants?: unknown;
  metadata?: unknown;
  source?: unknown;
  build?: unknown;
}

export interface OperationListResponse {
  data: OperationListEntry[];
  cursor: string | null;
}

export type ThreadEventStreamSelector = "thread" | "control" | `derived:${string}` | (string & {});

export interface StreamCursor {
  schema: string;
  stream_id: string;
  sequence: number;
  event_id: string;
}

export interface ThreadEvent {
  schema?: string;
  event_id?: string;
  stream_id?: string;
  sequence?: number;
  coordinates?: Record<string, unknown>;
  payload_schema?: string;
  payloadSchema?: string;
  created_at_ms?: number;
  eventId: string;
  kind: string;
  origin: string;
  provenance: Record<string, unknown> | null;
  atMs: number;
  payload: unknown;
}

// ---- Thread envelope: the manifest.bind.completed receipt, rendered ----
// What the thread can actually do, read off the witnessed bind receipt —
// never inferred from the manifest or asked of the model.
export interface ThreadEnvelopeBinding {
  name: string;
  artifactHash: string;
  grants: string[];
  /** Empty means the binding exposes the whole record. */
  operations: string[];
  /** Direct model/tool-router aliases from direct_tool rows. */
  directTools: { toolName: string; operation: string }[];
}

export interface ThreadEnvelope {
  refUri: string;
  manifestHash: string;
  modelProfileId: string;
  providerId: string;
  modelId: string;
  toolIds: string[];
  operationBindings: ThreadEnvelopeBinding[];
  /** Union of effect grants across the bound rows. */
  granted: string[];
  effectiveCwd: string;
  streaming: boolean | undefined;
  turnTimeoutMs: number | undefined;
  /** Override keys the caller actually exercised on thread/start. */
  overriddenKeys: string[];
}

export interface ThreadEventsListResponse {
  data: ThreadEvent[];
  cursor: string | null;
  streamCursor: StreamCursor | null;
}

export interface ThreadCouplingRow {
  id: string;
  role: string;
  triggerKind: string;
  triggerMatch: unknown;
  sourceStreams: string[];
  sourceKinds: string[];
  sinkStream: string | null;
  sinkKinds: string[];
  functionRef: string;
  artifactHash: string;
  operationName: string | null;
  grants: string[];
  budget: Record<string, unknown> | null;
  configHash: string | null;
  [key: string]: unknown;
}

export interface ThreadCouplingsListResponse {
  data: ThreadCouplingRow[];
  nextCursor: string | null;
  agentRef: string | null;
  manifestHash: string | null;
  bindEventId: string | null;
}

export interface ThreadApprovalEntry {
  approvalId: string;
  status: string;
  kind: string;
  eventId: string;
  suspendedEventId: string;
  requestEventId: string | null;
  turnId: string;
  callId: string;
  snapshotId: string | null;
  reason: string | null;
  [key: string]: unknown;
}

export interface ThreadApprovalsListResponse {
  data: ThreadApprovalEntry[];
  nextCursor: string | null;
}

export interface ApprovalResolveResponse {
  status: "resolved" | "already_resolved" | (string & {});
  approvalId: string;
  decision: "approved" | "denied";
  approved: boolean;
  reason: string | null;
  snapshotId: string | null;
  eventId: string;
  streamId: string;
  sequence: number;
  createdAtMs: number;
}

export interface ThreadWaitingEntry {
  kind: string;
  eventId: string;
  suspendedEventId?: string;
  requestEventId?: string | null;
  streamId?: string;
  sequence?: number;
  createdAtMs?: number;
  turnId: string | null;
  callId: string | null;
  snapshotId: string | null;
  approvalId: string | null;
  waitingOnEventId?: string | null;
  continuation: string | null;
  reason: string | null;
  payload?: unknown;
  sourceEventIds?: string[];
  [key: string]: unknown;
}

export interface ThreadWaitingListResponse {
  data: ThreadWaitingEntry[];
  nextCursor: string | null;
}

export interface ThreadDebugExportStream {
  selector: string;
  streamId: string;
  backend: Record<string, unknown>;
  ackClasses: string[];
  range: Record<string, unknown>;
  data: ThreadEvent[];
  eventCount: number;
  truncated: boolean;
  cursor: string | null;
  streamCursor: StreamCursor | null;
  [key: string]: unknown;
}

export interface ThreadDebugExportReceipt {
  eventId: string;
  streamId: string;
  sequence: number;
  kind: string;
  origin: string;
  payloadSchema: string;
  createdAtMs: number;
  [key: string]: unknown;
}

export interface ThreadDebugExportResponse {
  schema: "cooldis.debug.thread_export/1" | (string & {});
  threadId: string;
  generatedAtMs?: number;
  backend: Record<string, unknown>;
  ackClasses: string[];
  redaction: Record<string, unknown>;
  thread: unknown;
  streams: ThreadDebugExportStream[];
  receipts: ThreadDebugExportReceipt[];
}

export interface DirectoryEntry {
  fileName: string;
  isDirectory: boolean;
  isFile: boolean;
}

export interface ReadDirectoryResponse {
  entries: DirectoryEntry[];
}

export interface ReadFileResponse {
  dataBase64: string;
  text: string;
}

export interface ResourceNode {
  path: string;
  name: string;
  kind: "dir" | "file";
}

// ---- Registry surface: published tools + declared agent manifests ----
export interface ToolDef {
  id: string;
  name: string;
  version: string;
  artifactHash: string;
  summary: string;
  status: "published" | "draft";
  power: string; // host ABI power it draws on
  calls: number;
  source: string;
  inputs: string[];
}

export interface ManifestDef {
  id: string;
  name: string;
  version: string;
  summary: string;
  status: "published" | "draft";
  model: string;
  tools: string[]; // tool ids
  source: string;
}

export interface Tab {
  id: string;
  kind: "chat" | "file";
  title: string;
  icon: string;
  // chat
  threadId?: string;
  activeTurnId?: string;
  busy?: boolean;
  historyState?: "loading" | "ready" | "error";
  messages?: ChatMessage[];
  /** Thinking level sent with this tab's turns; "default" sends nothing. */
  thinking?: ThinkingLevel;
  // file
  filePath?: string;
}

export interface ChatMessage {
  id: string;
  kind?: "text" | "tool";
  role: "user" | "assistant" | "system";
  text: string;
  toolCall?: ChatToolCall;
  /** Model thought stream (agentThinking items/deltas), shown collapsed. */
  thinking?: string;
  /** UI-only details state for the thinking block. */
  thinkingOpen?: boolean;
  turnId?: string;
  live?: boolean;
}

export interface ChatToolCall {
  id: string;
  tool: string;
  status: "inProgress" | "completed" | "failed";
  arguments?: unknown;
  output?: string;
  success?: boolean | null;
  durationMs?: number | null;
}

/**
 * Effort levels the app-server accepts on turn/start ("default" is client-side:
 * send no thinking param and let the thread/daemon config decide).
 */
export type ThinkingLevel = "default" | "disabled" | "low" | "medium" | "high" | "xhigh" | "max";
export type ThinkingEffort = Exclude<ThinkingLevel, "default" | "disabled">;

export const THINKING_LEVELS: { id: ThinkingLevel; label: string }[] = [
  { id: "default", label: "Default" },
  { id: "disabled", label: "Off" },
  { id: "low", label: "Low" },
  { id: "medium", label: "Medium" },
  { id: "high", label: "High" },
  { id: "xhigh", label: "XHigh" },
  { id: "max", label: "Max" },
];

export function isThinkingLevel(value: unknown): value is ThinkingLevel {
  return typeof value === "string" && THINKING_LEVELS.some((level) => level.id === value);
}

export function isThinkingEffort(value: unknown): value is ThinkingEffort {
  return isThinkingLevel(value) && value !== "default" && value !== "disabled";
}
