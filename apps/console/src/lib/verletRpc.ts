import type {
  AgentDraftRequest,
  AgentListEntry,
  AgentListResponse,
  AgentPlanDiagnostic,
  AgentPlanResponse,
  AgentPublishResponse,
  ApprovalResolveResponse,
  ChatToolCall,
  DirectoryEntry,
  ModelListEntry,
  ModelListResponse,
  ModelProviderAuthListResponse,
  ModelProviderAuthStatus,
  OperationListEntry,
  OperationListResponse,
  PublishedAgentRecord,
  ReadDirectoryResponse,
  ReadFileResponse,
  ThreadRebindForkResponse,
  StreamCursor,
  ThreadApprovalEntry,
  ThreadApprovalsListResponse,
  ThreadCouplingRow,
  ThreadCouplingsListResponse,
  ThreadDebugExportReceipt,
  ThreadDebugExportResponse,
  ThreadDebugExportStream,
  ThreadEventsListResponse,
  ThreadEvent,
  ThreadEventStreamSelector,
  ThreadWaitingEntry,
  ThreadWaitingListResponse,
} from "./schema";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type RpcEvent =
  | {
      kind: "sent" | "received";
      at: number;
      method?: string;
      id?: string | number;
      payload: JsonValue;
    }
  | {
      kind: "status";
      at: number;
      message: string;
      payload?: JsonValue;
    }
  | {
      kind: "error";
      at: number;
      message: string;
      payload?: JsonValue;
    };

export type RpcNotification = {
  method: string;
  params?: JsonValue;
};

type PendingRequest = {
  method: string;
  resolve: (value: JsonValue) => void;
  reject: (error: Error) => void;
};

export class VerletRpcClient {
  private socket: WebSocket | null = null;
  private nextId = 1;
  private pending = new Map<string | number, PendingRequest>();
  private notifications = new Set<(notification: RpcNotification) => void>();
  private events = new Set<(event: RpcEvent) => void>();

  constructor(
    private readonly url: string,
    private readonly sessionToken?: string,
  ) {}

  get connected() {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  onNotification(callback: (notification: RpcNotification) => void) {
    this.notifications.add(callback);
    return () => this.notifications.delete(callback);
  }

  onEvent(callback: (event: RpcEvent) => void) {
    this.events.add(callback);
    return () => this.events.delete(callback);
  }

  async connect() {
    if (this.connected) {
      return;
    }

    await this.openSocket();
    await this.request("initialize", {
      clientInfo: {
        name: "verlet-console",
        title: "Verlet Console",
        version: "0.1.0",
      },
      capabilities: {
        experimentalApi: true,
        requestAttestation: false,
        optOutNotificationMethods: null,
      },
    });
    this.notify("initialized");
  }

  disconnect() {
    this.socket?.close(1000, "client disconnect");
    this.socket = null;
    for (const pending of this.pending.values()) {
      pending.reject(new Error("WebSocket disconnected"));
    }
    this.pending.clear();
    this.emit({ kind: "status", at: Date.now(), message: "Disconnected" });
  }

  request(method: string, params: JsonValue = {}) {
    if (!this.connected || !this.socket) {
      return Promise.reject(new Error("WebSocket is not connected"));
    }

    const id = this.nextId++;
    const payload = { id, method, params };
    const promise = new Promise<JsonValue>((resolve, reject) => {
      this.pending.set(id, { method, resolve, reject });
    });
    this.socket.send(JSON.stringify(payload));
    this.emit({ kind: "sent", at: Date.now(), method, id, payload });
    return promise;
  }

  async measureHealthRoundTrip(): Promise<number> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 1_500);
    const t0 = performance.now();
    try {
      await fetch(healthUrlForRpcEndpoint(this.url), {
        cache: "no-store",
        mode: "no-cors",
        signal: controller.signal,
      });
      return performance.now() - t0;
    } finally {
      clearTimeout(timeout);
    }
  }

  async listModels(): Promise<ModelListResponse> {
    return parseModelListResponse(await this.request("model/list"));
  }

  async modelProviderAuthStatus(providerId?: string): Promise<ModelProviderAuthListResponse> {
    return parseModelProviderAuthListResponse(
      await this.request("modelProvider/auth/status", providerId ? { providerId } : {}),
    );
  }

  async setModelProviderAuth(providerId: string, apiKey: string): Promise<ModelProviderAuthStatus> {
    const response = await this.request("modelProvider/auth/set", { providerId, apiKey });
    return requiredModelProviderAuth(response);
  }

  async deleteModelProviderAuth(providerId: string): Promise<ModelProviderAuthStatus> {
    const response = await this.request("modelProvider/auth/delete", { providerId });
    return requiredModelProviderAuth(response);
  }

  async listAgents(): Promise<AgentListResponse> {
    return parseAgentListResponse(await this.request("agent/list"));
  }

  async readAgent(ref: string): Promise<PublishedAgentRecord> {
    return parseAgentRecord(await this.request("agent/read", { ref }));
  }

  async planAgentDraft(params: AgentDraftRequest): Promise<AgentPlanResponse> {
    return parseAgentPlanResponse(await this.request("agent/plan", buildAgentDraftParams(params)));
  }

  async publishAgentDraft(params: AgentDraftRequest): Promise<AgentPublishResponse> {
    return parseAgentPublishResponse(await this.request("agent/publish", buildAgentDraftParams(params)));
  }

  async rebindThread(params: {
    threadId: string;
    agentRef: string;
    reason?: "manifest_update" | "tool_added" | "model_changed" | "manual";
  }): Promise<ThreadRebindForkResponse> {
    return parseThreadRebindForkResponse(
      await this.request("thread/rebindFork", {
        threadId: params.threadId,
        agentRef: params.agentRef,
        reason: params.reason ?? "manifest_update",
      }),
    );
  }

  async listOperations(): Promise<OperationListResponse> {
    return parseOperationListResponse(await this.request("operation/list"));
  }

  async listThreadEvents(params: {
    threadId: string;
    stream?: ThreadEventStreamSelector;
    cursor?: string;
    streamCursor?: StreamCursor;
    limit?: number;
    kinds?: string[];
  }): Promise<ThreadEventsListResponse> {
    if (params.cursor && params.streamCursor) {
      throw new Error("thread/events/list accepts either cursor or streamCursor, not both");
    }
    const requestParams: { [key: string]: JsonValue } = { threadId: params.threadId };
    if (params.stream) requestParams.stream = params.stream;
    if (params.cursor) requestParams.cursor = params.cursor;
    if (params.streamCursor) requestParams.streamCursor = jsonObject(params.streamCursor);
    if (params.limit !== undefined) requestParams.limit = params.limit;
    if (params.kinds?.length) requestParams.kinds = params.kinds;
    return parseThreadEventsListResponse(await this.request("thread/events/list", requestParams));
  }

  async listThreadCouplings(params: { threadId: string; limit?: number }): Promise<ThreadCouplingsListResponse> {
    const requestParams: { [key: string]: JsonValue } = { threadId: params.threadId };
    if (params.limit !== undefined) requestParams.limit = params.limit;
    return parseThreadCouplingsListResponse(await this.request("thread/couplings/list", requestParams));
  }

  async listThreadApprovals(params: { threadId: string; limit?: number }): Promise<ThreadApprovalsListResponse> {
    const requestParams: { [key: string]: JsonValue } = { threadId: params.threadId };
    if (params.limit !== undefined) requestParams.limit = params.limit;
    return parseThreadApprovalsListResponse(await this.request("thread/approvals/list", requestParams));
  }

  async resolveApproval(params: {
    threadId: string;
    approvalId: string;
    decision: "approved" | "denied";
    reason?: string;
  }): Promise<ApprovalResolveResponse> {
    const requestParams: { [key: string]: JsonValue } = {
      threadId: params.threadId,
      approvalId: params.approvalId,
      decision: params.decision,
    };
    if (params.reason) requestParams.reason = params.reason;
    return parseApprovalResolveResponse(await this.request("approval/resolve", requestParams));
  }

  async listThreadWaiting(params: { threadId: string; limit?: number }): Promise<ThreadWaitingListResponse> {
    const requestParams: { [key: string]: JsonValue } = { threadId: params.threadId };
    if (params.limit !== undefined) requestParams.limit = params.limit;
    return parseThreadWaitingListResponse(await this.request("thread/waiting/list", requestParams));
  }

  async exportThreadDebug(params: {
    threadId: string;
    streams?: ThreadEventStreamSelector[];
    includeThread?: boolean;
    maxEventsPerStream?: number;
    redact?: boolean;
  }): Promise<ThreadDebugExportResponse> {
    const requestParams: { [key: string]: JsonValue } = { threadId: params.threadId };
    if (params.streams?.length) requestParams.streams = params.streams;
    if (params.includeThread !== undefined) requestParams.includeThread = params.includeThread;
    if (params.maxEventsPerStream !== undefined) requestParams.maxEventsPerStream = params.maxEventsPerStream;
    if (params.redact !== undefined) requestParams.redact = params.redact;
    return parseThreadDebugExportResponse(await this.request("thread/debug/export", requestParams));
  }

  async readDirectory(path: string): Promise<ReadDirectoryResponse> {
    return parseReadDirectoryResponse(await this.request("fs/readDirectory", { path }));
  }

  async readThreadTranscript(threadId: string): Promise<TranscriptItem[]> {
    const response = await this.request("thread/read", { threadId });
    const thread = getObject(response, "thread");
    const turns = optionalArray(thread, "turns") ?? [];
    return turns.flatMap(parseTranscriptTurn);
  }

  async readFile(path: string): Promise<ReadFileResponse> {
    const response = await this.request("fs/readFile", { path });
    const dataBase64 = getString(response, "dataBase64") ?? "";
    return {
      dataBase64,
      text: decodeBase64Text(dataBase64),
    };
  }

  notify(method: string, params?: JsonValue) {
    if (!this.connected || !this.socket) {
      throw new Error("WebSocket is not connected");
    }
    const payload: JsonValue = params === undefined ? { method } : { method, params };
    this.socket.send(JSON.stringify(payload));
    this.emit({ kind: "sent", at: Date.now(), method, payload });
  }

  private openSocket() {
    return new Promise<void>((resolve, reject) => {
      const protocols = this.sessionToken ? [`verlet-console-token.${this.sessionToken}`] : undefined;
      const socket = new WebSocket(this.url, protocols);
      this.socket = socket;

      socket.addEventListener("open", () => {
        this.emit({
          kind: "status",
          at: Date.now(),
          message: `Connected to ${this.url}`,
        });
        resolve();
      });
      socket.addEventListener("message", (event) => this.handleMessage(event.data));
      socket.addEventListener("error", () => {
        reject(new Error(`Could not connect to ${this.url}`));
      });
      socket.addEventListener("close", () => {
        for (const pending of this.pending.values()) {
          pending.reject(new Error("WebSocket closed"));
        }
        this.pending.clear();
        this.socket = null;
        this.emit({ kind: "status", at: Date.now(), message: "Socket closed" });
      });
    });
  }

  private handleMessage(raw: unknown) {
    const payload = parsePayload(raw);
    this.emit({
      kind: "received",
      at: Date.now(),
      method: objectString(payload, "method"),
      id: objectId(payload, "id"),
      payload,
    });

    const id = objectId(payload, "id");
    const pending = id === undefined ? undefined : this.pending.get(id);
    if (pending) {
      this.pending.delete(id!);
      const error = objectValue(payload, "error");
      if (error) {
        pending.reject(new Error(extractErrorMessage(error)));
      } else {
        pending.resolve(objectValue(payload, "result") ?? null);
      }
      return;
    }

    const method = objectString(payload, "method");
    if (method) {
      const notification = {
        method,
        params: objectValue(payload, "params"),
      };
      for (const callback of this.notifications) {
        callback(notification);
      }
    }
  }

  private emit(event: RpcEvent) {
    for (const callback of this.events) {
      callback(event);
    }
  }
}

export interface TranscriptItem {
  id: string;
  role: "user" | "assistant";
  /** agentThinking items carry the model's thought stream, not the reply. */
  kind: "message" | "thinking" | "tool";
  text: string;
  toolCall?: ChatToolCall;
  turnId?: string;
}

// Lenient by design: thread/read history is rendered best-effort, so an
// unrecognized item kind degrades to "skip this item", not a dead transcript.
function parseTranscriptTurn(turn: JsonValue): TranscriptItem[] {
  const turnId = optionalString(turn, "id");
  const items = optionalArray(turn, "items") ?? [];
  const parsed: TranscriptItem[] = [];
  for (const item of items) {
    const type = optionalString(item, "type") ?? "";
    const toolCall = parseToolCallItem(item);
    if (toolCall) {
      parsed.push({
        id: optionalString(item, "id") ?? `${turnId}:tool:${parsed.length}`,
        role: "assistant",
        kind: "tool",
        text: "",
        toolCall,
        turnId,
      });
      continue;
    }
    const kind = type === "agentThinking" ? ("thinking" as const) : ("message" as const);
    const role = type.toLowerCase().includes("user")
      ? "user"
      : type.toLowerCase().includes("agent") || type.toLowerCase().includes("assistant")
        ? "assistant"
        : undefined;
    if (!role) continue;
    const content = optionalArray(item, "content") ?? [];
    const text = content
      .map((part) => (optionalString(part, "type") === "text" ? (objectValue(part, "text") ?? "") : ""))
      .filter((part): part is string => typeof part === "string")
      .join("");
    if (!text) continue;
    parsed.push({ id: optionalString(item, "id") ?? `${turnId}:${parsed.length}`, role, kind, text, turnId });
  }
  return parsed;
}

export function parseToolCallItem(item: JsonValue | undefined): ChatToolCall | undefined {
  if (optionalString(item, "type") !== "dynamicToolCall") return undefined;
  const id = optionalString(item, "id");
  const tool = optionalString(item, "tool");
  if (!id || !tool) return undefined;
  const rawStatus = optionalString(item, "status");
  const status: ChatToolCall["status"] =
    rawStatus === "completed" || rawStatus === "failed" ? rawStatus : "inProgress";
  return {
    id,
    tool,
    status,
    arguments: objectValue(item, "arguments"),
    output: toolOutputText(item),
    success: optionalBoolean(item, "success") ?? null,
    durationMs: optionalNumber(item, "durationMs") ?? null,
  };
}

function toolOutputText(item: JsonValue | undefined) {
  const contentItems = optionalArray(item, "contentItems") ?? [];
  if (!contentItems.length) return undefined;
  const text = contentItems
    .map((part) => {
      const text = objectValue(part, "text");
      return typeof text === "string" ? text : "";
    })
    .filter(Boolean)
    .join("\n");
  return text || undefined;
}

export function textInput(text: string): JsonValue {
  return {
    type: "text",
    text,
    text_elements: [],
  };
}

export function getObject(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return child && typeof child === "object" && !Array.isArray(child) ? child : undefined;
}

export function getArray(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return Array.isArray(child) ? child : [];
}

export function getString(value: JsonValue | undefined, key: string) {
  return objectString(value, key);
}

function parsePayload(raw: unknown): JsonValue {
  if (typeof raw !== "string") {
    return { raw: String(raw) };
  }
  try {
    return JSON.parse(raw) as JsonValue;
  } catch {
    return { raw };
  }
}

function objectValue(value: JsonValue | undefined, key: string) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  return value[key];
}

function objectString(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "string" ? child : undefined;
}

function objectId(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "string" || typeof child === "number" ? child : undefined;
}

function extractErrorMessage(error: JsonValue) {
  const message = objectString(error, "message");
  return message ?? JSON.stringify(error);
}

function healthUrlForRpcEndpoint(endpoint: string): string {
  const url = new URL(endpoint);
  url.protocol = url.protocol === "wss:" ? "https:" : "http:";
  url.pathname = "/readyz";
  url.search = "";
  url.hash = "";
  return url.toString();
}

function parseModelListResponse(value: JsonValue): ModelListResponse {
  return {
    data: requiredArray(value, "data").map(parseModelListEntry),
    nextCursor: optionalString(value, "nextCursor") ?? null,
  };
}

function parseModelListEntry(value: JsonValue): ModelListEntry {
  const id = optionalString(value, "id") ?? optionalString(value, "model");
  if (!id) throw new Error("model/list entry missing required id or model");
  return {
    id,
    model: optionalString(value, "model"),
    providerId: optionalString(value, "providerId"),
    providerRef: optionalString(value, "providerRef"),
    modelRef: optionalString(value, "modelRef"),
    displayName: optionalString(value, "displayName"),
    name: optionalString(value, "name"),
    description: optionalString(value, "description"),
    contextWindowTokens: optionalNumber(value, "contextWindowTokens"),
    maxOutputTokens: optionalNumber(value, "maxOutputTokens"),
    metadata: optionalRecord(value, "metadata"),
    isDefault: optionalBoolean(value, "isDefault"),
  };
}

function parseModelProviderAuthListResponse(value: JsonValue): ModelProviderAuthListResponse {
  return {
    auth: optionalModelProviderAuth(value, "auth"),
    data: requiredArray(value, "data").map(parseModelProviderAuth),
    nextCursor: optionalString(value, "nextCursor") ?? null,
  };
}

function requiredModelProviderAuth(value: JsonValue): ModelProviderAuthStatus {
  const auth = optionalModelProviderAuth(value, "auth");
  if (!auth) throw new Error("model provider auth response missing auth");
  return auth;
}

function optionalModelProviderAuth(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return child && typeof child === "object" && !Array.isArray(child) ? parseModelProviderAuth(child) : null;
}

function parseModelProviderAuth(value: JsonValue): ModelProviderAuthStatus {
  return {
    providerId: requiredString(value, "providerId"),
    displayName: nullableString(value, "displayName") ?? null,
    configured: requiredBoolean(value, "configured"),
    source: nullableString(value, "source") ?? null,
    label: nullableString(value, "label") ?? null,
    authHeader: requiredBoolean(value, "authHeader"),
  };
}

function parseAgentListResponse(value: JsonValue): AgentListResponse {
  return {
    data: requiredArray(value, "data").map(parseAgentListEntry),
    cursor: optionalString(value, "cursor") ?? null,
  };
}

function parseAgentListEntry(value: JsonValue): AgentListEntry {
  const profile = getObject(value, "defaultModelProfile");
  return {
    name: requiredString(value, "name"),
    version: requiredString(value, "version"),
    refUri: requiredString(value, "refUri"),
    manifestHash: requiredString(value, "manifestHash"),
    title: optionalString(value, "title"),
    summary: optionalString(value, "summary"),
    defaultModelProfile: profile
      ? {
          id: requiredString(profile, "id"),
          providerRef: requiredString(profile, "providerRef"),
          modelRef: requiredString(profile, "modelRef"),
        }
      : null,
    toolIds: stringsFromArray(optionalArray(value, "toolIds") ?? []),
    aliases: (optionalArray(value, "aliases") ?? []).map((alias) => ({
      alias: requiredString(alias, "alias"),
      version: requiredString(alias, "version"),
    })),
    publishedAtMs: requiredNumber(value, "publishedAtMs"),
  };
}

function parseAgentRecord(value: JsonValue): PublishedAgentRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("agent/read response must be an object");
  }
  requiredString(value, "name");
  requiredString(value, "version");
  return value as PublishedAgentRecord;
}

export function buildAgentDraftParams(params: AgentDraftRequest): { [key: string]: JsonValue } {
  const payload: { [key: string]: JsonValue } = {};
  if (params.source !== undefined) payload.source = params.source;
  if (params.manifest !== undefined) payload.manifest = params.manifest as JsonValue;
  if (params.baseRef) payload.baseRef = params.baseRef;
  if (params.baseManifestHash) payload.baseManifestHash = params.baseManifestHash;
  if (params.expectedLatestVersion) payload.expectedLatestVersion = params.expectedLatestVersion;
  return payload;
}

export function parseAgentPlanResponse(value: JsonValue): AgentPlanResponse {
  return {
    plan: optionalRecord(value, "plan") ?? {},
    manifest: objectValue(value, "manifest") ?? null,
    source: requiredString(value, "source"),
    diagnostics: (optionalArray(value, "diagnostics") ?? []).map(parseAgentPlanDiagnostic),
    suggestedNextVersion: requiredString(value, "suggestedNextVersion"),
    base: optionalRecord(value, "base") ?? null,
  };
}

function parseAgentPlanDiagnostic(value: JsonValue): AgentPlanDiagnostic {
  return {
    code: requiredString(value, "code"),
    severity: optionalString(value, "severity"),
    message: requiredString(value, "message"),
    ref: optionalString(value, "ref"),
  };
}

export function parseAgentPublishResponse(value: JsonValue): AgentPublishResponse {
  return {
    record: parseAgentRecord(objectValue(value, "record") ?? null),
    manifest: objectValue(value, "manifest") ?? null,
    source: requiredString(value, "source"),
    latestAlias: optionalRecord(value, "latestAlias"),
  };
}

function parseThreadRebindForkResponse(value: JsonValue): ThreadRebindForkResponse {
  return {
    thread: optionalRecord(value, "thread") ?? {},
    fork: optionalRecord(value, "fork"),
  };
}

function parseOperationListResponse(value: JsonValue): OperationListResponse {
  return {
    data: requiredArray(value, "data").map(parseOperationListEntry),
    cursor: optionalString(value, "cursor") ?? null,
  };
}

function parseOperationListEntry(value: JsonValue): OperationListEntry {
  return {
    name: requiredString(value, "name"),
    activeArtifactHash: requiredString(value, "activeArtifactHash"),
    summary: nullableString(value, "summary"),
    manifest: objectValue(value, "manifest"),
    projections: objectValue(value, "projections"),
    interface: objectValue(value, "interface"),
    capabilityGrants: objectValue(value, "capabilityGrants"),
    metadata: objectValue(value, "metadata"),
    source: objectValue(value, "source"),
    build: objectValue(value, "build"),
  };
}

function parseThreadEventsListResponse(value: JsonValue): ThreadEventsListResponse {
  return {
    data: requiredArray(value, "data").map(parseThreadEvent),
    cursor: optionalString(value, "cursor") ?? null,
    streamCursor: nullableStreamCursor(value, "streamCursor") ?? null,
  };
}

function parseThreadEvent(value: JsonValue): ThreadEvent {
  const eventId = optionalString(value, "eventId") ?? requiredString(value, "event_id");
  const atMs = optionalNumber(value, "atMs") ?? requiredNumber(value, "created_at_ms");
  return {
    schema: optionalString(value, "schema"),
    event_id: optionalString(value, "event_id"),
    stream_id: optionalString(value, "stream_id"),
    sequence: optionalNumber(value, "sequence"),
    coordinates: optionalRecord(value, "coordinates"),
    payload_schema: optionalString(value, "payload_schema"),
    payloadSchema: optionalString(value, "payloadSchema"),
    created_at_ms: optionalNumber(value, "created_at_ms"),
    eventId,
    kind: requiredString(value, "kind"),
    origin: requiredString(value, "origin"),
    provenance: optionalRecord(value, "provenance") ?? null,
    atMs,
    payload: objectValue(value, "payload") ?? null,
  };
}

function parseThreadCouplingsListResponse(value: JsonValue): ThreadCouplingsListResponse {
  return {
    data: requiredArray(value, "data").map(parseThreadCouplingRow),
    nextCursor: optionalString(value, "nextCursor") ?? null,
    agentRef: nullableString(value, "agentRef") ?? null,
    manifestHash: nullableString(value, "manifestHash") ?? null,
    bindEventId: nullableString(value, "bindEventId") ?? null,
  };
}

function parseThreadCouplingRow(value: JsonValue): ThreadCouplingRow {
  return {
    ...recordFromJson(value),
    id: requiredString(value, "id"),
    role: requiredString(value, "role"),
    triggerKind: requiredString(value, "triggerKind"),
    triggerMatch: objectValue(value, "triggerMatch") ?? null,
    sourceStreams: stringsFromArray(optionalArray(value, "sourceStreams") ?? []),
    sourceKinds: stringsFromArray(optionalArray(value, "sourceKinds") ?? []),
    sinkStream: nullableString(value, "sinkStream") ?? null,
    sinkKinds: stringsFromArray(optionalArray(value, "sinkKinds") ?? []),
    functionRef: requiredString(value, "functionRef"),
    artifactHash: requiredString(value, "artifactHash"),
    operationName: nullableString(value, "operationName") ?? null,
    grants: stringsFromArray(optionalArray(value, "grants") ?? []),
    budget: optionalRecord(value, "budget") ?? null,
    configHash: nullableString(value, "configHash") ?? null,
  };
}

function parseThreadApprovalsListResponse(value: JsonValue): ThreadApprovalsListResponse {
  return {
    data: requiredArray(value, "data").map(parseThreadApprovalEntry),
    nextCursor: optionalString(value, "nextCursor") ?? null,
  };
}

function parseThreadApprovalEntry(value: JsonValue): ThreadApprovalEntry {
  return {
    ...recordFromJson(value),
    approvalId: requiredString(value, "approvalId"),
    status: requiredString(value, "status"),
    kind: requiredString(value, "kind"),
    eventId: requiredString(value, "eventId"),
    suspendedEventId: requiredString(value, "suspendedEventId"),
    requestEventId: nullableString(value, "requestEventId") ?? null,
    turnId: requiredString(value, "turnId"),
    callId: requiredString(value, "callId"),
    snapshotId: nullableString(value, "snapshotId") ?? null,
    reason: nullableString(value, "reason") ?? null,
  };
}

function parseApprovalResolveResponse(value: JsonValue): ApprovalResolveResponse {
  const decision = requiredString(value, "decision");
  if (decision !== "approved" && decision !== "denied") {
    throw new Error(`response has unsupported approval decision ${decision}`);
  }
  return {
    status: requiredString(value, "status"),
    approvalId: requiredString(value, "approvalId"),
    decision,
    approved: requiredBoolean(value, "approved"),
    reason: nullableString(value, "reason") ?? null,
    snapshotId: nullableString(value, "snapshotId") ?? null,
    eventId: requiredString(value, "eventId"),
    streamId: requiredString(value, "streamId"),
    sequence: requiredNumber(value, "sequence"),
    createdAtMs: requiredNumber(value, "createdAtMs"),
  };
}

function parseThreadWaitingListResponse(value: JsonValue): ThreadWaitingListResponse {
  return {
    data: requiredArray(value, "data").map(parseThreadWaitingEntry),
    nextCursor: optionalString(value, "nextCursor") ?? null,
  };
}

function parseThreadWaitingEntry(value: JsonValue): ThreadWaitingEntry {
  return {
    ...recordFromJson(value),
    kind: requiredString(value, "kind"),
    eventId: requiredString(value, "eventId"),
    suspendedEventId: nullableString(value, "suspendedEventId") ?? undefined,
    requestEventId: nullableString(value, "requestEventId") ?? undefined,
    streamId: nullableString(value, "streamId") ?? undefined,
    sequence: optionalNumber(value, "sequence"),
    createdAtMs: optionalNumber(value, "createdAtMs"),
    turnId: nullableString(value, "turnId") ?? null,
    callId: nullableString(value, "callId") ?? null,
    snapshotId: nullableString(value, "snapshotId") ?? null,
    approvalId: nullableString(value, "approvalId") ?? null,
    waitingOnEventId: nullableString(value, "waitingOnEventId") ?? undefined,
    continuation: nullableString(value, "continuation") ?? null,
    reason: nullableString(value, "reason") ?? null,
    payload: objectValue(value, "payload") ?? undefined,
    sourceEventIds: stringsFromArray(optionalArray(value, "sourceEventIds") ?? []),
  };
}

function parseThreadDebugExportResponse(value: JsonValue): ThreadDebugExportResponse {
  return {
    schema: requiredString(value, "schema"),
    threadId: requiredString(value, "threadId"),
    generatedAtMs: optionalNumber(value, "generatedAtMs"),
    backend: optionalRecord(value, "backend") ?? {},
    ackClasses: stringsFromArray(optionalArray(value, "ackClasses") ?? []),
    redaction: optionalRecord(value, "redaction") ?? {},
    thread: objectValue(value, "thread") ?? null,
    streams: requiredArray(value, "streams").map(parseThreadDebugExportStream),
    receipts: requiredArray(value, "receipts").map(parseThreadDebugExportReceipt),
  };
}

function parseThreadDebugExportStream(value: JsonValue): ThreadDebugExportStream {
  return {
    ...recordFromJson(value),
    selector: requiredString(value, "selector"),
    streamId: requiredString(value, "streamId"),
    backend: optionalRecord(value, "backend") ?? {},
    ackClasses: stringsFromArray(optionalArray(value, "ackClasses") ?? []),
    range: optionalRecord(value, "range") ?? {},
    data: requiredArray(value, "data").map(parseThreadEvent),
    eventCount: requiredNumber(value, "eventCount"),
    truncated: requiredBoolean(value, "truncated"),
    cursor: optionalString(value, "cursor") ?? null,
    streamCursor: nullableStreamCursor(value, "streamCursor") ?? null,
  };
}

function parseThreadDebugExportReceipt(value: JsonValue): ThreadDebugExportReceipt {
  return {
    ...recordFromJson(value),
    eventId: requiredString(value, "eventId"),
    streamId: requiredString(value, "streamId"),
    sequence: requiredNumber(value, "sequence"),
    kind: requiredString(value, "kind"),
    origin: requiredString(value, "origin"),
    payloadSchema: requiredString(value, "payloadSchema"),
    createdAtMs: requiredNumber(value, "createdAtMs"),
  };
}

function parseReadDirectoryResponse(value: JsonValue): ReadDirectoryResponse {
  return {
    entries: requiredArray(value, "entries").map(parseDirectoryEntry),
  };
}

function parseDirectoryEntry(value: JsonValue): DirectoryEntry {
  return {
    fileName: requiredString(value, "fileName"),
    isDirectory: requiredBoolean(value, "isDirectory"),
    isFile: requiredBoolean(value, "isFile"),
  };
}

function optionalArray(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return Array.isArray(child) ? child : undefined;
}

function stringsFromArray(value: JsonValue[]) {
  return value.map((item) => {
    if (typeof item !== "string" || !item.trim()) throw new Error("expected non-empty string array item");
    return item;
  });
}

function nullableStreamCursor(value: JsonValue | undefined, key: string): StreamCursor | null | undefined {
  const child = objectValue(value, key);
  if (child === undefined || child === null) return child;
  return {
    schema: requiredString(child, "schema"),
    stream_id: requiredString(child, "stream_id"),
    sequence: requiredNumber(child, "sequence"),
    event_id: requiredString(child, "event_id"),
  };
}

function nullableString(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "string" ? child : child === null ? null : undefined;
}

function optionalString(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "string" && child.trim() ? child : undefined;
}

function optionalNumber(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "number" ? child : undefined;
}

function optionalBoolean(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return typeof child === "boolean" ? child : undefined;
}

function optionalRecord(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  return child && typeof child === "object" && !Array.isArray(child)
    ? (child as Record<string, unknown>)
    : undefined;
}

function requiredArray(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  if (!Array.isArray(child)) throw new Error(`response missing required array ${key}`);
  return child;
}

function requiredString(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  if (typeof child !== "string" || !child.trim()) throw new Error(`response missing required string ${key}`);
  return child;
}

function requiredNumber(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  if (typeof child !== "number") throw new Error(`response missing required number ${key}`);
  return child;
}

function requiredBoolean(value: JsonValue | undefined, key: string) {
  const child = objectValue(value, key);
  if (typeof child !== "boolean") throw new Error(`response missing required boolean ${key}`);
  return child;
}

function recordFromJson(value: JsonValue) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function jsonObject(value: object): { [key: string]: JsonValue } {
  return JSON.parse(JSON.stringify(value)) as { [key: string]: JsonValue };
}

function decodeBase64Text(dataBase64: string) {
  const binary = globalThis.atob(dataBase64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new TextDecoder().decode(bytes);
}
