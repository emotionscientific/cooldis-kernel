import {
  CooldisRpcClient,
  getArray,
  getObject,
  getString,
  parseToolCallItem,
  textInput,
  type JsonValue,
  type RpcEvent,
  type RpcNotification,
} from "./cooldisRpc";
import { agentRecordRef } from "./agentManifestDraft";
import { mergeTranscriptMessages } from "./chatTranscript";
import { mapThreadStatus, normalizeProvider, reboundThreadFromResponse } from "./threadProjection";
import {
  MODES,
  isThinkingEffort,
  isThinkingLevel,
  type AgentDraftRequest,
  type AgentListEntry,
  type AgentPlanResponse,
  type AgentPublishResponse,
  type ApprovalResolveResponse,
  type ChatToolCall,
  type ChatMessage,
  type ManifestDef,
  type ModeId,
  type ModelListEntry,
  type ModelInfo,
  type ModelProviderAuthStatus as RpcProviderAuthStatus,
  type OperationListEntry,
  type PublishedAgentRecord,
  type ResourceNode,
  type Tab,
  type ThinkingLevel,
  type ThreadEnvelope,
  type ThreadEnvelopeBinding,
  type ThreadEvent,
  type ThreadRebindForkResponse,
  type Thread,
  type ToolDef,
  type ThreadApprovalsListResponse,
  type ThreadCouplingsListResponse,
  type ThreadDebugExportResponse,
  type ThreadWaitingListResponse,
} from "./schema";
import type {
  ConnectionProfile,
  ConnectionProfileKind,
  DaemonStatus,
  DesktopRequestApi,
  ProviderAuthStatus,
  QuitPolicy,
  RuntimeStatus,
  StartPolicy,
} from "./desktopRpc";

let tabSeq = 1;
const uid = (p: string) => `${p}_${(tabSeq++).toString(36)}_${Date.now().toString(36).slice(-4)}`;
type LoadSlice =
  | "config"
  | "models"
  | "threads"
  | "agents"
  | "operations"
  | "resources"
  | "threadEvents"
  | "threadEnvelope"
  | "threadCouplings"
  | "threadApprovals"
  | "threadWaiting"
  | "threadDebugExport"
  | "files";

const RECONNECT_DELAY_MS = 3000;
const RECONNECT_MAX_ATTEMPTS = 5;
const DEFAULT_ACCENT = "#6e7bf2";

type ConsoleConfig = {
  rpcUrl?: string;
  sessionToken?: string;
};

declare global {
  interface Window {
    __COOLDIS_CONSOLE_CONFIG__?: ConsoleConfig;
  }
}

function injectedConsoleConfig(): ConsoleConfig | undefined {
  return typeof window === "undefined" ? undefined : window.__COOLDIS_CONSOLE_CONFIG__;
}

function defaultRpcEndpoint() {
  const config = injectedConsoleConfig();
  if (config?.rpcUrl) return config.rpcUrl;
  if (typeof window !== "undefined" && window.location.protocol.startsWith("http")) {
    const url = new URL("/rpc", window.location.href);
    url.protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return url.toString();
  }
  return "ws://127.0.0.1:49200/rpc";
}

const DEFAULT_ENDPOINT = defaultRpcEndpoint();
const HAS_BUNDLED_CONSOLE_SESSION = Boolean(injectedConsoleConfig()?.sessionToken || injectedConsoleConfig()?.rpcUrl);

// UI preferences persisted across launches (daemon-side config lives in the
// desktop config file, never here).
const PREFS_KEY = "cooldis.console.prefs.v1";
type Prefs = {
  endpoint?: string;
  accent?: string;
  defaultThinking?: ThinkingLevel;
  defaultAgentRef?: string;
  connectionProfile?: ConnectionProfile;
};
export type SettingsSection = "connection" | "appearance" | "chat" | "shortcuts" | "about";

const CONNECTION_KINDS = new Set<ConnectionProfileKind>(["local-managed", "local-external", "remote"]);

function isStartPolicy(value: unknown): value is StartPolicy {
  return value === "ask" || value === "auto" || value === "leave-offline";
}

function isQuitPolicy(value: unknown): value is QuitPolicy {
  return value === "ask" || value === "stop-managed" || value === "leave-running";
}

function providerAuthStatusFromRpc(auth: RpcProviderAuthStatus | null): ProviderAuthStatus | null {
  if (!auth) return null;
  return {
    providerId: auth.providerId,
    displayName: auth.displayName,
    configured: auth.configured,
    source: auth.source,
    label: auth.label,
    stateHome: null,
    lastError: null,
  };
}

function parseConnectionProfile(value: unknown): ConnectionProfile | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const profile = value as Record<string, unknown>;
  const kind = profile.kind;
  const endpoint = typeof profile.endpoint === "string" ? profile.endpoint.trim() : "";
  if (!CONNECTION_KINDS.has(kind as ConnectionProfileKind) || !endpoint) return undefined;
  return {
    kind: kind as ConnectionProfileKind,
    endpoint,
    runtimePath: typeof profile.runtimePath === "string" ? profile.runtimePath : undefined,
    startPolicy: isStartPolicy(profile.startPolicy) ? profile.startPolicy : "ask",
    quitPolicy: isQuitPolicy(profile.quitPolicy) ? profile.quitPolicy : "ask",
  };
}

function loadPrefs(): Prefs {
  if (typeof localStorage === "undefined") return {};
  try {
    const parsed = JSON.parse(localStorage.getItem(PREFS_KEY) ?? "{}") as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const prefs = parsed as Record<string, unknown>;
    return {
      endpoint: typeof prefs.endpoint === "string" && prefs.endpoint.trim() ? prefs.endpoint : undefined,
      accent: typeof prefs.accent === "string" && /^#[0-9a-f]{6}$/i.test(prefs.accent) ? prefs.accent : undefined,
      defaultThinking: isThinkingLevel(prefs.defaultThinking) ? prefs.defaultThinking : undefined,
      defaultAgentRef: typeof prefs.defaultAgentRef === "string" ? prefs.defaultAgentRef.trim() || undefined : undefined,
      connectionProfile: parseConnectionProfile(prefs.connectionProfile),
    };
  } catch {
    return {};
  }
}

const initialPrefs = loadPrefs();

// Electrobun injects this global at document start, so native mode is knowable
// synchronously — before first paint, unlike the async host-bridge probe.
const isElectrobun = typeof window !== "undefined" && "__electrobunWebviewId" in window;

export class AppState {
  // connection
  connectionProfile = $state<ConnectionProfile | null>(HAS_BUNDLED_CONSOLE_SESSION ? null : (initialPrefs.connectionProfile ?? null));
  endpoint = $state(
    HAS_BUNDLED_CONSOLE_SESSION
      ? DEFAULT_ENDPOINT
      : (initialPrefs.connectionProfile?.endpoint ?? initialPrefs.endpoint ?? DEFAULT_ENDPOINT),
  );
  startPolicy = $state<StartPolicy>(initialPrefs.connectionProfile?.startPolicy ?? "ask");
  quitPolicy = $state<QuitPolicy>(initialPrefs.connectionProfile?.quitPolicy ?? "ask");
  client: CooldisRpcClient | null = $state(null);
  connected = $state(false);
  status = $state<"offline" | "connecting" | "ready">("offline");
  error = $state("");
  healthRttMs = $state<number | null>(null);

  // desktop (Electrobun) bridge
  native = $state(isElectrobun);
  hostInfo = $state<import("./desktopRpc").HostInfo | null>(null);
  desktop: unknown = $state(null);
  desktopRequest: DesktopRequestApi | null = $state(null);
  daemon = $state<DaemonStatus | null>(null);
  runtime = $state<RuntimeStatus | null>(null);
  providerAuth = $state<ProviderAuthStatus | null>(null);
  providerAuthBusy = $state(false);
  runtimeInstalling = $state(false);
  connectionSetupOpen = $state(false);
  startPromptOpen = $state(false);
  quitPromptOpen = $state(false);

  // navigation
  mode = $state<ModeId>("chat");
  tabs = $state<Tab[]>([]);
  activeTabId = $state<string | null>(null);
  selectedEntity = $state<{ kind: string; id: string } | null>(null);

  // layout
  sidebarOpen = $state(true);
  inspectorOpen = $state(true);
  paletteOpen = $state(false);
  settingsOpen = $state(false);
  settingsSection = $state<SettingsSection>("connection");
  runtimeOpen = $state(false);
  accent = $state(initialPrefs.accent ?? DEFAULT_ACCENT);
  defaultThinking = $state<ThinkingLevel>(initialPrefs.defaultThinking ?? "default");
  // Agent ref new threads start from; "" = ref-less start (the daemon binds its
  // default manifest — the console never hardcodes the default's name).
  defaultAgentRef = $state<string>(initialPrefs.defaultAgentRef ?? "");

  // data
  threads = $state<Thread[]>([]);
  tools = $state<ToolDef[]>([]);
  manifests = $state<ManifestDef[]>([]);
  agentsLoaded = $state(false);
  models = $state<ModelInfo[]>([]);
  resources = $state<ResourceNode[]>([]);
  resourceRoot = $state<string | null>(null);
  browsePath = $state<string | null>(null);
  threadEvents = $state<ThreadEvent[]>([]);
  threadEventsCursor = $state<string | null>(null);
  threadEventsThreadId = $state<string | null>(null);
  threadCouplings = $state<Record<string, ThreadCouplingsListResponse>>({});
  threadApprovals = $state<Record<string, ThreadApprovalsListResponse>>({});
  threadWaiting = $state<Record<string, ThreadWaitingListResponse>>({});
  threadDebugExports = $state<Record<string, ThreadDebugExportResponse>>({});
  startingThreadRefs = $state<Record<string, true>>({});
  // threadId -> bind-receipt envelope; null = fetched, thread has no bind
  // receipt (pre-manifest-lineage thread). Absent key = not fetched yet.
  threadEnvelopes = $state<Record<string, ThreadEnvelope | null>>({});
  threadEnvelopeErrorThreadId = $state<string | null>(null);
  agentRecords = $state<Record<string, PublishedAgentRecord>>({});
  fileContents = $state<Record<string, string>>({});
  fileDataBase64 = $state<Record<string, string>>({});
  events = $state<RpcEvent[]>([]);
  loadErrors = $state<Partial<Record<LoadSlice, string>>>({});

  runtimeModel = $state("");
  runtimeProvider = $state("");
  runtimeCwd = $state<string | null>(null);

  // threadId -> manifest binding for threads this session started with agentRef;
  // thread/list does not echo agent metadata back, so the label is client-side
  private threadAgents = new Map<string, { ref: string; name: string }>();
  private pendingTurnDeltas = new Map<string, { kind: "message" | "thinking"; delta: string }[]>();
  private envelopeRequests = new Map<string, Promise<void>>();

  // Endpoint the live (or in-flight) client actually targets; app.endpoint is
  // the editable field and can drift ahead of it until applyEndpoint commits.
  private activeEndpoint: string | null = null;
  private unNotif?: () => void;
  private unEvt?: () => void;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempts = 0;
  private manualDisconnect = false;
  private connectGeneration = 0;
  private connectingClient: CooldisRpcClient | null = null;

  // ---------- derived ----------
  get activeTab(): Tab | undefined {
    return this.tabs.find((t) => t.id === this.activeTabId);
  }
  get eventRate() {
    const cutoff = Date.now() - 60_000;
    return this.events.filter((e) => e.at >= cutoff).length;
  }
  get offlineModelOnly() {
    return this.models.length === 1 && this.models[0]?.provider === "local_offline" && this.models[0]?.id === "echo";
  }
  get modelInventoryLabel() {
    if (!this.connected) return "";
    const count = this.models.length;
    if (count <= 0) return "—";
    return `${count} ${count === 1 ? "model" : "models"}`;
  }
  get healthRttLabel() {
    if (this.healthRttMs === null) return "";
    return this.healthRttMs < 1 ? "<1ms" : `${Math.round(this.healthRttMs)}ms`;
  }
  get profileLabel() {
    switch (this.connectionProfile?.kind) {
      case "local-managed":
        return "Local managed";
      case "local-external":
        return "Local external";
      case "remote":
        return "Remote";
      default:
        return "Not configured";
    }
  }
  get canStopManagedDaemon() {
    return Boolean(this.daemon?.managed && this.daemon.running);
  }
  get canStartManagedDaemon() {
    return Boolean(this.native && this.connectionProfile?.kind === "local-managed" && !this.daemon?.running);
  }
  get canRestartManagedDaemon() {
    return Boolean(this.native && this.connectionProfile?.kind === "local-managed" && this.daemon?.running);
  }

  // ---------- connection ----------
  private profileWithPolicies(profile = this.connectionProfile): ConnectionProfile | null {
    if (!profile) return null;
    return {
      ...profile,
      endpoint: profile.endpoint || this.endpoint || DEFAULT_ENDPOINT,
      startPolicy: this.startPolicy,
      quitPolicy: this.quitPolicy,
    };
  }

  private async syncLifecyclePrefs(profile = this.profileWithPolicies()) {
    await this.desktopRequest?.setLifecyclePrefs({ profile }).catch(() => ({ ok: false }));
  }

  attachDesktop = (view: unknown, request: DesktopRequestApi) => {
    this.desktop = view;
    this.desktopRequest = request;
  };

  bootstrapDesktop = async () => {
    if (!this.desktopRequest) return;
    this.runtime = await this.desktopRequest.detectRuntime({});
    await this.refreshProviderAuth();
    await this.syncLifecyclePrefs();
    const profile = this.profileWithPolicies();
    if (!profile) {
      this.connectionSetupOpen = true;
      return;
    }

    if (profile.kind === "local-managed") {
      this.daemon = await this.desktopRequest.daemonStatus({ profile });
      this.endpoint = profile.endpoint;
      if (this.daemon.running) {
        await this.connect({ automatic: true });
      } else if (profile.startPolicy === "auto") {
        await this.startManagedDaemon({ rememberAuto: true });
      } else if (profile.startPolicy === "ask") {
        this.startPromptOpen = true;
      }
      return;
    }

    this.endpoint = profile.endpoint;
    await this.connect({ automatic: true });
  };

  connectFromProfile = async () => {
    if (this.native) {
      const profile = this.profileWithPolicies();
      if (!profile) {
        this.connectionSetupOpen = true;
        return;
      }
      this.endpoint = profile.endpoint;
      if (profile.kind === "local-managed") {
        if (!this.desktopRequest) return;
        this.daemon = await this.desktopRequest.daemonStatus({ profile });
        if (!this.daemon.running) {
          if (this.startPolicy === "auto") await this.startManagedDaemon({ rememberAuto: true });
          else if (this.startPolicy === "ask") this.startPromptOpen = true;
          else this.status = "offline";
          return;
        }
      }
    }
    await this.connect();
  };

  connect = async (options: { automatic?: boolean; retry?: boolean } = {}) => {
    if (this.connected || this.status === "connecting") return;
    if (options.automatic && this.manualDisconnect) return;
    if (!options.retry) this.clearReconnect();
    this.manualDisconnect = false;
    this.error = "";
    this.status = "connecting";
    const generation = ++this.connectGeneration;
    this.activeEndpoint = this.endpoint;
    const sessionToken = this.endpoint === DEFAULT_ENDPOINT ? injectedConsoleConfig()?.sessionToken : undefined;
    const client = new CooldisRpcClient(this.endpoint, sessionToken);
    this.connectingClient = client;
    const unNotif = client.onNotification(this.handleNotification);
    const unEvt = client.onEvent((e) => {
      this.events = [e, ...this.events].slice(0, 400);
      if (e.kind === "error") this.error = e.message;
      if (e.kind === "status" && e.message === "Socket closed" && this.client === client && this.connected) {
        this.handleSocketClosed();
      }
    });
    try {
      await client.connect();
      if (generation !== this.connectGeneration || this.manualDisconnect) {
        unNotif();
        unEvt();
        if (this.connectingClient === client) this.connectingClient = null;
        client.disconnect();
        return;
      }
      this.cleanup();
      this.unNotif = unNotif;
      this.unEvt = unEvt;
      this.connectingClient = null;
      this.client = client;
      this.connected = true;
      this.status = "ready";
      this.reconnectAttempts = 0;
      this.events = [];
      this.threads = [];
      this.threadEnvelopes = {};
      this.threadEnvelopeErrorThreadId = null;
      this.envelopeRequests.clear();
      this.agentsLoaded = false;
      await this.refresh();
    } catch (err) {
      unNotif();
      unEvt();
      if (this.connectingClient === client) this.connectingClient = null;
      if (generation !== this.connectGeneration || this.manualDisconnect) {
        client.disconnect();
        return;
      }
      this.error = msg(err);
      this.status = "offline";
      this.connected = false;
      client.disconnect();
      this.scheduleReconnect();
    }
  };

  disconnect = (options: { manual?: boolean } = {}) => {
    this.manualDisconnect = options.manual ?? true;
    this.connectGeneration += 1;
    this.clearReconnect();
    this.cleanup();
    this.connectingClient?.disconnect();
    this.connectingClient = null;
    this.client?.disconnect();
    this.client = null;
    this.connected = false;
    this.status = "offline";
    this.tools = [];
    this.manifests = [];
    this.agentsLoaded = false;
    this.models = [];
    this.resources = [];
    this.resourceRoot = null;
    this.browsePath = null;
    this.agentRecords = {};
    this.threadEvents = [];
    this.threadEventsCursor = null;
    this.threadEventsThreadId = null;
    this.threadEnvelopes = {};
    this.threadEnvelopeErrorThreadId = null;
    this.envelopeRequests.clear();
    this.loadErrors = {};
    this.runtimeModel = "";
    this.runtimeProvider = "";
    this.runtimeCwd = null;
    this.threadAgents.clear();
  };

  toggleConnection = () => (this.connected ? this.disconnect() : void this.connectFromProfile());

  /**
   * Commit an edited endpoint: if a connection is live (or in flight) against
   * a different URL, reconnect to the new one. Without this, the topbar would
   * show the edited endpoint with "connected" state while the socket still
   * points at the old daemon — a lying affordance (dogfood 2026-06-12).
   */
  applyEndpoint = () => {
    const next = this.endpoint.trim();
    this.endpoint = next;
    if (this.connectionProfile) {
      this.connectionProfile = this.profileWithPolicies({ ...this.connectionProfile, endpoint: next });
      void this.syncLifecyclePrefs();
    }
    const active = this.connected || this.status === "connecting";
    if (!active || this.activeEndpoint === next) return;
    this.disconnect({ manual: false });
    void this.connectFromProfile();
  };

  configureProfile = async (kind: ConnectionProfileKind, endpoint: string, options: { connect?: boolean } = {}) => {
    const normalized = endpoint.trim();
    if (!isWebSocketEndpoint(normalized)) {
      this.error = "Use a ws:// or wss:// endpoint.";
      return;
    }
    const runtimePath = kind === "local-managed" ? (this.runtime?.path ?? undefined) : undefined;
    this.connectionProfile = {
      kind,
      endpoint: normalized,
      runtimePath,
      startPolicy: this.startPolicy,
      quitPolicy: this.quitPolicy,
    };
    this.endpoint = normalized;
    this.connectionSetupOpen = false;
    this.startPromptOpen = false;
    await this.syncLifecyclePrefs();
    if (options.connect) await this.connectFromProfile();
  };

  installRuntime = async () => {
    if (!this.desktopRequest || this.runtimeInstalling) return;
    this.runtimeInstalling = true;
    this.error = "";
    try {
      this.runtime = await this.desktopRequest.installRuntime({});
      if (!this.runtime.installed && this.runtime.lastError) this.error = this.runtime.lastError;
    } catch (err) {
      this.error = msg(err);
    } finally {
      this.runtimeInstalling = false;
    }
  };

  refreshRuntime = async () => {
    if (!this.desktopRequest) return;
    this.runtime = await this.desktopRequest.detectRuntime({}).catch(() => this.runtime);
    await this.refreshProviderAuth();
  };

  private currentProviderAuthId() {
    if (this.providerAuth?.providerId) return this.providerAuth.providerId;
    if (this.runtimeProvider && this.runtimeProvider !== "local_offline") return this.runtimeProvider;
    return this.models.find((model) => model.provider && model.provider !== "local_offline")?.provider ?? this.runtimeProvider;
  }

  refreshProviderAuth = async () => {
    if (this.desktopRequest) {
      this.providerAuth = await this.desktopRequest.providerAuthStatus({}).catch((err) => ({
        providerId: this.currentProviderAuthId() ?? "provider",
        displayName: null,
        configured: false,
        source: null,
        label: null,
        stateHome: null,
        lastError: msg(err),
      }));
      return this.providerAuth;
    }
    if (!this.client?.connected) return this.providerAuth;
    const providerId = this.currentProviderAuthId();
    const status = await this.client.modelProviderAuthStatus(providerId).catch((err) => ({
      auth: {
        providerId: providerId ?? "provider",
        displayName: null,
        configured: false,
        source: null,
        label: null,
        authHeader: true,
      },
      data: [],
      nextCursor: null,
      lastError: msg(err),
    }));
    this.providerAuth = providerAuthStatusFromRpc(status.auth);
    if (this.providerAuth && "lastError" in status) {
      this.providerAuth.lastError = status.lastError ?? null;
    }
    return this.providerAuth;
  };

  setProviderAuth = async (apiKey: string) => {
    if (this.providerAuthBusy) return;
    const providerId = this.currentProviderAuthId();
    if (!providerId) {
      this.error = "No provider is available for credential storage.";
      return;
    }
    this.providerAuthBusy = true;
    this.error = "";
    try {
      if (this.desktopRequest) {
        this.providerAuth = await this.desktopRequest.providerAuthSet({ providerId, apiKey });
      } else if (this.client?.connected) {
        this.providerAuth = providerAuthStatusFromRpc(await this.client.setModelProviderAuth(providerId, apiKey));
      }
    } catch (err) {
      this.error = msg(err);
    } finally {
      this.providerAuthBusy = false;
    }
  };

  deleteProviderAuth = async () => {
    if (this.providerAuthBusy) return;
    const providerId = this.currentProviderAuthId();
    if (!providerId) {
      this.error = "No provider is available for credential storage.";
      return;
    }
    this.providerAuthBusy = true;
    this.error = "";
    try {
      if (this.desktopRequest) {
        this.providerAuth = await this.desktopRequest.providerAuthDelete({ providerId });
      } else if (this.client?.connected) {
        this.providerAuth = providerAuthStatusFromRpc(await this.client.deleteModelProviderAuth(providerId));
      }
    } catch (err) {
      this.error = msg(err);
    } finally {
      this.providerAuthBusy = false;
    }
  };

  startManagedDaemon = async (options: { rememberAuto?: boolean } = {}) => {
    if (!this.desktopRequest) return;
    if (options.rememberAuto) this.startPolicy = "auto";
    const profile = this.profileWithPolicies();
    if (!profile) {
      this.connectionSetupOpen = true;
      return;
    }
    if (profile.kind !== "local-managed") {
      await this.connectFromProfile();
      return;
    }
    await this.syncLifecyclePrefs(profile);
    await this.refreshProviderAuth();
    if (this.providerAuth && !this.providerAuth.configured) {
      this.status = "offline";
      this.connectionSetupOpen = true;
      this.error =
        this.providerAuth.lastError ??
        `Add a ${this.providerAuth.providerId} provider credential before starting the managed daemon.`;
      return;
    }
    this.error = "";
    this.status = "connecting";
    this.daemon = await this.desktopRequest.ensureDaemon({ profile }).catch((err) => {
      this.error = msg(err);
      this.status = "offline";
      return null;
    });
    if (this.daemon?.running) {
      this.endpoint = this.daemon.endpoint;
      this.startPromptOpen = false;
      this.status = "offline";
      await this.connect({ automatic: true });
    } else {
      this.status = "offline";
      if (this.daemon?.lastError) this.error = this.daemon.lastError;
    }
  };

  stopManagedDaemon = async () => {
    if (!this.desktopRequest) return;
    this.disconnect({ manual: false });
    this.daemon = await this.desktopRequest.stopDaemon({ profile: this.profileWithPolicies() }).catch((err) => {
      this.error = msg(err);
      return this.daemon;
    });
  };

  restartManagedDaemon = async () => {
    if (!this.desktopRequest) return;
    const profile = this.profileWithPolicies();
    if (!profile) {
      this.connectionSetupOpen = true;
      return;
    }
    if (profile.kind !== "local-managed") {
      await this.connectFromProfile();
      return;
    }
    this.disconnect({ manual: false });
    await this.syncLifecyclePrefs(profile);
    await this.refreshProviderAuth();
    if (this.providerAuth && !this.providerAuth.configured) {
      this.status = "offline";
      this.connectionSetupOpen = true;
      this.error =
        this.providerAuth.lastError ??
        `Add a ${this.providerAuth.providerId} provider credential before restarting the managed daemon.`;
      return;
    }
    this.error = "";
    this.status = "connecting";
    this.daemon = await this.desktopRequest.restartDaemon({ profile }).catch((err) => {
      this.error = msg(err);
      this.status = "offline";
      return null;
    });
    if (this.daemon?.running) {
      this.endpoint = this.daemon.endpoint;
      this.startPromptOpen = false;
      this.status = "offline";
      await this.connect({ automatic: true });
    } else {
      this.status = "offline";
      if (this.daemon?.lastError) this.error = this.daemon.lastError;
    }
  };

  requestQuit = () => {
    if (this.native && this.canStopManagedDaemon && this.quitPolicy === "ask") {
      this.quitPromptOpen = true;
      return;
    }
    const policy = this.canStopManagedDaemon ? this.quitPolicy : "leave-running";
    void this.finishQuit(policy, false);
  };

  finishQuit = async (policy: QuitPolicy, remember: boolean) => {
    if (remember) this.quitPolicy = policy;
    if (remember && this.connectionProfile) {
      this.connectionProfile = this.profileWithPolicies({ ...this.connectionProfile, quitPolicy: policy });
    }
    await this.syncLifecyclePrefs();
    await this.desktopRequest?.requestAppQuit({ quitPolicy: policy, remember }).catch(() => undefined);
  };

  toggleWindowZoom = async () => {
    await this.desktopRequest?.toggleWindowZoom({}).catch((err) => {
      console.warn("toggleWindowZoom failed:", err);
    });
  };

  private scheduleReconnect() {
    if (this.manualDisconnect || this.reconnectTimer || this.reconnectAttempts >= RECONNECT_MAX_ATTEMPTS) return;
    this.reconnectAttempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect({ automatic: true, retry: true });
    }, RECONNECT_DELAY_MS);
  }

  private clearReconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
  }

  private handleSocketClosed() {
    this.cleanup();
    this.client = null;
    this.connected = false;
    this.status = "offline";
    if (!this.manualDisconnect) this.scheduleReconnect();
  }

  refresh = async () => {
    if (!this.client) return;
    const client = this.client;
    try {
      this.healthRttMs = await client.measureHealthRoundTrip();
    } catch {
      if (this.client === client) this.healthRttMs = null;
    }
    const [modelsResult, threadsResult, configResult, agentsResult, operationsResult] = await Promise.allSettled([
      client.listModels(),
      client.request("thread/list"),
      client.request("config/read"),
      client.listAgents(),
      client.listOperations(),
    ]);

    if (configResult.status === "fulfilled") {
      this.clearLoadError("config");
      const config = getObject(configResult.value, "config");
      this.runtimeProvider = getString(config, "model_provider") ?? this.runtimeProvider;
      this.runtimeModel = getString(config, "model") ?? this.runtimeModel;
      const configCwd = getString(config, "cwd");
      if (configCwd && isAbsolutePath(configCwd)) {
        const root = normalizePath(configCwd);
        this.runtimeCwd = root;
        if (this.resourceRoot !== root) {
          this.resourceRoot = root;
          this.browsePath = root;
        } else if (!this.browsePath || !pathWithinRoot(this.browsePath, root)) {
          this.browsePath = root;
        }
      }
    } else {
      this.recordLoadError("config", configResult.reason);
    }

    if (modelsResult.status === "fulfilled") {
      this.clearLoadError("models");
      this.models = modelsResult.value.data.map((m) => modelInfoFromRpc(m, this.runtimeProvider));
      const def = modelsResult.value.data.find((m) => m.isDefault) ?? modelsResult.value.data[0];
      if (def && !this.runtimeModel) {
        this.runtimeModel = modelId(def);
        this.runtimeProvider = modelProvider(def, this.runtimeProvider);
      }
    } else {
      this.models = [];
      this.recordLoadError("models", modelsResult.reason);
    }

    await this.refreshProviderAuth();

    if (threadsResult.status === "fulfilled") {
      this.clearLoadError("threads");
      const rawThreads = getArray(threadsResult.value, "data");
      const threads = rawThreads.map((t, i) => {
        const preview = nonEmptyString(t, "preview") ?? "";
        const model = nonEmptyString(t, "model") ?? this.runtimeModel;
        const id = nonEmptyString(t, "id") ?? `thread_${i}`;
        const agent = this.threadAgents.get(id);
        return {
          id,
          agentRef: agent?.ref,
          agentName: agent?.name,
          title: nonEmptyString(t, "name") ?? (preview || "Untitled thread"),
          preview,
          model,
          provider: normalizeProvider(nonEmptyString(t, "modelProvider"), model, this.runtimeProvider),
          status: mapThreadStatus(getString(t, "status")),
          thinking: thinkingLabel((t as Record<string, unknown>)?.thinking),
          turns: Array.isArray((t as Record<string, unknown>)?.turns)
            ? ((t as Record<string, unknown[]>)?.turns?.length ?? 0)
            : typeof (t as Record<string, unknown>)?.turns === "number"
              ? (t as Record<string, number>).turns
              : 0,
          updatedAt: numberField(t, "updatedAt") ?? Date.now(),
        };
      });
      this.threads = threads;
      this.pruneThreadAgents(new Set(threads.map((thread) => thread.id)));
    } else {
      this.threads = [];
      this.recordLoadError("threads", threadsResult.reason);
    }

    if (agentsResult.status === "fulfilled") {
      this.clearLoadError("agents");
      this.manifests = agentsResult.value.data.map(manifestFromAgent);
      this.agentsLoaded = true;
    } else {
      this.manifests = [];
      this.agentsLoaded = false;
      this.recordLoadError("agents", agentsResult.reason);
    }

    if (operationsResult.status === "fulfilled") {
      this.clearLoadError("operations");
      this.tools = operationsResult.value.data.map(toolFromOperation);
    } else {
      this.tools = [];
      this.recordLoadError("operations", operationsResult.reason);
    }

    await this.browse(this.browsePath ?? this.resourceRoot);
  };

  browse = async (path: string | null) => {
    const root = this.resourceRoot;
    let target = path ? normalizePath(path) : null;
    if (target && root && !pathWithinRoot(target, root)) target = root;
    if (!this.client || !target || !isAbsolutePath(target)) {
      this.resources = [];
      this.browsePath = null;
      this.clearLoadError("resources");
      return;
    }
    try {
      const directory = await this.client.readDirectory(target);
      this.clearLoadError("resources");
      this.browsePath = target;
      this.resources = directory.entries.map((entry) => ({
        path: childPath(target, entry.fileName),
        name: entry.fileName,
        kind: entry.isDirectory ? ("dir" as const) : ("file" as const),
      }));
    } catch (err) {
      this.resources = [];
      this.recordLoadError("resources", err);
    }
  };

  browseUp = () => {
    const path = this.browsePath;
    if (!path || !this.resourceRoot || path === this.resourceRoot) return;
    const parent = path.replace(/\/+$/, "").split("/").slice(0, -1).join("/") || "/";
    void this.browse(pathWithinRoot(parent, this.resourceRoot) ? parent : this.resourceRoot);
  };

  loadThreadEvents = async (threadId: string, cursor?: string) => {
    if (!this.client) return;
    try {
      const shouldAppend = Boolean(cursor && this.threadEventsThreadId === threadId);
      const page = await this.client.listThreadEvents({ threadId, cursor: shouldAppend ? cursor : undefined });
      this.clearLoadError("threadEvents");
      this.threadEventsThreadId = threadId;
      this.threadEvents = shouldAppend ? [...this.threadEvents, ...page.data] : page.data;
      this.threadEventsCursor = page.cursor;
    } catch (err) {
      this.threadEvents = [];
      this.threadEventsCursor = null;
      this.threadEventsThreadId = threadId;
      this.recordLoadError("threadEvents", err);
    }
  };

  loadThreadControlSurfaces = async (threadId: string) => {
    await Promise.allSettled([
      this.loadThreadCouplings(threadId),
      this.loadThreadApprovals(threadId),
      this.loadThreadWaiting(threadId),
      this.loadThreadDebugExport(threadId),
    ]);
  };

  loadThreadCouplings = async (threadId: string) => {
    const client = this.client;
    if (!client) return;
    try {
      const page = await client.listThreadCouplings({ threadId });
      if (this.client !== client) return;
      this.threadCouplings = { ...this.threadCouplings, [threadId]: page };
      this.clearLoadError("threadCouplings");
    } catch (err) {
      if (this.client !== client) return;
      this.recordLoadError("threadCouplings", err);
    }
  };

  loadThreadApprovals = async (threadId: string) => {
    const client = this.client;
    if (!client) return;
    try {
      const page = await client.listThreadApprovals({ threadId });
      if (this.client !== client) return;
      this.threadApprovals = { ...this.threadApprovals, [threadId]: page };
      this.clearLoadError("threadApprovals");
    } catch (err) {
      if (this.client !== client) return;
      this.recordLoadError("threadApprovals", err);
    }
  };

  loadThreadWaiting = async (threadId: string) => {
    const client = this.client;
    if (!client) return;
    try {
      const page = await client.listThreadWaiting({ threadId });
      if (this.client !== client) return;
      this.threadWaiting = { ...this.threadWaiting, [threadId]: page };
      this.clearLoadError("threadWaiting");
    } catch (err) {
      if (this.client !== client) return;
      this.recordLoadError("threadWaiting", err);
    }
  };

  loadThreadDebugExport = async (threadId: string) => {
    const client = this.client;
    if (!client) return;
    try {
      const bundle = await client.exportThreadDebug({
        threadId,
        streams: ["thread", "control"],
        includeThread: true,
        maxEventsPerStream: 200,
        redact: true,
      });
      if (this.client !== client) return;
      this.threadDebugExports = { ...this.threadDebugExports, [threadId]: bundle };
      this.clearLoadError("threadDebugExport");
    } catch (err) {
      if (this.client !== client) return;
      this.recordLoadError("threadDebugExport", err);
    }
  };

  resolveApproval = async (
    threadId: string,
    approvalId: string,
    decision: "approved" | "denied",
    reason?: string,
  ): Promise<ApprovalResolveResponse | undefined> => {
    const client = this.client;
    if (!client) return undefined;
    const response = await client.resolveApproval({ threadId, approvalId, decision, reason });
    if (this.client !== client) return response;
    await Promise.allSettled([
      this.loadThreadApprovals(threadId),
      this.loadThreadWaiting(threadId),
      this.loadThreadEvents(threadId),
      this.loadThreadDebugExport(threadId),
    ]);
    return response;
  };

  /**
   * Fetch and cache the thread's bind-receipt envelope (idempotent). The
   * receipt is bind-time and immutable for the thread's life, so a cached
   * entry — including null for receipt-less legacy threads — is never refetched.
   */
  ensureThreadEnvelope = async (threadId: string) => {
    if (!this.client || threadId in this.threadEnvelopes) return;
    const existing = this.envelopeRequests.get(threadId);
    if (existing) return existing;

    const client = this.client;
    if (this.threadEnvelopeErrorThreadId === threadId) {
      this.threadEnvelopeErrorThreadId = null;
      this.clearLoadError("threadEnvelope");
    }
    const request = this.fetchThreadEnvelope(client, threadId);
    this.envelopeRequests.set(threadId, request);
    try {
      await request;
    } finally {
      if (this.envelopeRequests.get(threadId) === request) this.envelopeRequests.delete(threadId);
    }
  };

  private async fetchThreadEnvelope(client: CooldisRpcClient, threadId: string) {
    try {
      let receipt: unknown;
      let cursor: string | undefined;
      const seenCursors = new Set<string>();
      while (true) {
        const page = await client.listThreadEvents({ threadId, cursor, kinds: ["manifest.bind.completed"] });
        if (page.data.length) receipt = page.data[page.data.length - 1].payload;
        const nextCursor = page.cursor || undefined;
        if (!nextCursor) break;
        if (seenCursors.has(nextCursor)) {
          throw new Error("thread/events/list returned a non-advancing cursor while loading the bind receipt.");
        }
        seenCursors.add(nextCursor);
        cursor = nextCursor;
      }
      if (this.client !== client) return;
      this.clearLoadError("threadEnvelope");
      this.threadEnvelopeErrorThreadId = null;
      this.threadEnvelopes = { ...this.threadEnvelopes, [threadId]: envelopeFromBindReceipt(receipt) };
    } catch (err) {
      if (this.client !== client) return;
      this.threadEnvelopeErrorThreadId = threadId;
      this.recordLoadError("threadEnvelope", err);
      // Leave the cache key absent so a later look can retry.
    }
  }

  readAgent = async (ref: string) => {
    if (!this.client) return undefined;
    try {
      const record = await this.client.readAgent(ref);
      this.clearLoadError("agents");
      this.agentRecords = { ...this.agentRecords, [ref]: record };
      return record;
    } catch (err) {
      const { [ref]: _record, ...agentRecords } = this.agentRecords;
      this.agentRecords = agentRecords;
      this.recordLoadError("agents", err);
      return undefined;
    }
  };

  planAgentDraft = async (draft: AgentDraftRequest): Promise<AgentPlanResponse | undefined> => {
    if (!this.client) return undefined;
    try {
      const plan = await this.client.planAgentDraft(draft);
      this.clearLoadError("agents");
      return plan;
    } catch (err) {
      this.recordLoadError("agents", err);
      throw err;
    }
  };

  publishAgentDraft = async (draft: AgentDraftRequest): Promise<AgentPublishResponse> => {
    if (!this.client) throw new Error("Connect to a Cooldis app-server before publishing an agent.");
    const published = await this.client.publishAgentDraft(draft);
    const ref = agentRecordRef(published.record);
    this.agentRecords = { ...this.agentRecords, [ref]: published.record };
    this.selectedEntity = { kind: "manifest", id: ref };
    await this.refresh();
    return published;
  };

  publishAgentDraftAndContinue = async (
    draft: AgentDraftRequest,
    threadId: string,
  ): Promise<{ published: AgentPublishResponse; rebind: ThreadRebindForkResponse }> => {
    if (!this.client) throw new Error("Connect to a Cooldis app-server before continuing a thread.");
    const published = await this.publishAgentDraft(draft);
    const agentRef = agentRecordRef(published.record);
    const rebind = await this.client.rebindThread({ threadId, agentRef, reason: "manifest_update" });
    this.openReboundThread(rebind, agentRef);
    void this.refresh();
    return { published, rebind };
  };

  readFile = async (path: string) => {
    if (!this.client) return "";
    try {
      const file = await this.client.readFile(path);
      this.clearLoadError("files");
      this.fileContents = { ...this.fileContents, [path]: file.text };
      this.fileDataBase64 = { ...this.fileDataBase64, [path]: file.dataBase64 };
      return file.text;
    } catch (err) {
      const { [path]: _text, ...fileContents } = this.fileContents;
      const { [path]: _base64, ...fileDataBase64 } = this.fileDataBase64;
      this.fileContents = fileContents;
      this.fileDataBase64 = fileDataBase64;
      this.recordLoadError("files", err);
      return "";
    }
  };

  persistPrefs = (
    endpoint = this.endpoint,
    accent = this.accent,
    defaultThinking = this.defaultThinking,
    defaultAgentRef = this.defaultAgentRef,
  ) => {
    if (typeof localStorage === "undefined") return;
    try {
      const connectionProfile = this.profileWithPolicies();
      localStorage.setItem(
        PREFS_KEY,
        JSON.stringify({ endpoint, accent, defaultThinking, defaultAgentRef, connectionProfile }),
      );
      void this.syncLifecyclePrefs(connectionProfile);
    } catch {
      /* private mode / quota — prefs just don't persist */
    }
  };

  useDaemonEndpoint = (endpoint: string) => {
    if (!this.endpoint || this.endpoint === DEFAULT_ENDPOINT) {
      this.endpoint = endpoint;
      if (this.connectionProfile) {
        this.connectionProfile = this.profileWithPolicies({ ...this.connectionProfile, endpoint });
      }
    }
  };

  openSettings = (section: SettingsSection = this.settingsSection) => {
    this.settingsSection = section;
    this.runtimeOpen = false;
    this.paletteOpen = false;
    this.settingsOpen = true;
  };

  closeSettings = () => {
    this.settingsOpen = false;
  };

  toggleSettings = (section?: SettingsSection) => {
    if (this.settingsOpen && (!section || this.settingsSection === section)) {
      this.closeSettings();
      return;
    }
    this.openSettings(section ?? this.settingsSection);
  };

  /** Dispatch a standalone desktop application-menu action. */
  handleMenuAction = (action: string) => {
    if (action.startsWith("mode:")) {
      const mode = MODES.find((m) => m.id === action.slice("mode:".length));
      if (mode) this.setMode(mode.id);
      return;
    }
    switch (action) {
      case "settings":
        this.toggleSettings();
        break;
      case "about":
        this.openSettings("about");
        break;
      case "newThread":
        void this.newThreadFromDefault();
        break;
      case "closeTab":
        if (this.mode === "chat" && this.activeTabId) this.closeTab(this.activeTabId);
        break;
      case "toggleSidebar":
        this.sidebarOpen = !this.sidebarOpen;
        break;
      case "toggleInspector":
        this.inspectorOpen = !this.inspectorOpen;
        break;
      case "commandPalette":
        this.runtimeOpen = false;
        this.paletteOpen = !this.paletteOpen;
        break;
      case "quit":
        this.requestQuit();
        break;
    }
  };

  // ---------- navigation ----------
  setMode = (m: ModeId) => {
    const entering = this.mode !== m;
    this.mode = m;
    this.settingsOpen = false;
    this.sidebarOpen = true;
    // The registry view reflects out-of-band publishes (CLI `cooldis tool|agent
    // publish`) on entry rather than only on turn completion.
    if (entering && m === "registry" && this.connected) void this.refresh();
  };

  cycleMode = (dir: number) => {
    const i = MODES.findIndex((x) => x.id === this.mode);
    const next = MODES[(i + dir + MODES.length) % MODES.length];
    this.setMode(next.id);
  };

  openTab = (tab: Omit<Tab, "id"> & { id?: string }) => {
    const existing = this.tabs.find(
      (t) => t.kind === tab.kind && t.threadId === tab.threadId && t.filePath === tab.filePath,
    );
    if (existing) {
      this.activeTabId = existing.id;
      return existing;
    }
    const full: Tab = {
      ...tab,
      id: tab.id ?? uid(tab.kind),
      ...(tab.kind === "chat" ? { thinking: tab.thinking ?? this.defaultThinking } : {}),
    };
    this.tabs = [...this.tabs, full];
    this.activeTabId = full.id;
    // return the proxied entry, not `full` — mutations through the raw object
    // bypass $state reactivity and the UI would never see them
    return this.tabs[this.tabs.length - 1];
  };

  closeTab = (id: string) => {
    const i = this.tabs.findIndex((t) => t.id === id);
    if (i === -1) return;
    this.tabs = this.tabs.filter((t) => t.id !== id);
    if (this.activeTabId === id) {
      this.activeTabId = this.tabs[Math.max(0, i - 1)]?.id ?? null;
    }
  };

  openFile = (path: string) => {
    this.mode = "chat";
    this.openTab({ kind: "file", title: path.split("/").pop() ?? path, icon: "FileCode", filePath: path });
  };

  openThread = (thread: Thread) => {
    this.mode = "chat";
    this.selectedEntity = { kind: "thread", id: thread.id };
    const tab = this.openTab({
      kind: "chat",
      title: thread.title.replace(/^↳\s*/, ""),
      icon: "MessagesSquare",
      threadId: thread.id,
    });
    tab.thinking ??= this.defaultThinking;
    if (tab.messages === undefined && tab.historyState === undefined) {
      void this.loadHistory(tab);
    }
    return tab;
  };

  private loadHistory = async (tab: Tab) => {
    if (!this.client || !tab.threadId) {
      tab.messages = [];
      tab.historyState = "ready";
      return;
    }
    tab.historyState = "loading";
    try {
      const items = await this.client.readThreadTranscript(tab.threadId);
      // streamed messages may have landed while history was loading; keep them after it
      const streamed = tab.messages ?? [];
      tab.messages = mergeTranscriptMessages(items, streamed);
      tab.historyState = "ready";
    } catch (err) {
      tab.messages = tab.messages ?? [];
      tab.historyState = "error";
      tab.messages.push({ id: uid("m"), role: "system", text: `Could not load thread history: ${msg(err)}` });
    }
  };

  /**
   * Start a thread on the connected app-server. With `agentRef`, the thread is
   * manifest-backed: the runtime takes provider, model, and cwd from the
   * published record (passing them alongside agentRef is rejected), and emits
   * compile/bind receipts as thread events before turn 1. Without `agentRef`
   * the start is deliberately param-less so the daemon binds its default
   * manifest (thread-lineage rule) rather than the legacy explicit-param path.
   */
  newThread = async (agentRef?: string, options: { preserveError?: boolean; cwd?: string } = {}) => {
    if (!this.connected || !this.client) {
      this.error = "Connect to a Cooldis app-server before creating a thread.";
      return;
    }
    const startKey = agentRef ?? "";
    if (this.startingThreadRefs[startKey]) return;
    if (!options.preserveError) this.error = "";
    this.startingThreadRefs = { ...this.startingThreadRefs, [startKey]: true };
    const manifest = agentRef ? this.manifests.find((m) => m.id === agentRef) : undefined;
    try {
      const cwd = options.cwd?.trim();
      const params: { [key: string]: JsonValue } = { ephemeral: false };
      if (agentRef) params.agentRef = agentRef;
      // The manifest must allowlist default_cwd; if it doesn't, the daemon's
      // teaching error surfaces in the error bar rather than being pre-masked.
      if (cwd) params.runtimeOverrides = { defaultCwd: cwd };
      const res = await this.client.request("thread/start", params);
      const thread = getObject(res, "thread");
      const id = getString(thread, "id") ?? uid("thr");
      const agentName = agentRef ? (manifest?.name ?? agentRef) : undefined;
      if (agentRef && agentName) this.threadAgents.set(id, { ref: agentRef, name: agentName });
      const model = getString(res, "model") ?? getString(thread, "model") ?? manifest?.model ?? this.runtimeModel;
      const t: Thread = {
        id,
        agentRef,
        agentName,
        title: "New thread",
        preview: "",
        model,
        provider: normalizeProvider(getString(thread, "modelProvider") ?? getString(res, "modelProvider"), model, this.runtimeProvider),
        status: "idle",
        turns: 0,
        updatedAt: Date.now(),
      };
      this.threads = [t, ...this.threads];
      const tab = this.openThread(t);
      tab.messages = [];
      tab.historyState = "ready";
      void this.ensureThreadEnvelope(id);
    } catch (err) {
      this.error = msg(err);
    } finally {
      const { [startKey]: _started, ...rest } = this.startingThreadRefs;
      this.startingThreadRefs = rest;
    }
  };

  private openReboundThread(response: ThreadRebindForkResponse, agentRef: string) {
    const manifest = this.manifests.find((m) => m.id === agentRef);
    const thread = reboundThreadFromResponse(response, {
      agentRef,
      manifest,
      runtimeModel: this.runtimeModel,
      runtimeProvider: this.runtimeProvider,
      fallbackId: uid("thr"),
      nowMs: Date.now(),
    });
    this.threadAgents.set(thread.id, { ref: agentRef, name: thread.agentName ?? agentRef });
    this.threads = [thread, ...this.threads.filter((candidate) => candidate.id !== thread.id)];
    const tab = this.openThread(thread);
    tab.messages = [];
    tab.historyState = "ready";
    void this.ensureThreadEnvelope(thread.id);
  }

  /**
   * Start a thread from the configured default agent (settings preference).
   * A stale preference (manifest no longer published) falls back to a ref-less
   * start with a non-blocking notice rather than failing the start.
   */
  newThreadFromDefault = async () => {
    const ref = this.defaultAgentRef;
    if (ref && this.agentsLoaded && !this.manifests.some((m) => m.id === ref)) {
      this.error = `Default agent ${ref} is not in the registry — started from the daemon default instead.`;
      return this.newThread(undefined, { preserveError: true });
    }
    return this.newThread(ref || undefined);
  };

  // ---------- chat ----------
  send = async (text: string) => {
    const tab = this.activeTab;
    if (!tab || tab.kind !== "chat" || !text.trim()) return;
    tab.messages = tab.messages ?? [];
    const userId = uid("m");
    const liveId = uid("m");
    tab.messages.push({ id: userId, role: "user", text });
    tab.messages.push({ id: liveId, role: "assistant", text: "", live: true });
    tab.activeTurnId = "";
    tab.busy = true;
    this.titleFromFirstMessage(tab, text);

    if (!this.connected || !this.client || !tab.threadId) {
      this.error = "Connect to a Cooldis app-server before sending a turn.";
      tab.busy = false;
      const live = tab.messages.find((m) => m.id === liveId);
      if (live) live.live = false;
      return;
    }
    try {
      const thinking = thinkingParam(tab.thinking ?? this.defaultThinking);
      const res = await this.client.request("turn/start", {
        threadId: tab.threadId,
        input: [textInput(text)],
        ...(thinking ? { thinking } : {}),
      });
      const turn = getObject(res, "turn");
      tab.activeTurnId = getString(turn, "id") ?? "";
      for (const message of tab.messages) {
        if (message.id === userId || message.id === liveId) message.turnId = tab.activeTurnId;
      }
      if (tab.activeTurnId) this.flushPendingTurnDeltas(tab, tab.activeTurnId);
    } catch (err) {
      tab.busy = false;
      const live = tab.messages.find((m) => m.id === liveId);
      if (live) live.live = false;
      tab.messages.push({ id: uid("m"), role: "system", text: `Turn failed: ${msg(err)}` });
    }
  };

  private titleFromFirstMessage(tab: Tab, text: string) {
    const thread = this.threads.find((t) => t.id === tab.threadId);
    const title = text.length > 48 ? `${text.slice(0, 48)}…` : text;
    if (thread && (thread.title === "New thread" || thread.title === "Untitled thread")) {
      thread.title = title;
      thread.preview = text;
      this.threads = [...this.threads];
    }
    if (tab.title === "New thread" || tab.title === "Untitled thread") tab.title = title;
  }

  interrupt = async () => {
    const tab = this.activeTab;
    if (!tab || tab.kind !== "chat") return;
    tab.busy = false;
    tab.messages?.forEach((m) => (m.live = false));
    if (this.client && tab.threadId && tab.activeTurnId) {
      try {
        await this.client.request("turn/interrupt", { threadId: tab.threadId, turnId: tab.activeTurnId });
      } catch (err) {
        this.error = msg(err);
      }
    }
  };

  // ---------- RPC notifications ----------
  private handleNotification = (n: RpcNotification) => {
    if (n.method === "turn/started") {
      const turnId = notificationTurnId(n);
      const tab =
        this.notificationTab(n) ??
        this.singlePendingBusyTab();
      if (tab?.kind === "chat" && turnId) {
        tab.activeTurnId = turnId;
        tab.messages?.forEach((message) => {
          if (message.live && !message.turnId) message.turnId = turnId;
        });
        this.flushPendingTurnDeltas(tab, turnId);
      }
    }
    if (n.method === "item/agentMessage/delta" || n.method === "item/agentThinking/delta") {
      const delta = getString(n.params, "delta") ?? "";
      const turnId = notificationTurnId(n);
      const kind = n.method === "item/agentThinking/delta" ? "thinking" : "message";
      if (!this.applyDeltaNotification(n, kind, delta) && turnId) {
        const pending = this.pendingTurnDeltas.get(turnId) ?? [];
        pending.push({ kind, delta });
        this.pendingTurnDeltas.set(turnId, pending);
      }
    }
    if (n.method === "item/started" || n.method === "item/completed") {
      const toolCall = parseToolCallItem(getObject(n.params, "item"));
      if (toolCall) this.applyToolCallNotification(n, toolCall);
    }
    if (n.method === "error") {
      // turn errors arrive as { error: { message, code? }, threadId, turnId }
      const message =
        getString(getObject(n.params, "error"), "message") ??
        getString(n.params, "message") ??
        "The runtime reported an error.";
      for (const tab of this.notificationTabs(n)) {
        if (!tab.busy) continue;
        tab.busy = false;
        tab.messages?.forEach((m) => (m.live = false));
        tab.messages?.push({ id: uid("m"), role: "system", text: message });
      }
    }
    if (n.method === "turn/completed") {
      const turnId = notificationTurnId(n);
      const turn = getObject(n.params, "turn");
      const items = getArray(turn, "items");
      const tabs = this.notificationTabs(n);
      for (const tab of tabs.length ? tabs : this.tabs.filter((t) => t.kind === "chat")) {
        if (turnId) this.flushPendingTurnDeltas(tab, turnId);
        if (items.length) this.applyTurnToolItems(tab, turnId, items);
        tab.busy = false;
        tab.messages?.forEach((m) => (m.live = false));
        this.pruneEmptyAssistantSegments(tab, turnId);
      }
      if (turnId) this.pendingTurnDeltas.delete(turnId);
      void this.refresh();
    }
  };

  private notificationTab(n: RpcNotification) {
    const threadId = notificationThreadId(n);
    const turnId = notificationTurnId(n);
    return this.tabs.find(
      (tab) =>
        tab.kind === "chat" &&
        ((threadId && tab.threadId === threadId) ||
          (turnId && (tab.activeTurnId === turnId || tab.messages?.some((message) => message.turnId === turnId)))),
    );
  }

  private notificationTabs(n: RpcNotification) {
    const target = this.notificationTab(n);
    if (target) return [target];
    return this.tabs.filter((tab) => tab.kind === "chat");
  }

  private singlePendingBusyTab() {
    const tabs = this.tabs.filter((tab) => tab.kind === "chat" && tab.busy && !tab.activeTurnId);
    return tabs.length === 1 ? tabs[0] : undefined;
  }

  private applyDeltaNotification(n: RpcNotification, kind: "message" | "thinking", delta: string) {
    const turnId = notificationTurnId(n);
    const tab = this.notificationTab(n) ?? (!turnId ? this.singleLiveDeltaTab() : undefined);
    if (!tab) return false;
    if (turnId) this.flushPendingTurnDeltas(tab, turnId);
    return this.applyTurnDelta(tab, turnId, kind, delta);
  }

  private singleLiveDeltaTab() {
    const tabs = this.tabs.filter(
      (tab) =>
        tab.kind === "chat" &&
        tab.messages?.some((message) => message.role === "assistant" && message.kind !== "tool" && message.live),
    );
    return tabs.length === 1 ? tabs[0] : undefined;
  }

  private applyToolCallNotification(n: RpcNotification, toolCall: ChatToolCall) {
    const turnId = notificationTurnId(n);
    const tab = this.notificationTab(n) ?? this.singlePendingBusyTab() ?? this.singleLiveDeltaTab();
    if (!tab) return false;
    return this.upsertToolCall(tab, turnId, toolCall);
  }

  private applyTurnToolItems(tab: Tab, turnId: string | undefined, items: JsonValue[]) {
    for (const item of items) {
      const toolCall = parseToolCallItem(item);
      if (toolCall) this.upsertToolCall(tab, turnId, toolCall);
    }
  }

  private upsertToolCall(tab: Tab, turnId: string | undefined, toolCall: ChatToolCall) {
    tab.messages = tab.messages ?? [];
    const messages = tab.messages;
    const existing = messages.find((message) => message.kind === "tool" && message.toolCall?.id === toolCall.id);
    if (existing) {
      existing.toolCall = {
        ...existing.toolCall,
        ...toolCall,
        output: toolCall.output ?? existing.toolCall?.output,
      };
      existing.live = toolCall.status === "inProgress";
      if (turnId) existing.turnId = turnId;
      return true;
    }

    const message: ChatMessage = {
      id: `tool_${toolCall.id}`,
      kind: "tool",
      role: "assistant",
      text: "",
      toolCall,
      turnId,
      live: toolCall.status === "inProgress",
    };
    const liveAssistantIndex = findLastIndex(
      messages,
      (candidate) =>
        candidate.role === "assistant" &&
        candidate.kind !== "tool" &&
        candidate.live === true &&
        (!turnId || !candidate.turnId || candidate.turnId === turnId),
    );
    if (liveAssistantIndex >= 0) {
      const liveAssistant = messages[liveAssistantIndex];
      if (turnId && !liveAssistant.turnId) liveAssistant.turnId = turnId;
      const hasContent = Boolean(liveAssistant.text || liveAssistant.thinking);
      if (hasContent) liveAssistant.live = false;
      messages.splice(hasContent ? liveAssistantIndex + 1 : liveAssistantIndex, 0, message);
    } else {
      messages.push(message);
    }
    if (turnId) {
      tab.activeTurnId = turnId;
      const liveAssistant = messages.find(
        (candidate) => candidate.role === "assistant" && candidate.kind !== "tool" && candidate.live && !candidate.turnId,
      );
      if (liveAssistant) liveAssistant.turnId = turnId;
    }
    return true;
  }

  private applyTurnDelta(tab: Tab, turnId: string | undefined, kind: "message" | "thinking", delta: string) {
    const messages = tab.messages;
    if (!messages) return false;
    const exact = turnId
      ? findLastMessage(
          messages,
          (message) =>
            message.role === "assistant" && message.kind !== "tool" && message.live === true && message.turnId === turnId,
        )
      : undefined;
    const fallback = findLastMessage(
      messages,
      (message) =>
        message.role === "assistant" && message.kind !== "tool" && message.live === true && (!turnId || !message.turnId),
    );
    let message = exact ?? fallback;
    if (!message) {
      message = { id: uid("m"), kind: "text", role: "assistant", text: "", turnId, live: true };
      messages.push(message);
    }

    const hadText = Boolean(message.text);
    if (kind === "thinking") {
      message.thinking = (message.thinking ?? "") + delta;
      if (!message.text) message.thinkingOpen ??= true;
    } else {
      message.text += delta;
      if (!hadText && message.text) message.thinkingOpen = false;
    }
    message.live = true;
    if (turnId) {
      message.turnId = turnId;
      if (!tab.activeTurnId) tab.activeTurnId = turnId;
    }
    return true;
  }

  private pruneEmptyAssistantSegments(tab: Tab, turnId: string | undefined) {
    if (!tab.messages) return;
    tab.messages = tab.messages.filter(
      (message) =>
        !(
          message.role === "assistant" &&
          message.kind !== "tool" &&
          !message.text &&
          !message.thinking &&
          (!turnId || message.turnId === turnId)
        ),
    );
  }

  private flushPendingTurnDeltas(tab: Tab, turnId: string) {
    const pending = this.pendingTurnDeltas.get(turnId);
    if (!pending?.length) return;
    const remaining = pending.filter((entry) => !this.applyTurnDelta(tab, turnId, entry.kind, entry.delta));
    if (remaining.length) this.pendingTurnDeltas.set(turnId, remaining);
    else this.pendingTurnDeltas.delete(turnId);
  }

  private cleanup() {
    this.unNotif?.();
    this.unEvt?.();
    this.unNotif = undefined;
    this.unEvt = undefined;
  }

  private pruneThreadAgents(threadIds: Set<string>) {
    for (const id of this.threadAgents.keys()) {
      if (!threadIds.has(id)) this.threadAgents.delete(id);
    }
  }

  private clearLoadError(slice: LoadSlice) {
    if (!this.loadErrors[slice]) return;
    const { [slice]: _removed, ...rest } = this.loadErrors;
    this.loadErrors = rest;
  }

  private recordLoadError(slice: LoadSlice, err: unknown) {
    const message = msg(err);
    this.loadErrors = { ...this.loadErrors, [slice]: message };
  }
}

function msg(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

function findLastIndex<T>(items: T[], predicate: (item: T) => boolean) {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (predicate(items[index])) return index;
  }
  return -1;
}

function findLastMessage(messages: ChatMessage[], predicate: (message: ChatMessage) => boolean) {
  const index = findLastIndex(messages, predicate);
  return index >= 0 ? messages[index] : undefined;
}

/** Map the UI thinking level onto the turn/start wire shape; "default" sends nothing. */
function thinkingParam(level: unknown): JsonValue | undefined {
  if (!isThinkingLevel(level) || level === "default") return undefined;
  if (level === "disabled") return { type: "disabled" };
  return { type: "effort", effort: level };
}

function thinkingLabel(value: unknown): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const config = value as Record<string, unknown>;
  switch (config.type) {
    case "effort":
      return isThinkingEffort(config.effort) ? `effort: ${config.effort}` : undefined;
    case "budget":
      return typeof config.budgetTokens === "number" &&
        Number.isInteger(config.budgetTokens) &&
        config.budgetTokens >= 0
        ? `budget: ${config.budgetTokens}`
        : undefined;
    case "disabled":
      return "disabled";
    default:
      return undefined;
  }
}

function numberField(value: unknown, key: string): number | undefined {
  if (!value || typeof value !== "object") return undefined;
  const v = (value as Record<string, unknown>)[key];
  if (typeof v === "number") return v < 1e12 ? v * 1000 : v; // seconds -> ms
  return undefined;
}

function nonEmptyString(value: JsonValue | undefined, key: string) {
  const text = getString(value, key)?.trim();
  return text || undefined;
}

function isAbsolutePath(path: string) {
  return path.startsWith("/") || /^[A-Za-z]:[\\/]/.test(path);
}

function normalizePath(path: string) {
  const normalized = path.replace(/\/+$/, "");
  return normalized || "/";
}

function pathWithinRoot(path: string, root: string) {
  const target = normalizePath(path);
  const normalizedRoot = normalizePath(root);
  return normalizedRoot === "/" || target === normalizedRoot || target.startsWith(`${normalizedRoot}/`);
}

function notificationTurnId(n: RpcNotification) {
  const turn = getObject(n.params, "turn");
  return getString(n.params, "turnId") ?? getString(n.params, "turn_id") ?? getString(turn, "id");
}

function notificationThreadId(n: RpcNotification) {
  const turn = getObject(n.params, "turn");
  const thread = getObject(n.params, "thread");
  return (
    getString(n.params, "threadId") ??
    getString(n.params, "thread_id") ??
    getString(turn, "threadId") ??
    getString(turn, "thread_id") ??
    getString(thread, "id")
  );
}

function modelInfoFromRpc(model: ModelListEntry, runtimeProvider: string): ModelInfo {
  const id = modelId(model);
  const provider = modelProvider(model, runtimeProvider);
  return {
    id,
    name: model.displayName ?? model.name ?? id,
    provider,
    context: modelContext(model),
    kind: provider === "local_offline" || id === "echo" ? "echo" : "chat",
  };
}

function modelId(model: ModelListEntry) {
  return model.id || model.model || "model";
}

function modelProvider(model: ModelListEntry, fallback: string) {
  if (model.providerId) return model.providerId;
  if (model.providerRef) return model.providerRef.replace(/^provider:\/\//, "");
  return fallback;
}

function modelContext(model: ModelListEntry) {
  if (model.contextWindowTokens && model.maxOutputTokens) {
    return `${formatNumber(model.contextWindowTokens)} context / ${formatNumber(model.maxOutputTokens)} output`;
  }
  if (model.contextWindowTokens) return `${formatNumber(model.contextWindowTokens)} context`;
  return model.description ?? "—";
}

function manifestFromAgent(agent: AgentListEntry): ManifestDef {
  return {
    id: agent.refUri || `${agent.name}@${agent.version}`,
    name: agent.title ?? agent.name,
    version: agent.version,
    summary: agent.summary ?? "",
    status: "published",
    model: agent.defaultModelProfile?.modelRef ?? "",
    tools: agent.toolIds,
    source: agent.refUri,
  };
}

/**
 * Parse a manifest.bind.completed payload (kernel AgentManifestBindReceipt,
 * snake_case on the wire) into the rendered envelope. Lenient: a payload
 * missing the identity fields degrades to null, not a broken inspector.
 */
function envelopeFromBindReceipt(payload: unknown): ThreadEnvelope | null {
  const refUri = stringField(payload, "ref_uri");
  const manifestHash = stringField(payload, "manifest_hash");
  if (!refUri || !manifestHash) return null;
  const runtime = recordField(payload, "effective_runtime");
  return {
    refUri,
    manifestHash,
    modelProfileId: stringField(payload, "model_profile_id") ?? "",
    providerId: stringField(payload, "provider_id") ?? "",
    modelId: stringField(payload, "model_id") ?? "",
    toolIds: stringArrayField(payload, "tool_ids"),
    operationBindings: arrayField(payload, "operation_bindings").map(envelopeBinding),
    granted: stringArrayField(payload, "granted"),
    effectiveCwd: stringField(runtime, "default_cwd") ?? "",
    streaming: booleanField(runtime, "streaming"),
    turnTimeoutMs: numberRecordField(runtime, "turn_timeout_ms"),
    overriddenKeys: stringArrayField(payload, "overridden_keys"),
  };
}

function envelopeBinding(value: unknown): ThreadEnvelopeBinding {
  return {
    name: stringField(value, "name") ?? "",
    artifactHash: stringField(value, "artifact_hash") ?? "",
    grants: stringArrayField(value, "grants"),
    operations: stringArrayField(value, "operations"),
    directTools: arrayField(value, "direct_tools").map((tool) => ({
      toolName: stringField(tool, "tool_name") ?? "",
      operation: stringField(tool, "operation") ?? "",
    })),
  };
}

function toolFromOperation(operation: OperationListEntry): ToolDef {
  return {
    id: operation.name,
    name: operationName(operation),
    version: operation.activeArtifactHash ? operation.activeArtifactHash.slice(0, 12) : "",
    artifactHash: operation.activeArtifactHash,
    summary: operation.summary ?? "",
    status: "published",
    power: operationPower(operation),
    calls: 0,
    source: operationSource(operation),
    inputs: operationInputs(operation),
  };
}

function operationName(operation: OperationListEntry) {
  const identity = recordField(operation.interface, "identity");
  return (
    stringField(identity, "displayName") ??
    stringField(identity, "display_name") ??
    stringField(identity, "name") ??
    operation.name
  );
}

function operationPower(operation: OperationListEntry) {
  const operations = arrayField(operation.capabilityGrants, "operations");
  const first = recordAt(operations, 0);
  return stringField(first, "kind") ?? "operation";
}

function operationSource(operation: OperationListEntry) {
  const source = operation.source;
  if (typeof source === "string") return source;
  return stringField(source, "path") ?? operation.name;
}

function operationInputs(operation: OperationListEntry) {
  const operations = arrayField(operation.interface, "operations");
  const names = operations.map((entry) => stringField(entry, "name")).filter((name): name is string => Boolean(name));
  return names.length ? names : [operation.name];
}

function childPath(root: string, name: string) {
  if (root === "/") return `/${name}`;
  return `${root.replace(/\/$/, "")}/${name}`;
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

function isWebSocketEndpoint(value: string) {
  try {
    const url = new URL(value);
    return (url.protocol === "ws:" || url.protocol === "wss:") && Boolean(url.hostname);
  } catch {
    return false;
  }
}

function recordField(value: unknown, key: string) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  return (value as Record<string, unknown>)[key];
}

function stringField(value: unknown, key: string) {
  const child = recordField(value, key);
  return typeof child === "string" && child.trim() ? child : undefined;
}

function arrayField(value: unknown, key: string) {
  const child = recordField(value, key);
  return Array.isArray(child) ? child : [];
}

function stringArrayField(value: unknown, key: string) {
  return arrayField(value, key).filter((item): item is string => typeof item === "string");
}

function booleanField(value: unknown, key: string) {
  const child = recordField(value, key);
  return typeof child === "boolean" ? child : undefined;
}

function numberRecordField(value: unknown, key: string) {
  const child = recordField(value, key);
  return typeof child === "number" ? child : undefined;
}

function recordAt(value: unknown[], index: number) {
  const child = value[index];
  return child && typeof child === "object" && !Array.isArray(child) ? child : undefined;
}

export const app = new AppState();
