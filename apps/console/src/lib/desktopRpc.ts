// Shared shape for the standalone desktop host. The kernel-bundled console is
// browser-only, so this stays dependency-free in this repo copy.
type RPCSchema<T> = T;

export interface HostInfo {
  platform: string;
  arch: string;
  hostname: string;
  bun: string;
  cpus: number;
  totalMemGB: number;
}

export interface LocalAgent {
  id: string;
  name: string;
  bin: string;
  pid: number | null;
  status: "running" | "stopped" | "error";
  startedAt: number | null;
}

export type ConnectionProfileKind = "local-managed" | "local-external" | "remote";
export type StartPolicy = "ask" | "auto" | "leave-offline";
export type QuitPolicy = "ask" | "stop-managed" | "leave-running";

export interface ConnectionProfile {
  kind: ConnectionProfileKind;
  endpoint: string;
  runtimePath?: string;
  startPolicy: StartPolicy;
  quitPolicy: QuitPolicy;
}

export interface RuntimeStatus {
  installed: boolean;
  path: string | null;
  version: string | null;
  source: "env" | "install-root" | "path" | null;
  installRoot: string;
  binDir: string;
  installing: boolean;
  lastError: string | null;
  log?: string;
}

/** State of the local Cooldis runtime daemon (`cooldis rpc`) the host supervises. */
export interface DaemonStatus {
  running: boolean;
  /** true when this host process started it (vs. attached to an external one). */
  managed: boolean;
  /** Launch shape used by the desktop host. */
  mode: "external" | "rpc" | "daemon" | "remote";
  profileKind: ConnectionProfileKind;
  endpoint: string;
  pid: number | null;
  bin: string | null;
  provider: string | null;
  model: string | null;
  configPath: string | null;
  desktopConfigPath: string | null;
  secretSource: string | null;
  startedAt: number | null;
  lastError: string | null;
}

export interface ProviderAuthStatus {
  providerId: string;
  displayName: string | null;
  configured: boolean;
  source: string | null;
  label: string | null;
  stateHome: string | null;
  lastError: string | null;
}

export type DesktopRequestApi = {
  getHostInfo(params: {}): Promise<HostInfo>;
  listLocalAgents(params: {}): Promise<LocalAgent[]>;
  spawnLocalAgent(params: { bin: string; args?: string[]; name?: string }): Promise<LocalAgent>;
  stopLocalAgent(params: { id: string }): Promise<{ ok: boolean }>;
  openExternal(params: { url: string }): Promise<{ ok: boolean }>;
  detectRuntime(params: {}): Promise<RuntimeStatus>;
  installRuntime(params: { version?: string }): Promise<RuntimeStatus>;
  setLifecyclePrefs(params: { profile: ConnectionProfile | null }): Promise<{ ok: boolean }>;
  ensureDaemon(params: { profile: ConnectionProfile }): Promise<DaemonStatus>;
  daemonStatus(params: { profile?: ConnectionProfile | null }): Promise<DaemonStatus>;
  stopDaemon(params: { profile?: ConnectionProfile | null }): Promise<DaemonStatus>;
  restartDaemon(params: { profile: ConnectionProfile }): Promise<DaemonStatus>;
  providerAuthStatus(params: { providerId?: string }): Promise<ProviderAuthStatus>;
  providerAuthSet(params: { providerId?: string; apiKey: string }): Promise<ProviderAuthStatus>;
  providerAuthDelete(params: { providerId?: string }): Promise<ProviderAuthStatus>;
  toggleWindowZoom(params: {}): Promise<{ ok: boolean; maximized: boolean }>;
  requestAppQuit(params: { quitPolicy: QuitPolicy; remember?: boolean }): Promise<{ ok: boolean }>;
};

export type CooldisDesktopRPC = {
  bun: RPCSchema<{
    requests: {
      getHostInfo: { params: {}; response: HostInfo };
      listLocalAgents: { params: {}; response: LocalAgent[] };
      spawnLocalAgent: { params: { bin: string; args?: string[]; name?: string }; response: LocalAgent };
      stopLocalAgent: { params: { id: string }; response: { ok: boolean } };
      openExternal: { params: { url: string }; response: { ok: boolean } };
      // Local runtime daemon lifecycle.
      detectRuntime: { params: {}; response: RuntimeStatus };
      installRuntime: { params: { version?: string }; response: RuntimeStatus };
      setLifecyclePrefs: { params: { profile: ConnectionProfile | null }; response: { ok: boolean } };
      ensureDaemon: { params: { profile: ConnectionProfile }; response: DaemonStatus };
      daemonStatus: { params: { profile?: ConnectionProfile | null }; response: DaemonStatus };
      stopDaemon: { params: { profile?: ConnectionProfile | null }; response: DaemonStatus };
      restartDaemon: { params: { profile: ConnectionProfile }; response: DaemonStatus };
      providerAuthStatus: { params: { providerId?: string }; response: ProviderAuthStatus };
      providerAuthSet: { params: { providerId?: string; apiKey: string }; response: ProviderAuthStatus };
      providerAuthDelete: { params: { providerId?: string }; response: ProviderAuthStatus };
      toggleWindowZoom: { params: {}; response: { ok: boolean; maximized: boolean } };
      requestAppQuit: { params: { quitPolicy: QuitPolicy; remember?: boolean }; response: { ok: boolean } };
    };
    messages: {
      log: { msg: string };
    };
  }>;
  webview: RPCSchema<{
    requests: {};
    messages: {
      agentEvent: { id: string; status: LocalAgent["status"]; line?: string };
      daemonEvent: { status: DaemonStatus };
      /** Custom application-menu item clicked in the native menu bar. */
      menuAction: { action: string };
    };
  }>;
};
