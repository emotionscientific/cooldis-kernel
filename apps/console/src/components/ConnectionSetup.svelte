<script lang="ts">
  import { app } from "../lib/app.svelte";
  import type { ConnectionProfileKind } from "../lib/desktopRpc";
  import Icon from "./Icon.svelte";

  const LOCAL_ENDPOINT = "ws://127.0.0.1:49200/rpc";

  let selected = $state<ConnectionProfileKind>(app.connectionProfile?.kind ?? "local-managed");
  let localEndpoint = $state(app.connectionProfile?.kind?.startsWith("local") ? app.connectionProfile.endpoint : LOCAL_ENDPOINT);
  let remoteEndpoint = $state(app.connectionProfile?.kind === "remote" ? app.connectionProfile.endpoint : "wss://");
  let rememberAutoStart = $state(app.startPolicy === "auto");

  const open = $derived(app.connectionSetupOpen || app.startPromptOpen);
  const runtimeInstalled = $derived(Boolean(app.runtime?.installed));
  const runtimeLabel = $derived(
    app.runtime?.installed
      ? `${app.runtime.version ?? "installed"} · ${app.runtime.path ?? "verlet"}`
      : "Not installed",
  );
  const providerAuthTitle = $derived(app.providerAuth?.displayName ?? app.providerAuth?.providerId ?? "Provider");
  const providerAuthStatus = $derived(
    app.providerAuth?.configured
      ? `${app.providerAuth.source ?? "stored"}${app.providerAuth.label ? ` · ${app.providerAuth.label}` : ""}`
      : "missing",
  );

  let providerApiKey = $state("");

  $effect(() => {
    if (open && app.connectionProfile) {
      selected = app.connectionProfile.kind;
      if (app.connectionProfile.kind === "remote") remoteEndpoint = app.connectionProfile.endpoint;
      else localEndpoint = app.connectionProfile.endpoint;
      rememberAutoStart = app.startPolicy === "auto";
    }
  });

  $effect(() => {
    if (open && selected === "local-managed" && app.native) void app.refreshProviderAuth();
  });

  async function installRuntime() {
    await app.installRuntime();
    if (app.runtime?.installed) selected = "local-managed";
  }

  async function saveProviderAuth() {
    await app.setProviderAuth(providerApiKey);
    if (app.providerAuth?.configured) providerApiKey = "";
  }

  async function deleteProviderAuth() {
    await app.deleteProviderAuth();
  }

  async function startLocalManaged() {
    app.startPolicy = rememberAutoStart ? "auto" : "ask";
    await app.configureProfile("local-managed", localEndpoint, { connect: false });
    await app.startManagedDaemon({ rememberAuto: rememberAutoStart });
  }

  async function saveLocalOffline() {
    app.startPolicy = "leave-offline";
    await app.configureProfile("local-managed", localEndpoint, { connect: false });
  }

  async function saveExternal() {
    app.startPolicy = "ask";
    await app.configureProfile("local-external", localEndpoint, { connect: true });
  }

  async function saveRemote() {
    app.startPolicy = "ask";
    await app.configureProfile("remote", remoteEndpoint, { connect: true });
  }
</script>

{#if open}
  <div class="setup-scrim" role="presentation"></div>
  <div class="setup-dialog" role="dialog" aria-label="Connection setup" tabindex="-1">
    <div class="setup-head">
      <div>
        <h2>Connect Verlet</h2>
        <p>{app.startPromptOpen ? "Start the saved local runtime or choose another endpoint." : "Choose where this console should connect."}</p>
      </div>
      {#if app.connectionProfile}
        <button class="icon-btn" title="Close" aria-label="Close" onclick={() => { app.connectionSetupOpen = false; app.startPromptOpen = false; }}>
          <Icon name="X" size={16} />
        </button>
      {/if}
    </div>

    <div class="setup-grid">
      <button class="setup-choice" class:active={selected === "local-managed"} onclick={() => (selected = "local-managed")}>
        <Icon name="HardDrive" size={18} />
        <span>Local managed</span>
      </button>
      <button class="setup-choice" class:active={selected === "local-external"} onclick={() => (selected = "local-external")}>
        <Icon name="Cable" size={18} />
        <span>Local external</span>
      </button>
      <button class="setup-choice" class:active={selected === "remote"} onclick={() => (selected = "remote")}>
        <Icon name="Cloud" size={18} />
        <span>Remote</span>
      </button>
    </div>

    {#if selected === "local-managed"}
      <div class="setup-panel">
        <dl class="kv">
          <dt>Runtime</dt><dd class="mono">{runtimeLabel}</dd>
          {#if app.daemon}<dt>Daemon</dt><dd>{app.daemon.running ? (app.daemon.managed ? "managed" : "external") : "stopped"}</dd>{/if}
        </dl>
        <label class="field">
          <span>Endpoint</span>
          <input class="input" bind:value={localEndpoint} spellcheck="false" />
        </label>
        {#if app.native}
          <div class="credential-panel" class:missing={!app.providerAuth?.configured}>
            <dl class="kv">
              <dt>Provider</dt><dd>{providerAuthTitle}</dd>
              <dt>Credential</dt><dd>{providerAuthStatus}</dd>
              {#if app.providerAuth?.stateHome}<dt>Store</dt><dd class="mono">{app.providerAuth.stateHome}</dd>{/if}
            </dl>
            <label class="field">
              <span>API key</span>
              <input
                class="input"
                type="password"
                bind:value={providerApiKey}
                autocomplete="off"
                spellcheck="false"
                placeholder="Paste provider API key"
              />
            </label>
            <div class="row wrap">
              <button class="btn" class:primary={!app.providerAuth?.configured} onclick={saveProviderAuth} disabled={!providerApiKey.trim() || app.providerAuthBusy}>
                <Icon name="KeyRound" size={14} />
                {app.providerAuthBusy ? "Saving…" : "Save credential"}
              </button>
              {#if app.providerAuth?.configured}
                <button class="btn danger" onclick={deleteProviderAuth} disabled={app.providerAuthBusy}>
                  <Icon name="Trash2" size={14} />
                  Delete
                </button>
              {/if}
            </div>
            {#if app.providerAuth?.lastError}
              <p class="env-note danger">{app.providerAuth.lastError}</p>
            {/if}
          </div>
        {/if}
        {#if !runtimeInstalled}
          <div class="row">
            <button class="btn primary" onclick={installRuntime} disabled={app.runtimeInstalling}>
              <Icon name="Download" size={14} />
              {app.runtimeInstalling ? "Installing…" : "Install runtime"}
            </button>
            <button class="btn" onclick={() => void app.refreshRuntime()}>
              <Icon name="RefreshCcw" size={14} />
              Check again
            </button>
          </div>
        {:else}
          <label class="check-row">
            <input type="checkbox" bind:checked={rememberAutoStart} />
            <span>Start automatically on later launches</span>
          </label>
          <div class="row">
            <button class="btn primary" onclick={startLocalManaged}>
              <Icon name="Play" size={14} />
              Start daemon
            </button>
            <button class="btn" onclick={saveLocalOffline}>
              <Icon name="CirclePause" size={14} />
              Save offline
            </button>
          </div>
        {/if}
      </div>
    {:else if selected === "local-external"}
      <div class="setup-panel">
        <label class="field">
          <span>Endpoint</span>
          <input class="input" bind:value={localEndpoint} spellcheck="false" />
        </label>
        <div class="row">
          <button class="btn primary" onclick={saveExternal}>
            <Icon name="Plug" size={14} />
            Save and connect
          </button>
        </div>
      </div>
    {:else}
      <div class="setup-panel">
        <label class="field">
          <span>Remote endpoint</span>
          <input class="input" bind:value={remoteEndpoint} spellcheck="false" />
        </label>
        <div class="row">
          <button class="btn primary" onclick={saveRemote}>
            <Icon name="Cloud" size={14} />
            Save and connect
          </button>
        </div>
      </div>
    {/if}
  </div>
{/if}
