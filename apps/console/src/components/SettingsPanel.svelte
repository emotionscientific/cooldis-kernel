<script lang="ts">
  import { app } from "../lib/app.svelte";
  import type { SettingsSection } from "../lib/app.svelte";
  import { THINKING_LEVELS, isThinkingLevel } from "../lib/schema";
  import Icon from "./Icon.svelte";

  const sections: { id: SettingsSection; label: string; icon: string }[] = [
    { id: "connection", label: "Connection", icon: "Wifi" },
    { id: "appearance", label: "Appearance", icon: "Palette" },
    { id: "chat", label: "Chat", icon: "MessagesSquare" },
    { id: "shortcuts", label: "Shortcuts", icon: "Keyboard" },
    { id: "about", label: "About", icon: "Info" },
  ];

  const accents = ["#6e7bf2", "#58a6ff", "#bc8cff", "#3fb950", "#f0883e", "#db61a2"];

  const shortcuts: { keys: string; label: string }[] = [
    { keys: "⌘K", label: "Command palette" },
    { keys: "⌘N", label: "New thread" },
    { keys: "⌘,", label: "Settings" },
    { keys: "⌘B", label: "Toggle sidebar" },
    { keys: "⌘.", label: "Toggle inspector" },
    { keys: "⌘W", label: "Close tab" },
    { keys: "⌘1–4", label: "Switch mode" },
  ];

  function applyAccent(c: string) {
    app.accent = c;
  }

  function setDefaultThinking(e: Event) {
    const value = (e.currentTarget as HTMLSelectElement).value;
    if (isThinkingLevel(value)) app.defaultThinking = value;
  }

  function setDefaultAgent(e: Event) {
    app.defaultAgentRef = (e.currentTarget as HTMLSelectElement).value;
  }

  function openConnectionSetup() {
    app.connectionSetupOpen = true;
    app.startPromptOpen = false;
  }

  const providerAuthTitle = $derived(app.providerAuth?.displayName ?? app.providerAuth?.providerId ?? "Provider");
  const providerAuthStatus = $derived(
    app.providerAuth?.configured
      ? `${app.providerAuth.source ?? "stored"}${app.providerAuth.label ? ` · ${app.providerAuth.label}` : ""}`
      : "missing",
  );
  const defaultAgentStale = $derived(
    app.agentsLoaded && !!app.defaultAgentRef && !app.manifests.some((m) => m.id === app.defaultAgentRef),
  );

  let providerApiKey = $state("");

  async function saveProviderAuth() {
    await app.setProviderAuth(providerApiKey);
    if (app.providerAuth?.configured) providerApiKey = "";
  }

  async function deleteProviderAuth() {
    await app.deleteProviderAuth();
  }
</script>

<div class="settings-panel" role="region" aria-label="Settings">
  <nav class="settings-nav">
    <div class="settings-title"><Icon name="Settings" size={15} /> Settings</div>
    {#each sections as s}
      <button class:active={app.settingsSection === s.id} onclick={() => (app.settingsSection = s.id)}>
        <Icon name={s.icon} size={14} /> {s.label}
      </button>
    {/each}
  </nav>

  <div class="settings-body">
    <button class="settings-close" onclick={app.closeSettings} aria-label="Close settings" title="Close (Esc)">
      <Icon name="X" size={16} />
    </button>
    <div class="settings-content">
      {#if app.settingsSection === "connection"}
        <h3>Connection</h3>
        <dl class="kv">
          <dt>Profile</dt><dd>{app.profileLabel}</dd>
          <dt>Policy</dt><dd>{app.startPolicy} / {app.quitPolicy}</dd>
          {#if app.native && app.runtime}
            <dt>Runtime</dt>
            <dd class="mono">{app.runtime.installed ? (app.runtime.path ?? "verlet") : "not installed"}</dd>
          {/if}
          {#if app.providerAuth}
            <dt>Provider auth</dt>
            <dd>{app.providerAuth.configured ? (app.providerAuth.source ?? "stored") : "missing"}</dd>
          {/if}
        </dl>
        <label class="field">
          <span>RPC endpoint</span>
          <input
            class="input"
            bind:value={app.endpoint}
            spellcheck="false"
            onchange={app.applyEndpoint}
            onkeydown={(e) => e.key === "Enter" && app.applyEndpoint()}
          />
        </label>
        <div class="row">
          <button class="btn" class:primary={!app.connected} onclick={app.toggleConnection} disabled={app.status === "connecting"}>
            <Icon name={app.connected ? "Power" : "Plug"} size={14} />
            {app.connected ? "Disconnect" : app.status === "connecting" ? "Connecting" : "Connect"}
          </button>
          {#if app.canStartManagedDaemon}
            <button class="btn" onclick={() => void app.startManagedDaemon({ rememberAuto: app.startPolicy === "auto" })} disabled={app.status === "connecting"}>
              <Icon name="Play" size={14} />
              Start daemon
            </button>
          {:else if app.canRestartManagedDaemon}
            <button class="btn" onclick={() => void app.restartManagedDaemon()} disabled={app.status === "connecting"}>
              <Icon name="RefreshCcw" size={14} />
              Restart daemon
            </button>
          {/if}
          {#if app.native}
            <button class="btn" onclick={openConnectionSetup}>
              <Icon name="Cable" size={14} />
              Setup…
            </button>
          {/if}
          {#if app.canStopManagedDaemon}
            <button class="btn danger" onclick={() => void app.stopManagedDaemon()}>
              <Icon name="Power" size={14} />
              Stop daemon
            </button>
          {/if}
          <span class="hint">
            {app.connected ? "Live runtime" : "Offline"}
            {#if app.healthRttLabel}· Health {app.healthRttLabel}{/if}
          </span>
        </div>
        {#if app.providerAuth && app.providerAuth.providerId !== "local_offline"}
          <div class="credential-panel" class:missing={!app.providerAuth.configured}>
            <dl class="kv">
              <dt>Provider</dt><dd>{providerAuthTitle}</dd>
              <dt>Credential</dt><dd>{providerAuthStatus}</dd>
              {#if app.providerAuth.stateHome}<dt>Store</dt><dd class="mono">{app.providerAuth.stateHome}</dd>{/if}
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
              <button class="btn" class:primary={!app.providerAuth.configured} onclick={saveProviderAuth} disabled={!providerApiKey.trim() || app.providerAuthBusy}>
                <Icon name="KeyRound" size={14} />
                {app.providerAuthBusy ? "Saving…" : "Save credential"}
              </button>
              {#if app.providerAuth.configured}
                <button class="btn danger" onclick={deleteProviderAuth} disabled={app.providerAuthBusy}>
                  <Icon name="Trash2" size={14} />
                  Delete
                </button>
              {/if}
            </div>
            {#if app.providerAuth.lastError}
              <p class="env-note danger">{app.providerAuth.lastError}</p>
            {/if}
          </div>
        {/if}
        {#if app.native && app.hostInfo}
          <h3>Desktop host</h3>
          <dl class="kv">
            <dt>Platform</dt><dd>{app.hostInfo.platform} / {app.hostInfo.arch}</dd>
            <dt>Host</dt><dd>{app.hostInfo.hostname}</dd>
            <dt>Runtime</dt><dd>bun {app.hostInfo.bun}</dd>
            <dt>Capacity</dt><dd>{app.hostInfo.cpus} cpu · {app.hostInfo.totalMemGB} GB</dd>
            {#if app.daemon}
              <dt>Daemon</dt><dd>{app.daemon.mode}{app.daemon.managed ? " · app-started" : ""}</dd>
              {#if app.daemon.desktopConfigPath}<dt>App config</dt><dd class="mono">{app.daemon.desktopConfigPath}</dd>{/if}
              {#if app.daemon.secretSource}<dt>Credential</dt><dd>{app.daemon.secretSource}</dd>{/if}
            {/if}
          </dl>
        {/if}

      {:else if app.settingsSection === "appearance"}
        <h3>Accent color</h3>
        <div class="swatches">
          {#each accents as c}
            <button
              class="swatch"
              class:active={app.accent === c}
              style="--c:{c}"
              onclick={() => applyAccent(c)}
              aria-label={c}
            ></button>
          {/each}
        </div>
        <p class="hint">Theme is tuned for low-light operator use. Light mode is on the roadmap.</p>

      {:else if app.settingsSection === "chat"}
        <h3>Thinking</h3>
        <label class="field" style="max-width: 280px">
          <span>Default thinking level for new threads</span>
          <select class="input" value={app.defaultThinking} onchange={setDefaultThinking}>
            {#each THINKING_LEVELS as level (level.id)}
              <option value={level.id}>{level.label}</option>
            {/each}
          </select>
        </label>
        <p class="hint">
          "Default" leaves the level to the daemon's provider config. The level can be changed
          per thread from the composer; it is sent with each turn.
        </p>

        <h3>Agent</h3>
        <label class="field" style="max-width: 280px">
          <span>Default agent for new threads</span>
          <select class="input" value={app.defaultAgentRef} onchange={setDefaultAgent}>
            <option value="">Default (daemon manifest)</option>
            {#if defaultAgentStale}
              <option value={app.defaultAgentRef}>{app.defaultAgentRef} (missing)</option>
            {/if}
            {#each app.manifests as m (m.id)}
              <option value={m.id}>{m.name} · {m.version}</option>
            {/each}
          </select>
        </label>
        <p class="hint">
          "Default" starts threads without an agent ref; the daemon binds its default manifest.
          The chevron next to "+" picks an agent for a single thread without changing this.
          {#if defaultAgentStale}
            The saved agent is not in the registry — new threads fall back to Default until it is
            published again or another agent is selected.
          {/if}
        </p>

      {:else if app.settingsSection === "shortcuts"}
        <h3>Keyboard</h3>
        <ul class="shortcuts">
          {#each shortcuts as s}
            <li><span class="kbd">{s.keys}</span> <span>{s.label}</span></li>
          {/each}
        </ul>
        <p class="hint">Shortcuts are also available from the application menu when running as a desktop app.</p>

      {:else}
        <h3>Verlet Console</h3>
        <p class="hint">
          Operator control plane for local and remote agents.
          {app.native ? "Running as a native desktop app." : "Running in the browser."}
        </p>
        <dl class="kv">
          <dt>Version</dt><dd>0.1.0</dd>
          <dt>Mode</dt><dd>{app.native ? "Electrobun desktop" : "Web"}</dd>
        </dl>
      {/if}
    </div>
  </div>
</div>
