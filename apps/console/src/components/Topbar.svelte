<script lang="ts">
  import { app } from "../lib/app.svelte";
  import Icon from "./Icon.svelte";
  import brandUrl from "../assets/brand.png";

  // Electrobun marks regions draggable via these class names (WKWebView has no
  // -webkit-app-region). Interactive controls must opt out with -no-drag.
  const NODRAG = "electrobun-webkit-app-region-no-drag";

  const endpointHost = $derived(app.endpoint.replace(/^wss?:\/\//, "").replace(/\/rpc$/, ""));
  const stateLabel = $derived(app.connected ? "connected" : app.status === "connecting" ? "connecting" : "offline");
  let runtimeDialog = $state<HTMLElement>();

  $effect(() => {
    if (app.runtimeOpen) queueMicrotask(() => runtimeDialog?.focus());
  });

  function openCommandPalette() {
    app.runtimeOpen = false;
    app.paletteOpen = true;
  }

  function openConnectionSetup() {
    app.runtimeOpen = false;
    app.connectionSetupOpen = true;
    app.startPromptOpen = false;
  }

  function onRuntimeKeydown(event: KeyboardEvent) {
    if (app.runtimeOpen && event.key === "Escape") {
      event.preventDefault();
      app.runtimeOpen = false;
    }
  }

  function onTopbarDoubleClick(event: MouseEvent) {
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest(`.${NODRAG}, button, input, select, textarea, a, [role="button"]`)) return;
    event.preventDefault();
    void app.toggleWindowZoom();
  }
</script>

<svelte:window onkeydown={onRuntimeKeydown} />

<!-- svelte-ignore a11y_no_static_element_interactions -->
<header
  class="topbar electrobun-webkit-app-region-drag"
  class:native={app.native}
  aria-label="Global toolbar"
  ondblclick={onTopbarDoubleClick}
>
  <div class="brand">
    <span class="logo"><img src={brandUrl} alt="Verlet" /></span>
    <b>Verlet</b>
  </div>

  <div class="topbar-spacer"></div>

  <button class="cmdk-trigger {NODRAG}" onclick={openCommandPalette} aria-label="Open command palette">
    <Icon name="Search" size={14} />
    <span class="label">Search threads, tools, agents…</span>
    <span class="kbd">⌘K</span>
  </button>

  <div class="topbar-spacer"></div>

  <button
    class="conn-pill {NODRAG}"
    class:on={app.connected}
    class:connecting={app.status === "connecting"}
    onclick={() => (app.runtimeOpen = !app.runtimeOpen)}
    title="Runtime connection"
  >
    <span
      class="dot"
      class:ok={app.connected}
      class:warn={app.status === "connecting"}
      class:pulse={app.status === "connecting"}
    ></span>
    <span class="mono">{endpointHost}</span>
    {#if !app.connected}
      <span aria-hidden="true">·</span>
      <span>{stateLabel}</span>
    {/if}
  </button>

  <div class="topbar-divider"></div>

  <button
    class="icon-btn {NODRAG}"
    class:active={app.sidebarOpen}
    onclick={() => (app.sidebarOpen = !app.sidebarOpen)}
    title="Toggle sidebar (⌘B)"><Icon name="PanelLeft" size={16} /></button>
  <button
    class="icon-btn {NODRAG}"
    class:active={app.inspectorOpen}
    onclick={() => (app.inspectorOpen = !app.inspectorOpen)}
    title="Toggle inspector (⌘.)"><Icon name="PanelRight" size={16} /></button>
</header>

{#if app.runtimeOpen}
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <div class="runtime-scrim" role="presentation" onclick={() => (app.runtimeOpen = false)}></div>
  <div class="runtime-pop" role="dialog" aria-label="Runtime connection" tabindex="-1" bind:this={runtimeDialog}>
    <h4>Runtime</h4>
    <dl class="kv">
      <dt>Profile</dt><dd>{app.profileLabel}</dd>
      <dt>Endpoint</dt><dd class="mono">{app.endpoint}</dd>
      <dt>State</dt><dd>{stateLabel}</dd>
      {#if app.native && app.connectionProfile?.kind === "local-managed" && app.daemon}
        <dt>Daemon</dt><dd>{app.daemon.running ? (app.daemon.managed ? "running · app-started" : "running · external") : "stopped"}</dd>
      {/if}
      {#if app.native && app.runtime}
        <dt>Runtime</dt><dd class="mono">{app.runtime.installed ? (app.runtime.path ?? "verlet") : "not installed"}</dd>
      {/if}
      {#if app.connected}
        <dt>Models</dt><dd>{app.modelInventoryLabel}</dd>
        {#if app.runtimeCwd}<dt>Workspace</dt><dd class="mono">{app.runtimeCwd}</dd>{/if}
        {#if app.healthRttLabel}<dt>Health RTT</dt><dd>{app.healthRttLabel}</dd>{/if}
      {/if}
    </dl>
    {#if !app.connected}
      <p class="hint">
        {#if app.native && !app.connectionProfile}
          Choose a connection profile to continue.
        {:else if app.connectionProfile?.kind === "local-managed"}
          Local daemon is {app.daemon?.running ? "running" : "stopped"}. Restart picks up a new runtime binary or daemon config.
        {:else if app.connectionProfile?.kind === "remote"}
          Remote auth is not configured in this pass; the URL must already accept this console.
        {:else}
          No daemon answered at this endpoint. Start the external daemon outside this app.
        {/if}
      </p>
    {/if}
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
        <button class="btn" onclick={() => void app.restartManagedDaemon()} disabled={app.status === "connecting"} title="Restart local daemon and reconnect">
          <Icon name="RefreshCcw" size={14} />
          Restart
        </button>
      {/if}
      <button class="btn" onclick={app.native ? openConnectionSetup : () => app.openSettings("connection")}>
        <Icon name={app.native ? "Cable" : "Settings"} size={14} />
        {app.native ? "Setup…" : "Settings…"}
      </button>
    </div>
  </div>
{/if}
