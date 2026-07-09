<script lang="ts">
  import { onMount } from "svelte";
  import { Pane, PaneGroup, PaneResizer } from "paneforge";
  import { app } from "./lib/app.svelte";
  import { MODES } from "./lib/schema";
  import Topbar from "./components/Topbar.svelte";
  import Rail from "./components/Rail.svelte";
  import Sidebar from "./components/Sidebar.svelte";
  import Workspace from "./components/Workspace.svelte";
  import Inspector from "./components/Inspector.svelte";
  import CommandPalette from "./components/CommandPalette.svelte";
  import SettingsPanel from "./components/SettingsPanel.svelte";
  import ConnectionSetup from "./components/ConnectionSetup.svelte";
  import QuitPrompt from "./components/QuitPrompt.svelte";
  import Icon from "./components/Icon.svelte";

  let sidebarPane = $state<any>();
  let inspectorPane = $state<any>();

  // UI prefs: apply the accent globally and persist alongside connection/chat settings.
  $effect(() => {
    const endpoint = app.endpoint;
    const accent = app.accent;
    const defaultThinking = app.defaultThinking;
    const defaultAgentRef = app.defaultAgentRef;
    const connectionProfile = app.connectionProfile;
    const startPolicy = app.startPolicy;
    const quitPolicy = app.quitPolicy;
    document.documentElement.style.setProperty("--accent", accent);
    void connectionProfile;
    void startPolicy;
    void quitPolicy;
    app.persistPrefs(endpoint, accent, defaultThinking, defaultAgentRef);
  });

  function sync(pane: any, open: boolean) {
    if (!pane) return;
    const collapsed = pane.isCollapsed?.();
    if (open && collapsed) pane.expand?.();
    else if (!open && !collapsed) pane.collapse?.();
  }
  $effect(() => {
    if (!app.settingsOpen) sync(sidebarPane, app.sidebarOpen);
  });
  $effect(() => {
    if (!app.settingsOpen) sync(inspectorPane, app.inspectorOpen);
  });

  function onKeydown(e: KeyboardEvent) {
    const meta = e.metaKey || e.ctrlKey;
    if (e.key === "Escape" && app.settingsOpen) {
      e.preventDefault();
      app.closeSettings();
      return;
    }
    if (meta && e.key.toLowerCase() === "k") {
      e.preventDefault();
      app.runtimeOpen = false;
      app.paletteOpen = !app.paletteOpen;
      return;
    }
    if (meta && e.key === ",") {
      e.preventDefault();
      app.toggleSettings();
      return;
    }
    if (!meta) return;
    if (e.key.toLowerCase() === "b") {
      e.preventDefault();
      app.sidebarOpen = !app.sidebarOpen;
    } else if (e.key.toLowerCase() === "w") {
      e.preventDefault();
      if (app.mode === "chat" && app.activeTabId) app.closeTab(app.activeTabId);
    } else if (e.key === ".") {
      e.preventDefault();
      app.inspectorOpen = !app.inspectorOpen;
    } else {
      const mode = MODES.find((m) => m.key === e.key);
      if (mode) {
        e.preventDefault();
        app.setMode(mode.id);
      }
    }
  }

  onMount(() => {
    if (!app.native) void app.connect();
  });
</script>

<svelte:window onkeydown={onKeydown} />

<div class="app" class:native={app.native}>
  <Topbar />
  <div class="shell-body">
    <Rail />
    {#if app.settingsOpen}
      <SettingsPanel />
    {:else}
      <PaneGroup direction="horizontal" autoSaveId="cooldis.shell.h" style="height:100%;flex:1;min-width:0">
        <Pane
          order={1}
          defaultSize={19}
          minSize={14}
          maxSize={30}
          collapsible
          collapsedSize={0}
          bind:this={sidebarPane}
          onCollapse={() => (app.sidebarOpen = false)}
          onExpand={() => (app.sidebarOpen = true)}
        >
          <Sidebar />
        </Pane>
        <PaneResizer class="resizer" />

        <Pane order={2} minSize={32}>
          <Workspace />
        </Pane>
        <PaneResizer class="resizer" />

        <Pane
          order={3}
          defaultSize={23}
          minSize={16}
          maxSize={36}
          collapsible
          collapsedSize={0}
          bind:this={inspectorPane}
          onCollapse={() => (app.inspectorOpen = false)}
          onExpand={() => (app.inspectorOpen = true)}
        >
          <Inspector />
        </Pane>
      </PaneGroup>
    {/if}
  </div>
</div>

<CommandPalette />
<ConnectionSetup />
<QuitPrompt />

{#if app.error}
  <div class="error-toast" role="alert">
    <Icon name="TriangleAlert" size={14} />
    <span>{app.error}</span>
    <button type="button" onclick={() => (app.error = "")} aria-label="Dismiss error">
      <Icon name="X" size={13} />
    </button>
  </div>
{/if}
