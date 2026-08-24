<script lang="ts">
  import { app } from "../lib/app.svelte";
  import Icon from "./Icon.svelte";
  import ChatView from "./views/ChatView.svelte";
  import ActivityView from "./views/ActivityView.svelte";
  import WorkspaceView from "./views/WorkspaceView.svelte";
  import FileView from "./views/FileView.svelte";
  import RegistryView from "./views/RegistryView.svelte";
  import ThreadStartMenu from "./ThreadStartMenu.svelte";

  const tab = $derived(app.activeTab);
</script>

<div class="pane-fill workspace">
  {#if app.mode === "registry"}
    <RegistryView />
  {:else if app.mode === "workspace"}
    <WorkspaceView />
  {:else if app.mode === "activity"}
    <ActivityView />
  {:else}
    {#if !tab}
      <div class="empty">
        <span class="ic"><Icon name="MessagesSquare" size={20} /></span>
        <h3>Verlet Console</h3>
        {#if app.connected}
          <p>Pick a thread on the left, start a new one, or hit <span class="kbd">⌘K</span>.</p>
          <div style="display:flex;gap:8px;margin-top:6px">
            <ThreadStartMenu buttonClass="btn primary" iconSize={13} label="New thread" />
          </div>
        {:else}
          <p>
            Connection lost. Restart <span class="mono">verlet console</span>
            and reload this page.
          </p>
        {/if}
      </div>
    {:else if tab.kind === "chat"}
      {#key tab.id}<ChatView {tab} />{/key}
    {:else if tab.kind === "file"}
      {#key tab.id}<FileView {tab} />{/key}
    {/if}
  {/if}
</div>
