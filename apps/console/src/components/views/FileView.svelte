<script lang="ts">
  import { app } from "../../lib/app.svelte";
  import type { Tab } from "../../lib/schema";
  import Icon from "../Icon.svelte";

  let { tab }: { tab: Tab } = $props();

  let loadingPath = $state<string | null>(null);
  let loadError = $state<string | null>(null);
  let attemptedPath = $state<string | null>(null);

  const content = $derived(tab.filePath !== undefined ? app.fileContents[tab.filePath] : undefined);

  $effect(() => {
    const path = tab.filePath;
    if (!path) {
      loadingPath = null;
      loadError = null;
      attemptedPath = null;
      return;
    }
    if (app.fileContents[path] !== undefined) {
      if (loadingPath === path) loadingPath = null;
      if (attemptedPath === path) loadError = null;
      return;
    }
    if (attemptedPath === path || loadingPath === path) return;
    attemptedPath = path;
    loadingPath = path;
    loadError = null;
    app.readFile(path).then(() => {
      if (tab.filePath !== path) return;
      if (loadingPath === path) loadingPath = null;
      if (app.fileContents[path] === undefined) {
        loadError = app.loadErrors.files ?? "Failed to read file.";
      }
    });
  });
</script>

<div class="view">
  <div class="view-toolbar">
    <h1 style="font-weight:500"><span class="mono" style="font-size:13px">{tab.filePath}</span></h1>
    <span class="chip muted">read-only</span>
    <div style="flex:1"></div>
  </div>
  <div class="view-scroll">
    {#if content !== undefined}
      <pre class="mono" style="margin:0;padding:14px 18px;font-size:12px;line-height:1.55;white-space:pre-wrap;word-break:break-word">{content}</pre>
    {:else if loadingPath === tab.filePath}
      <div class="empty">
        <span class="ic"><Icon name="FileCode" size={20} /></span>
        <p>Loading…</p>
      </div>
    {:else if loadError}
      <div class="empty">
        <span class="ic"><Icon name="FileCode" size={20} /></span>
        <h3>Could not read file</h3>
        <p class="mono">{loadError}</p>
      </div>
    {:else}
      <div class="empty">
        <span class="ic"><Icon name="FileCode" size={20} /></span>
        <p>No file selected.</p>
      </div>
    {/if}
  </div>
</div>
