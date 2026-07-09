<script lang="ts">
  import { app } from "../lib/app.svelte";
  import Icon from "./Icon.svelte";
  import ThreadStartMenu from "./ThreadStartMenu.svelte";

  let query = $state("");

  const listTitle = $derived(
    {
      chat: "Threads",
      registry: "Registry",
      workspace: "Files",
      activity: "Recent",
    }[app.mode] ?? "",
  );

  const showSearch = $derived(app.mode === "chat" || app.mode === "registry");

  const ql = $derived(query.trim().toLowerCase());
  const filteredThreads = $derived(
    app.threads.filter((t) => !ql || (t.title + t.preview).toLowerCase().includes(ql)),
  );
  const filteredTools = $derived(app.tools.filter((t) => !ql || (t.name + t.summary).toLowerCase().includes(ql)));
  const filteredManifests = $derived(
    app.manifests.filter((m) => !ql || (m.name + m.summary).toLowerCase().includes(ql)),
  );

  function fmtAgo(ts: number) {
    const s = Math.floor((Date.now() - ts) / 1000);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m`;
    return `${Math.floor(s / 3600)}h`;
  }
</script>

<div class="pane-fill sidebar">
  <div class="sidebar-list">
    <div class="list-head">
      <h2>{listTitle}</h2>
      {#if app.mode === "chat"}
        <ThreadStartMenu />
      {/if}
    </div>

    {#if showSearch}
      <div class="list-head" style="padding-top:0">
        <div class="toolbar-search" style="flex:1">
          <Icon name="Search" size={13} />
          <input style="flex:1" placeholder="Filter…" bind:value={query} spellcheck="false" />
        </div>
      </div>
    {/if}

    <div class="list-scroll">
      {#if app.mode === "chat"}
        {#each filteredThreads as t (t.id)}
          <button
            class="row-item"
            class:active={app.selectedEntity?.id === t.id}
            class:child={!!t.parentId}
            onclick={() => app.openThread(t)}
          >
            <span
              class="dot"
              class:ok={t.status === "running"}
              class:danger={t.status === "error"}
              class:pulse={t.status === "running"}
            ></span>
            <span class="title">{t.title.replace(/^↳\s*/, "")}</span>
            <span class="sub">{t.preview || "no messages yet"}</span>
            <span class="meta" style="grid-column:3;grid-row:2">{fmtAgo(t.updatedAt)}</span>
            {#if t.agentName}<span class="chip muted mono" style="grid-column:3;grid-row:1" title={t.agentRef}>{t.agentName}</span>{/if}
          </button>
        {:else}
          <div class="empty small">
            <span class="ic"><Icon name="Inbox" size={18} /></span>
            <p>{app.connected ? "No threads yet — start one with the + above." : "Connect to a runtime to load threads."}</p>
          </div>
        {/each}
      {:else if app.mode === "registry"}
        {#if filteredTools.length}
          <div class="list-section">Tools</div>
          {#each filteredTools as t (t.id)}
            <button
              class="row-item"
              class:active={app.selectedEntity?.kind === "tool" && app.selectedEntity?.id === t.id}
              onclick={() => (app.selectedEntity = { kind: "tool", id: t.id })}
            >
              <Icon name="Wrench" size={14} />
              <span class="title mono">{t.name}</span>
              <span class="chip muted">v{t.version}</span>
              <span class="sub">{t.summary}</span>
            </button>
          {/each}
        {/if}
        {#if filteredManifests.length}
          <div class="list-section">Agents</div>
          {#each filteredManifests as m (m.id)}
            <button
              class="row-item"
              class:active={app.selectedEntity?.kind === "manifest" && app.selectedEntity?.id === m.id}
              onclick={() => (app.selectedEntity = { kind: "manifest", id: m.id })}
            >
              <Icon name="Bot" size={14} />
              <span class="title mono">{m.name}</span>
              <span class="chip muted">v{m.version}</span>
              <span class="sub">{m.summary}</span>
            </button>
          {/each}
        {/if}
        {#if !filteredTools.length && !filteredManifests.length}
          <div class="empty small">
            <span class="ic"><Icon name="Library" size={18} /></span>
            <p>{app.connected ? "Nothing published in this workspace yet." : "Connect to a runtime to load the registry."}</p>
          </div>
        {/if}
      {:else if app.mode === "workspace"}
        {#each app.resources as r (r.path)}
          <button
            class="row-item"
            onclick={() => (r.kind === "file" ? app.openFile(r.path) : app.browse(r.path))}
          >
            <Icon name={r.kind === "dir" ? "Folder" : "FileCode"} size={14} />
            <span class="title">{r.name}</span>
          </button>
        {:else}
          <div class="empty small">
            <span class="ic"><Icon name="FolderTree" size={18} /></span>
            <p>{app.connected ? (app.loadErrors.resources ?? "No files here.") : "Connect to a runtime to browse workspace files."}</p>
          </div>
        {/each}
      {:else}
        {#each app.events.slice(0, 30) as e}
          <div class="row-item" style="cursor:default">
            <span class="dot" class:ok={e.kind === "received"} class:danger={e.kind === "error"} class:warn={e.kind === "status"}></span>
            <span class="title mono" style="font-size:var(--fs-xs)">{("method" in e && e.method) || ("message" in e && e.message) || e.kind}</span>
          </div>
        {:else}
          <div class="empty small">
            <span class="ic"><Icon name="Activity" size={18} /></span>
            <p>No RPC traffic yet.</p>
          </div>
        {/each}
      {/if}
    </div>
  </div>
</div>
