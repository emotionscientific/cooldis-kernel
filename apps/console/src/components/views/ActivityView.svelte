<script lang="ts">
  import { app } from "../../lib/app.svelte";
  import Icon from "../Icon.svelte";

  let source = $state<"client" | "thread">("client");
  let kind = $state<"all" | "sent" | "received" | "error">("all");
  let selectedThreadId = $state("");
  let loadingThread = $state(false);

  const events = $derived(app.events.filter((e) => kind === "all" || e.kind === kind));
  const threadEvents = $derived(app.threadEventsThreadId === selectedThreadId ? app.threadEvents : []);
  const threadError = $derived(app.threadEventsThreadId === selectedThreadId ? app.loadErrors.threadEvents : undefined);

  function title(e: (typeof app.events)[number]) {
    return ("method" in e && e.method) || ("message" in e && e.message) || e.kind;
  }
  function time(at: number) {
    return new Date(at).toLocaleTimeString(undefined, { hour12: false });
  }

  async function loadThread(cursor?: string) {
    if (!selectedThreadId) return;
    const pageCursor = app.threadEventsThreadId === selectedThreadId ? cursor : undefined;
    loadingThread = true;
    try {
      await app.loadThreadEvents(selectedThreadId, pageCursor);
    } finally {
      loadingThread = false;
    }
  }

  function shortId(id: string) {
    return id.length > 12 ? `${id.slice(0, 12)}…` : id;
  }
  function detailJson(e: (typeof app.threadEvents)[number]) {
    return JSON.stringify({ provenance: e.provenance, payload: e.payload }, null, 2);
  }
</script>

<div class="view">
  <div class="view-toolbar">
    <h1>Activity</h1>
    <span class="sub">{source === "client" ? `${app.eventRate} events / min` : "durable thread event stream · receipts"}</span>
    <div style="flex:1"></div>
    <div class="toolbar-search" style="padding:2px; gap:2px">
      <button class="drawer-tab" class:active={source === "client"} onclick={() => (source = "client")}>client log</button>
      <button class="drawer-tab" class:active={source === "thread"} onclick={() => (source = "thread")}>thread history</button>
    </div>
    {#if source === "client"}
      <div class="toolbar-search" style="padding:2px; gap:2px">
        {#each ["all", "sent", "received", "error"] as f}
          <button class="drawer-tab" class:active={kind === f} onclick={() => (kind = f as typeof kind)}>{f}</button>
        {/each}
      </div>
    {/if}
  </div>
  <div class="view-scroll">
    {#if source === "client"}
      <table class="table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Kind</th>
            <th>Method / Message</th>
            <th>ID</th>
          </tr>
        </thead>
        <tbody>
          {#each events as e}
            <tr>
              <td class="mono">{time(e.at)}</td>
              <td>
                <span class="chip {e.kind === 'received' ? 'ok' : e.kind === 'error' ? 'danger' : 'muted'}">
                  {e.kind}
                </span>
              </td>
              <td><span class="strong mono">{title(e)}</span></td>
              <td class="mono">{"id" in e && e.id != null ? e.id : ""}</td>
            </tr>
          {:else}
            <tr><td colspan="4"><div class="empty small"><span class="ic"><Icon name="Activity" size={18} /></span><p>{app.connected ? "No RPC traffic yet this session." : "Connect to a runtime to watch RPC traffic."}</p></div></td></tr>
          {/each}
        </tbody>
      </table>
    {:else}
      <div style="display:flex;gap:8px;align-items:center;padding:10px 14px">
        <select bind:value={selectedThreadId} style="min-width:240px">
          <option value="" disabled>Select a thread…</option>
          {#each app.threads as t (t.id)}
            <option value={t.id}>{t.title} · {shortId(t.id)}</option>
          {/each}
        </select>
        <button class="btn" disabled={!selectedThreadId || loadingThread} onclick={() => loadThread()}>
          <Icon name="Activity" size={13} /> Load
        </button>
        {#if app.threadEventsCursor && app.threadEventsThreadId === selectedThreadId}
          <button class="btn ghost" disabled={loadingThread} onclick={() => loadThread(app.threadEventsCursor ?? undefined)}>
            Load more
          </button>
        {/if}
      </div>
      {#if threadError}
        <div class="empty small">
          <span class="ic"><Icon name="Activity" size={18} /></span>
          <p class="mono">{threadError}</p>
        </div>
      {/if}
      <table class="table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Kind</th>
            <th>Origin</th>
            <th>Event</th>
          </tr>
        </thead>
        <tbody>
          {#each threadEvents as e (e.eventId)}
            <tr>
              <td class="mono">{time(e.atMs)}</td>
              <td><span class="strong mono">{e.kind}</span></td>
              <td>
                <span class="chip {e.origin === 'discharged' ? 'ok' : 'muted'}">{e.origin}</span>
              </td>
              <td style="white-space:normal">
                <details>
                  <summary class="mono" style="cursor:pointer">{shortId(e.eventId)}</summary>
                  <pre class="mono" style="margin:6px 0 2px;font-size:11px;line-height:1.5;white-space:pre-wrap;word-break:break-word;max-width:72ch;overflow:auto">{detailJson(e)}</pre>
                </details>
              </td>
            </tr>
          {:else}
            <tr>
              <td colspan="4">
                <div class="empty small">
                  <span class="ic"><Icon name="Activity" size={18} /></span>
                  <p>{app.threadEventsThreadId === selectedThreadId && selectedThreadId ? "No events recorded for this thread." : "Pick a thread to read its durable event stream — receipts included."}</p>
                </div>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
</div>
