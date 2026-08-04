<script lang="ts">
  import { app } from "../../lib/app.svelte";
  import Icon from "../Icon.svelte";

  let expanded = $state<Record<string, boolean>>({});
  function toggle(id: string) {
    expanded[id] = !expanded[id];
  }

  function isFilePath(source: string) {
    return source.startsWith("/");
  }
  function toolName(id: string) {
    return app.tools.find((t) => t.id === id)?.name ?? id;
  }
  function startingThread(id: string) {
    return app.startingThreadRefs[id] === true;
  }
</script>

<div class="view registry">
  <header class="view-head">
    <div class="view-head-main">
      <h1>Registry</h1>
      <p>Everything published to this workspace — tools and the agents declared from them.</p>
    </div>
    <button class="btn" disabled={!app.connected} title="Reload published tools and agents" onclick={() => void app.refresh()}>
      <Icon name="RefreshCw" size={14} /> Refresh
    </button>
    <button class="btn primary" disabled title="Publishing is CLI-only today — `verlet tool publish` / `verlet agent publish`">
      <Icon name="Plus" size={14} /> New tool
    </button>
  </header>

  <div class="reg-scroll">
    <section class="reg-section">
      <h2 class="reg-section-title"><Icon name="Wrench" size={13} /> Tools <span class="reg-count">{app.tools.length}</span></h2>
      <div class="reg-list">
        {#each app.tools as t (t.id)}
          <div class="reg-card" class:open={expanded[t.id]} class:sel={app.selectedEntity?.kind === "tool" && app.selectedEntity?.id === t.id}>
            <button class="reg-card-head" onclick={() => { toggle(t.id); app.selectedEntity = { kind: "tool", id: t.id }; }}>
              <span class="reg-chev" class:open={expanded[t.id]}><Icon name="ChevronRight" size={14} /></span>
              <span class="reg-icon"><Icon name="Wrench" size={15} /></span>
              <span class="reg-name mono">{t.name}</span>
              <span class="reg-ver">v{t.version}</span>
              <span class="badge" class:draft={t.status === "draft"}>{t.status}</span>
              <span class="reg-summary">{t.summary}</span>
            </button>
            {#if expanded[t.id]}
              <div class="reg-body">
                <dl class="reg-kv">
                  <dt>ABI power</dt><dd><span class="pill">{t.power}</span></dd>
                  <dt>Inputs</dt><dd>{#each t.inputs as p}<span class="pill mono">{p}</span>{/each}</dd>
                  <dt>Source</dt><dd>{#if isFilePath(t.source)}<button class="srclink" onclick={() => app.openFile(t.source)}><span class="mono">{t.source}</span> <Icon name="ArrowUpRight" size={12} /></button>{:else}<span class="mono">{t.source}</span>{/if}</dd>
                </dl>
              </div>
            {/if}
          </div>
        {:else}
          <div class="empty small">
            <span class="ic"><Icon name="Wrench" size={18} /></span>
            <p>{app.connected ? "No tools published in this workspace yet — publish one with `verlet tool publish`." : "Connect to a runtime to load published tools."}</p>
          </div>
        {/each}
      </div>
    </section>

    <section class="reg-section">
      <h2 class="reg-section-title"><Icon name="Bot" size={13} /> Agents <span class="reg-count">{app.manifests.length}</span></h2>
      <div class="reg-list">
        {#each app.manifests as m (m.id)}
          <div class="reg-card" class:open={expanded[m.id]} class:sel={app.selectedEntity?.kind === "manifest" && app.selectedEntity?.id === m.id}>
            <button class="reg-card-head" onclick={() => { toggle(m.id); app.selectedEntity = { kind: "manifest", id: m.id }; }}>
              <span class="reg-chev" class:open={expanded[m.id]}><Icon name="ChevronRight" size={14} /></span>
              <span class="reg-icon manifest"><Icon name="Bot" size={15} /></span>
              <span class="reg-name mono">{m.name}</span>
              <span class="reg-ver">v{m.version}</span>
              <span class="badge" class:draft={m.status === "draft"}>{m.status}</span>
              <span class="reg-summary">{m.summary}</span>
              <span class="reg-meta mono">{m.model}</span>
            </button>
            {#if expanded[m.id]}
              <div class="reg-body">
                <dl class="reg-kv">
                  <dt>Model</dt><dd><span class="pill mono">{m.model || "runtime default"}</span></dd>
                  <dt>Tools</dt><dd>{#each m.tools as tid}<button class="pill mono link" onclick={() => { app.selectedEntity = { kind: "tool", id: tid }; }}>{toolName(tid)}</button>{:else}<span style="color:var(--tx-faint)">none</span>{/each}</dd>
                  <dt>Source</dt><dd>{#if isFilePath(m.source)}<button class="srclink" onclick={() => app.openFile(m.source)}><span class="mono">{m.source}</span> <Icon name="ArrowUpRight" size={12} /></button>{:else}<span class="mono">{m.source}</span>{/if}</dd>
                </dl>
                <div style="margin-top:10px">
                  <button
                    class="btn primary"
                    disabled={!app.connected || startingThread(m.id)}
                    aria-busy={startingThread(m.id)}
                    title="Start a thread bound to this manifest"
                    onclick={() => void app.newThread(m.id)}
                  >
                    <Icon name={startingThread(m.id) ? "RefreshCw" : "MessagesSquare"} size={13} class={startingThread(m.id) ? "spin" : ""} />
                    {startingThread(m.id) ? "Starting…" : "Start thread"}
                  </button>
                </div>
              </div>
            {/if}
          </div>
        {:else}
          <div class="empty small">
            <span class="ic"><Icon name="Bot" size={18} /></span>
            <p>{app.connected ? "No agents declared in this workspace yet — publish one with `verlet agent publish`." : "Connect to a runtime to load declared agents."}</p>
          </div>
        {/each}
      </div>
    </section>
  </div>
</div>
