<script lang="ts">
  import { app } from "../../lib/app.svelte";
  import { THINKING_LEVELS, isThinkingLevel, type ChatToolCall, type Tab } from "../../lib/schema";
  import Icon from "../Icon.svelte";
  import Markdown from "../Markdown.svelte";

  let { tab }: { tab: Tab } = $props();
  let draft = $state("");
  let scroller: HTMLElement | undefined = $state();

  const thread = $derived(app.threads.find((t) => t.id === tab.threadId));
  const loadingHistory = $derived(tab.historyState === "loading");
  const selectedThinking = $derived(tab.thinking ?? app.defaultThinking);

  $effect(() => {
    // autoscroll on new content
    void tab.messages?.map((m) => `${m.text}\0${m.thinking ?? ""}\0${m.toolCall?.status ?? ""}\0${m.toolCall?.output ?? ""}`).join("");
    queueMicrotask(() => scroller?.scrollTo({ top: scroller.scrollHeight }));
  });

  function submit(e: Event) {
    e.preventDefault();
    const text = draft.trim();
    if (!text || tab.busy || loadingHistory) return;
    draft = "";
    void app.send(text);
  }

  function onKey(e: KeyboardEvent) {
    if (e.key === "Enter" && !e.shiftKey) {
      submit(e);
    }
  }

  function setThinking(e: Event) {
    const value = (e.currentTarget as HTMLSelectElement).value;
    tab.thinking = isThinkingLevel(value) ? value : app.defaultThinking;
  }

  function formatValue(value: unknown) {
    if (value === undefined || value === null) return "";
    if (typeof value === "string") return value;
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  function toolStatusLabel(status: ChatToolCall["status"]) {
    if (status === "inProgress") return "Running";
    return status === "completed" ? "Completed" : "Failed";
  }

  function durationLabel(durationMs: number | null | undefined) {
    if (durationMs === null || durationMs === undefined) return "";
    return durationMs >= 1000 ? `${(durationMs / 1000).toFixed(1)}s` : `${durationMs}ms`;
  }
</script>

<div class="view">
  <div class="view-toolbar">
    <h1>{tab.title}</h1>
    <span class="sub mono">{tab.threadId}</span>
    <div style="flex:1"></div>
    <span class="chip muted mono">{thread?.model ?? app.runtimeModel}</span>
    {#if tab.busy}
      <button class="btn danger" onclick={app.interrupt}><Icon name="Square" size={13} /> Interrupt</button>
    {/if}
  </div>

  <div class="chat">
    <div class="messages" bind:this={scroller}>
      {#if tab.historyState === "loading" && !tab.messages?.length}
        <div class="empty">
          <span class="ic"><Icon name="MessagesSquare" size={20} /></span>
          <p>Loading thread history…</p>
        </div>
      {:else}
        {#each tab.messages ?? [] as m (m.id)}
          {#if m.role === "system"}
            <div class="msg system"><span class="pill">{m.text}</span></div>
          {:else if m.kind === "tool" && m.toolCall}
            <div class="msg assistant tool-call">
              <span class="ava tool-ava"><Icon name="Terminal" size={14} /></span>
              <div>
                <div class="who">
                  Tool
                  <span class="tool-status {m.toolCall.status}">{toolStatusLabel(m.toolCall.status)}</span>
                  {#if durationLabel(m.toolCall.durationMs)}
                    <span class="tool-duration">{durationLabel(m.toolCall.durationMs)}</span>
                  {/if}
                </div>
                <div class="tool-card {m.toolCall.status}">
                  <div class="tool-title">
                    <Icon name="Terminal" size={13} />
                    <code>{m.toolCall.tool}</code>
                    {#if m.live}<span class="caret"></span>{/if}
                  </div>
                  {#if m.toolCall.arguments !== undefined}
                    <div class="tool-section">
                      <div class="tool-section-title">Args</div>
                      <pre>{formatValue(m.toolCall.arguments)}</pre>
                    </div>
                  {/if}
                  {#if m.toolCall.output}
                    <div class="tool-section">
                      <div class="tool-section-title">Output</div>
                      <pre>{m.toolCall.output}</pre>
                    </div>
                  {/if}
                </div>
              </div>
            </div>
          {:else}
            <div class="msg {m.role}">
              <span class="ava">{m.role === "user" ? "U" : "A"}</span>
              <div>
                <div class="who">{m.role === "user" ? "You" : "Assistant"}</div>
                {#if m.role === "assistant"}
                  <div class="body md-body">
                    {#if m.thinking}
                      <details class="thinking" bind:open={m.thinkingOpen}>
                        <summary>{m.live && !m.text ? "Thinking…" : "Thought process"}</summary>
                        <div class="thinking-body"><Markdown text={m.thinking} /></div>
                      </details>
                    {/if}
                    <Markdown text={m.text} />{#if m.live}<span class="caret"></span>{/if}
                  </div>
                {:else}
                  <div class="body">{m.text}{#if m.live}<span class="caret"></span>{/if}</div>
                {/if}
              </div>
            </div>
          {/if}
        {:else}
          <div class="empty">
            <span class="ic"><Icon name="MessagesSquare" size={20} /></span>
            {#if app.offlineModelOnly}
              <h3>Echo runtime</h3>
              <p>This daemon has no chat provider configured — turns echo back. Configure a provider in the daemon's verlet.toml.</p>
            {:else}
              <h3>Start the conversation</h3>
              <p>Send a prompt to open a turn on this thread.</p>
            {/if}
          </div>
        {/each}
      {/if}
    </div>

    <form class="composer" onsubmit={submit}>
      <div class="composer-box">
        <textarea
          rows="2"
          bind:value={draft}
          onkeydown={onKey}
          placeholder={app.connected
            ? loadingHistory
              ? "Loading thread history…"
              : "Message the agent…  (Enter to send, Shift+Enter for newline)"
            : "Offline — start a daemon to send messages"}
          disabled={!app.connected || loadingHistory}
        ></textarea>
        <div class="composer-actions">
          <label class="composer-thinking" title="Thinking level for this thread's turns">
            <Icon name="Brain" size={13} />
            <select value={selectedThinking} onchange={setThinking} disabled={!app.connected}>
              {#each THINKING_LEVELS as level (level.id)}
                <option value={level.id}>{level.label}</option>
              {/each}
            </select>
          </label>
          <span class="spacer"></span>
          <button class="btn primary" type="submit" disabled={!app.connected || loadingHistory || !draft.trim() || tab.busy}>
            <Icon name="Send" size={13} /> Send
          </button>
        </div>
      </div>
    </form>
  </div>
</div>
