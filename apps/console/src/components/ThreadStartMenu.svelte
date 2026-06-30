<script lang="ts">
  import { tick } from "svelte";
  import { app } from "../lib/app.svelte";
  import Icon from "./Icon.svelte";

  let {
    buttonClass = "icon-btn",
    iconSize = 15,
    label = "",
    title = "New thread",
  }: {
    buttonClass?: string;
    iconSize?: number;
    label?: string;
    title?: string;
  } = $props();

  let open = $state(false);
  let firstItem = $state<HTMLButtonElement>();
  let startCwd = $state("");

  function startFromAgent(ref: string) {
    open = false;
    void app.newThread(ref || undefined, { cwd: startCwd || undefined });
  }

  async function toggle() {
    open = !open;
    if (!open) return;
    app.runtimeOpen = false;
    await tick();
    firstItem?.focus();
  }

  function onKeydown(event: KeyboardEvent) {
    if (!open || event.key !== "Escape") return;
    event.preventDefault();
    open = false;
  }

  $effect(() => {
    if (open && !app.connected) open = false;
  });
</script>

<svelte:window onkeydown={onKeydown} />

<div class="thread-start-menu">
  <button
    class={buttonClass}
    {title}
    aria-haspopup="menu"
    aria-expanded={open}
    aria-controls="agent-start-menu"
    disabled={!app.connected}
    onclick={() => void toggle()}
  >
    <Icon name="Plus" size={iconSize} />
    {#if label}<span>{label}</span>{/if}
  </button>
  {#if open}
    <button class="agent-menu-scrim" tabindex="-1" aria-label="Close agent menu" onclick={() => (open = false)}></button>
    <div id="agent-start-menu" class="agent-menu" role="menu" aria-label="Start thread from agent">
      <label class="agent-menu-cwd">
        <span>Working directory</span>
        <input class="mono" placeholder={app.runtimeCwd ?? "daemon workspace"} bind:value={startCwd} spellcheck="false" />
      </label>
      <button class="agent-menu-item" role="menuitem" bind:this={firstItem} onclick={() => startFromAgent("")}>
        <span class="agent-menu-name">Default</span>
        <span class="agent-menu-meta">daemon default manifest</span>
      </button>
      {#each app.manifests as m (m.id)}
        <button class="agent-menu-item" role="menuitem" onclick={() => startFromAgent(m.id)}>
          <span class="agent-menu-name">{m.name}</span>
          <span class="agent-menu-meta">{m.version} · {m.model}</span>
        </button>
      {/each}
    </div>
  {/if}
</div>
