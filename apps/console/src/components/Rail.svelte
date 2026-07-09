<script lang="ts">
  import { app } from "../lib/app.svelte";
  import { MODES } from "../lib/schema";
  import Icon from "./Icon.svelte";

  function counts(id: string): number | undefined {
    if (id === "chat") return app.threads.length || undefined;
    return undefined;
  }
</script>

<nav class="rail" aria-label="Primary navigation">
  <div class="rail-group">
    {#each MODES as m}
      {@const n = counts(m.id)}
      <button
        class="rail-item"
        class:active={app.mode === m.id && !app.settingsOpen}
        onclick={() => app.setMode(m.id)}
        aria-label={m.label}
      >
        <Icon name={m.icon} size={17} />
        {#if n}<span class="rail-badge">{n}</span>{/if}
        <span class="rail-tip">
          {m.label}
          {#if m.key}<kbd>⌘{m.key}</kbd>{/if}
        </span>
      </button>
    {/each}
  </div>
  <div class="rail-group rail-bottom">
    <button
      class="rail-item"
      class:active={app.settingsOpen}
      onclick={() => app.toggleSettings()}
      aria-label="Settings"
    >
      <Icon name="Settings" size={17} />
      <span class="rail-tip">Settings <kbd>⌘,</kbd></span>
    </button>
  </div>
</nav>
