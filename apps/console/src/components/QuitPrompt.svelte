<script lang="ts">
  import { app } from "../lib/app.svelte";
  import Icon from "./Icon.svelte";

  let remember = $state(false);
</script>

{#if app.quitPromptOpen}
  <div class="setup-scrim" role="presentation"></div>
  <div class="quit-dialog" role="dialog" aria-label="Quit Verlet Console" tabindex="-1">
    <div class="setup-head">
      <div>
        <h2>Quit Verlet?</h2>
        <p>The local daemon was started by this app.</p>
      </div>
      <button class="icon-btn" title="Cancel" aria-label="Cancel" onclick={() => (app.quitPromptOpen = false)}>
        <Icon name="X" size={16} />
      </button>
    </div>

    <label class="check-row">
      <input type="checkbox" bind:checked={remember} />
      <span>Remember this choice</span>
    </label>

    <div class="row">
      <button class="btn danger" onclick={() => void app.finishQuit("stop-managed", remember)}>
        <Icon name="Power" size={14} />
        Shut down daemon
      </button>
      <button class="btn primary" onclick={() => void app.finishQuit("leave-running", remember)}>
        <Icon name="LogOut" size={14} />
        Leave running
      </button>
      <button class="btn" onclick={() => (app.quitPromptOpen = false)}>Cancel</button>
    </div>
  </div>
{/if}
