<script lang="ts">
  import { StreamingMarkdown } from "../lib/markdown/streaming";

  let { text }: { text: string } = $props();

  // Per-message engine: stable blocks render once, only the open tail re-parses
  // as deltas append. `version` ticks when an async code highlight resolves so
  // the derived re-pulls the upgraded html for the same text.
  let version = $state(0);
  const md = new StreamingMarkdown({ onAsyncUpdate: () => (version += 1) });
  const blocks = $derived.by(() => {
    void version;
    return md.update(text);
  });

  // One delegated handler for the copy buttons the engine renders into fences.
  function onClick(e: MouseEvent) {
    const btn = e.target instanceof Element ? e.target.closest(".md-copy") : null;
    if (!(btn instanceof HTMLElement)) return;
    const code = btn.parentElement?.querySelector("pre")?.textContent ?? "";
    const resetText = btn.textContent || "Copy";
    void (async () => {
      try {
        if (!navigator.clipboard) throw new Error("Clipboard API unavailable");
        await navigator.clipboard.writeText(code);
        btn.textContent = "Copied";
      } catch {
        btn.textContent = "Copy failed";
      } finally {
        setTimeout(() => {
          if (btn.isConnected) btn.textContent = resetText;
        }, 1200);
      }
    })();
  }
</script>

<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
<div class="md" onclick={onClick}>
  {#each blocks as b (b.key)}
    <!-- html is sanitized by the engine (DOMPurify) before it ever reaches here -->
    {@html b.html}
  {/each}
</div>
