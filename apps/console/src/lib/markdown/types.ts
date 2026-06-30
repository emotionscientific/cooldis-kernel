/** Shared types for the streaming markdown engine (ticket 0026). */

/**
 * One rendered top-level markdown block. `html` is ALWAYS DOMPurify-sanitized —
 * safe to hand to `{@html}` directly; consumers must never concatenate raw model
 * text into it.
 */
export interface RenderedBlock {
  /**
   * Identity for keyed `{#each}`. Stable blocks keep the same key for the
   * lifetime of the message (index-based); the open tail block reuses a single
   * sentinel key so Svelte patches it in place instead of remounting.
   */
  key: string;
  /** Sanitized HTML for this block. */
  html: string;
  /**
   * True once the block can never change again as the message grows. Stable
   * blocks are parsed + sanitized exactly once and served from cache (modulo
   * async highlight upgrades, which replace the cached html and fire
   * `onAsyncUpdate`).
   */
  stable: boolean;
}

export interface StreamingMarkdownOptions {
  /**
   * Fired when a previously returned block's html has changed out of band
   * (a code-fence highlight resolved). The consumer should call `update()`
   * again with the same text to pick up the new html.
   */
  onAsyncUpdate?: () => void;
}
