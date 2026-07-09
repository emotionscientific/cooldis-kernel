import createDOMPurify from "dompurify";
import { Marked, type RendererObject, type Token, type Tokens } from "marked";
import { highlightCode } from "./highlight";
import type { RenderedBlock, StreamingMarkdownOptions } from "./types";

type Sanitizer = (html: string) => string;
type Purifier = { sanitize: (html: string) => string };
type FenceEntry =
  | { state: "pending"; promise: Promise<string | null> }
  | { state: "done"; html: string | null };
type StableBlock = {
  key: string;
  hash: string;
  source: string;
  html: string;
  fenceKeys: Set<string>;
};
type RenderedSource = {
  html: string;
  fenceKeys: Set<string>;
};

const lexer = new Marked({ gfm: true, async: false });
const TAIL_KEY = "md-tail";

let purifier: Purifier | null = null;
let sanitizerForTests: Sanitizer | null = null;

/**
 * Incremental GFM renderer for streaming chat output.
 *
 * Algorithm (pinned — see ticket 0026):
 * - `marked.lexer(fullText)` splits the accumulated text into top-level blocks.
 *   Every block except the last is STABLE: parse (marked) + sanitize (DOMPurify)
 *   once, cache the html keyed by block index, never recompute. Only the open
 *   tail block re-parses on each delta, so per-delta cost is O(tail), not O(n).
 * - Tail-block patching: before parsing the tail, close unterminated constructs
 *   so raw markers never flash mid-stream — an odd number of ``` fences gets a
 *   closing fence appended; unbalanced `**`, `*`, and `` ` `` runs get closed.
 *   Stable blocks are never patched (they were complete by construction).
 * - Fenced code: rendered as
 *   `<div class="md-fence" data-md-lang="<lang>"><button class="md-copy" type="button">Copy</button><pre><code>…</code></pre></div>`
 *   with escaped plain code. Any CLOSED fence (in a stable block or a closed
 *   tail fence) is handed to `highlightCode` keyed by content hash; when the
 *   promise resolves, the cached block html is rebuilt with the shiki `<pre>`
 *   swapped in and `onAsyncUpdate` fires. Unknown languages / failures resolve
 *   to null and the plain block stays (cache the null — no retry storms).
 * - Sanitization is the LAST step for every html string this module emits,
 *   including highlight upgrades. Raw HTML in the source markdown is sanitized
 *   away, not rendered.
 *
 * `update()` expects monotonically growing text (streaming appends). If the new
 * text is not an extension of the previous call's text, the internal cache
 * resets transparently and everything re-renders once.
 */
export class StreamingMarkdown {
  private previousText = "";
  private generation = 0;
  private stableBlocks = new Map<number, StableBlock>();
  private fences = new Map<string, FenceEntry>();

  constructor(private readonly opts: StreamingMarkdownOptions = {}) {}

  /** Render `fullText`, reusing cached stable blocks. Synchronous. */
  update(fullText: string): RenderedBlock[] {
    if (!fullText.startsWith(this.previousText)) {
      this.reset();
    }
    this.previousText = fullText;

    const tokens = lexer.lexer(fullText);
    const blocks: RenderedBlock[] = [];
    let blockIndex = 0;

    for (let tokenIndex = 0; tokenIndex < tokens.length; tokenIndex += 1) {
      const token = tokens[tokenIndex];
      if (token.type === "space") continue;

      const stable = tokenIndex < tokens.length - 1;
      blocks.push(
        stable ? this.renderStableBlock(blockIndex, token.raw) : this.renderTailBlock(token.raw),
      );
      blockIndex += 1;
    }

    return blocks;
  }

  /** Drop all cached state (e.g. when a message is replaced wholesale). */
  reset(): void {
    this.previousText = "";
    this.stableBlocks.clear();
    this.fences.clear();
    this.generation += 1;
  }

  private renderStableBlock(index: number, source: string): RenderedBlock {
    const hash = hashText(source);
    const cached = this.stableBlocks.get(index);
    if (cached?.hash === hash) {
      return { key: cached.key, html: cached.html, stable: true };
    }

    for (const cachedIndex of this.stableBlocks.keys()) {
      if (cachedIndex >= index) this.stableBlocks.delete(cachedIndex);
    }

    const rendered = this.renderSource(source, true);
    const block = {
      key: `md-${index}-${hash}`,
      hash,
      source,
      html: rendered.html,
      fenceKeys: rendered.fenceKeys,
    };
    this.stableBlocks.set(index, block);
    return { key: block.key, html: block.html, stable: true };
  }

  private renderTailBlock(source: string): RenderedBlock {
    const patched = patchTail(source);
    const rendered = this.renderSource(patched, !hasOpenFence(source));
    return { key: TAIL_KEY, html: rendered.html, stable: false };
  }

  private renderSource(source: string, allowHighlight: boolean): RenderedSource {
    const fenceKeys = new Set<string>();
    const renderer: RendererObject<string, string> = {
      code: (token: Tokens.Code) => {
        const lang = cleanLang(token.lang ?? "");
        const key = fenceKey(token.text, lang);
        const highlighted = this.fenceHtml(key);
        fenceKeys.add(key);
        if (allowHighlight) this.startHighlight(token.text, lang, key);
        return renderFence(token.text, lang, highlighted);
      },
      html: () => "",
    };
    const marked = new Marked({ gfm: true, async: false, renderer });
    const html = marked.parser(marked.lexer(source) as Token[]);
    return { html: sanitize(html), fenceKeys };
  }

  private fenceHtml(key: string) {
    const entry = this.fences.get(key);
    return entry?.state === "done" ? entry.html : undefined;
  }

  private startHighlight(code: string, lang: string, key: string) {
    if (this.fences.has(key)) return;

    const generation = this.generation;
    const promise = highlightCode(code, lang);
    this.fences.set(key, { state: "pending", promise });
    promise.then((html) => {
      if (this.generation !== generation) return;

      this.fences.set(key, { state: "done", html });
      if (html === null) return;

      for (const block of this.stableBlocks.values()) {
        if (!block.fenceKeys.has(key)) continue;
        const rendered = this.renderSource(block.source, true);
        block.html = rendered.html;
        block.fenceKeys = rendered.fenceKeys;
      }
      this.opts.onAsyncUpdate?.();
    });
  }
}

export function __setSanitizerForTests(sanitizer: Sanitizer | null) {
  sanitizerForTests = sanitizer;
}

function sanitize(html: string) {
  if (sanitizerForTests) return sanitizerForTests(html);

  const window = globalThis.window;
  if (!window) return html;

  purifier ??= (createDOMPurify as unknown as (window: Window) => Purifier)(window);
  return purifier.sanitize(html);
}

function renderFence(code: string, lang: string, highlighted: string | null | undefined) {
  const pre = highlighted ?? `<pre><code>${escapeHtml(code)}</code></pre>`;
  return `<div class="md-fence" data-md-lang="${escapeAttribute(lang)}"><button class="md-copy" type="button">Copy</button>${pre}</div>\n`;
}

function patchTail(source: string) {
  if (hasOpenFence(source)) {
    return `${source}${source.endsWith("\n") ? "" : "\n"}\`\`\``;
  }

  const closers: string[] = [];
  if (countUnescaped(source, "`") % 2 === 1) closers.push("`");
  if (countUnescaped(source, "**") % 2 === 1) closers.push("**");
  if (countSingleStars(source) % 2 === 1) closers.push("*");
  return `${source}${closers.join("")}`;
}

function hasOpenFence(source: string) {
  return (source.match(/^ {0,3}```/gm)?.length ?? 0) % 2 === 1;
}

function countUnescaped(source: string, marker: string) {
  let count = 0;
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] === "\\") {
      index += 1;
      continue;
    }
    if (source.startsWith(marker, index)) {
      count += 1;
      index += marker.length - 1;
    }
  }
  return count;
}

function countSingleStars(source: string) {
  let count = 0;
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] === "\\") {
      index += 1;
      continue;
    }
    if (source[index] !== "*") continue;
    if (source[index - 1] === "*" || source[index + 1] === "*") continue;
    if ((index === 0 || source[index - 1] === "\n") && source[index + 1] === " ") continue;
    count += 1;
  }
  return count;
}

function cleanLang(lang: string) {
  return lang.trim().toLowerCase().replace(/^language-/, "").split(/\s+/)[0] ?? "";
}

function fenceKey(code: string, lang: string) {
  return `${lang}:${hashText(code)}:${code.length}`;
}

function hashText(text: string) {
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escapeAttribute(value: string) {
  return escapeHtml(value).replace(/'/g, "&#39;");
}
