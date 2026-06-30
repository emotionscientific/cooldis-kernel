import type { HighlighterCore, LanguageInput, ThemeInput } from "shiki/types";

/**
 * Lazy shiki highlighter (ticket 0026).
 *
 * - Loads on first use via dynamic import of `shiki/core` +
 *   `shiki/engine/javascript` (JS regex engine — no WASM) with the fixed
 *   language set below and the `github-dark-default` theme; theme background is
 *   overridden in CSS, not here.
 * - `highlightCode` resolves to the shiki `<pre>` html for a completed fence,
 *   or null when the language isn't in HIGHLIGHT_LANGS or highlighting fails
 *   (callers keep their plain escaped block). Results — including nulls — are
 *   cached by (lang, code) so repeated content costs nothing; cap the cache at
 *   ~200 entries, evicting oldest.
 * - Callers sanitize the returned html; this module does not.
 */

export const HIGHLIGHT_LANGS = [
  "typescript",
  "javascript",
  "tsx",
  "json",
  "bash",
  "python",
  "rust",
  "html",
  "css",
  "toml",
  "yaml",
  "markdown",
  "sql",
  "diff",
] as const;

/** Common aliases models emit, normalized onto HIGHLIGHT_LANGS. */
export const LANG_ALIASES: Record<string, (typeof HIGHLIGHT_LANGS)[number]> = {
  ts: "typescript",
  js: "javascript",
  jsx: "tsx",
  sh: "bash",
  shell: "bash",
  zsh: "bash",
  py: "python",
  rs: "rust",
  yml: "yaml",
  md: "markdown",
};

type HighlightLang = (typeof HIGHLIGHT_LANGS)[number];
type Highlighter = HighlighterCore;

const THEME = "github-dark-default";
const MAX_CACHE_ENTRIES = 200;
const HIGHLIGHT_LANG_SET = new Set<string>(HIGHLIGHT_LANGS);
const highlightCache = new Map<string, Promise<string | null>>();

const LANG_LOADERS: Record<HighlightLang, () => Promise<{ default: unknown }>> = {
  typescript: () => import("shiki/langs/typescript.mjs"),
  javascript: () => import("shiki/langs/javascript.mjs"),
  tsx: () => import("shiki/langs/tsx.mjs"),
  json: () => import("shiki/langs/json.mjs"),
  bash: () => import("shiki/langs/bash.mjs"),
  python: () => import("shiki/langs/python.mjs"),
  rust: () => import("shiki/langs/rust.mjs"),
  html: () => import("shiki/langs/html.mjs"),
  css: () => import("shiki/langs/css.mjs"),
  toml: () => import("shiki/langs/toml.mjs"),
  yaml: () => import("shiki/langs/yaml.mjs"),
  markdown: () => import("shiki/langs/markdown.mjs"),
  sql: () => import("shiki/langs/sql.mjs"),
  diff: () => import("shiki/langs/diff.mjs"),
};

let highlighterPromise: Promise<Highlighter> | null = null;

export function highlightCode(code: string, lang: string): Promise<string | null> {
  const normalizedLang = normalizeLang(lang);
  const cacheKey = `${normalizedLang ?? cleanLang(lang)}:${hashText(code)}:${code.length}`;
  const cached = highlightCache.get(cacheKey);
  if (cached) {
    highlightCache.delete(cacheKey);
    highlightCache.set(cacheKey, cached);
    return cached;
  }

  const promise = normalizedLang ? highlightWithShiki(code, normalizedLang) : Promise.resolve(null);
  highlightCache.set(cacheKey, promise);
  trimCache();
  return promise;
}

async function highlightWithShiki(code: string, lang: HighlightLang) {
  try {
    const highlighter = await getHighlighter();
    return highlighter.codeToHtml(code, { lang, theme: THEME });
  } catch {
    return null;
  }
}

function getHighlighter() {
  highlighterPromise ??= createHighlighter();
  return highlighterPromise;
}

async function createHighlighter() {
  const [{ createHighlighterCore }, { createJavaScriptRegexEngine }, themeModule, ...langModules] =
    await Promise.all([
      import("shiki/core"),
      import("shiki/engine/javascript"),
      import("shiki/themes/github-dark-default.mjs"),
      ...HIGHLIGHT_LANGS.map((lang) => LANG_LOADERS[lang]()),
    ]);

  return createHighlighterCore({
    engine: createJavaScriptRegexEngine(),
    langs: langModules.map((module) => module.default as LanguageInput),
    themes: [themeModule.default as ThemeInput],
  });
}

function normalizeLang(lang: string): HighlightLang | null {
  const cleaned = cleanLang(lang);
  const aliased = LANG_ALIASES[cleaned] ?? cleaned;
  return HIGHLIGHT_LANG_SET.has(aliased) ? (aliased as HighlightLang) : null;
}

function cleanLang(lang: string) {
  return lang.trim().toLowerCase().replace(/^language-/, "").split(/\s+/)[0] ?? "";
}

function trimCache() {
  while (highlightCache.size > MAX_CACHE_ENTRIES) {
    const oldest = highlightCache.keys().next().value;
    if (oldest === undefined) return;
    highlightCache.delete(oldest);
  }
}

function hashText(text: string) {
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}
