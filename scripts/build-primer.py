#!/usr/bin/env python3
"""Build the in-repo primer source into its committed HTML and PDF.

Source lives under docs/primer/src, and output lives under docs/primer.
Markers in the Markdown:
  [figure:ID]            inline figures/ID.svg with its caption from
                         captions.md (one `## ID` block each)
  ::: {.agent-prompt}    a copyable prompt for the reader's coding agent
  ::: {.how-to-read}     the one-time disclaimer box
  ::: {.lineage}         optional CS-lineage side panel
  [audit: ...]           renders as an amber badge (strip before external send)

PDF via headless Chrome. `--check` rebuilds the HTML in memory and compares
it with the committed file without writing.
"""
from __future__ import annotations
import argparse, re, subprocess, sys, shutil, html
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = ROOT / "docs" / "primer" / "src"
SRC = SRC_ROOT / "agents-in-version-control.md"
CAPTIONS = SRC_ROOT / "captions.md"
FIGS = SRC_ROOT / "figures"
OUT_HTML = ROOT / "docs" / "primer" / "agents-in-version-control.html"
OUT_PDF = ROOT / "docs" / "primer" / "agents-in-version-control.pdf"


def pandoc(md: str, *extra: str) -> str:
    return subprocess.run(
        ["pandoc", "-f", "markdown+smart+fenced_divs", "-t", "html", *extra],
        input=md, capture_output=True, text=True, encoding="utf-8", check=True).stdout


def captions() -> dict[str, str]:
    out, cur, buf = {}, None, []
    for line in CAPTIONS.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            if cur: out[cur] = pandoc("\n".join(buf)).strip()
            cur, buf = line[3:].strip(), []
        else:
            buf.append(line)
    if cur: out[cur] = pandoc("\n".join(buf)).strip()
    return out


def inject_figures(body: str, caps: dict[str, str]) -> str:
    n = [0]
    def rep(m):
        fid = m.group(1)
        svg = (FIGS / f"{fid}.svg").read_text(encoding="utf-8").strip()
        n[0] += 1
        cap = caps.get(fid, "")
        return (f'<figure class="primer-fig" id="fig-{fid}">{svg}'
                f'<figcaption><span class="fignum">Figure {n[0]}.</span> {cap}</figcaption></figure>')
    return re.sub(r'<p>\[figure:([a-z0-9-]+)\]</p>', rep, body)


def mark_audits(body: str) -> str:
    return re.sub(r'\[audit:?\s*([^\]]*)\]',
                  lambda m: f'<span class="audit">audit{": " + html.escape(m.group(1).strip()) if m.group(1).strip() else ""}</span>',
                  body)


def wrap_prompts(body: str) -> str:
    # pandoc emits <div class="agent-prompt"><p>..</p></div>; add header + copy button
    def rep(m):
        inner = m.group(1)
        text = html.unescape(re.sub(r'<[^>]+>', '', inner)).strip()
        return ('<aside class="agent-prompt">'
                '<div class="prompt-head"><span>Ask your agent</span>'
                '<button type="button" class="copy" data-copy="' + html.escape(text, quote=True) + '">Copy prompt</button></div>'
                + inner + '</aside>')
    return re.sub(r'<div class="agent-prompt">(.*?)</div>', rep, body, flags=re.S)


def wrap_lineage(body: str) -> str:
    # pandoc emits <div class="lineage">..</div>; make it a labeled aside
    return re.sub(r'<div class="lineage">(.*?)</div>',
                  lambda m: '<aside class="lineage"><div class="lineage-head">Lineage <span>optional reading</span></div>' + m.group(1) + '</aside>',
                  body, flags=re.S)


CSS = (SRC_ROOT / "primer.css").read_text(encoding="utf-8")

JS = """
(() => {
  const links = [...document.querySelectorAll('nav.side a')];
  const byId = new Map(links.map(a => [a.getAttribute('href').slice(1), a]));
  const heads = [...document.querySelectorAll('main h1[id], main h2[id]')].filter(h => byId.has(h.id));
  let current = null;
  const mark = () => {
    const y = window.scrollY + 120;
    let best = heads[0];
    for (const h of heads) { if (h.offsetTop <= y) best = h; else break; }
    if (best && best !== current) {
      current = best;
      links.forEach(a => a.classList.remove('active'));
      const a = byId.get(best.id); a.classList.add('active');
      a.scrollIntoView({ block: 'nearest' });
    }
  };
  window.addEventListener('scroll', mark, { passive: true }); mark();
})();
document.querySelectorAll('button.copy').forEach(b => b.addEventListener('click', async () => {
  try { await navigator.clipboard.writeText(b.dataset.copy); b.textContent = 'Copied'; setTimeout(() => b.textContent = 'Copy prompt', 1500); }
  catch (e) { b.textContent = 'Select and copy'; }
}));
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    flags = parser.add_mutually_exclusive_group()
    flags.add_argument("--pdf", action="store_true", help="also render the PDF with headless Chrome")
    flags.add_argument("--check", action="store_true", help="check the committed HTML without writing")
    args = parser.parse_args()
    if not shutil.which("pandoc"):
        print("pandoc 3.10.1 is required; install it with `brew install pandoc`", file=sys.stderr)
        return 2

    md = SRC.read_text(encoding="utf-8")
    m = re.match(r'^---\n(.*?)\n---\n', md, re.S)
    meta = dict(re.findall(r'^(\w+):\s*"?(.*?)"?\s*$', m.group(1), re.M)) if m else {}
    body_md = md[m.end():] if m else md
    body_md = re.sub(r'<!--.*?-->', '', body_md, flags=re.S)
    body = pandoc(body_md)
    body = inject_figures(body, captions())
    body = wrap_prompts(body)
    body = wrap_lineage(body)
    body = mark_audits(body)
    toc = [(i, " ".join(t.split())) for i, t in re.findall(r'<h1 id="([^"]+)">(.*?)</h1>', body, re.S)]
    toc_html = "".join(f'<li><a href="#{i}">{t}</a></li>' for i, t in toc)
    heads = re.findall(r'<h([12]) id="([^"]+)">(.*?)</h\1>', body, re.S)
    side_html = "".join(
        f'<li class="lv{lv}"><a href="#{i}">{html.escape(re.sub(r"<[^>]+>", "", " ".join(t.split())))}</a></li>'
        for lv, i, t in heads if not i.startswith("appendix") or lv == "1")
    page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(meta.get('title','Primer'))}</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Fredoka:wght@500;600&family=Nunito:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap">
<style>{CSS}</style></head>
<body><div class="layout">
<nav class="side" aria-label="Contents"><div class="side-title">{html.escape(meta.get('title',''))}</div><ol>{side_html}</ol></nav>
<main>
<header class="title">
<h1>{html.escape(meta.get('title',''))}</h1>
<p class="subtitle">{html.escape(meta.get('subtitle',''))}</p>
<p class="byline">{html.escape(meta.get('author',''))} · {html.escape(meta.get('date',''))}</p>
<nav class="toc"><ol>{toc_html}</ol></nav>
</header>
{body}
</main></div><script>{JS}</script></body></html>"""
    page_bytes = page.encode("utf-8")
    if args.check:
        if OUT_HTML.exists() and OUT_HTML.read_bytes() == page_bytes:
            print("primer HTML is current")
            return 0
        version = subprocess.run(
            ["pandoc", "--version"], capture_output=True, text=True,
            encoding="utf-8", check=True).stdout.splitlines()[0]
        print("primer HTML is stale; run `just primer` to rebuild it", file=sys.stderr)
        print(version, file=sys.stderr)
        return 1

    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_bytes(page_bytes)
    print("wrote", OUT_HTML.relative_to(ROOT))
    if args.pdf:
        chrome_paths = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            shutil.which("google-chrome"), shutil.which("chromium")]
        chrome = next((c for c in chrome_paths if c and Path(c).exists()), None)
        if not chrome:
            searched = [chrome_paths[0], "google-chrome on PATH", "chromium on PATH"]
            print("no Chrome found; searched: " + ", ".join(searched), file=sys.stderr)
            return 1
        OUT_PDF.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                        "--virtual-time-budget=4000",
                        f"--print-to-pdf={OUT_PDF}", OUT_HTML.as_uri()],
                       check=True, capture_output=True)
        print("wrote", OUT_PDF.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
