import { afterEach, expect, test } from "bun:test";
import { highlightCode } from "./highlight";
import { __setSanitizerForTests, StreamingMarkdown } from "./streaming";

afterEach(() => {
  __setSanitizerForTests(null);
});

test("reuses stable block html and keys across appends", () => {
  let sanitizeCount = 0;
  __setSanitizerForTests((html) => `${html}<!-- sanitize:${(sanitizeCount += 1)} -->`);

  const renderer = new StreamingMarkdown();
  const first = renderer.update("alpha\n\nbravo");
  const alphaHtml = first[0].html;
  const alphaKey = first[0].key;
  const tailKey = first[1].key;

  const second = renderer.update("alpha\n\nbravo grows");
  expect(second[0].stable).toBe(true);
  expect(second[0].key).toBe(alphaKey);
  expect(second[0].html).toBe(alphaHtml);
  expect(second[1].key).toBe(tailKey);
  expect(second[1].html).not.toBe(first[1].html);

  const third = renderer.update("alpha\n\nbravo grows\n\ncharlie");
  const bravoHtml = third[1].html;
  const bravoKey = third[1].key;
  const fourth = renderer.update("alpha\n\nbravo grows\n\ncharlie extends");
  expect(fourth[0].html).toBe(alphaHtml);
  expect(fourth[1].html).toBe(bravoHtml);
  expect(fourth[1].key).toBe(bravoKey);
});

test("resets cached stable blocks when text is not an extension", () => {
  let sanitizeCount = 0;
  __setSanitizerForTests((html) => `${html}<!-- sanitize:${(sanitizeCount += 1)} -->`);

  const renderer = new StreamingMarkdown();
  const first = renderer.update("same\n\nold tail");
  const firstStableHtml = first[0].html;

  const replaced = renderer.update("same\n\nnew tail");
  expect(replaced[0].key).toBe(first[0].key);
  expect(replaced[0].html).not.toBe(firstStableHtml);
});

test("patches unterminated emphasis and fences on the tail block", () => {
  __setSanitizerForTests((html) => html);
  const renderer = new StreamingMarkdown();

  const strong = renderer.update("before **bold")[0].html;
  expect(strong).toContain("<strong>bold</strong>");
  expect(strong).not.toContain("**");

  renderer.reset();
  const em = renderer.update("before *em")[0].html;
  expect(em).toContain("<em>em</em>");
  expect(em).not.toContain("*em");

  renderer.reset();
  const code = renderer.update("before `code")[0].html;
  expect(code).toContain("<code>code</code>");
  expect(code).not.toContain("`code");

  renderer.reset();
  const fence = renderer.update("```ts\nconst x = 1;")[0];
  expect(fence.stable).toBe(false);
  expect(fence.html).toContain('class="md-fence"');
  expect(fence.html).toContain("const x = 1;");
  expect(fence.html).not.toContain("```");
});

test("sanitizes script tags, event handlers, and javascript urls", () => {
  __setSanitizerForTests(stripUnsafe);
  const renderer = new StreamingMarkdown();
  const html = renderer
    .update(
      [
        "<script>alert(1)</script>",
        '<img src="x" onerror="alert(1)">',
        "[bad](javascript:alert(1))",
      ].join("\n\n"),
    )
    .map((block) => block.html)
    .join("");

  expect(html).not.toContain("<script");
  expect(html).not.toContain("onerror");
  expect(html).not.toContain("javascript:");
});

test("renders GFM tables and task lists", () => {
  __setSanitizerForTests((html) => html);
  const renderer = new StreamingMarkdown();
  const html = renderer
    .update(
      [
        "| A | B |",
        "| - | - |",
        "| 1 | 2 |",
        "",
        "- [x] done",
        "- [ ] todo",
      ].join("\n"),
    )
    .map((block) => block.html)
    .join("");

  expect(html).toContain("<table>");
  expect(html).toContain("<th>A</th>");
  expect(html).toContain("<td>1</td>");
  expect(html).toContain('type="checkbox"');
  expect(html).toContain('checked=""');
});

test("upgrades completed fences with cached shiki html", async () => {
  __setSanitizerForTests((html) => html);
  let updates = 0;
  const text = "```ts\nconst answer: number = 42;\n```";
  const renderer = new StreamingMarkdown({ onAsyncUpdate: () => (updates += 1) });

  const before = renderer.update(text)[0].html;
  expect(before).toContain("<pre><code>");

  await highlightCode("const answer: number = 42;", "ts");
  await Promise.resolve();
  await Bun.sleep(0);

  const after = renderer.update(text)[0].html;
  expect(updates).toBeGreaterThan(0);
  expect(after).toContain("shiki");
});

function stripUnsafe(html: string) {
  return html
    .replace(/<script\b[\s\S]*?<\/script>/gi, "")
    .replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]*)/gi, "")
    .replace(/\s+(href|src)\s*=\s*"javascript:[^"]*"/gi, "")
    .replace(/\s+(href|src)\s*=\s*'javascript:[^']*'/gi, "")
    .replace(/\s+(href|src)\s*=\s*javascript:[^\s>]*/gi, "");
}
