import { expect, test } from "bun:test";
import { highlightCode } from "./highlight";

test("highlightCode caches repeated content and returns null for unknown languages", async () => {
  const code = "const answer: number = 42;";
  const first = highlightCode(code, "ts");
  const second = highlightCode(code, "ts");

  expect(second).toBe(first);
  const html = await first;
  expect(html).toContain("<pre");
  expect(html).toContain("shiki");

  const unknown = highlightCode("wat", "definitely-not-a-language");
  expect(highlightCode("wat", "definitely-not-a-language")).toBe(unknown);
  expect(await unknown).toBeNull();
});
