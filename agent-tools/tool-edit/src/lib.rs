//! `edit` — replace exact text spans in one file.
//!
//! Ported from Pi's edit tool (`core/tools/edit.ts`, `edit-diff.ts`).
//! This is the tool with the tricky logic; the implementation ticket ports
//! Pi's test fixtures alongside it.
//!
//! Pinned semantics:
//! - `edits` apply as one atomic batch: all succeed or the file is
//!   untouched.
//! - Each `old_text` must match exactly once. Match strategy per Pi:
//!   exact match first, then a normalized pass (NFKC, smart quotes to
//!   ASCII, en/em dashes to hyphens, trailing whitespace stripped per
//!   line) that must still be unique.
//! - Zero matches, multiple matches, or overlapping edit spans are
//!   errors naming the offending `old_text` (first 100 chars).
//! - `old_text == new_text` is an error ("no change").
//! - Result returns a unified diff of the applied change; the model uses
//!   it to confirm the edit landed where intended.
//! - Line endings: file's existing endings are preserved; `old_text`
//!   matching normalizes CRLF to LF for comparison.

use unicode_normalization::UnicodeNormalization as _;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct EditArgs {
    /// Path of the file to edit (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: std::path::PathBuf,
    /// Replacements to apply as one atomic batch.
    pub edits: Vec<Edit>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Edit {
    /// Text to find. Must match exactly one location in the file.
    pub old_text: String,
    /// Replacement text.
    pub new_text: String,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct EditOutput {
    /// Unified diff of the applied change.
    pub diff: String,
    pub edits_applied: u32,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "edit",
        description: "Edit a file by replacing exact text. Each old_text must \
                      match exactly one location; all edits in one call apply \
                      atomically. Returns a diff of the change. Read the file \
                      first and copy old_text exactly, including whitespace.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path of the file to edit (relative or absolute)"},
                "edits": {
                    "type": "array",
                    "description": "Replacements to apply atomically",
                    "items": {
                        "type": "object",
                        "properties": {
                            "old_text": {"type": "string", "description": "Text to find (must be unique in the file)"},
                            "new_text": {"type": "string", "description": "Replacement text"}
                        },
                        "required": ["old_text", "new_text"]
                    },
                    "minItems": 1
                }
            },
            "required": ["path", "edits"]
        }),
        effect_class: verlet_tool_core::EffectClass::Idempotent,
    }
}

pub fn run(
    args: EditArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<EditOutput, verlet_tool_core::ToolError> {
    if args.edits.is_empty() {
        return Err(verlet_tool_core::ToolError::InvalidArgs(
            "edits must contain at least one replacement".to_owned(),
        ));
    }

    let edits = args
        .edits
        .iter()
        .enumerate()
        .map(|(index, edit)| {
            if edit.old_text.is_empty() {
                return Err(verlet_tool_core::ToolError::InvalidArgs(format!(
                    "edits[{index}].old_text must not be empty"
                )));
            }
            if edit.old_text == edit.new_text {
                return Err(verlet_tool_core::ToolError::InvalidArgs(format!(
                    "{} equals new_text; the edit would make no change",
                    edit_label(index, &edit.old_text)
                )));
            }
            Ok(NormalizedEdit {
                old_text: normalize_to_lf(&edit.old_text),
                new_text: normalize_to_lf(&edit.new_text),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let bytes = fs.read_file(&args.path)?;
    let (bom, text_bytes) = if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        (&bytes[..3], &bytes[3..])
    } else {
        (&bytes[..0], bytes.as_slice())
    };
    let content = std::str::from_utf8(text_bytes).map_err(|error| {
        verlet_tool_core::ToolError::Failed(format!(
            "file {} is not valid UTF-8: {error}",
            args.path.display()
        ))
    })?;
    let line_ending = detect_line_ending(content);
    let base_content = normalize_to_lf(content);

    let uses_normalized_match = edits
        .iter()
        .any(|edit| find_text(&base_content, &edit.old_text).used_normalized_match);
    let replacement_base = if uses_normalized_match {
        normalize_for_match(&base_content)
    } else {
        base_content.clone()
    };

    let mut matched_edits = Vec::with_capacity(edits.len());
    for (index, edit) in edits.iter().enumerate() {
        let match_result = find_text(&replacement_base, &edit.old_text);
        if !match_result.found {
            return Err(verlet_tool_core::ToolError::Failed(format!(
                "{} was not found in {}",
                edit_label(index, &args.edits[index].old_text),
                args.path.display()
            )));
        }

        let occurrences = count_occurrences(&replacement_base, &edit.old_text);
        if occurrences != 1 {
            return Err(verlet_tool_core::ToolError::Failed(format!(
                "{} matched {occurrences} locations in {}; old_text must match exactly once",
                edit_label(index, &args.edits[index].old_text),
                args.path.display()
            )));
        }

        matched_edits.push(MatchedEdit {
            edit_index: index,
            match_index: match_result.index,
            match_length: match_result.match_length,
            new_text: edit.new_text.clone(),
        });
    }

    matched_edits.sort_by_key(|edit| edit.match_index);
    for pair in matched_edits.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if previous.match_index + previous.match_length > current.match_index {
            return Err(verlet_tool_core::ToolError::Failed(format!(
                "{} overlaps {} in {}",
                edit_label(
                    previous.edit_index,
                    &args.edits[previous.edit_index].old_text
                ),
                edit_label(current.edit_index, &args.edits[current.edit_index].old_text),
                args.path.display()
            )));
        }
    }

    let new_content = if uses_normalized_match {
        apply_replacements_preserving_unchanged_lines(
            &base_content,
            &replacement_base,
            &matched_edits,
        )?
    } else {
        apply_replacements(&replacement_base, &matched_edits, 0)
    };
    if new_content == base_content {
        return Err(verlet_tool_core::ToolError::InvalidArgs(format!(
            "{} produced no change",
            edit_label(0, &args.edits[0].old_text)
        )));
    }

    let path = args.path.display().to_string();
    let text_diff = similar::TextDiff::from_lines(&base_content, &new_content);
    let diff = text_diff
        .unified_diff()
        .context_radius(4)
        .header(&path, &path)
        .to_string();
    if diff.len() > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    let restored_content = restore_line_endings(&new_content, line_ending);
    let mut output_bytes = Vec::with_capacity(bom.len() + restored_content.len());
    output_bytes.extend_from_slice(bom);
    output_bytes.extend_from_slice(restored_content.as_bytes());
    fs.write_file(&args.path, &output_bytes)?;

    Ok(EditOutput {
        diff,
        edits_applied: u32::try_from(args.edits.len()).unwrap_or(u32::MAX),
    })
}

#[derive(Clone, Copy)]
enum LineEnding {
    Lf,
    CrLf,
}

struct NormalizedEdit {
    old_text: String,
    new_text: String,
}

struct MatchResult {
    found: bool,
    index: usize,
    match_length: usize,
    used_normalized_match: bool,
}

#[derive(Clone)]
struct MatchedEdit {
    edit_index: usize,
    match_index: usize,
    match_length: usize,
    new_text: String,
}

#[derive(Clone, Copy)]
struct LineSpan {
    start: usize,
    end: usize,
}

struct ReplacementGroup {
    start_line: usize,
    end_line: usize,
    replacements: Vec<MatchedEdit>,
}

fn edit_label(index: usize, old_text: &str) -> String {
    let preview = old_text.chars().take(100).collect::<String>();
    format!("edits[{index}].old_text {preview:?}")
}

fn detect_line_ending(content: &str) -> LineEnding {
    match (content.find("\r\n"), content.find('\n')) {
        (Some(crlf), Some(lf)) if crlf == lf.saturating_sub(1) => LineEnding::CrLf,
        _ => LineEnding::Lf,
    }
}

fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn restore_line_endings(text: &str, ending: LineEnding) -> String {
    match ending {
        LineEnding::Lf => text.to_owned(),
        LineEnding::CrLf => text.replace('\n', "\r\n"),
    }
}

fn normalize_for_match(text: &str) -> String {
    text.nfkc()
        .map(|character| match character {
            '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
            '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
            '\u{2013}' | '\u{2014}' => '-',
            character => character,
        })
        .collect::<String>()
        .split('\n')
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

fn find_text(content: &str, old_text: &str) -> MatchResult {
    if let Some(index) = content.find(old_text) {
        return MatchResult {
            found: true,
            index,
            match_length: old_text.len(),
            used_normalized_match: false,
        };
    }

    let normalized_content = normalize_for_match(content);
    let normalized_old_text = normalize_for_match(old_text);
    if normalized_old_text.is_empty() {
        return MatchResult {
            found: false,
            index: 0,
            match_length: 0,
            used_normalized_match: false,
        };
    }
    match normalized_content.find(&normalized_old_text) {
        Some(index) => MatchResult {
            found: true,
            index,
            match_length: normalized_old_text.len(),
            used_normalized_match: true,
        },
        None => MatchResult {
            found: false,
            index: 0,
            match_length: 0,
            used_normalized_match: false,
        },
    }
}

fn count_occurrences(content: &str, old_text: &str) -> usize {
    let normalized_content = normalize_for_match(content);
    let normalized_old_text = normalize_for_match(old_text);
    if normalized_old_text.is_empty() {
        return 0;
    }
    normalized_content
        .match_indices(&normalized_old_text)
        .count()
}

fn split_lines_with_endings(content: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, character) in content.char_indices() {
        if character == '\n' {
            lines.push(&content[start..index + 1]);
            start = index + 1;
        }
    }
    if start < content.len() {
        lines.push(&content[start..]);
    }
    lines
}

fn line_spans(content: &str) -> Vec<LineSpan> {
    let mut offset = 0;
    split_lines_with_endings(content)
        .into_iter()
        .map(|line| {
            let span = LineSpan {
                start: offset,
                end: offset + line.len(),
            };
            offset = span.end;
            span
        })
        .collect()
}

fn replacement_line_range(
    lines: &[LineSpan],
    replacement: &MatchedEdit,
) -> Result<(usize, usize), verlet_tool_core::ToolError> {
    let replacement_start = replacement.match_index;
    let replacement_end = replacement.match_index + replacement.match_length;
    let start_line = lines
        .iter()
        .position(|line| replacement_start >= line.start && replacement_start < line.end)
        .ok_or_else(|| {
            verlet_tool_core::ToolError::Failed(
                "replacement range is outside the normalized file content".to_owned(),
            )
        })?;
    let mut end_line = start_line;
    while end_line < lines.len() && lines[end_line].end < replacement_end {
        end_line += 1;
    }
    if end_line >= lines.len() {
        return Err(verlet_tool_core::ToolError::Failed(
            "replacement range is outside the normalized file content".to_owned(),
        ));
    }
    Ok((start_line, end_line + 1))
}

fn apply_replacements(content: &str, replacements: &[MatchedEdit], offset: usize) -> String {
    let mut result = content.to_owned();
    for replacement in replacements.iter().rev() {
        let start = replacement.match_index - offset;
        result.replace_range(
            start..start + replacement.match_length,
            &replacement.new_text,
        );
    }
    result
}

fn apply_replacements_preserving_unchanged_lines(
    original_content: &str,
    normalized_content: &str,
    replacements: &[MatchedEdit],
) -> Result<String, verlet_tool_core::ToolError> {
    let original_lines = split_lines_with_endings(original_content);
    let normalized_lines = line_spans(normalized_content);
    if original_lines.len() != normalized_lines.len() {
        return Err(verlet_tool_core::ToolError::Failed(
            "normalized matching changed the file's line count".to_owned(),
        ));
    }

    let mut groups = Vec::<ReplacementGroup>::new();
    for replacement in replacements {
        let (start_line, end_line) = replacement_line_range(&normalized_lines, replacement)?;
        if let Some(group) = groups.last_mut() {
            if start_line < group.end_line {
                group.end_line = group.end_line.max(end_line);
                group.replacements.push(replacement.clone());
                continue;
            }
        }
        groups.push(ReplacementGroup {
            start_line,
            end_line,
            replacements: vec![replacement.clone()],
        });
    }

    let mut original_line_index = 0;
    let mut result = String::new();
    for group in groups {
        result.push_str(&original_lines[original_line_index..group.start_line].concat());
        let group_start = normalized_lines[group.start_line].start;
        let group_end = normalized_lines[group.end_line - 1].end;
        result.push_str(&apply_replacements(
            &normalized_content[group_start..group_end],
            &group.replacements,
            group_start,
        ));
        original_line_index = group.end_line;
    }
    result.push_str(&original_lines[original_line_index..].concat());
    Ok(result)
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, edits: &[(&str, &str)]) -> crate::EditArgs {
        crate::EditArgs {
            path: std::path::PathBuf::from(path),
            edits: edits
                .iter()
                .map(|(old_text, new_text)| crate::Edit {
                    old_text: (*old_text).to_owned(),
                    new_text: (*new_text).to_owned(),
                })
                .collect(),
        }
    }

    #[test]
    fn replaces_one_exact_unique_match_and_returns_a_unified_diff() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should replace text in file".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "Hello, world!\n").unwrap();

        let output =
            crate::run(args("file.txt", &[("world", "testing")]), &fs(root.path())).unwrap();

        assert_eq!(output.edits_applied, 1);
        assert!(output.diff.starts_with("--- file.txt\n+++ file.txt\n@@"));
        assert!(output.diff.contains("-Hello, world!"));
        assert!(output.diff.contains("+Hello, testing!"));
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"Hello, testing!\n"
        );
    }

    #[test]
    fn replaces_a_normalized_only_smart_quote_match() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match smart single quotes to ASCII quotes".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "console.log(‘hello’);\nkeep trailing   \n",
        )
        .unwrap();

        crate::run(
            args(
                "file.txt",
                &[("console.log('hello');", "console.log('world');")],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "console.log('world');\nkeep trailing   \n"
        );
    }

    #[test]
    fn strips_trailing_whitespace_only_from_lines_touched_by_a_normalized_match() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match text with trailing whitespace stripped".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "line one   \nline two  \nline three   \n",
        )
        .unwrap();

        crate::run(
            args("file.txt", &[("line one\nline two\n", "replacement\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "replacement\nline three   \n"
        );
    }

    #[test]
    fn replaces_compatibility_equivalent_unicode_content() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match compatibility-equivalent Unicode forms".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "ＡＢＣ１２３\ncafe\u{301}\n").unwrap();

        crate::run(
            args("file.txt", &[("ABC123\ncafé\n", "XYZ789\ncoffee\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "XYZ789\ncoffee\n"
        );
    }

    #[test]
    fn replaces_normalized_en_and_em_dashes() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match Unicode dashes to ASCII hyphen".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "range: 1–5\nbreak—here\n").unwrap();

        crate::run(
            args(
                "file.txt",
                &[("range: 1-5\nbreak-here", "range: 10-50\nbreak--here")],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "range: 10-50\nbreak--here\n"
        );
    }

    #[test]
    fn rejects_an_ambiguous_match_and_names_the_old_text() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail if text appears multiple times".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "foo foo foo").unwrap();

        let error = crate::run(args("file.txt", &[("foo", "bar")]), &fs(root.path())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[0].old_text \"foo\" matched 3 locations in file.txt; old_text must match exactly once"
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"foo foo foo"
        );
    }

    #[test]
    fn rejects_ambiguity_after_normalization() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should detect duplicates after fuzzy normalization".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "‘hello’\n’hello‘\n").unwrap();

        let error = crate::run(
            args("file.txt", &[("'hello'", "replacement")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[0].old_text \"'hello'\" matched 2 locations in file.txt; old_text must match exactly once"
        );
    }

    #[test]
    fn rejects_a_zero_match_and_names_the_old_text() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail if text not found".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "Hello, world!").unwrap();

        let error = crate::run(
            args("file.txt", &[("nonexistent", "testing")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[0].old_text \"nonexistent\" was not found in file.txt"
        );
    }

    #[test]
    fn rejects_overlapping_edits_and_names_both_old_texts() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail when multi-edit regions overlap".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo\nthree\n").unwrap();

        let error = crate::run(
            args(
                "file.txt",
                &[
                    ("one\ntwo\n", "ONE\nTWO\n"),
                    ("two\nthree\n", "TWO\nTHREE\n"),
                ],
            ),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[0].old_text \"one\\ntwo\\n\" overlaps edits[1].old_text \"two\\nthree\\n\" in file.txt"
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"one\ntwo\nthree\n"
        );
    }

    #[test]
    fn applies_a_reverse_ordered_multi_edit_batch_against_the_original() {
        // Pi source cases: packages/coding-agent/test/tools.test.ts,
        // "should replace multiple disjoint regions in one call" and
        // "should match edits against the original file, not incrementally".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "foo\nbar\nbaz\n").unwrap();

        let output = crate::run(
            args("file.txt", &[("bar\n", "BAR\n"), ("foo\n", "foo bar\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(output.edits_applied, 2);
        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "foo bar\nBAR\nbaz\n"
        );
    }

    #[test]
    fn applies_exact_and_normalized_matches_in_one_batch() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should support fuzzy matching in multi-edit mode".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "exact target\nconsole.log(‘hello’);\n",
        )
        .unwrap();

        crate::run(
            args(
                "file.txt",
                &[
                    ("console.log('hello');\n", "console.log('world');\n"),
                    ("exact target\n", "EXACT\n"),
                ],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "EXACT\nconsole.log('world');\n"
        );
    }

    #[test]
    fn matches_lf_text_and_preserves_a_crlf_file_and_bom() {
        // Pi source cases: packages/coding-agent/test/tools.test.ts,
        // "should preserve CRLF line endings after edit" and
        // "should preserve UTF-8 BOM after edit".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            b"\xef\xbb\xbffirst\r\nsecond\r\nthird\r\n",
        )
        .unwrap();

        let output = crate::run(
            args("file.txt", &[("second\n", "REPLACED\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert!(!output.diff.contains('\r'));
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"\xef\xbb\xbffirst\r\nREPLACED\r\nthird\r\n"
        );
    }

    #[test]
    fn leaves_the_file_byte_identical_when_the_second_edit_fails() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should not partially apply edits when one edit fails".
        let root = tempfile::tempdir().unwrap();
        let original = b"alpha\r\nbeta\r\ngamma\r\n";
        std::fs::write(root.path().join("file.txt"), original).unwrap();

        let error = crate::run(
            args(
                "file.txt",
                &[("alpha\n", "ALPHA\n"), ("missing\n", "MISSING\n")],
            ),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[1].old_text \"missing\\n\" was not found in file.txt"
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            original
        );
    }

    #[test]
    fn rejects_an_empty_batch_empty_old_text_and_no_op_edit() {
        // Pi source case for the empty batch:
        // packages/coding-agent/test/tools.test.ts,
        // "should fail when edits is empty".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let fs = fs(root.path());

        let empty_batch = crate::run(args("file.txt", &[]), &fs).unwrap_err();
        let empty_old = crate::run(args("file.txt", &[("", "new")]), &fs).unwrap_err();
        let no_op = crate::run(args("file.txt", &[("content", "content")]), &fs).unwrap_err();

        assert_eq!(
            empty_batch.to_string(),
            "invalid arguments: edits must contain at least one replacement"
        );
        assert_eq!(
            empty_old.to_string(),
            "invalid arguments: edits[0].old_text must not be empty"
        );
        assert_eq!(
            no_op.to_string(),
            "invalid arguments: edits[0].old_text \"content\" equals new_text; the edit would make no change"
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"content"
        );
    }

    #[test]
    fn rejects_a_normalized_replacement_that_restores_the_original_text() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "‘hello’\n").unwrap();

        let error = crate::run(
            args("file.txt", &[("'hello'", "‘hello’")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid arguments: edits[0].old_text \"'hello'\" produced no change"
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "‘hello’\n"
        );
    }

    #[test]
    fn error_previews_truncate_old_text_to_100_unicode_characters() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let preview = "é".repeat(100);
        let old_text = format!("{preview}not shown");

        let error = crate::run(
            crate::EditArgs {
                path: std::path::PathBuf::from("file.txt"),
                edits: vec![crate::Edit {
                    old_text,
                    new_text: "replacement".to_owned(),
                }],
            },
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("edits[0].old_text {preview:?} was not found in file.txt")
        );
    }
}
