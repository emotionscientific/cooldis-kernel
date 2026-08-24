//! `grep` — search file contents for a pattern.
//!
//! Pi's version spawns `rg --json` (auto-downloading ripgrep when
//! missing). Ours runs ripgrep's engine in-process — the `grep-regex` and
//! `grep-searcher` crates search over any reader — fed by the same
//! `ToolFs` walk as `tool-glob`. No spawned binaries, no downloads, and
//! the whole reason this exists as a function tool is preserved: the
//! pattern arrives as a JSON string, no shell quoting, and the output is
//! bounded `file:line` rows.
//!
//! Pinned semantics (mirroring Pi's flags):
//! - Regex by default; `literal: true` = fixed-string. `ignore_case`
//!   as named. Same walk rules as `tool-glob` (gitignore, hidden files
//!   included, `.git/` skipped), with optional `glob` file filter.
//! - Output rows: `path:line: text` with root-relative paths; `context`
//!   adds N lines before/after with `path:line- text` separators, blocks
//!   joined by `--` lines (rg's grouped format).
//! - Match lines longer than 500 chars are clipped with `…`.
//! - `limit` (default 100) caps match count; search stops early when
//!   reached and the result carries `limit_reached`.
//! - Binary files (NUL in first 8KB) are skipped.
//! - Deterministic file order (same as glob's sort), so identical state
//!   yields identical output on every backend.

pub const DEFAULT_LIMIT: u64 = 100;
pub const MAX_LINE_CHARS: usize = 500;
const BINARY_SNIFF_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GrepArgs {
    /// Search pattern (regex, or literal string with `literal: true`).
    pub pattern: String,
    /// Directory or file to search (default: workspace root).
    pub path: Option<std::path::PathBuf>,
    /// Filter files by glob pattern, e.g. `*.ts`.
    pub glob: Option<String>,
    #[serde(default)]
    pub ignore_case: bool,
    #[serde(default)]
    pub literal: bool,
    /// Lines of context before and after each match (default 0).
    pub context: Option<u64>,
    /// Maximum number of matches (default 100).
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct GrepOutput {
    /// Rendered match rows (`path:line: text`).
    pub text: String,
    pub match_count: u64,
    pub limit_reached: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "grep",
        description: "Search file contents for a pattern. Returns matching lines \
                      with file paths and line numbers. Regex by default; set \
                      literal for exact strings. Respects .gitignore. Stops at \
                      the match limit and says so.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Search pattern (regex or literal string)"},
                "path": {"type": "string", "description": "Directory or file to search (default: current directory)"},
                "glob": {"type": "string", "description": "Filter files by glob pattern, e.g. '*.ts' or '**/*.spec.ts'"},
                "ignore_case": {"type": "boolean", "description": "Case-insensitive search (default: false)"},
                "literal": {"type": "boolean", "description": "Treat pattern as literal string instead of regex (default: false)"},
                "context": {"type": "number", "description": "Number of lines to show before and after each match (default: 0)"},
                "limit": {"type": "number", "description": "Maximum number of matches to return (default: 100)"}
            },
            "required": ["pattern"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: GrepArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<GrepOutput, verlet_tool_core::ToolError> {
    let limit = args.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 {
        return Err(verlet_tool_core::ToolError::InvalidArgs(
            "limit must be at least 1".to_owned(),
        ));
    }

    let mut matcher_builder = grep_regex::RegexMatcherBuilder::new();
    matcher_builder
        .case_insensitive(args.ignore_case)
        .fixed_strings(args.literal);
    let matcher = matcher_builder.build(&args.pattern).map_err(|error| {
        verlet_tool_core::ToolError::InvalidArgs(format!(
            "invalid search pattern {:?}: {error}",
            args.pattern
        ))
    })?;
    let glob_matcher = args
        .glob
        .as_deref()
        .map(|pattern| {
            globset::Glob::new(pattern)
                .map(|glob| glob.compile_matcher())
                .map_err(|error| {
                    verlet_tool_core::ToolError::InvalidArgs(format!(
                        "invalid glob pattern {pattern:?}: {error}"
                    ))
                })
        })
        .transpose()?;
    let root = args.path.unwrap_or_else(|| std::path::PathBuf::from("."));
    let files = verlet_tool_core::walk_files(&root, fs)?;
    let context = usize::try_from(args.context.unwrap_or(0)).unwrap_or(usize::MAX);
    let mut blocks = Vec::new();
    let mut rows = Vec::new();
    let mut match_count = 0_u64;
    let mut limit_reached = false;

    for file in files {
        if glob_matcher
            .as_ref()
            .is_some_and(|glob| !glob.is_match(std::path::Path::new(&file.relative_path)))
        {
            continue;
        }

        let bytes = fs.read_file(&file.path)?;
        if bytes.iter().take(BINARY_SNIFF_BYTES).any(|byte| *byte == 0) {
            continue;
        }

        let remaining = limit.saturating_sub(match_count);
        let mut searcher_builder = grep_searcher::SearcherBuilder::new();
        searcher_builder
            .line_number(true)
            .bom_sniffing(false)
            .max_matches(Some(remaining));
        let mut searcher = searcher_builder.build();
        let mut sink = MatchLineSink { lines: Vec::new() };
        searcher
            .search_slice(&matcher, &bytes, &mut sink)
            .map_err(|error| {
                verlet_tool_core::ToolError::Failed(format!(
                    "failed to search {}: {error}",
                    file.relative_path
                ))
            })?;

        match_count =
            match_count.saturating_add(u64::try_from(sink.lines.len()).unwrap_or(u64::MAX));
        let lines = file_lines(&bytes);
        if context == 0 {
            for line_number in sink.lines {
                rows.push(render_line(
                    &file.relative_path,
                    line_number,
                    true,
                    line_at(&lines, line_number),
                ));
            }
        } else {
            blocks.extend(render_context_blocks(
                &file.relative_path,
                &lines,
                &sink.lines,
                context,
            ));
        }

        if match_count >= limit {
            limit_reached = true;
            break;
        }
    }

    let text = if context == 0 {
        rows.join("\n")
    } else {
        blocks
            .into_iter()
            .map(|block| block.join("\n"))
            .collect::<Vec<_>>()
            .join("\n--\n")
    };
    if text.len() > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    Ok(GrepOutput {
        text,
        match_count,
        limit_reached,
    })
}

struct MatchLineSink {
    lines: Vec<u64>,
}

impl grep_searcher::Sink for MatchLineSink {
    type Error = std::io::Error;

    fn matched(
        &mut self,
        _searcher: &grep_searcher::Searcher,
        matched: &grep_searcher::SinkMatch<'_>,
    ) -> Result<bool, Self::Error> {
        if let Some(line_number) = matched.line_number() {
            self.lines.push(line_number);
        }
        Ok(true)
    }
}

fn render_context_blocks(
    path: &str,
    lines: &[&[u8]],
    matches: &[u64],
    context: usize,
) -> Vec<Vec<String>> {
    if matches.is_empty() {
        return Vec::new();
    }
    let line_count = u64::try_from(lines.len()).unwrap_or(u64::MAX);
    let context = u64::try_from(context).unwrap_or(u64::MAX);
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for &line_number in matches {
        let start = line_number.saturating_sub(context).max(1);
        let end = line_number.saturating_add(context).min(line_count);
        match ranges.last_mut() {
            Some((_, previous_end)) if start <= previous_end.saturating_add(1) => {
                *previous_end = (*previous_end).max(end);
            }
            _ => ranges.push((start, end)),
        }
    }

    ranges
        .into_iter()
        .map(|(start, end)| {
            (start..=end)
                .map(|line_number| {
                    render_line(
                        path,
                        line_number,
                        matches.binary_search(&line_number).is_ok(),
                        line_at(lines, line_number),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn render_line(path: &str, line_number: u64, is_match: bool, bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let text = text.strip_suffix('\r').unwrap_or(&text);
    let mut clipped = text.chars().take(MAX_LINE_CHARS).collect::<String>();
    if text.chars().count() > MAX_LINE_CHARS {
        clipped.push('…');
    }
    if is_match {
        format!("{path}:{line_number}: {clipped}")
    } else {
        format!("{path}:{line_number}- {clipped}")
    }
}

fn line_at<'a>(lines: &'a [&'a [u8]], line_number: u64) -> &'a [u8] {
    let index = usize::try_from(line_number.saturating_sub(1)).unwrap_or(usize::MAX);
    lines.get(index).copied().unwrap_or_default()
}

fn file_lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    if bytes.ends_with(b"\n") {
        lines.pop();
    }
    lines
}

#[cfg(test)]
mod tests {
    fn args(pattern: &str) -> crate::GrepArgs {
        crate::GrepArgs {
            pattern: pattern.to_owned(),
            path: None,
            glob: None,
            ignore_case: false,
            literal: false,
            context: None,
            limit: None,
        }
    }

    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    #[test]
    fn renders_plain_rows_in_deterministic_file_order() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("b.txt"), "needle b\n").unwrap();
        std::fs::write(root.path().join("a.txt"), "zero\nNeedle a\n").unwrap();
        let mut search = args("needle");
        search.ignore_case = true;

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "a.txt:2: Needle a\nb.txt:1: needle b");
        assert_eq!(output.match_count, 2);
        assert!(!output.limit_reached);
    }

    #[test]
    fn renders_grouped_context_blocks_with_separators() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "before a\nhit one\nafter a\ngap one\ngap two\nbefore b\nhit two\nafter b\n",
        )
        .unwrap();
        let mut search = args("hit");
        search.context = Some(1);

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            "file.txt:1- before a\n\
             file.txt:2: hit one\n\
             file.txt:3- after a\n\
             --\n\
             file.txt:6- before b\n\
             file.txt:7: hit two\n\
             file.txt:8- after b"
        );
    }

    #[test]
    fn clips_long_rendered_lines_at_five_hundred_characters() {
        let root = tempfile::tempdir().unwrap();
        let line = format!("{}needle", "x".repeat(crate::MAX_LINE_CHARS));
        std::fs::write(root.path().join("long.txt"), line).unwrap();

        let output = crate::run(args("needle"), &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            format!("long.txt:1: {}…", "x".repeat(crate::MAX_LINE_CHARS))
        );
    }

    #[test]
    fn literal_mode_matches_regex_metacharacters_exactly() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("literal.txt"), "value a+b [x] done\n").unwrap();
        let mut search = args("a+b [x]");
        search.literal = true;

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "literal.txt:1: value a+b [x] done");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn match_limit_stops_before_reading_the_next_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.txt"), "hit\n").unwrap();
        std::fs::write(root.path().join("b.txt"), "hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
        };
        let mut search = args("hit");
        search.limit = Some(1);

        let output = crate::run(search, &recording).unwrap();

        assert_eq!(output.text, "a.txt:1: hit");
        assert_eq!(output.match_count, 1);
        assert!(output.limit_reached);
        assert!(!recording
            .reads
            .borrow()
            .iter()
            .any(|path| path.ends_with("b.txt")));
    }

    #[test]
    fn skips_binary_files_with_a_nul_in_the_first_eight_kibibytes() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("binary.dat"), b"hit\0more").unwrap();
        std::fs::write(root.path().join("text.txt"), b"hit\n").unwrap();

        let output = crate::run(args("hit"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "text.txt:1: hit");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn a_nul_after_the_binary_sniff_window_does_not_skip_the_file() {
        let root = tempfile::tempdir().unwrap();
        let mut content = vec![b'x'; 8 * 1024];
        content.extend_from_slice(b"\0hit\n");
        std::fs::write(root.path().join("late-nul.dat"), content).unwrap();

        let output = crate::run(args("hit"), &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            "late-nul.dat:1: ".to_owned() + &"x".repeat(500) + "…"
        );
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn optional_glob_filters_root_relative_file_paths() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/code.rs"), "hit\n").unwrap();
        std::fs::write(root.path().join("src/code.txt"), "hit\n").unwrap();
        let mut search = args("hit");
        search.glob = Some("**/*.rs".to_owned());

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "src/code.rs:1: hit");
    }

    #[test]
    fn a_direct_file_search_uses_the_file_name_as_its_relative_path() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("one.txt"), "hit\n").unwrap();
        let mut search = args("hit");
        search.path = Some(std::path::PathBuf::from("one.txt"));

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "one.txt:1: hit");
    }

    #[test]
    fn invalid_regex_and_glob_patterns_are_argument_errors() {
        let root = tempfile::tempdir().unwrap();
        let regex_error = crate::run(args("["), &fs(root.path())).unwrap_err();
        let mut invalid_glob = args("hit");
        invalid_glob.glob = Some("[".to_owned());
        let glob_error = crate::run(invalid_glob, &fs(root.path())).unwrap_err();

        assert!(regex_error
            .to_string()
            .starts_with("invalid arguments: invalid search pattern"));
        assert!(glob_error
            .to_string()
            .starts_with("invalid arguments: invalid glob pattern"));
    }

    #[test]
    fn glob_and_grep_observe_the_same_shared_walk_file_set() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("nested")).unwrap();
        std::fs::create_dir(root.path().join("ignored")).unwrap();
        std::fs::create_dir(root.path().join(".git")).unwrap();
        std::fs::write(root.path().join(".gitignore"), "# needle\nignored/\n").unwrap();
        std::fs::write(
            root.path().join("nested/.gitignore"),
            "# needle\n*.skip\n!keep.skip\n",
        )
        .unwrap();
        std::fs::write(root.path().join("visible.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join(".hidden"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/visible.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/drop.skip"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/keep.skip"), "needle\n").unwrap();
        std::fs::write(root.path().join("ignored/unseen.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join(".git/unseen.txt"), "needle\n").unwrap();
        let filesystem = fs(root.path());

        let glob = verlet_tool_glob::run(
            verlet_tool_glob::GlobArgs {
                pattern: "**".to_owned(),
                path: None,
                limit: None,
            },
            &filesystem,
        )
        .unwrap();
        let grep = crate::run(args("needle"), &filesystem).unwrap();
        let grep_paths = grep
            .text
            .lines()
            .map(|line| line.split(':').next().unwrap().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(glob.paths, grep_paths);
    }

    struct RecordingFs {
        inner: verlet_tool_core::StdFs,
        reads: std::cell::RefCell<Vec<std::path::PathBuf>>,
    }

    impl verlet_tool_core::ToolFs for RecordingFs {
        fn read_file(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
            self.reads.borrow_mut().push(path.to_path_buf());
            verlet_tool_core::ToolFs::read_file(&self.inner, path)
        }

        fn write_file(
            &self,
            path: &std::path::Path,
            content: &[u8],
        ) -> Result<(), verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::write_file(&self.inner, path, content)
        }

        fn mkdir(
            &self,
            path: &std::path::Path,
            recursive: bool,
        ) -> Result<(), verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::mkdir(&self.inner, path, recursive)
        }

        fn stat(
            &self,
            path: &std::path::Path,
        ) -> Result<verlet_tool_core::FileStat, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::stat(&self.inner, path)
        }

        fn read_dir(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<verlet_tool_core::DirEntry>, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::read_dir(&self.inner, path)
        }

        fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::exists(&self.inner, path)
        }
    }
}
