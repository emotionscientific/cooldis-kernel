//! `glob` — find files by glob pattern (Pi calls this `find`).
//!
//! Pi's version shells out to `fd`; ours walks through [`ToolFs`] with
//! `globset` matching so every backend (native, vfs, wasm) traverses
//! identically. Gitignore awareness comes from parsing `.gitignore`
//! files with the `ignore` crate's buffer-based gitignore module during
//! the walk — not from the `ignore` crate's walker, which is welded to
//! `std::fs`.
//!
//! Pinned semantics:
//! - Pattern syntax: `globset` defaults with `**` support, matched
//!   against paths relative to the search root, `/`-separated.
//! - Respects `.gitignore` (per directory, nested, plus root
//!   `.git/info/exclude`); hidden files are included; `.git/` is always
//!   skipped.
//! - Results are root-relative `/`-separated paths, sorted
//!   lexicographically (Pi sorts by mtime for interactive use; we pin
//!   deterministic order instead — recorded deviation).
//! - `limit` (default 1000) caps results; the result carries a
//!   `limit_reached` flag so the model knows the listing is partial.

pub const DEFAULT_LIMIT: u64 = 1000;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GlobArgs {
    /// Glob pattern, e.g. `*.ts`, `**/*.json`, `src/**/*.spec.ts`.
    pub pattern: String,
    /// Directory to search in (default: workspace root).
    pub path: Option<std::path::PathBuf>,
    /// Maximum number of results (default 1000).
    pub limit: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct GlobOutput {
    /// Root-relative `/`-separated paths, sorted.
    pub paths: Vec<String>,
    pub limit_reached: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "glob",
        description: "Search for files by glob pattern, e.g. '*.ts' or \
                      '**/*.spec.ts'. Returns matching file paths relative to \
                      the search directory, sorted. Respects .gitignore.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern to match files, e.g. '*.ts', '**/*.json', or 'src/**/*.spec.ts'"},
                "path": {"type": "string", "description": "Directory to search in (default: current directory)"},
                "limit": {"type": "number", "description": "Maximum number of results (default: 1000)"}
            },
            "required": ["pattern"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: GlobArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<GlobOutput, verlet_tool_core::ToolError> {
    let limit = args.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 {
        return Err(verlet_tool_core::ToolError::InvalidArgs(
            "limit must be at least 1".to_owned(),
        ));
    }
    let matcher = globset::Glob::new(&args.pattern)
        .map_err(|error| {
            verlet_tool_core::ToolError::InvalidArgs(format!(
                "invalid glob pattern {:?}: {error}",
                args.pattern
            ))
        })?
        .compile_matcher();
    let root = args.path.unwrap_or_else(|| std::path::PathBuf::from("."));
    let files = verlet_tool_core::walk_files(&root, fs)?;
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut paths = Vec::new();
    let mut limit_reached = false;

    for file in files {
        if !matcher.is_match(std::path::Path::new(&file.relative_path)) {
            continue;
        }
        if paths.len() == limit {
            limit_reached = true;
            break;
        }
        paths.push(file.relative_path);
    }

    let rendered_bytes = paths
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(paths.len().saturating_sub(1));
    if rendered_bytes > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    Ok(GlobOutput {
        paths,
        limit_reached,
    })
}

#[cfg(test)]
mod tests {
    fn args(pattern: &str, limit: Option<u64>) -> crate::GlobArgs {
        crate::GlobArgs {
            pattern: pattern.to_owned(),
            path: None,
            limit,
        }
    }

    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    #[test]
    fn double_star_matches_hidden_files_with_relative_sorted_paths() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();
        std::fs::create_dir_all(root.path().join(".git")).unwrap();
        std::fs::write(root.path().join("src/b.json"), "b").unwrap();
        std::fs::write(root.path().join("src/a.json"), "a").unwrap();
        std::fs::write(root.path().join(".hidden.json"), "hidden").unwrap();
        std::fs::write(root.path().join(".git/secret.json"), "secret").unwrap();

        let output = crate::run(args("**/*.json", None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::GlobOutput {
                paths: vec![
                    ".hidden.json".to_owned(),
                    "src/a.json".to_owned(),
                    "src/b.json".to_owned(),
                ],
                limit_reached: false,
            }
        );
    }

    #[test]
    fn nested_gitignore_negation_and_info_exclude_are_respected() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("nested")).unwrap();
        std::fs::create_dir_all(root.path().join("ignored")).unwrap();
        std::fs::create_dir_all(root.path().join(".git/info")).unwrap();
        std::fs::write(
            root.path().join(".gitignore"),
            "ignored/\n*.tmp\n!keep.tmp\nnested/*.log\n",
        )
        .unwrap();
        std::fs::write(root.path().join("nested/.gitignore"), "!important.log\n").unwrap();
        std::fs::write(root.path().join(".git/info/exclude"), "excluded.txt\n").unwrap();
        std::fs::write(root.path().join("ignored/unseen.txt"), "unseen").unwrap();
        std::fs::write(root.path().join("drop.tmp"), "drop").unwrap();
        std::fs::write(root.path().join("keep.tmp"), "keep").unwrap();
        std::fs::write(root.path().join("excluded.txt"), "excluded").unwrap();
        std::fs::write(root.path().join("nested/drop.log"), "drop").unwrap();
        std::fs::write(root.path().join("nested/important.log"), "keep").unwrap();
        std::fs::write(root.path().join("nested/keep.txt"), "keep").unwrap();

        let output = crate::run(args("**", None), &fs(root.path())).unwrap();

        assert_eq!(
            output.paths,
            vec![
                ".gitignore".to_owned(),
                "keep.tmp".to_owned(),
                "nested/.gitignore".to_owned(),
                "nested/important.log".to_owned(),
                "nested/keep.txt".to_owned(),
            ]
        );
    }

    #[test]
    fn ignored_directories_are_pruned_without_being_read() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("ignored")).unwrap();
        std::fs::write(root.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::write(root.path().join("ignored/file.txt"), "hidden").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            read_dirs: std::cell::RefCell::new(Vec::new()),
        };

        crate::run(args("**", None), &recording).unwrap();

        assert!(!recording
            .read_dirs
            .borrow()
            .iter()
            .any(|path| path.ends_with("ignored")));
    }

    #[test]
    fn limit_caps_sorted_results_and_reports_a_partial_listing() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("c.txt"), "c").unwrap();
        std::fs::write(root.path().join("a.txt"), "a").unwrap();
        std::fs::write(root.path().join("b.txt"), "b").unwrap();

        let output = crate::run(args("**", Some(2)), &fs(root.path())).unwrap();

        assert_eq!(output.paths, vec!["a.txt".to_owned(), "b.txt".to_owned()]);
        assert!(output.limit_reached);
    }

    #[test]
    fn invalid_patterns_are_argument_errors() {
        let root = tempfile::tempdir().unwrap();

        let error = crate::run(args("[", None), &fs(root.path())).unwrap_err();

        assert!(error
            .to_string()
            .starts_with("invalid arguments: invalid glob pattern"));
    }

    struct RecordingFs {
        inner: verlet_tool_core::StdFs,
        read_dirs: std::cell::RefCell<Vec<std::path::PathBuf>>,
    }

    impl verlet_tool_core::ToolFs for RecordingFs {
        fn read_file(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
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
            self.read_dirs.borrow_mut().push(path.to_path_buf());
            verlet_tool_core::ToolFs::read_dir(&self.inner, path)
        }

        fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::exists(&self.inner, path)
        }
    }
}
