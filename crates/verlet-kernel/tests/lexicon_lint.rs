//! Enforces the kernel terminology table as a deterministic ratchet.
//!
//! The public orientation is `docs/kernel-invariants.md`. This test keeps
//! settled terminology decisions from quietly drifting in public kernel
//! surfaces.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const BASELINE: &str = include_str!("lexicon_lint_baseline.txt");
const HINT: &str = "see docs/kernel-invariants.md; either rename the kernel surface or add `// lexicon-allow: <word> - <reason>` on the offending line or the line above";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scope {
    Strict,
    ObserverTypeName,
    ProductSurface,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Rule {
    word: &'static str,
    segments: &'static [&'static str],
    scope: Scope,
}

const RULES: &[Rule] = &[
    Rule {
        word: "capsule",
        segments: &["capsule"],
        scope: Scope::Strict,
    },
    Rule {
        word: "observation_store",
        segments: &["observation", "store"],
        scope: Scope::Strict,
    },
    Rule {
        word: "context_store",
        segments: &["context", "store"],
        scope: Scope::Strict,
    },
    Rule {
        word: "observer",
        segments: &["observer"],
        scope: Scope::ObserverTypeName,
    },
    Rule {
        word: "hook",
        segments: &["hook"],
        scope: Scope::ProductSurface,
    },
    Rule {
        word: "memory",
        segments: &["memory"],
        scope: Scope::ProductSurface,
    },
    Rule {
        word: "subagent",
        segments: &["subagent"],
        scope: Scope::ProductSurface,
    },
    Rule {
        word: "mission",
        segments: &["mission"],
        scope: Scope::Strict,
    },
];

#[derive(Clone, Debug)]
struct LineLex {
    number: usize,
    raw: String,
    code: String,
    strings: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Occurrence {
    relative_path: String,
    line: usize,
    word: &'static str,
}

impl Occurrence {
    fn display(&self) -> String {
        format!("{}:{}: {}", self.relative_path, self.line, self.word)
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("kernel crate should live two levels below repo root")
        .to_path_buf()
}

#[test]
fn kernel_source_obeys_lexicon_banned_words() {
    let root = repo_root();
    let source_root = root.join("crates/verlet-kernel/src");
    let baseline = parse_baseline(BASELINE);
    let mut occurrences = Vec::new();

    for path in rust_files_under(&source_root) {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        let relative_path = path
            .strip_prefix(&root)
            .expect("source file should live under repo root")
            .to_string_lossy()
            .replace('\\', "/");
        occurrences.extend(scan_source(&relative_path, &source));
    }

    if let Err(message) = compare_to_baseline(&occurrences, &baseline) {
        panic!("{message}");
    }
}

fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    fn visit(path: &Path, files: &mut Vec<PathBuf>) {
        for entry in
            fs::read_dir(path).unwrap_or_else(|err| panic!("read dir {}: {err}", path.display()))
        {
            let path = entry
                .unwrap_or_else(|err| panic!("read dir entry {}: {err}", path.display()))
                .path();
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(root, &mut files);
    files.sort();
    files
}

fn scan_source(relative_path: &str, source: &str) -> Vec<Occurrence> {
    let lines = lex_source(source);
    let mut occurrences = Vec::new();
    let mut pending_serde_derive = false;
    let mut derive_attr_depth = 0i32;
    let mut derive_attr_has_serde = false;
    let mut serde_attr_depth = 0i32;
    let mut serde_attr_has_rename_or_tag = false;
    let mut serde_depth = 0i32;

    for (index, line) in lines.iter().enumerate() {
        let tokens = identifiers(&line.code);

        let starts_derive_attr = line.code.contains("#[derive");
        if starts_derive_attr {
            derive_attr_depth = bracket_delta(&line.code);
            derive_attr_has_serde = false;
        }
        let in_derive_attr = starts_derive_attr || derive_attr_depth > 0;
        if in_derive_attr {
            derive_attr_has_serde |= tokens
                .iter()
                .any(|token| token == "Serialize" || token == "Deserialize");
            if !starts_derive_attr {
                derive_attr_depth += bracket_delta(&line.code);
            }
            if derive_attr_depth <= 0 {
                if derive_attr_has_serde {
                    pending_serde_derive = true;
                }
                derive_attr_depth = 0;
                derive_attr_has_serde = false;
            }
        } else if is_serde_derive(&line.code) {
            pending_serde_derive = true;
        }

        let starts_serde_attr = line.code.contains("#[serde");
        if starts_serde_attr {
            serde_attr_depth = bracket_delta(&line.code);
            serde_attr_has_rename_or_tag = false;
        }
        let in_serde_attr = starts_serde_attr || serde_attr_depth > 0;
        if in_serde_attr {
            serde_attr_has_rename_or_tag |= tokens
                .iter()
                .any(|token| token == "rename" || token == "tag");
        }
        let scans_serde_attribute_values =
            (in_serde_attr && serde_attr_has_rename_or_tag) || is_serde_attribute(&line.code);

        if let Some(item) = item_name(&tokens) {
            for rule in RULES {
                match rule.scope {
                    Scope::ObserverTypeName if identifier_matches(item, rule) => {
                        push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                    }
                    _ => {}
                }
            }
        }

        if let Some(item) = public_item_name(&tokens) {
            for rule in product_rules() {
                if identifier_matches(item, rule) {
                    push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                }
            }
        }

        for ident in &tokens {
            for rule in strict_rules() {
                if identifier_matches(ident, rule) {
                    push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                }
            }
        }

        for string in &line.strings {
            for rule in strict_rules() {
                if text_matches(string, rule) {
                    push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                }
            }

            if scans_serde_attribute_values || is_event_kind_constant(&line.code) {
                for rule in product_rules() {
                    if text_matches(string, rule) {
                        push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                    }
                }
            }
        }

        if serde_depth > 0 {
            for field in serde_field_names(&line.code) {
                for rule in product_rules() {
                    if identifier_matches(&field, rule) {
                        push_if_not_allowed(relative_path, &lines, index, rule, &mut occurrences);
                    }
                }
            }
        }

        if pending_serde_derive && starts_type_item(&tokens) {
            serde_depth = brace_delta(&line.code).max(1);
            pending_serde_derive = false;
        } else if serde_depth > 0 {
            serde_depth += brace_delta(&line.code);
            if serde_depth <= 0 {
                serde_depth = 0;
            }
        } else if !in_derive_attr
            && !in_serde_attr
            && !line.code.trim_start().starts_with("#[")
            && !line.code.trim().is_empty()
        {
            pending_serde_derive = false;
        }

        if serde_attr_depth > 0 && !starts_serde_attr {
            serde_attr_depth += bracket_delta(&line.code);
        }
        if serde_attr_depth <= 0 {
            serde_attr_depth = 0;
            serde_attr_has_rename_or_tag = false;
        }
    }

    occurrences
}

fn push_if_not_allowed(
    relative_path: &str,
    lines: &[LineLex],
    index: usize,
    rule: &Rule,
    occurrences: &mut Vec<Occurrence>,
) {
    if is_allowed(lines, index, rule.word) {
        return;
    }
    occurrences.push(Occurrence {
        relative_path: relative_path.to_string(),
        line: lines[index].number,
        word: rule.word,
    });
}

fn strict_rules() -> impl Iterator<Item = &'static Rule> {
    RULES.iter().filter(|rule| rule.scope == Scope::Strict)
}

fn product_rules() -> impl Iterator<Item = &'static Rule> {
    RULES
        .iter()
        .filter(|rule| rule.scope == Scope::ProductSurface)
}

fn lex_source(source: &str) -> Vec<LineLex> {
    let mut lines = Vec::new();
    let mut in_block_comment = false;
    let mut normal_string = false;
    let mut escaped = false;
    let mut raw_string_hashes: Option<usize> = None;

    for (line_index, raw_line) in source.lines().enumerate() {
        let bytes = raw_line.as_bytes();
        let mut code = String::with_capacity(raw_line.len());
        let mut strings = Vec::new();
        let mut current_string = String::new();
        let mut i = 0;

        while i < bytes.len() {
            if let Some(hashes) = raw_string_hashes {
                if raw_string_ends_at(bytes, i, hashes) {
                    strings.push(std::mem::take(&mut current_string));
                    raw_string_hashes = None;
                    i += hashes + 1;
                } else {
                    current_string.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }

            if normal_string {
                let byte = bytes[i];
                if escaped {
                    current_string.push(byte as char);
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    strings.push(std::mem::take(&mut current_string));
                    normal_string = false;
                } else {
                    current_string.push(byte as char);
                }
                i += 1;
                continue;
            }

            if in_block_comment {
                if bytes.get(i) == Some(&b'*') && bytes.get(i + 1) == Some(&b'/') {
                    in_block_comment = false;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            if bytes.get(i) == Some(&b'/') && bytes.get(i + 1) == Some(&b'/') {
                break;
            }
            if bytes.get(i) == Some(&b'/') && bytes.get(i + 1) == Some(&b'*') {
                in_block_comment = true;
                i += 2;
                continue;
            }
            if let Some((prefix_len, hashes)) = raw_string_starts_at(bytes, i) {
                raw_string_hashes = Some(hashes);
                code.push(' ');
                i += prefix_len;
                continue;
            }
            if bytes.get(i) == Some(&b'"')
                || (bytes.get(i) == Some(&b'b') && bytes.get(i + 1) == Some(&b'"'))
            {
                normal_string = true;
                code.push(' ');
                i += if bytes[i] == b'b' { 2 } else { 1 };
                continue;
            }

            code.push(bytes[i] as char);
            i += 1;
        }

        if normal_string || raw_string_hashes.is_some() {
            strings.push(std::mem::take(&mut current_string));
        }

        lines.push(LineLex {
            number: line_index + 1,
            raw: raw_line.to_string(),
            code,
            strings,
        });
    }

    lines
}

fn raw_string_starts_at(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let mut i = start;
    if bytes.get(i) == Some(&b'b') {
        i += 1;
    }
    if bytes.get(i) != Some(&b'r') {
        return None;
    }
    i += 1;
    let hashes_start = i;
    while bytes.get(i) == Some(&b'#') {
        i += 1;
    }
    if bytes.get(i) == Some(&b'"') {
        Some((i - start + 1, i - hashes_start))
    } else {
        None
    }
}

fn raw_string_ends_at(bytes: &[u8], start: usize, hashes: usize) -> bool {
    if bytes.get(start) != Some(&b'"') {
        return false;
    }
    (0..hashes).all(|offset| bytes.get(start + 1 + offset) == Some(&b'#'))
}

fn identifiers(code: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_identifier = false;
    for ch in code.chars() {
        if in_identifier {
            if is_ident_continue(ch) {
                current.push(ch);
            } else {
                out.push(std::mem::take(&mut current));
                in_identifier = false;
                if is_ident_start(ch) {
                    current.push(ch);
                    in_identifier = true;
                }
            }
        } else if is_ident_start(ch) {
            current.push(ch);
            in_identifier = true;
        }
    }
    if in_identifier {
        out.push(current);
    }
    out
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

fn identifier_matches(identifier: &str, rule: &Rule) -> bool {
    contains_rule_segments(&split_identifier(identifier), rule)
}

fn text_matches(text: &str, rule: &Rule) -> bool {
    identifiers(text)
        .iter()
        .any(|identifier| identifier_matches(identifier, rule))
}

fn split_identifier(identifier: &str) -> Vec<String> {
    let mut segments = Vec::new();
    for snake_part in identifier.split('_').filter(|part| !part.is_empty()) {
        let mut current = String::new();
        let mut previous_lower_or_digit = false;
        for ch in snake_part.chars() {
            if ch.is_ascii_uppercase() && previous_lower_or_digit && !current.is_empty() {
                segments.push(current.to_ascii_lowercase());
                current.clear();
            }
            current.push(ch);
            previous_lower_or_digit = ch.is_lowercase() || ch.is_ascii_digit();
        }
        if !current.is_empty() {
            segments.push(current.to_ascii_lowercase());
        }
    }
    segments
}

fn contains_rule_segments(segments: &[String], rule: &Rule) -> bool {
    if rule.segments.is_empty() || segments.len() < rule.segments.len() {
        return false;
    }

    for (index, window) in segments.windows(rule.segments.len()).enumerate() {
        if !window
            .iter()
            .map(String::as_str)
            .eq(rule.segments.iter().copied())
        {
            continue;
        }
        if rule.word == "memory" && index > 0 && segments[index - 1] == "in" {
            continue;
        }
        return true;
    }
    false
}

fn item_name(tokens: &[String]) -> Option<&str> {
    for (index, token) in tokens.iter().enumerate() {
        if matches!(
            token.as_str(),
            "struct" | "enum" | "trait" | "type" | "union"
        ) {
            return tokens.get(index + 1).map(String::as_str);
        }
    }
    None
}

fn public_item_name(tokens: &[String]) -> Option<&str> {
    if !tokens.iter().any(|token| token == "pub") {
        return None;
    }
    for (index, token) in tokens.iter().enumerate() {
        if matches!(
            token.as_str(),
            "struct" | "enum" | "trait" | "type" | "fn" | "const" | "static" | "mod"
        ) {
            return tokens.get(index + 1).map(String::as_str);
        }
    }
    None
}

fn starts_type_item(tokens: &[String]) -> bool {
    tokens.iter().any(|token| {
        matches!(
            token.as_str(),
            "struct" | "enum" | "trait" | "type" | "union"
        )
    })
}

fn is_serde_derive(code: &str) -> bool {
    code.contains("#[derive") && (code.contains("Serialize") || code.contains("Deserialize"))
}

fn is_serde_attribute(code: &str) -> bool {
    code.contains("#[serde") && (code.contains("rename") || code.contains("tag"))
}

fn is_event_kind_constant(code: &str) -> bool {
    let tokens = identifiers(code);
    tokens
        .iter()
        .any(|token| token == "const" || token == "static")
        && tokens
            .iter()
            .any(|token| token == "EVENT_KIND" || token == "KIND" || token.ends_with("_KIND"))
}

fn serde_field_names(code: &str) -> Vec<String> {
    if code.trim_start().starts_with("#[") {
        return Vec::new();
    }

    let mut fields = Vec::new();
    for part in code.split(',') {
        let Some((before_colon, _)) = part.split_once(':') else {
            continue;
        };
        let tokens = identifiers(before_colon);
        if let Some(name) = tokens.last() {
            if !matches!(name.as_str(), "pub" | "crate" | "super" | "self") {
                fields.push(name.clone());
            }
        }
    }
    fields
}

fn brace_delta(code: &str) -> i32 {
    code.bytes().fold(0, |delta, byte| match byte {
        b'{' => delta + 1,
        b'}' => delta - 1,
        _ => delta,
    })
}

fn bracket_delta(code: &str) -> i32 {
    code.bytes().fold(0, |delta, byte| match byte {
        b'[' => delta + 1,
        b']' => delta - 1,
        _ => delta,
    })
}

fn is_allowed(lines: &[LineLex], index: usize, word: &str) -> bool {
    allow_marker_matches(&lines[index].raw, word)
        || index
            .checked_sub(1)
            .is_some_and(|previous| allow_marker_matches(&lines[previous].raw, word))
}

fn allow_marker_matches(line: &str, word: &str) -> bool {
    let Some(marker_index) = line.find("lexicon-allow:") else {
        return false;
    };
    let marker = line[marker_index + "lexicon-allow:".len()..].trim_start();
    marker
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .next()
        == Some(word)
}

fn parse_baseline(text: &str) -> BTreeMap<(String, String), usize> {
    let mut baseline = BTreeMap::new();
    for (line_index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut fields = line.split_whitespace();
        let path = fields.next().unwrap_or_default();
        let word = fields.next().unwrap_or_default();
        let count = fields.next().unwrap_or_default();
        if fields.next().is_some() || path.is_empty() || word.is_empty() || count.is_empty() {
            panic!(
                "invalid lexicon baseline entry at line {}: expected `<relative path> <word> <count>`",
                line_index + 1
            );
        }
        if !RULES.iter().any(|rule| rule.word == word) {
            panic!(
                "invalid lexicon baseline entry at line {}: unknown word `{word}`",
                line_index + 1
            );
        }
        let count = count.parse::<usize>().unwrap_or_else(|err| {
            panic!(
                "invalid lexicon baseline entry at line {}: count `{count}` is not a number: {err}",
                line_index + 1
            )
        });
        if count == 0 {
            panic!(
                "invalid lexicon baseline entry at line {}: count must be positive",
                line_index + 1
            );
        }
        if baseline
            .insert((path.to_string(), word.to_string()), count)
            .is_some()
        {
            panic!(
                "invalid lexicon baseline entry at line {}: duplicate `{path} {word}`",
                line_index + 1
            );
        }
    }
    baseline
}

fn compare_to_baseline(
    occurrences: &[Occurrence],
    baseline: &BTreeMap<(String, String), usize>,
) -> Result<(), String> {
    let mut actual_counts = BTreeMap::<(String, String), usize>::new();
    let mut new_occurrences = Vec::new();

    for occurrence in occurrences {
        let key = (
            occurrence.relative_path.clone(),
            occurrence.word.to_string(),
        );
        let count = actual_counts
            .entry(key.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let baseline_count = baseline.get(&key).copied().unwrap_or(0);
        if *count > baseline_count {
            new_occurrences.push(occurrence);
        }
    }

    let mut shrunk = Vec::new();
    for ((path, word), baseline_count) in baseline {
        let actual_count = actual_counts
            .get(&(path.clone(), word.clone()))
            .copied()
            .unwrap_or(0);
        if actual_count < *baseline_count {
            shrunk.push((path, word, actual_count));
        }
    }

    if new_occurrences.is_empty() && shrunk.is_empty() {
        return Ok(());
    }

    let mut message = String::new();
    if !new_occurrences.is_empty() {
        message.push_str("new lexicon banned-word occurrences:\n");
        for occurrence in new_occurrences {
            message.push_str("  ");
            message.push_str(&occurrence.display());
            message.push('\n');
        }
    }
    if !shrunk.is_empty() {
        message.push_str("lexicon baseline debt shrank:\n");
        for (path, word, actual_count) in shrunk {
            message.push_str("  ");
            message.push_str(path);
            message.push(' ');
            message.push_str(word);
            message.push_str(": debt shrank; update baseline to ");
            message.push_str(&actual_count.to_string());
            message.push('\n');
        }
    }
    message.push_str(HINT);
    Err(message)
}

fn baseline_counts(occurrences: &[Occurrence]) -> BTreeMap<(String, String), usize> {
    let mut counts = BTreeMap::new();
    for occurrence in occurrences {
        counts
            .entry((
                occurrence.relative_path.clone(),
                occurrence.word.to_string(),
            ))
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    counts
}

#[test]
fn scanner_flags_new_banned_identifier() {
    let source = "pub struct CapsuleRunner;\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert_eq!(occurrences.len(), 1);
    assert!(
        occurrences
            .iter()
            .all(|occurrence| occurrence.word == "capsule")
    );
}

#[test]
fn scanner_respects_allow_marker_on_previous_line() {
    let source = "// lexicon-allow: memory - wasm linear memory export name\npub const MEMORY_KIND: &str = \"memory\";\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert!(occurrences.is_empty(), "{occurrences:#?}");
}

#[test]
fn scanner_count_baseline_suppresses_existing_debt() {
    let source = "pub struct CapsuleRunner;\npub struct CapsuleBinding;\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    let baseline = baseline_counts(&occurrences);
    assert!(compare_to_baseline(&occurrences, &baseline).is_ok());
}

#[test]
fn count_baseline_reports_shrunk_debt() {
    let source = "pub struct CapsuleRunner;\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    let baseline = BTreeMap::from([(
        (
            "crates/verlet-kernel/src/example.rs".to_string(),
            "capsule".to_string(),
        ),
        2,
    )]);
    let message = compare_to_baseline(&occurrences, &baseline).unwrap_err();
    assert!(message.contains("debt shrank; update baseline to 1"));
}

#[test]
fn count_baseline_reports_new_debt_with_line_and_hint() {
    let source = "pub struct CapsuleRunner;\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    let message = compare_to_baseline(&occurrences, &BTreeMap::new()).unwrap_err();
    assert!(message.contains("crates/verlet-kernel/src/example.rs:1: capsule"));
    assert!(message.contains(HINT));
}

#[test]
fn scanner_ignores_comments_and_product_words_outside_public_surface() {
    let source = r#"
// capsule hook memory subagent
fn local_hook_memory_subagent() {
    let hook_memory_subagent = "hook memory subagent";
}
"#;
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert!(occurrences.is_empty(), "{occurrences:#?}");
}

#[test]
fn scanner_handles_crlf_and_raw_string_literals() {
    let source = "pub const EVENT_KIND: &str = r#\"capsule_ready\"#;\r\n// capsule prose\r\n";
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert_eq!(
        occurrences
            .iter()
            .map(|occurrence| (occurrence.word, occurrence.line))
            .collect::<Vec<_>>(),
        vec![("capsule", 1)]
    );
}

#[test]
fn scanner_exempts_in_memory_computing_compound() {
    let source = r#"
pub struct InMemorySessionStore;
pub fn in_memory() {}
pub fn agent_memory() {}
"#;
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert_eq!(
        occurrences
            .iter()
            .map(|occurrence| occurrence.word)
            .collect::<Vec<_>>(),
        vec!["memory"]
    );
    assert_eq!(occurrences[0].line, 4);
}

#[test]
fn scanner_handles_multiline_serde_attrs_and_derives() {
    let source = r#"
#[derive(
    Serialize,
    Deserialize,
)]
#[serde(
    tag = "hook_event_name",
)]
pub enum RuntimeEnvelope {
    Started,
}
"#;
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert_eq!(
        occurrences
            .iter()
            .map(|occurrence| (occurrence.word, occurrence.line))
            .collect::<Vec<_>>(),
        vec![("hook", 7)]
    );
}

#[test]
fn scanner_keeps_multiline_derive_for_serde_field_names() {
    let source = r#"
#[derive(
    Serialize,
    Deserialize,
)]
pub struct RuntimeEnvelope {
    pub hook: String,
}
"#;
    let occurrences = scan_source("crates/verlet-kernel/src/example.rs", source);
    assert_eq!(
        occurrences
            .iter()
            .map(|occurrence| (occurrence.word, occurrence.line))
            .collect::<Vec<_>>(),
        vec![("hook", 7)]
    );
}
