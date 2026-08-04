//! Enforces the public threat-model registry as an append-only documentation ratchet.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

const BASELINE: &str = include_str!("threat_model_ids.txt");
const ALLOWED_AREAS: &[&str] = &[
    "INGRESS", "AUTHZ", "EXEC", "STORE", "SECRET", "SUPPLY", "DOS",
];
const ALLOWED_STATUSES: &[&str] = &["OPEN", "MITIGATED", "ACCEPTED"];
const ALLOWED_SEVERITIES: &[&str] = &["High", "Medium", "Low"];
const REQUIRED_FIELDS: &[&str] = &[
    "Status",
    "Severity",
    "Threat",
    "Affected surface",
    "Mitigation",
    "Deterministic guard",
];

#[derive(Debug)]
struct ThreatEntry {
    id: String,
    area: String,
    number: usize,
    title: String,
    line: usize,
    fields: BTreeMap<String, String>,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("kernel crate should live two levels below repo root")
        .to_path_buf()
}

#[test]
fn threat_model_registry_is_well_formed() {
    let root = repo_root();
    let doc_path = root.join("docs/threat-model.md");
    let source = fs::read_to_string(&doc_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", doc_path.display()));
    let entries = parse_entries(&source).unwrap_or_else(|message| panic!("{message}"));

    validate_registry(&root, &entries).unwrap_or_else(|message| panic!("{message}"));
    validate_baseline(&entries, BASELINE).unwrap_or_else(|message| panic!("{message}"));
}

fn parse_entries(source: &str) -> Result<Vec<ThreatEntry>, String> {
    let mut entries = Vec::new();
    let mut current: Option<ThreatEntry> = None;

    for (index, line) in source.lines().enumerate() {
        let line_number = index + 1;
        if line.starts_with("## ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            if let Some(heading) = line.strip_prefix("## TM-") {
                current = Some(parse_heading(heading, line_number)?);
            }
            continue;
        }

        let Some(entry) = current.as_mut() else {
            continue;
        };
        let Some(field) = line.strip_prefix("- ") else {
            continue;
        };
        let Some((name, value)) = field.split_once(": ") else {
            continue;
        };
        if !REQUIRED_FIELDS.contains(&name) {
            continue;
        }
        if value.trim().is_empty() {
            return Err(format!(
                "{}:{} field {name:?} must not be empty",
                entry.id, line_number
            ));
        }
        if entry
            .fields
            .insert(name.to_string(), value.trim().to_string())
            .is_some()
        {
            return Err(format!(
                "{}:{} field {name:?} appears more than once",
                entry.id, line_number
            ));
        }
    }
    if let Some(entry) = current {
        entries.push(entry);
    }
    if entries.is_empty() {
        return Err("threat model contains no TM entries".to_string());
    }
    Ok(entries)
}

fn parse_heading(heading: &str, line: usize) -> Result<ThreatEntry, String> {
    let (id_suffix, title) = heading.split_once(": ").ok_or_else(|| {
        format!("docs/threat-model.md:{line}: expected `## TM-<AREA>-<NNN>: <title>`")
    })?;
    let id = format!("TM-{id_suffix}");
    let (area, number) = id_suffix
        .rsplit_once('-')
        .ok_or_else(|| format!("docs/threat-model.md:{line}: invalid threat id {id:?}"))?;
    if area.is_empty() || number.len() != 3 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "docs/threat-model.md:{line}: invalid threat id {id:?}; expected TM-<AREA>-<NNN>"
        ));
    }
    let number = number.parse::<usize>().map_err(|err| {
        format!("docs/threat-model.md:{line}: invalid threat number in {id:?}: {err}")
    })?;
    if number == 0 {
        return Err(format!(
            "docs/threat-model.md:{line}: threat numbering starts at 001"
        ));
    }
    if title.trim().is_empty() {
        return Err(format!(
            "docs/threat-model.md:{line}: threat {id} has an empty title"
        ));
    }

    Ok(ThreatEntry {
        id,
        area: area.to_string(),
        number,
        title: title.trim().to_string(),
        line,
        fields: BTreeMap::new(),
    })
}

fn validate_registry(root: &Path, entries: &[ThreatEntry]) -> Result<(), String> {
    let mut ids = BTreeSet::new();
    let mut numbers_by_area = BTreeMap::<&str, Vec<usize>>::new();

    for entry in entries {
        if !ids.insert(entry.id.as_str()) {
            return Err(format!(
                "docs/threat-model.md:{}: duplicate threat id {}",
                entry.line, entry.id
            ));
        }
        if !ALLOWED_AREAS.contains(&entry.area.as_str()) {
            return Err(format!(
                "docs/threat-model.md:{}: {} uses unknown area {:?}; allowed: {}",
                entry.line,
                entry.id,
                entry.area,
                ALLOWED_AREAS.join(", ")
            ));
        }
        if entry.title.is_empty() {
            return Err(format!("{} has an empty title", entry.id));
        }
        numbers_by_area
            .entry(entry.area.as_str())
            .or_default()
            .push(entry.number);

        for field in REQUIRED_FIELDS {
            if !entry.fields.contains_key(*field) {
                return Err(format!(
                    "docs/threat-model.md:{}: {} is missing required field {field:?}",
                    entry.line, entry.id
                ));
            }
        }
        let status = entry.fields["Status"].as_str();
        if !ALLOWED_STATUSES.contains(&status) {
            return Err(format!(
                "{} has invalid status {status:?}; allowed: {}",
                entry.id,
                ALLOWED_STATUSES.join(", ")
            ));
        }
        let severity = entry.fields["Severity"].as_str();
        if !ALLOWED_SEVERITIES.contains(&severity) {
            return Err(format!(
                "{} has invalid severity {severity:?}; allowed: {}",
                entry.id,
                ALLOWED_SEVERITIES.join(", ")
            ));
        }
        validate_code_refs(root, entry)?;
    }

    for area in ALLOWED_AREAS {
        let numbers = numbers_by_area.get(area).ok_or_else(|| {
            format!("threat model must retain at least one entry in seed area {area}")
        })?;
        for (index, number) in numbers.iter().enumerate() {
            let expected = index + 1;
            if *number != expected {
                return Err(format!(
                    "area {area} must be ordered and contiguous from 001; expected {expected:03}, found {number:03}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_code_refs(root: &Path, entry: &ThreatEntry) -> Result<(), String> {
    let refs = backtick_values(&entry.fields["Affected surface"])?;
    if refs.is_empty() {
        return Err(format!(
            "{} affected surface must contain at least one backtick-delimited repo-relative path",
            entry.id
        ));
    }
    for reference in refs {
        let path = Path::new(reference);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
            || reference.contains('\\')
        {
            return Err(format!(
                "{} has non-repo-relative code ref {reference:?}",
                entry.id
            ));
        }
        let resolved = root.join(path);
        if !resolved.is_file() {
            return Err(format!(
                "{} code ref {reference:?} does not resolve to an existing file",
                entry.id
            ));
        }
    }
    Ok(())
}

fn backtick_values(value: &str) -> Result<Vec<&str>, String> {
    let mut values = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find('`') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('`') else {
            return Err(format!("unclosed backtick in affected surface {value:?}"));
        };
        let found = &rest[..end];
        if found.is_empty() {
            return Err("affected surface contains an empty code ref".to_string());
        }
        values.push(found);
        rest = &rest[end + 1..];
    }
    Ok(values)
}

fn validate_baseline(entries: &[ThreatEntry], baseline: &str) -> Result<(), String> {
    let baseline_ids = baseline
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let current_ids = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<Vec<_>>();
    if baseline_ids != current_ids {
        return Err(format!(
            "threat-model ID baseline differs from docs/threat-model.md\nexpected append-only baseline: {baseline_ids:?}\ncurrent document ids: {current_ids:?}\nentries must never be deleted or renumbered; append a new ID to both files"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_rejects_non_three_digit_ids() {
        let error = parse_entries("## TM-EXEC-1: bad\n").unwrap_err();
        assert!(error.contains("expected TM-<AREA>-<NNN>"));
    }

    #[test]
    fn affected_surface_parser_rejects_unclosed_backticks() {
        let error = backtick_values("`crates/verlet-kernel/src/lib.rs").unwrap_err();
        assert!(error.contains("unclosed backtick"));
    }
}
