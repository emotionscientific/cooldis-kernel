use super::*;
use std::fs;
use uuid::Uuid;

#[test]
fn publish_skill_directory_is_deterministic_and_preserves_entries() {
    let root = temp_root("skill-package-deterministic");
    let package_dir = root.join("karl-skills");
    write_skill(
        &package_dir,
        "frontmatter",
        r#"---
name: frontmatter-skill
description: Uses declared metadata
trigger_hint: when metadata matters
---
# Frontmatter Skill

Body with metadata.
"#,
    );
    write_skill(
        &package_dir,
        "plain",
        r#"# Plain Skill

First plain description line.

More body.
"#,
    );
    write_skill(
        &package_dir,
        "設計",
        r#"# 設計

Unicode description line.
"#,
    );
    let registry = LocalSkillRegistry::new(root.join("skills-registry"));

    let first = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir: package_dir.clone(),
            name: None,
        })
        .unwrap();
    let second = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        first.ref_uri(),
        format!("skill://karl-skills@sha256:{}", first.active_artifact_hash)
    );
    assert_eq!(
        first
            .package
            .skills
            .iter()
            .map(|skill| skill.name.as_str())
            .collect::<Vec<_>>(),
        vec!["frontmatter-skill", "plain", "設計"]
    );
    assert_eq!(
        first.package.skills[0].trigger_hint.as_deref(),
        Some("when metadata matters")
    );
    assert_eq!(
        first.package.skills[1].description,
        "First plain description line."
    );
    assert_eq!(
        first.package.render_index(),
        "frontmatter-skill — Uses declared metadata\nplain — First plain description line.\n設計 — Unicode description line.\n"
    );
    assert_eq!(
        registry
            .load_version_record("karl-skills", &first.active_artifact_hash)
            .unwrap()
            .package,
        first.package
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn malformed_frontmatter_names_the_skill_file() {
    let root = temp_root("skill-package-bad-frontmatter");
    let package_dir = root.join("bad-skills");
    let skill_file = package_dir.join("broken").join("SKILL.md");
    write_skill(
        &package_dir,
        "broken",
        r#"---
name: broken
description "missing colon"
---
body
"#,
    );
    let registry = LocalSkillRegistry::new(root.join("skills-registry"));

    let err = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap_err();

    let text = err.to_string();
    assert!(text.contains("malformed frontmatter"));
    assert!(text.contains(&skill_file.display().to_string()));
    let _ = fs::remove_dir_all(root);
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", Uuid::now_v7()))
}

fn write_skill(package_dir: &Path, name: &str, body: &str) {
    let dir = package_dir.join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("SKILL.md"), body).unwrap();
}
