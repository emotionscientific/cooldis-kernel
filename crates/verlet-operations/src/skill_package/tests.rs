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
    let active_record_path = registry.record_path("karl-skills").unwrap();
    let first_active_record = fs::read(&active_record_path).unwrap();
    let second = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir,
            name: None,
        })
        .unwrap();

    assert_eq!(first.active_artifact_hash, second.active_artifact_hash);
    assert_eq!(
        fs::read(&active_record_path).unwrap(),
        first_active_record,
        "an identical re-publish must leave the active record stable"
    );
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
    assert_eq!(
        fs::read_dir(root.join("skills-registry/versions/karl-skills"))
            .unwrap()
            .count(),
        1,
        "an identical re-publish must not create another version record"
    );

    let plain_file = root.join("karl-skills/plain/SKILL.md");
    fs::write(
        &plain_file,
        "# Plain Skill\n\nChanged description.\n\nChanged body.\n",
    )
    .unwrap();
    let changed = registry
        .publish_directory(PublishSkillPackageRequest {
            package_dir: root.join("karl-skills"),
            name: None,
        })
        .unwrap();

    assert_ne!(changed.active_artifact_hash, first.active_artifact_hash);
    assert_eq!(
        registry
            .load_record("karl-skills")
            .unwrap()
            .active_artifact_hash,
        changed.active_artifact_hash
    );
    assert_eq!(
        registry
            .load_version_record("karl-skills", &first.active_artifact_hash)
            .unwrap()
            .package,
        first.package,
        "publishing a new latest version must preserve prior pinned versions"
    );
    assert_eq!(
        fs::read_dir(root.join("skills-registry/versions/karl-skills"))
            .unwrap()
            .count(),
        2
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn declared_skill_refs_distinguish_floating_and_pinned_without_masking_hash_errors() {
    let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    assert_eq!(
        DeclaredSkillPackageRef::parse("skill://karl-skills").unwrap(),
        DeclaredSkillPackageRef::Floating {
            name: "karl-skills".to_string(),
        }
    );
    assert_eq!(
        DeclaredSkillPackageRef::parse(&format!("skill://karl-skills@sha256:{hash}")).unwrap(),
        DeclaredSkillPackageRef::Pinned(SkillPackageRef {
            name: "karl-skills".to_string(),
            artifact_hash: hash.to_string(),
        })
    );

    let bad_hash = DeclaredSkillPackageRef::parse("skill://karl-skills@sha256:short")
        .unwrap_err()
        .to_string();
    assert!(bad_hash.contains("artifact hash"), "{bad_hash}");
    assert!(bad_hash.contains("sha256 hex digest"), "{bad_hash}");
    let bad_name = DeclaredSkillPackageRef::parse("skill://bad/name")
        .unwrap_err()
        .to_string();
    assert!(bad_name.contains("record name"), "{bad_name}");
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

#[test]
fn frontmatter_name_does_not_require_a_directory_name_fallback() {
    let entry = SkillPackageEntry::from_skill_body(
        Path::new("/"),
        "---\nname: declared-skill\ndescription: Declared description.\n---\n# Declared Skill\n"
            .to_string(),
    )
    .unwrap();

    assert_eq!(entry.name, "declared-skill");
    assert_eq!(entry.description, "Declared description.");
}

#[test]
fn missing_frontmatter_name_still_requires_a_directory_name_fallback() {
    for body in [
        "# Plain Skill\n\nPlain description.\n",
        "---\ndescription: Declared description.\n---\n# Declared Skill\n",
        "---\nname: \"\"\ndescription: Declared description.\n---\n# Declared Skill\n",
    ] {
        let error =
            SkillPackageEntry::from_skill_body(Path::new("/"), body.to_string()).unwrap_err();

        assert!(error.to_string().contains("has no unicode name"), "{error}");
    }
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", Uuid::now_v7()))
}

fn write_skill(package_dir: &Path, name: &str, body: &str) {
    let dir = package_dir.join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("SKILL.md"), body).unwrap();
}
