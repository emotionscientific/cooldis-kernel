#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillImportAsset {
    pub relative_path: String,
    pub resource_name: String,
    pub ref_uri: String,
    bytes: Vec<u8>,
    source_path: std::path::PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SkillImportPlan {
    pub package: crate::skill_package::SkillPackage,
    pub references: Vec<String>,
    pub assets: Vec<SkillImportAsset>,
    pub omitted_scripts: Vec<String>,
    pub ignored_hooks: Vec<String>,
    pub skipped_files: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishedSkillImport {
    pub skill: crate::skill_package::PublishedSkillPackageRecord,
    pub blobs: Vec<crate::blob_store::PublishedBlobRecord>,
}

impl SkillImportPlan {
    pub fn from_directory(
        skill_dir: &std::path::Path,
        package_name: Option<&str>,
    ) -> crate::VerletResult<Self> {
        let metadata = std::fs::symlink_metadata(skill_dir).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill import directory {}: {err}",
                skill_dir.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill import does not follow symlink {}",
                skill_dir.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill import input {} is not a directory",
                skill_dir.display()
            )));
        }
        let package_name = match package_name {
            Some(package_name) => crate::operation_store::validate_record_name(package_name)?,
            None => {
                let inferred_name = skill_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        crate::VerletOperationsError::RuntimeFactory(format!(
                            "skill import directory {} has no package name; pass --name",
                            skill_dir.display()
                        ))
                    })?;
                crate::operation_store::validate_record_name(inferred_name)?
            }
        };
        let mut files = Vec::new();
        collect_files(skill_dir, skill_dir, &mut files)?;
        files.sort_by(|left, right| left.0.cmp(&right.0));
        let skill_file = skill_dir.join("SKILL.md");
        let original_body = std::fs::read_to_string(&skill_file).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read imported skill file {}: {err}",
                skill_file.display()
            ))
        })?;

        let mut references = Vec::new();
        let mut reference_bodies = Vec::new();
        let mut asset_sources = Vec::new();
        let mut omitted_scripts = Vec::new();
        let mut ignored_hooks = Vec::new();
        let mut skipped_files = Vec::new();
        for (relative_path, path) in files {
            if relative_path == "SKILL.md" {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read skill import component {}: {err}",
                    path.display()
                ))
            })?;
            if is_hook_shaped(&relative_path, &bytes) {
                ignored_hooks.push(relative_path);
            } else if relative_path.starts_with("scripts/") {
                omitted_scripts.push(relative_path);
            } else if is_direct_markdown_reference(&relative_path) {
                let body = String::from_utf8(bytes).map_err(|err| {
                    crate::VerletOperationsError::RuntimeFactory(format!(
                        "imported reference {} is not valid UTF-8: {err}",
                        path.display()
                    ))
                })?;
                references.push(relative_path.clone());
                reference_bodies.push((relative_path, body));
            } else if relative_path.starts_with("assets/") {
                asset_sources.push((relative_path, path, bytes));
            } else {
                skipped_files.push(relative_path);
            }
        }

        let mut compiled_body = original_body;
        if !reference_bodies.is_empty() {
            start_appended_section(&mut compiled_body);
            compiled_body.push_str("## Imported references\n");
            for (relative_path, body) in reference_bodies {
                compiled_body.push_str("\n### `");
                compiled_body.push_str(&relative_path);
                compiled_body.push_str("`\n\n");
                compiled_body.push_str(&body);
                if !compiled_body.ends_with('\n') {
                    compiled_body.push('\n');
                }
            }
        }
        if !omitted_scripts.is_empty() {
            start_appended_section(&mut compiled_body);
            compiled_body.push_str(
                "## Import degradation\n\nScripts were omitted during import and are unavailable:\n",
            );
            for script in &omitted_scripts {
                compiled_body.push_str("- `");
                compiled_body.push_str(script);
                compiled_body.push_str("`\n");
            }
        }

        let mut entry =
            crate::skill_package::SkillPackageEntry::from_skill_body(skill_dir, compiled_body)?;
        if !omitted_scripts.is_empty() {
            entry
                .description
                .push_str(" Import degradation: scripts omitted: ");
            entry.description.push_str(&omitted_scripts.join(", "));
            entry.description.push('.');
        }
        let package = crate::skill_package::SkillPackage::from_entries(&package_name, vec![entry])?;
        let assets = asset_sources
            .into_iter()
            .enumerate()
            .map(|(index, (relative_path, source_path, bytes))| {
                let hash = crate::operation_store::wasm_sha256(&bytes);
                SkillImportAsset {
                    relative_path,
                    resource_name: asset_resource_name(&package_name, index + 1),
                    ref_uri: format!("resource://artifact/sha256:{hash}"),
                    bytes,
                    source_path,
                }
            })
            .collect();
        Ok(Self {
            package,
            references,
            assets,
            omitted_scripts,
            ignored_hooks,
            skipped_files,
        })
    }

    pub fn artifact_hash(&self) -> crate::VerletResult<String> {
        Ok(crate::operation_store::wasm_sha256(
            &self.package.to_artifact_bytes()?,
        ))
    }

    pub fn pinned_ref(&self) -> crate::VerletResult<String> {
        Ok(format!(
            "skill://{}@sha256:{}",
            self.package.name,
            self.artifact_hash()?
        ))
    }

    pub fn floating_ref(&self) -> String {
        format!("skill://{}", self.package.name)
    }

    pub fn manifest_fragment(&self) -> crate::VerletResult<String> {
        let mut out = format!(
            "[[resources]]\nname = {:?}\nkind = \"skill\"\nref = {:?}\n",
            self.package.name,
            self.pinned_ref()?
        );
        for asset in &self.assets {
            out.push_str(&format!(
                "\n[[resources]]\nname = {:?}\nkind = \"blob\"\nref = {:?}\n",
                asset.resource_name, asset.ref_uri
            ));
        }
        Ok(out)
    }

    pub fn publish(
        &self,
        skill_registry: &crate::skill_package::LocalSkillRegistry,
        blob_registry: &crate::blob_store::LocalBlobRegistry,
    ) -> crate::VerletResult<PublishedSkillImport> {
        let skill = skill_registry.publish_package(self.package.clone())?;
        let mut blobs = Vec::with_capacity(self.assets.len());
        for asset in &self.assets {
            let record = blob_registry.publish_bytes(
                asset.bytes.clone(),
                Some(&asset.resource_name),
                Some(asset.source_path.clone()),
            )?;
            if record.ref_uri != asset.ref_uri {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "published asset {} ref {:?} did not match planned ref {:?}",
                    asset.relative_path, record.ref_uri, asset.ref_uri
                )));
            }
            blobs.push(record);
        }
        Ok(PublishedSkillImport { skill, blobs })
    }
}

fn collect_files(
    root: &std::path::Path,
    directory: &std::path::Path,
    files: &mut Vec<(String, std::path::PathBuf)>,
) -> crate::VerletResult<()> {
    let entries = std::fs::read_dir(directory).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to read skill import directory {}: {err}",
            directory.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill import entry in {}: {err}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to inspect skill import component {}: {err}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill import does not follow symlink {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            files.push((relative_slash_path(root, &path)?, path));
        }
    }
    Ok(())
}

fn relative_slash_path(
    root: &std::path::Path,
    path: &std::path::Path,
) -> crate::VerletResult<String> {
    let relative = path.strip_prefix(root).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "skill import component {} escaped {}: {err}",
            path.display(),
            root.display()
        ))
    })?;
    let mut segments = Vec::new();
    for component in relative.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill import component {} has a non-normal path",
                path.display()
            )));
        };
        segments.push(segment.to_str().ok_or_else(|| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "skill import component {} has a non-Unicode path",
                path.display()
            ))
        })?);
    }
    Ok(segments.join("/"))
}

fn is_direct_markdown_reference(relative_path: &str) -> bool {
    let mut parts = relative_path.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some("references"), Some(file), None) if file.ends_with(".md")
    )
}

fn is_hook_shaped(relative_path: &str, bytes: &[u8]) -> bool {
    let lower = relative_path.to_ascii_lowercase();
    let parts = lower.split('/').collect::<Vec<_>>();
    if parts[..parts.len().saturating_sub(1)]
        .iter()
        .any(|part| matches!(*part, "hooks" | ".hooks" | "mcp" | ".mcp"))
    {
        return true;
    }
    let file = parts.last().copied().unwrap_or_default();
    let config_extension = [".json", ".toml", ".yaml", ".yml"]
        .iter()
        .any(|extension| file.ends_with(extension));
    if !config_extension {
        return false;
    }
    let stem_is_authority = file == ".mcp.json"
        || file.starts_with("hook.")
        || file.starts_with("hooks.")
        || file.starts_with("mcp.")
        || file.starts_with("mcp-")
        || file.starts_with("mcp_")
        || file.starts_with("mcpserver")
        || file.starts_with("mcp-server")
        || file.starts_with("mcp_server");
    if stem_is_authority {
        return true;
    }
    let content = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    contains_json_key(&content, "hooks")
        || contains_json_key(&content, "mcpservers")
        || contains_json_key(&content, "mcp_servers")
        || content.lines().any(|line| {
            let line = line.trim_start();
            [
                "hooks:",
                "hooks =",
                "mcpservers:",
                "mcpservers =",
                "mcp_servers:",
                "mcp_servers =",
            ]
            .iter()
            .any(|prefix| line.starts_with(prefix))
        })
}

fn contains_json_key(content: &str, key: &str) -> bool {
    let quoted = format!("\"{key}\"");
    let mut remaining = content;
    while let Some(index) = remaining.find(&quoted) {
        let after = &remaining[index + quoted.len()..];
        if after.trim_start().starts_with(':') {
            return true;
        }
        remaining = after;
    }
    false
}

fn start_appended_section(body: &mut String) {
    if !body.ends_with('\n') {
        body.push('\n');
    }
    if !body.ends_with("\n\n") {
        body.push('\n');
    }
}

fn asset_resource_name(package_name: &str, index: usize) -> String {
    let suffix = format!("_asset_{index}");
    let keep = 128_usize.saturating_sub(suffix.len());
    format!(
        "{}{}",
        &package_name[..package_name.len().min(keep)],
        suffix
    )
}

#[cfg(test)]
mod tests {

    #[test]
    fn import_plan_converts_all_supported_components_and_leaves_authority_inert() {
        let root = temp_root("skill-import-components");
        let skill_dir = root.join("fixture-skill");
        let original = r#"---
name: fixture-skill
description: Uses fixtures.
---
# Fixture Skill

Original body.
"#;
        write(&skill_dir.join("SKILL.md"), original.as_bytes());
        write(
            &skill_dir.join("references/guide.md"),
            b"# Guide\n\nReference body.\n",
        );
        write(&skill_dir.join("assets/icon.bin"), &[0, 1, 2, 3]);
        write(&skill_dir.join("assets/hooks.json"), br#"{"hooks": []}"#);
        write(&skill_dir.join("scripts/check.py"), b"print('check')\n");
        write(
            &skill_dir.join("scripts/nested/task.ts"),
            b"console.log('task')\n",
        );
        write(&skill_dir.join("hooks.json"), br#"{"hooks": []}"#);
        write(&skill_dir.join(".mcp.json"), br#"{"mcpServers": {}}"#);

        let plan = crate::skill_import::SkillImportPlan::from_directory(
            &skill_dir,
            Some("fixture-package"),
        )
        .unwrap();

        assert_eq!(plan.package.name, "fixture-package");
        assert_eq!(plan.package.skills.len(), 1);
        let entry = &plan.package.skills[0];
        assert!(entry.body.starts_with(original));
        assert!(entry.body.contains("## Imported references"));
        assert!(entry.body.contains("### `references/guide.md`"));
        assert!(entry.body.contains("Reference body."));
        assert!(entry.body.contains("## Import degradation"));
        assert!(entry.body.contains("- `scripts/check.py`"));
        assert!(entry.body.contains("- `scripts/nested/task.ts`"));
        assert!(entry.description.contains("scripts/check.py"));
        assert!(entry.description.contains("scripts/nested/task.ts"));
        assert!(plan.package.render_index().contains("scripts omitted"));
        assert_eq!(
            plan.omitted_scripts,
            vec!["scripts/check.py", "scripts/nested/task.ts"]
        );
        assert_eq!(
            plan.ignored_hooks,
            vec![".mcp.json", "assets/hooks.json", "hooks.json"]
        );
        assert_eq!(plan.assets.len(), 1);
        assert_eq!(plan.assets[0].relative_path, "assets/icon.bin");
        assert_eq!(
            plan.assets[0].ref_uri,
            "resource://artifact/sha256:054edec1d0211f624fed0cbca9d4f9400b0e491c43742af2c5b0abebf0c990d8"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn import_plan_reports_unclassified_files_without_renumbering_assets() {
        let root = temp_root("skill-import-skipped-files");
        let skill_dir = root.join("fixture-skill");
        write(
            &skill_dir.join("SKILL.md"),
            b"# Fixture Skill\n\nFixture description.",
        );
        write(&skill_dir.join("assets/one.bin"), b"one");
        write(&skill_dir.join("assets/two.bin"), b"two");

        let before =
            crate::skill_import::SkillImportPlan::from_directory(&skill_dir, None).unwrap();
        write(&skill_dir.join("README.md"), b"not imported\n");
        write(
            &skill_dir.join("references/nested/extra.md"),
            b"not a direct reference\n",
        );
        let after = crate::skill_import::SkillImportPlan::from_directory(&skill_dir, None).unwrap();

        assert_eq!(
            before
                .assets
                .iter()
                .map(|asset| (&asset.relative_path, &asset.resource_name))
                .collect::<Vec<_>>(),
            after
                .assets
                .iter()
                .map(|asset| (&asset.relative_path, &asset.resource_name))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            after.skipped_files,
            vec!["README.md", "references/nested/extra.md"]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn import_plan_appends_after_frontmatter_body_without_a_trailing_newline() {
        let root = temp_root("skill-import-appended-body");
        let skill_dir = root.join("fixture-skill");
        let original =
            "---\nname: fixture-skill\ndescription: Uses fixtures.\n---\n# Fixture Skill";
        write(&skill_dir.join("SKILL.md"), original.as_bytes());
        write(&skill_dir.join("references/guide.md"), b"Guide body");
        write(&skill_dir.join("scripts/check.py"), b"print('check')\n");

        let plan = crate::skill_import::SkillImportPlan::from_directory(&skill_dir, None).unwrap();
        let body = &plan.package.skills[0].body;

        assert!(body.starts_with(original));
        assert!(body.contains(
            "# Fixture Skill\n\n## Imported references\n\n### `references/guide.md`\n\nGuide body\n\n## Import degradation"
        ));
        assert_eq!(plan.package.skills[0].name, "fixture-skill");
        assert!(
            plan.package.skills[0]
                .description
                .starts_with("Uses fixtures.")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn hook_sniffing_covers_toml_and_yaml_keys_but_not_markdown_references() {
        let root = temp_root("skill-import-hook-config-formats");
        let skill_dir = root.join("fixture-skill");
        write(
            &skill_dir.join("SKILL.md"),
            b"---\nname: fixture-skill\ndescription: Fixture description.\n---\n# Fixture Skill\n",
        );
        write(
            &skill_dir.join("assets/agent.toml"),
            b"hooks = [{ command = \"check\" }]\n",
        );
        write(
            &skill_dir.join("assets/servers.yaml"),
            b"mcpServers:\n  local:\n    command: serve\n",
        );
        write(
            &skill_dir.join("references/hooks.md"),
            b"# Hooks\n\nThis is documentation, not configuration.\n",
        );
        write(
            &skill_dir.join("assets/labels.json"),
            br#"{"category":"hooks","description":"mcp_servers documentation"}"#,
        );

        let plan = crate::skill_import::SkillImportPlan::from_directory(&skill_dir, None).unwrap();

        assert_eq!(
            plan.ignored_hooks,
            vec!["assets/agent.toml", "assets/servers.yaml"]
        );
        assert_eq!(plan.references, vec!["references/hooks.md"]);
        assert_eq!(plan.assets.len(), 1);
        assert_eq!(plan.assets[0].relative_path, "assets/labels.json");
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn import_rejects_a_symlinked_root_directory() {
        let root = temp_root("skill-import-symlinked-root");
        let target = root.join("target-skill");
        write(
            &target.join("SKILL.md"),
            b"# Fixture Skill\n\nFixture description.\n",
        );
        let link = root.join("linked-skill");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let error =
            crate::skill_import::SkillImportPlan::from_directory(&link, Some("fixture-skill"))
                .unwrap_err();

        assert!(error.to_string().contains("does not follow symlink"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn explicit_package_name_does_not_require_a_unicode_directory_name() {
        use std::os::unix::ffi::OsStringExt as _;

        let root = temp_root("skill-import-explicit-name");
        let skill_dir = root.join(std::ffi::OsString::from_vec(b"fixture-\xff".to_vec()));
        write(
            &skill_dir.join("SKILL.md"),
            b"---\nname: fixture-skill\ndescription: Fixture description.\n---\n# Fixture Skill\n",
        );

        let plan = crate::skill_import::SkillImportPlan::from_directory(
            &skill_dir,
            Some("fixture-package"),
        )
        .unwrap();

        assert_eq!(plan.package.name, "fixture-package");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn publishing_an_import_twice_reuses_skill_and_blob_versions() {
        let root = temp_root("skill-import-idempotent");
        let skill_dir = root.join("fixture-skill");
        write(
            &skill_dir.join("SKILL.md"),
            b"# Fixture Skill\n\nFixture description.\n",
        );
        write(&skill_dir.join("assets/payload.txt"), b"same payload\n");
        let plan = crate::skill_import::SkillImportPlan::from_directory(&skill_dir, None).unwrap();
        let skills = crate::skill_package::LocalSkillRegistry::new(root.join("skills"));
        let blobs = crate::blob_store::LocalBlobRegistry::new(root.join("blobs"));

        let first = plan.publish(&skills, &blobs).unwrap();
        let second = plan.publish(&skills, &blobs).unwrap();

        assert_eq!(
            first.skill.active_artifact_hash,
            second.skill.active_artifact_hash
        );
        assert_eq!(first.blobs[0].ref_uri, second.blobs[0].ref_uri);
        assert_eq!(
            std::fs::read_dir(root.join("skills/versions/fixture-skill"))
                .unwrap()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_dir(root.join("blobs/records/artifact"))
                .unwrap()
                .count(),
            1
        );
        let _ = std::fs::remove_dir_all(root);
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("verlet-{label}-{}", uuid::Uuid::now_v7()))
    }

    fn write(path: &std::path::Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}
