//! Kit manifests and installed-kit records.
//!
//! A kit is the distribution surface for tool packages: a directory holding
//! one or more member packages (each a `verlet.tool.toml` package) plus a
//! declared tool set to make available on install. The lexicon law: a kit is
//! surface grammar, never a kernel primitive. Installing a kit builds and
//! publishes each member package through the normal gate and records the
//! declared tool set; nothing resolves against the kit at run time, only
//! against the content-addressed operations its installation published.

use sha2::Digest as _;
use std::io::Write as _;

pub const KIT_KIND: &str = "verlet.kit";
pub const KIT_SCHEMA_VERSION: u32 = 0;
pub const KIT_MANIFEST_FILE_NAME: &str = "verlet.kit.toml";
pub const INSTALLED_KIT_SCHEMA_VERSION: u32 = 0;

/// The parsed `verlet.kit.toml`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitManifest {
    pub kind: String,
    pub schema_version: u32,
    pub identity: KitIdentity,
    /// Member package directories, relative to the kit root. Each must hold
    /// a loadable `verlet.tool.toml`.
    pub packages: Vec<std::path::PathBuf>,
    /// The tool set installing this kit makes available.
    pub tools: Vec<KitToolDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitIdentity {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// One row of the kit's declared tool set: which operation of which member
/// package becomes which model-facing tool.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KitToolDeclaration {
    /// Model-facing tool name; unique within the kit.
    pub tool_name: String,
    /// The member package this row references, by its
    /// `identity.name` in that package's manifest.
    pub package: String,
    /// The operation name inside the member package.
    pub operation: String,
    /// Effect class of the resulting manifest row, as the kebab-case
    /// serialization of the agent-manifest `EffectClass` ("pure",
    /// "idempotent", "at-most-once"). Kept as a validated string here so
    /// this crate does not grow an agent-schema dependency.
    #[serde(default = "default_effect_class")]
    pub effect_class: String,
}

fn default_effect_class() -> String {
    "at-most-once".to_owned()
}

/// The kebab-case effect-class values a kit row may declare, mirroring the
/// agent-manifest `EffectClass` enum.
pub const KIT_EFFECT_CLASSES: [&str; 3] = ["pure", "idempotent", "at-most-once"];

/// A kit loaded from disk: the manifest plus its resolved root and the
/// sha256 of the manifest text (the kit's source identity for receipts).
#[derive(Clone, Debug)]
pub struct KitSource {
    pub manifest_path: std::path::PathBuf,
    pub kit_root: std::path::PathBuf,
    pub source_hash: String,
    pub manifest: KitManifest,
}

impl KitSource {
    /// Load and validate a kit from `path` (the kit directory or the
    /// manifest file itself, mirroring `ToolPackageSource::load`).
    ///
    /// Validation, all failures `RuntimeFactory` errors naming the field:
    /// - `kind` is [`KIT_KIND`], `schema_version` is [`KIT_SCHEMA_VERSION`];
    /// - `identity.name` passes the operation-store record-name rules
    ///   (`operation_store::validate_record_name`);
    /// - at least one member in `packages`; member paths are relative, stay
    ///   inside the kit root after normalization, and each loads as a
    ///   `ToolPackageSource` (which runs the package's own validation);
    /// - member package `identity.name`s are unique within the kit;
    /// - at least one row in `tools`; `tool_name`s are unique; each row's
    ///   `package` names a member and its `operation` exists in that
    ///   member's manifest `operations`; `effect_class` is one of
    ///   [`KIT_EFFECT_CLASSES`].
    ///
    /// Returns the source with `source_hash` = sha256 hex of the manifest
    /// bytes.
    pub fn load(path: impl AsRef<std::path::Path>) -> crate::VerletResult<Self> {
        let manifest_path = if path.as_ref().is_dir() {
            path.as_ref().join(KIT_MANIFEST_FILE_NAME)
        } else {
            path.as_ref().to_path_buf()
        };
        let manifest_path = std::fs::canonicalize(&manifest_path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "kit manifest_path {} could not be resolved: {err}",
                manifest_path.display()
            ))
        })?;
        let kit_root = manifest_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let bytes = std::fs::read(&manifest_path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read kit manifest_path {}: {err}",
                manifest_path.display()
            ))
        })?;
        let manifest: KitManifest = toml::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "invalid kit manifest_path {}: {err}",
                manifest_path.display()
            ))
        })?;
        validate_kit_manifest(&manifest, &kit_root)?;
        Ok(Self {
            manifest_path,
            kit_root,
            source_hash: kit_source_hash(&bytes),
            manifest,
        })
    }

    /// The member `ToolPackageSource`s in manifest order, reloaded from the
    /// paths `load` validated.
    pub fn member_packages(
        &self,
    ) -> crate::VerletResult<Vec<crate::tool_package::ToolPackageSource>> {
        load_member_packages(&self.manifest, &self.kit_root)
    }
}

/// The durable record of one installed kit: the receipt `kit install`
/// writes, and the input the default manifest synthesizes tool rows from.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledKitRecord {
    pub schema_version: u32,
    /// Kit `identity.name`; also the record's file name.
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    pub source: InstalledKitSource,
    /// sha256 of the kit manifest text at install time.
    pub source_hash: String,
    pub installed_at_ms: u64,
    pub tools: Vec<InstalledKitTool>,
}

/// Where the installed kit came from.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InstalledKitSource {
    /// A local directory install.
    Path { path: std::path::PathBuf },
    /// A git clone; `commit` pins the exact tree that was installed.
    Git { url: String, commit: String },
}

/// One installed tool row: everything the default manifest needs to emit a
/// `direct_tool` row without consulting the kit again.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledKitTool {
    pub tool_name: String,
    /// Pinned ref, always `op://<record>/<operation>@sha256:<hash>`.
    pub operation_ref: String,
    /// Kebab-case effect class, from the kit declaration.
    pub effect_class: String,
    /// The published record's capability grants at install time, for
    /// attachment-config derivation.
    pub required_capabilities: std::collections::BTreeSet<String>,
}

/// Store of installed-kit records: one JSON file per kit under the kits
/// root (`<name>.json`), written atomically (temp file + rename, like the
/// operation store's records).
#[derive(Clone, Debug)]
pub struct InstalledKitStore {
    root: std::path::PathBuf,
}

impl InstalledKitStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn record_path(&self, name: &str) -> std::path::PathBuf {
        self.root.join(format!("{name}.json"))
    }

    /// Write (or overwrite) the record for `record.name`. Creates the root
    /// if missing. Reinstalling a kit is an overwrite, not an error: the
    /// record reflects the latest install.
    pub fn save(&self, record: &InstalledKitRecord) -> crate::VerletResult<()> {
        validate_installed_kit_record(record)?;
        let path = self.record_path(&record.name);
        std::fs::create_dir_all(&self.root).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to create installed-kit root {}: {err}",
                self.root.display()
            ))
        })?;
        let tmp_path = self
            .root
            .join(format!(".verlet.tmp.{}", uuid::Uuid::now_v7()));
        let bytes = serde_json::to_vec_pretty(record).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to encode installed-kit record name {:?}: {err}",
                record.name
            ))
        })?;
        {
            let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to create temp installed-kit record {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(&bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to write temp installed-kit record {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to sync temp installed-kit record {}: {err}",
                    tmp_path.display()
                ))
            })?;
        }
        std::fs::rename(&tmp_path, &path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to atomically install installed-kit record {}: {err}",
                path.display()
            ))
        })
    }

    pub fn load(&self, name: &str) -> crate::VerletResult<Option<InstalledKitRecord>> {
        let name = validate_kit_record_name("name", name)?;
        let path = self.record_path(&name);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read installed-kit record {}: {err}",
                    path.display()
                )));
            }
        };
        let record: InstalledKitRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to decode installed-kit record {}: {err}",
                path.display()
            ))
        })?;
        validate_installed_kit_record(&record)?;
        if record.name != name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "installed-kit record {} has name {:?}, expected {:?}",
                path.display(),
                record.name,
                name
            )));
        }
        Ok(Some(record))
    }

    /// All records, sorted by kit name. A missing root is an empty list.
    pub fn list(&self) -> crate::VerletResult<Vec<InstalledKitRecord>> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read installed-kit root {}: {err}",
                    self.root.display()
                )));
            }
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read installed-kit entry in {}: {err}",
                    self.root.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            names.push(validate_kit_record_name("name", name)?);
        }
        names.sort();
        names
            .into_iter()
            .map(|name| {
                self.load(&name)?.ok_or_else(|| {
                    crate::VerletOperationsError::RuntimeFactory(format!(
                        "installed-kit record {:?} disappeared while listing",
                        name
                    ))
                })
            })
            .collect()
    }

    /// Remove the record for `name`; Ok(false) when absent.
    pub fn remove(&self, name: &str) -> crate::VerletResult<bool> {
        let name = validate_kit_record_name("name", name)?;
        let path = self.record_path(&name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to remove installed-kit record {}: {err}",
                path.display()
            ))),
        }
    }
}

fn validate_kit_manifest(
    manifest: &KitManifest,
    kit_root: &std::path::Path,
) -> crate::VerletResult<()> {
    if manifest.kind != KIT_KIND {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "kit kind must be {KIT_KIND:?}, got {:?}",
            manifest.kind
        )));
    }
    if manifest.schema_version != KIT_SCHEMA_VERSION {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "kit schema_version {} is not supported",
            manifest.schema_version
        )));
    }
    validate_kit_record_name("identity.name", &manifest.identity.name)?;
    let members = load_member_packages(manifest, kit_root)?;
    let mut member_operations = std::collections::BTreeMap::new();
    for member in members {
        let name = member.manifest.identity.name.clone();
        let operations = member
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if member_operations.insert(name.clone(), operations).is_some() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages identity.name {name:?} is duplicated"
            )));
        }
    }
    if manifest.tools.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(
            "kit tools must declare at least one row".to_string(),
        ));
    }
    let mut tool_names = std::collections::BTreeSet::new();
    for (index, tool) in manifest.tools.iter().enumerate() {
        if !tool_names.insert(tool.tool_name.clone()) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit tools.tool_name {:?} is duplicated at tools[{index}]",
                tool.tool_name
            )));
        }
        let Some(operations) = member_operations.get(&tool.package) else {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit tools.package {:?} at tools[{index}] does not name a member package",
                tool.package
            )));
        };
        if !operations.contains(&tool.operation) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit tools.operation {:?} at tools[{index}] does not exist in package {:?}",
                tool.operation, tool.package
            )));
        }
        if !KIT_EFFECT_CLASSES.contains(&tool.effect_class.as_str()) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit tools.effect_class {:?} at tools[{index}] must be one of {}",
                tool.effect_class,
                KIT_EFFECT_CLASSES.join(", ")
            )));
        }
    }
    Ok(())
}

fn load_member_packages(
    manifest: &KitManifest,
    kit_root: &std::path::Path,
) -> crate::VerletResult<Vec<crate::tool_package::ToolPackageSource>> {
    if manifest.packages.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(
            "kit packages must declare at least one member".to_string(),
        ));
    }
    let mut members = Vec::with_capacity(manifest.packages.len());
    for (index, relative_path) in manifest.packages.iter().enumerate() {
        if relative_path.as_os_str().is_empty() || relative_path.is_absolute() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages[{index}] must be a non-empty relative path, got {}",
                relative_path.display()
            )));
        }
        let joined = kit_root.join(relative_path);
        let member_root = std::fs::canonicalize(&joined).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages[{index}] {} could not be resolved: {err}",
                relative_path.display()
            ))
        })?;
        if !member_root.starts_with(kit_root) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages[{index}] {} resolves outside the kit root {}",
                relative_path.display(),
                kit_root.display()
            )));
        }
        if !member_root.is_dir() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages[{index}] {} must name a member package directory",
                relative_path.display()
            )));
        }
        let member = crate::tool_package::ToolPackageSource::load(&member_root).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "kit packages[{index}] {} failed to load as a tool package: {err}",
                relative_path.display()
            ))
        })?;
        members.push(member);
    }
    Ok(members)
}

fn validate_kit_record_name(field: &str, name: &str) -> crate::VerletResult<String> {
    crate::operation_store::validate_record_name(name).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!("kit {field} is invalid: {err}"))
    })
}

fn validate_installed_kit_record(record: &InstalledKitRecord) -> crate::VerletResult<()> {
    if record.schema_version != INSTALLED_KIT_SCHEMA_VERSION {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "installed-kit schema_version {} is not supported",
            record.schema_version
        )));
    }
    let name = validate_kit_record_name("name", &record.name)?;
    if name != record.name {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "installed-kit name {:?} did not normalize to itself",
            record.name
        )));
    }
    if record.source_hash.len() != 64
        || !record
            .source_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "installed-kit source_hash {:?} is not a sha256 hex digest",
            record.source_hash
        )));
    }
    Ok(())
}

/// The kits root that pairs with an operations registry root: the sibling
/// `kits/` directory (`.verlet/operations` maps to `.verlet/kits`). A root
/// with no usable parent falls back to `kits` beside the working directory.
pub fn kits_root_for_operations_registry_root(root: &std::path::Path) -> std::path::PathBuf {
    match root.parent() {
        Some(parent) if parent != std::path::Path::new("") => parent.join("kits"),
        _ => std::path::PathBuf::from("kits"),
    }
}

/// sha256 hex of `bytes`, matching the operation store's hashing.
pub fn kit_source_hash(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    #[test]
    fn kit_source_loads_directory_and_manifest_paths() {
        let root = valid_kit_root("kit-source-loads");
        let expected_hash = super::kit_source_hash(
            &std::fs::read(root.join(super::KIT_MANIFEST_FILE_NAME)).unwrap(),
        );

        let from_directory = super::KitSource::load(&root).unwrap();
        let from_manifest =
            super::KitSource::load(root.join(super::KIT_MANIFEST_FILE_NAME)).unwrap();

        assert_eq!(
            from_directory.kit_root,
            std::fs::canonicalize(&root).unwrap()
        );
        assert_eq!(from_directory.manifest_path, from_manifest.manifest_path);
        assert_eq!(from_directory.source_hash, expected_hash);
        assert_eq!(from_directory.member_packages().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn kit_source_rejects_wrong_kind() {
        assert_manifest_error("wrong-kind", "kind", |manifest, _| {
            manifest.kind = "other.kit".to_string();
        });
    }

    #[test]
    fn kit_source_rejects_unsupported_schema_version() {
        assert_manifest_error("wrong-schema", "schema_version", |manifest, _| {
            manifest.schema_version = 1;
        });
    }

    #[test]
    fn kit_source_rejects_invalid_identity_name() {
        assert_manifest_error("invalid-name", "identity.name", |manifest, _| {
            manifest.identity.name = "../kit".to_string();
        });
    }

    #[test]
    fn kit_source_rejects_empty_packages() {
        assert_manifest_error("empty-packages", "packages", |manifest, _| {
            manifest.packages.clear();
        });
    }

    #[test]
    fn kit_source_rejects_absolute_package_path() {
        assert_manifest_error("absolute-package", "packages", |manifest, root| {
            manifest.packages = vec![root.join("member-a")];
        });
    }

    #[test]
    fn kit_source_rejects_package_path_outside_root() {
        let root = valid_kit_root("outside-package");
        let outside = root
            .parent()
            .unwrap()
            .join(format!("kit-member-outside-{}", uuid::Uuid::now_v7()));
        write_tool_package(&outside, "outside", "outside_op");
        let mut manifest = read_manifest(&root);
        manifest.packages =
            vec![std::path::PathBuf::from("../").join(outside.file_name().unwrap())];
        write_manifest(&root, &manifest);

        let error = super::KitSource::load(&root).unwrap_err().to_string();

        assert!(error.contains("packages"), "{error}");
        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(outside);
    }

    #[test]
    fn kit_source_rejects_member_that_is_not_a_tool_package() {
        assert_manifest_error("bad-member", "packages", |manifest, root| {
            std::fs::create_dir_all(root.join("not-a-package")).unwrap();
            manifest.packages = vec![std::path::PathBuf::from("not-a-package")];
        });
    }

    #[test]
    fn kit_source_rejects_duplicate_member_names() {
        assert_manifest_error("duplicate-members", "packages", |manifest, root| {
            write_tool_package(&root.join("member-b"), "member-a", "other_op");
            manifest.packages.push(std::path::PathBuf::from("member-b"));
        });
    }

    #[test]
    fn kit_source_rejects_empty_tools() {
        assert_manifest_error("empty-tools", "tools", |manifest, _| {
            manifest.tools.clear();
        });
    }

    #[test]
    fn kit_source_rejects_duplicate_tool_names() {
        assert_manifest_error("duplicate-tools", "tools.tool_name", |manifest, _| {
            manifest.tools.push(manifest.tools[0].clone());
        });
    }

    #[test]
    fn kit_source_rejects_tool_with_unknown_package() {
        assert_manifest_error("unknown-package", "tools.package", |manifest, _| {
            manifest.tools[0].package = "missing".to_string();
        });
    }

    #[test]
    fn kit_source_rejects_tool_with_unknown_operation() {
        assert_manifest_error("unknown-operation", "tools.operation", |manifest, _| {
            manifest.tools[0].operation = "missing".to_string();
        });
    }

    #[test]
    fn kit_source_rejects_invalid_effect_class() {
        assert_manifest_error("bad-effect", "tools.effect_class", |manifest, _| {
            manifest.tools[0].effect_class = "read-only".to_string();
        });
    }

    #[test]
    fn installed_kit_store_overwrites_lists_loads_and_removes_records() {
        let root = temp_root("installed-kit-store");
        let store = super::InstalledKitStore::new(root.join("kits"));
        assert!(store.list().unwrap().is_empty());
        assert_eq!(store.load("alpha").unwrap(), None);
        assert!(!store.remove("alpha").unwrap());

        let mut alpha = installed_record("alpha", "1.0.0", 1);
        let bravo = installed_record("bravo", "2.0.0", 2);
        store.save(&bravo).unwrap();
        store.save(&alpha).unwrap();
        assert_eq!(store.list().unwrap(), vec![alpha.clone(), bravo]);

        alpha.version = Some("1.1.0".to_string());
        alpha.installed_at_ms = 3;
        store.save(&alpha).unwrap();
        assert_eq!(store.load("alpha").unwrap(), Some(alpha.clone()));
        assert!(store.remove("alpha").unwrap());
        assert_eq!(store.load("alpha").unwrap(), None);
        assert!(!store.remove("alpha").unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    fn assert_manifest_error(
        label: &str,
        field: &str,
        mutate: impl FnOnce(&mut super::KitManifest, &std::path::Path),
    ) {
        let root = valid_kit_root(label);
        let mut manifest = read_manifest(&root);
        mutate(&mut manifest, &root);
        write_manifest(&root, &manifest);

        let error = super::KitSource::load(&root).unwrap_err().to_string();

        assert!(error.contains(field), "expected {field:?} in {error:?}");
        let _ = std::fs::remove_dir_all(root);
    }

    fn valid_kit_root(label: &str) -> std::path::PathBuf {
        let root = temp_root(label);
        write_tool_package(&root.join("member-a"), "member-a", "read");
        write_manifest(
            &root,
            &super::KitManifest {
                kind: super::KIT_KIND.to_string(),
                schema_version: super::KIT_SCHEMA_VERSION,
                identity: super::KitIdentity {
                    name: "fixture-kit".to_string(),
                    version: Some("1.0.0".to_string()),
                    description: Some("Fixture kit.".to_string()),
                },
                packages: vec![std::path::PathBuf::from("member-a")],
                tools: vec![super::KitToolDeclaration {
                    tool_name: "read".to_string(),
                    package: "member-a".to_string(),
                    operation: "read".to_string(),
                    effect_class: "pure".to_string(),
                }],
            },
        );
        root
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "verlet-kit-package-{label}-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_tool_package(root: &std::path::Path, name: &str, operation: &str) {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(
            root.join("verlet.tool.toml"),
            format!(
                r#"kind = "cooldis.tool"
schema_version = 0

[identity]
name = "{name}"

[runtime]
kind = "wasm32-unknown-unknown"
bin_path = "fixture.wasm"

[[operations]]
name = "{operation}"
input_schema = "input.json"
output_schema = "output.json"

[operations.command]
name = "{operation}"
"#
            ),
        )
        .unwrap();
    }

    fn read_manifest(root: &std::path::Path) -> super::KitManifest {
        toml::from_str(&std::fs::read_to_string(root.join(super::KIT_MANIFEST_FILE_NAME)).unwrap())
            .unwrap()
    }

    fn write_manifest(root: &std::path::Path, manifest: &super::KitManifest) {
        std::fs::write(
            root.join(super::KIT_MANIFEST_FILE_NAME),
            toml::to_string_pretty(manifest).unwrap(),
        )
        .unwrap();
    }

    fn installed_record(
        name: &str,
        version: &str,
        installed_at_ms: u64,
    ) -> super::InstalledKitRecord {
        super::InstalledKitRecord {
            schema_version: super::INSTALLED_KIT_SCHEMA_VERSION,
            name: name.to_string(),
            version: Some(version.to_string()),
            source: super::InstalledKitSource::Path {
                path: std::path::PathBuf::from("/fixture/kit"),
            },
            source_hash: "0".repeat(64),
            installed_at_ms,
            tools: vec![super::InstalledKitTool {
                tool_name: "read".to_string(),
                operation_ref: format!("op://member-a/read@sha256:{}", "1".repeat(64)),
                effect_class: "pure".to_string(),
                required_capabilities: std::collections::BTreeSet::new(),
            }],
        }
    }
}
