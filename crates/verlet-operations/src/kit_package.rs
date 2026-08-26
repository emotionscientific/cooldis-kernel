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
        let _ = path;
        todo!("EMO-607: load and validate verlet.kit.toml")
    }

    /// The member `ToolPackageSource`s in manifest order, reloaded from the
    /// paths `load` validated.
    pub fn member_packages(
        &self,
    ) -> crate::VerletResult<Vec<crate::tool_package::ToolPackageSource>> {
        todo!("EMO-607: reload member packages")
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
        let _ = record;
        todo!("EMO-607: atomic installed-kit record write")
    }

    pub fn load(&self, name: &str) -> crate::VerletResult<Option<InstalledKitRecord>> {
        let _ = name;
        todo!("EMO-607: load installed-kit record")
    }

    /// All records, sorted by kit name. A missing root is an empty list.
    pub fn list(&self) -> crate::VerletResult<Vec<InstalledKitRecord>> {
        todo!("EMO-607: list installed-kit records")
    }

    /// Remove the record for `name`; Ok(false) when absent.
    pub fn remove(&self, name: &str) -> crate::VerletResult<bool> {
        let _ = name;
        todo!("EMO-607: remove installed-kit record")
    }
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
