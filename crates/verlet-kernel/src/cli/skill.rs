//! The `skill` subcommand family.

pub(super) async fn run_skill(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_skill_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "publish" => print_skill_publish_help(),
            "import" => print_skill_import_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown skill subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "publish" => skill_publish(args).await,
        "import" => skill_import(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown skill subcommand {subcommand:?}"
        ))),
    }
}

pub(super) async fn skill_publish(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_skill_publish_args(args)?;
    if options.help {
        print_skill_publish_help();
        return Ok(());
    }
    let package_dir = options
        .package_dir
        .ok_or_else(|| crate::cli::usage_error("skill publish requires <dir>"))?;
    let registry_root = skill_registry_root(options.registry_root);
    let registry = verlet_operations::skill_package::LocalSkillRegistry::new(registry_root);
    let record = registry.publish_directory(
        verlet_operations::skill_package::PublishSkillPackageRequest {
            package_dir,
            name: options.name,
        },
    )?;
    println!("published {}", record.name);
    println!("artifact {}", record.active_artifact_hash);
    println!("ref {}", record.ref_uri());
    println!("floating skill://{}", record.name);
    println!("record {}", registry.record_path(&record.name)?.display());
    for skill in record.package.skills {
        println!("skill {}", skill.name);
    }
    Ok(())
}

pub(super) async fn skill_import(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_skill_import_args(args)?;
    if options.help {
        print_skill_import_help();
        return Ok(());
    }
    let skill_dir = options
        .skill_dir
        .ok_or_else(|| crate::cli::usage_error("skill import requires <dir>"))?;
    let skill_registry_root = skill_registry_root(options.registry_root);
    let blob_registry_root = options
        .blob_registry_root
        .unwrap_or_else(crate::agent::manifest::default_blob_registry_root);
    let plan = verlet_operations::skill_import::SkillImportPlan::from_directory(
        &skill_dir,
        options.name.as_deref(),
    )?;

    let record_path = if options.dry_run {
        println!("dry-run {}", plan.package.name);
        None
    } else {
        let skill_registry =
            verlet_operations::skill_package::LocalSkillRegistry::new(&skill_registry_root);
        let published = plan.publish(
            &skill_registry,
            &verlet_operations::blob_store::LocalBlobRegistry::new(&blob_registry_root),
        )?;
        println!("published {}", published.skill.name);
        Some(skill_registry.record_path(&published.skill.name)?)
    };
    println!("artifact {}", plan.artifact_hash()?);
    println!("ref {}", plan.pinned_ref()?);
    println!("floating {}", plan.floating_ref());
    if let Some(record_path) = record_path {
        println!("record {}", record_path.display());
    }
    for skill in &plan.package.skills {
        println!("skill {}", skill.name);
    }
    for reference in &plan.references {
        println!("reference {reference} appended");
    }
    for asset in &plan.assets {
        println!("blob {} {}", asset.relative_path, asset.ref_uri);
    }
    for script in &plan.omitted_scripts {
        println!("omitted script {script}");
    }
    for hook in &plan.ignored_hooks {
        println!("ignored hook {hook}");
    }
    for file in &plan.skipped_files {
        println!("skipped file {file}");
    }
    println!("manifest fragment:");
    print!("{}", plan.manifest_fragment()?);
    Ok(())
}

#[derive(Debug)]
pub(super) struct SkillPublishArgs {
    package_dir: Option<std::path::PathBuf>,
    name: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct SkillImportArgs {
    skill_dir: Option<std::path::PathBuf>,
    name: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    blob_registry_root: Option<std::path::PathBuf>,
    dry_run: bool,
    help: bool,
}

pub(super) fn parse_skill_publish_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<SkillPublishArgs> {
    let mut package_dir = None;
    let mut name = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--name" => {
                name = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--name",
                )?)
            }
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown skill publish argument {other:?}"
                )));
            }
            _ => {
                if package_dir.is_some() {
                    return Err(crate::cli::usage_error(
                        "skill publish accepts exactly one <dir>",
                    ));
                }
                package_dir = Some(std::path::PathBuf::from(arg));
            }
        }
    }
    Ok(SkillPublishArgs {
        package_dir,
        name,
        registry_root,
        help,
    })
}

pub(super) fn parse_skill_import_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<SkillImportArgs> {
    let mut skill_dir = None;
    let mut name = None;
    let mut registry_root = None;
    let mut blob_registry_root = None;
    let mut dry_run = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--name" => {
                name = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--name",
                )?)
            }
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            "--blob-registry-root" => {
                blob_registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--blob-registry-root",
                )?)
            }
            "--dry-run" => dry_run = true,
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown skill import argument {other:?}"
                )));
            }
            _ => {
                if skill_dir.is_some() {
                    return Err(crate::cli::usage_error(
                        "skill import accepts exactly one <dir>",
                    ));
                }
                skill_dir = Some(std::path::PathBuf::from(arg));
            }
        }
    }
    Ok(SkillImportArgs {
        skill_dir,
        name,
        registry_root,
        blob_registry_root,
        dry_run,
        help,
    })
}

pub(super) fn skill_registry_root(registry_root: Option<std::path::PathBuf>) -> std::path::PathBuf {
    registry_root.unwrap_or_else(|| {
        let legacy = std::path::PathBuf::from(concat!(".", "cool", "dis/skills"));
        if std::path::Path::new(".verlet").exists() || !legacy.exists() {
            std::path::PathBuf::from(".verlet/skills")
        } else {
            eprintln!(
                "warning: {} is deprecated; existing state will continue to be used in place through v0.3.0",
                legacy.display()
            );
            legacy
        }
    })
}

pub(super) fn print_skill_help() {
    println!(
        "verlet skill\n\
\n\
Usage:\n\
  verlet skill publish <dir> [--registry-root .verlet/skills] [--name <package>]\n\
  verlet skill import <dir> [--registry-root .verlet/skills] [--blob-registry-root .verlet/blobs] [--name <package>] [--dry-run]\n\
\n\
Skills are markdown context resources. Publishing turns a directory of\n\
<name>/SKILL.md files into one content-addressed skill:// package for agent\n\
manifest resource rows. Import compiles one ecosystem SKILL.md directory into\n\
the same package and blob registries.\n"
    );
}

pub(super) fn print_skill_publish_help() {
    println!(
        "verlet skill publish\n\
\n\
Usage:\n\
  verlet skill publish <dir> [--registry-root .verlet/skills] [--name <package>]\n\
\n\
Publishes a deterministic skill package from <dir>/<skill>/SKILL.md files.\n\
Optional frontmatter may declare name, description, and trigger_hint; without\n\
frontmatter, the skill name is the directory name and the description is the\n\
first non-heading markdown line. Prints both the pinned content-addressed ref\n\
and the floating package-name ref.\n"
    );
}

pub(super) fn print_skill_import_help() {
    println!(
        "verlet skill import\n\
\n\
Usage:\n\
  verlet skill import <dir> [--registry-root .verlet/skills] [--blob-registry-root .verlet/blobs] [--name <package>] [--dry-run]\n\
\n\
Compiles one conventional SKILL.md directory into an ordinary published skill\n\
package. Markdown files directly under references/ are appended to the skill\n\
body and assets/ files publish as immutable blobs. Scripts are not converted;\n\
their paths are written into the body and model-visible index as degradation.\n\
Hook and MCP configuration remains inert and is reported as ignored. Files\n\
outside those classes are skipped and reported. Prints\n\
pinned and floating skill refs, blob refs, and ready-to-paste [[resources]]\n\
rows. --dry-run prints the same deterministic plan without registry writes.\n"
    );
}
