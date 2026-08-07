//! The `blob` subcommand family.

pub(super) async fn run_blob(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_blob_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "publish" => print_blob_publish_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown blob subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "publish" => blob_publish(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown blob subcommand {subcommand:?}"
        ))),
    }
}

pub(super) async fn blob_publish(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_blob_publish_args(args)?;
    if options.help {
        print_blob_publish_help();
        return Ok(());
    }
    let file = options
        .file
        .ok_or_else(|| crate::cli::usage_error("blob publish requires <file>"))?;
    let registry_root = options
        .registry_root
        .unwrap_or_else(crate::agent::manifest::default_blob_registry_root);
    let registry = verlet_operations::blob_store::LocalBlobRegistry::new(registry_root);
    let record = registry.publish_file(&file, options.name.as_deref())?;
    println!("published blob");
    println!("artifact {}", record.artifact_hash);
    println!("content_hash {}", record.content_sha256);
    println!("ref {}", record.ref_uri);
    println!(
        "record {}",
        registry
            .version_record_path(&record.artifact_hash)?
            .display()
    );
    Ok(())
}

#[derive(Debug)]
pub(super) struct BlobPublishArgs {
    file: Option<std::path::PathBuf>,
    name: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

pub(super) fn parse_blob_publish_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<BlobPublishArgs> {
    let mut file = None;
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
                    "unknown blob publish argument {other:?}"
                )));
            }
            _ => {
                if file.is_some() {
                    return Err(crate::cli::usage_error(
                        "blob publish accepts exactly one <file>",
                    ));
                }
                file = Some(std::path::PathBuf::from(arg));
            }
        }
    }
    Ok(BlobPublishArgs {
        file,
        name,
        registry_root,
        help,
    })
}

pub(super) fn print_blob_help() {
    println!(
        "verlet blob\n\
\n\
Usage:\n\
  verlet blob publish <file> [--registry-root .verlet/blobs] [--name <name>]\n\
\n\
Blobs are immutable text or binary artifacts addressable as\n\
resource://artifact/sha256:<hash>. Agent manifests use blob resources for\n\
folder-first prompts and other static context inputs.\n"
    );
}

pub(super) fn print_blob_publish_help() {
    println!(
        "verlet blob publish\n\
\n\
Usage:\n\
  verlet blob publish <file> [--registry-root .verlet/blobs] [--name <name>]\n\
\n\
Publishes a file as a content-addressed blob artifact and prints the immutable\n\
resource://artifact/sha256:<hash> ref for manifest resource rows.\n"
    );
}
