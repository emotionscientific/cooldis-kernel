//! The `kit` subcommand family: install, list, and remove tool kits.
//!
//! A kit is surface grammar (see `verlet_operations::kit_package`): install
//! lowers it to ordinary package publishes plus one installed-kit record.
//! After install, the daemon's default manifest synthesizes a `direct_tool`
//! row per installed tool at its next startup; nothing here touches threads
//! directly.

pub(crate) async fn run_kit(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_kit_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "install" => print_kit_install_help(),
            "list" => print_kit_list_help(),
            "remove" => print_kit_remove_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "install" => kit_install(args).await,
        "list" => kit_list(args).await,
        "remove" => kit_remove(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown kit subcommand {other:?}"
        ))),
    }
}

pub(crate) fn print_kit_help() {
    println!("Usage: verlet kit <subcommand>");
    println!();
    println!("Subcommands:");
    println!("  install <path>   build, publish, and record a kit's tools");
    println!("  list             list installed kits");
    println!("  remove <name>    remove an installed kit's record");
}

fn print_kit_install_help() {
    println!("Usage: verlet kit install <kit-dir> [--registry-root <path>] [--kits-root <path>]");
    println!();
    println!("Loads verlet.kit.toml from <kit-dir>, builds and publishes each");
    println!("member package into the operation registry, and writes the");
    println!("installed-kit record. The daemon's default manifest picks up the");
    println!("kit's tools at its next startup.");
}

fn print_kit_list_help() {
    println!("Usage: verlet kit list [--kits-root <path>] [--json]");
}

fn print_kit_remove_help() {
    println!("Usage: verlet kit remove <name> [--kits-root <path>]");
    println!();
    println!("Removes the installed-kit record. Published operations stay in");
    println!("the registry; only the default-manifest availability goes away.");
}

/// Install pipeline, in order:
/// 1. `KitSource::load(<kit-dir>)` (validation happens there).
/// 2. For each member package, `build_tool_package` (the same proof gate as
///    `verlet tool publish --package`: fixtures run, interface computed),
///    then `LocalOperationRegistry::publish_artifact` with the package
///    identity name as the record name, exactly as `tool_publish` does.
/// 3. Resolve each kit tool row to its pinned ref
///    `op://<record>/<operation>@sha256:<active_artifact_hash>` and collect
///    the record's capability grants.
/// 4. Write the `InstalledKitRecord` via `InstalledKitStore::save` under
///    the kits root (default [`default_kits_root`]).
/// 5. Print a receipt: kit name, version, source hash, each published
///    record with artifact hash, each tool row with its pinned ref, the
///    record path, and a reminder that the default manifest refreshes at
///    the next daemon startup.
///
/// All-or-nothing on the record: the record is written only after every
/// member published. Publishes that succeeded before a later failure are
/// harmless (content-addressed, unreferenced) and are reported as such.
async fn kit_install(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let _ = (args, default_kits_root());
    todo!("EMO-607: kit install")
}

/// Human list: one line per kit (name, version, tool count, source); with
/// `--json`, the raw records.
async fn kit_list(args: Vec<std::ffi::OsString>) -> crate::kernel::runtime_host::VerletResult<()> {
    let _ = args;
    todo!("EMO-607: kit list")
}

async fn kit_remove(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let _ = args;
    todo!("EMO-607: kit remove")
}

/// Project-scoped kits root, sibling of the operations registry.
pub(crate) fn default_kits_root() -> std::path::PathBuf {
    std::path::PathBuf::from(".verlet/kits")
}
