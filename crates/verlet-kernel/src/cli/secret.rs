//! The `secret` subcommand family and shared local metadata-store access.

use std::io::Read as _;
#[cfg(test)]
mod tests;

pub(super) async fn run_secret(mut args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_secret_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "import" => print_secret_import_help(),
            "set" => print_secret_set_help(),
            "list" => print_secret_list_help(),
            "status" => print_secret_status_help(),
            "delete" => print_secret_delete_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown secret subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "import" => secret_import(args).await,
        "set" => secret_set(args).await,
        "list" => secret_list(args).await,
        "status" => secret_status(args).await,
        "delete" => secret_delete(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown secret subcommand {subcommand:?}"
        ))),
    }
}

pub(super) async fn secret_import(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_secret_import_args(args)?;
    if options.help {
        print_secret_import_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret import requires <name>"))?;
    let from_env = options
        .from_env
        .ok_or_else(|| crate::cli::usage_error("secret import requires --from-env <ENV>"))?;
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .import_secret_from_env(&name, &from_env)
        .await
        .map_err(secret_cli_error)?;
    println!("imported secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

pub(super) async fn secret_set(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_secret_set_args(args)?;
    if options.help {
        print_secret_set_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret set requires <name>"))?;
    if !options.value_stdin {
        return Err(crate::cli::usage_error("secret set requires --value-stdin"));
    }
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(crate::cli::io_error)?;
    let value = trim_stdin_secret_value(value);
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .set_secret(&name, value, crate::SecretSourceKind::Stdin, None)
        .await
        .map_err(secret_cli_error)?;
    println!("stored secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

pub(super) async fn secret_list(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_secret_list_args(args, "secret list")?;
    if options.help {
        print_secret_list_help();
        return Ok(());
    }
    let store = open_secret_store(options.state_home).await?;
    let statuses = store.list().await.map_err(secret_cli_error)?;
    if statuses.is_empty() {
        println!("no secrets");
        return Ok(());
    }
    for status in statuses {
        println!(
            "{}\t{}\tupdated_at_ms={}",
            status.name,
            secret_source_display(&status),
            status.updated_at_ms
        );
    }
    Ok(())
}

pub(super) async fn secret_status(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_secret_name_args(args, "secret status")?;
    if options.help {
        print_secret_status_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret status requires <name>"))?;
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .status(&name)
        .await
        .map_err(secret_cli_error)?
        .ok_or_else(|| crate::cli::usage_error(format!("secret {name:?} was not found")))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!("failed to encode secret status: {err}"))
        })?
    );
    Ok(())
}

pub(super) async fn secret_delete(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    let options = parse_secret_name_args(args, "secret delete")?;
    if options.help {
        print_secret_delete_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret delete requires <name>"))?;
    let store = open_secret_store(options.state_home).await?;
    if store.delete_secret(&name).await.map_err(secret_cli_error)? {
        println!("deleted secret {name}");
    } else {
        println!("secret {name} was not found");
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct SecretImportArgs {
    name: Option<String>,
    from_env: Option<String>,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct SecretSetArgs {
    name: Option<String>,
    value_stdin: bool,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct SecretNameArgs {
    name: Option<String>,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct SecretListArgs {
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

pub(super) fn parse_secret_import_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<SecretImportArgs> {
    let mut name = None;
    let mut from_env = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--from-env" => {
                from_env = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--from-env",
                )?)
            }
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown secret import argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "secret import accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretImportArgs {
        name,
        from_env,
        state_home,
        help,
    })
}

pub(super) fn parse_secret_set_args(
    args: Vec<std::ffi::OsString>,
) -> crate::VerletResult<SecretSetArgs> {
    let mut name = None;
    let mut value_stdin = false;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--value-stdin" => value_stdin = true,
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown secret set argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "secret set accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretSetArgs {
        name,
        value_stdin,
        state_home,
        help,
    })
}

pub(super) fn parse_secret_name_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::VerletResult<SecretNameArgs> {
    let mut name = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(format!(
                        "{command} accepts exactly one <name>"
                    )));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretNameArgs {
        name,
        state_home,
        help,
    })
}

pub(super) fn parse_secret_list_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::VerletResult<SecretListArgs> {
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(SecretListArgs { state_home, help })
}

pub(super) fn default_project_state_home() -> std::path::PathBuf {
    let legacy = std::path::PathBuf::from(concat!(".", "cool", "dis/state"));
    if std::path::Path::new(".verlet").exists() || !legacy.exists() {
        std::path::PathBuf::from(".verlet/state")
    } else {
        eprintln!(
            "warning: {} is deprecated; existing state will continue to be used in place through v0.3.0",
            legacy.display()
        );
        legacy
    }
}

pub(super) fn default_user_state_home() -> crate::VerletResult<std::path::PathBuf> {
    Ok(crate::cli::console::default_user_verlet_home()?.join("state"))
}

pub(super) fn metadata_store_path_for_state_home(
    state_home: Option<std::path::PathBuf>,
    default_state_home: std::path::PathBuf,
) -> std::path::PathBuf {
    state_home
        .unwrap_or(default_state_home)
        .join("metadata.sqlite3")
}

pub(super) async fn open_secret_store(
    state_home: Option<std::path::PathBuf>,
) -> crate::VerletResult<crate::SqliteSecretStore> {
    crate::SqliteSecretStore::open(metadata_store_path_for_state_home(
        state_home,
        default_user_state_home()?,
    ))
    .await
    .map_err(|err| {
        if turso_cross_process_lock_error(&err.to_string()) {
            cross_process_database_guidance("stop the daemon and retry")
        } else {
            secret_cli_error(err)
        }
    })
}

pub(super) async fn open_provider_store(
    state_home: Option<std::path::PathBuf>,
) -> crate::VerletResult<crate::SqliteMetadataStore> {
    let store = crate::SqliteMetadataStore::open(metadata_store_path_for_state_home(
        state_home,
        default_user_state_home()?,
    ))
    .await
    .map_err(|err| {
        if turso_cross_process_lock_error(&err.to_string()) {
            cross_process_database_guidance(
                "use the running daemon's modelProvider RPC or stop the daemon and retry",
            )
        } else {
            provider_cli_error(err)
        }
    })?;
    crate::seed_default_llm_providers(&store)
        .await
        .map_err(provider_cli_error)?;
    Ok(store)
}

pub(super) fn cross_process_database_guidance(alternative: &str) -> crate::VerletError {
    crate::cli::usage_error(format!(
        "another process holds this database (most likely the verlet daemon); {alternative}"
    ))
}

pub(super) fn turso_cross_process_lock_error(message: &str) -> bool {
    // turso 0.7.0-pre.18 erases LimboError::LockingError through turso::Error.
    let Some((_, engine_error)) = message.split_once("sqlite engine error: ") else {
        return false;
    };
    engine_error == "Locking error: Failed locking file. File is locked by another process"
        || (engine_error.starts_with("Locking error: Failed locking file '")
            && engine_error.ends_with("'. File is locked by another process"))
}

pub(super) fn secret_cli_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(format!("secret store failed: {err}"))
}

pub(super) fn provider_cli_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(format!("provider store failed: {err}"))
}

pub(super) fn secret_source_display(status: &crate::SecretStatus) -> String {
    match (&status.source_kind, status.source_label.as_deref()) {
        (crate::SecretSourceKind::Env, Some(label)) => format!("env:{label}"),
        (crate::SecretSourceKind::Env, None) => "env".to_string(),
        (crate::SecretSourceKind::Stdin, _) => "stdin".to_string(),
        (crate::SecretSourceKind::Local, Some(label)) => format!("local:{label}"),
        (crate::SecretSourceKind::Local, None) => "local".to_string(),
    }
}

pub(super) fn trim_stdin_secret_value(mut value: String) -> String {
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    value
}

pub(super) fn print_secret_help() {
    println!(
        "verlet secret\n\
\n\
Usage:\n\
  verlet secret import <name> --from-env <ENV> [--state-home ~/.verlet/state]\n\
  verlet secret set <name> --value-stdin [--state-home ~/.verlet/state]\n\
  verlet secret list [--state-home ~/.verlet/state]\n\
  verlet secret status <name> [--state-home ~/.verlet/state]\n\
  verlet secret delete <name> [--state-home ~/.verlet/state]\n\
\n\
Stores local secret refs for host-mediated tool calls. List and status output\n\
redact values; tool runtimes receive only manifest-declared secret names.\n"
    );
}

pub(super) fn print_secret_import_help() {
    println!(
        "verlet secret import\n\
\n\
Usage:\n\
  verlet secret import <name> --from-env <ENV> [--state-home ~/.verlet/state]\n\
\n\
Imports a local environment variable into the Verlet secret store under a\n\
stable secret name such as EXAMPLE_API_KEY.\n"
    );
}

pub(super) fn print_secret_set_help() {
    println!(
        "verlet secret set\n\
\n\
Usage:\n\
  verlet secret set <name> --value-stdin [--state-home ~/.verlet/state]\n\
\n\
Stores a secret value read from stdin. The stored value is never printed.\n"
    );
}

pub(super) fn print_secret_list_help() {
    println!(
        "verlet secret list\n\
\n\
Usage:\n\
  verlet secret list [--state-home ~/.verlet/state]\n\
\n\
Lists configured secret refs without printing secret values.\n"
    );
}

pub(super) fn print_secret_status_help() {
    println!(
        "verlet secret status\n\
\n\
Usage:\n\
  verlet secret status <name> [--state-home ~/.verlet/state]\n\
\n\
Prints redacted metadata for one secret ref.\n"
    );
}

pub(super) fn print_secret_delete_help() {
    println!(
        "verlet secret delete\n\
\n\
Usage:\n\
  verlet secret delete <name> [--state-home ~/.verlet/state]\n\
\n\
Deletes a local secret ref.\n"
    );
}
