//! The `secret` subcommand family.

use std::io::Read as _;

pub(crate) async fn run_secret(
    mut args: Vec<std::ffi::OsString>,
    client: Option<crate::cli::InstanceClient>,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
    let action = subcommand.to_string_lossy();
    if !matches!(
        action.as_ref(),
        "import" | "set" | "list" | "status" | "delete"
    ) {
        return Err(crate::cli::usage_error(format!(
            "unknown secret subcommand {action:?}"
        )));
    }
    let mut client = client.ok_or_else(|| {
        crate::cli::usage_error("secret command did not receive an instance connection")
    })?;
    let result = match action.as_ref() {
        "import" => secret_import(args, &mut client).await,
        "set" => secret_set(args, &mut client).await,
        "list" => secret_list(args, &mut client).await,
        "status" => secret_status(args, &mut client).await,
        "delete" => secret_delete(args, &mut client).await,
        _ => unreachable!("validated secret action"),
    };
    let close = client.close().await;
    result?;
    close
}

pub(crate) async fn secret_import(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
    let value = std::env::var(&from_env).map_err(|_| {
        secret_cli_error(
            verlet_metadata::secret_store::SecretStoreError::MissingEnv {
                secret_name: name.clone(),
                env_name: from_env.clone(),
            },
        )
    })?;
    let result = client
        .secret_set(
            &name,
            &value,
            verlet_metadata::secret_store::SecretSourceKind::Env,
            Some(&from_env),
        )
        .await?;
    let status = required_secret_status(&result, "secret/set")?;
    println!("imported secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

pub(crate) async fn secret_set(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
    let result = client
        .secret_set(
            &name,
            &value,
            verlet_metadata::secret_store::SecretSourceKind::Stdin,
            None,
        )
        .await?;
    let status = required_secret_status(&result, "secret/set")?;
    println!("stored secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

pub(crate) async fn secret_list(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_secret_list_args(args, "secret list")?;
    if options.help {
        print_secret_list_help();
        return Ok(());
    }
    let result = client.secret_list().await?;
    let statuses = serde_json::from_value::<Vec<verlet_metadata::secret_store::SecretStatus>>(
        result
            .get("data")
            .cloned()
            .ok_or_else(|| crate::cli::usage_error("secret/list response did not include data"))?,
    )
    .map_err(|error| crate::cli::usage_error(format!("invalid secret/list response: {error}")))?;
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

pub(crate) async fn secret_status(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_secret_name_args(args, "secret status")?;
    if options.help {
        print_secret_status_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret status requires <name>"))?;
    let result = client.secret_status(&name).await?;
    let status = optional_secret_status(&result, "secret/status")?
        .ok_or_else(|| crate::cli::usage_error(format!("secret {name:?} was not found")))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to encode secret status: {err}"
            ))
        })?
    );
    Ok(())
}

pub(crate) async fn secret_delete(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_secret_name_args(args, "secret delete")?;
    if options.help {
        print_secret_delete_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("secret delete requires <name>"))?;
    let result = client.secret_delete(&name).await?;
    let deleted = result
        .get("deleted")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| crate::cli::usage_error("secret/delete response did not include deleted"))?;
    if deleted {
        println!("deleted secret {name}");
    } else {
        println!("secret {name} was not found");
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct SecretImportArgs {
    name: Option<String>,
    from_env: Option<String>,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct SecretSetArgs {
    name: Option<String>,
    value_stdin: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct SecretNameArgs {
    name: Option<String>,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct SecretListArgs {
    help: bool,
}

pub(crate) fn parse_secret_import_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<SecretImportArgs> {
    let mut name = None;
    let mut from_env = None;
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
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
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
        help,
    })
}

pub(crate) fn parse_secret_set_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<SecretSetArgs> {
    let mut name = None;
    let mut value_stdin = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--value-stdin" => value_stdin = true,
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
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
        help,
    })
}

pub(crate) fn parse_secret_name_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<SecretNameArgs> {
    let mut name = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
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
    Ok(SecretNameArgs { name, help })
}

pub(crate) fn parse_secret_list_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<SecretListArgs> {
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(SecretListArgs { help })
}

pub(crate) fn default_user_state_home()
-> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    Ok(crate::cli::console::default_user_verlet_home()?.join("state"))
}

pub(crate) fn secret_cli_error(
    err: impl std::fmt::Display,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!("secret store failed: {err}"))
}

fn required_secret_status(
    result: &serde_json::Value,
    method: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_metadata::secret_store::SecretStatus> {
    optional_secret_status(result, method)?
        .ok_or_else(|| crate::cli::usage_error(format!("{method} response did not include status")))
}

fn optional_secret_status(
    result: &serde_json::Value,
    method: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_metadata::secret_store::SecretStatus>>
{
    let status = result.get("status").cloned().ok_or_else(|| {
        crate::cli::usage_error(format!("{method} response did not include status"))
    })?;
    serde_json::from_value(status)
        .map_err(|error| crate::cli::usage_error(format!("invalid {method} response: {error}")))
}

pub(crate) fn secret_source_display(
    status: &verlet_metadata::secret_store::SecretStatus,
) -> String {
    match (&status.source_kind, status.source_label.as_deref()) {
        (verlet_metadata::secret_store::SecretSourceKind::Env, Some(label)) => {
            format!("env:{label}")
        }
        (verlet_metadata::secret_store::SecretSourceKind::Env, None) => "env".to_string(),
        (verlet_metadata::secret_store::SecretSourceKind::Stdin, _) => "stdin".to_string(),
        (verlet_metadata::secret_store::SecretSourceKind::Local, Some(label)) => {
            format!("local:{label}")
        }
        (verlet_metadata::secret_store::SecretSourceKind::Local, None) => "local".to_string(),
    }
}

pub(crate) fn trim_stdin_secret_value(mut value: String) -> String {
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    value
}

pub(crate) fn print_secret_help() {
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

pub(crate) fn print_secret_import_help() {
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

pub(crate) fn print_secret_set_help() {
    println!(
        "verlet secret set\n\
\n\
Usage:\n\
  verlet secret set <name> --value-stdin [--state-home ~/.verlet/state]\n\
\n\
Stores a secret value read from stdin. The stored value is never printed.\n"
    );
}

pub(crate) fn print_secret_list_help() {
    println!(
        "verlet secret list\n\
\n\
Usage:\n\
  verlet secret list [--state-home ~/.verlet/state]\n\
\n\
Lists configured secret refs without printing secret values.\n"
    );
}

pub(crate) fn print_secret_status_help() {
    println!(
        "verlet secret status\n\
\n\
Usage:\n\
  verlet secret status <name> [--state-home ~/.verlet/state]\n\
\n\
Prints redacted metadata for one secret ref.\n"
    );
}

pub(crate) fn print_secret_delete_help() {
    println!(
        "verlet secret delete\n\
\n\
Usage:\n\
  verlet secret delete <name> [--state-home ~/.verlet/state]\n\
\n\
Deletes a local secret ref.\n"
    );
}
