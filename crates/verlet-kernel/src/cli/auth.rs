//! The `auth` subcommand family.

use std::io::Read as _;
use verlet_metadata::provider_store::LlmProviderAuthStore as _;
use verlet_metadata::provider_store::LlmProviderCatalogStore as _;

pub(super) async fn run_auth(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_auth_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "status" => auth_status(args).await,
        "set" => auth_set(args).await,
        "delete" => auth_delete(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown auth subcommand {other:?}; use `verlet auth --help`"
        ))),
    }
}

pub(super) async fn auth_status(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_name_args(args, "auth status")?;
    if options.help {
        print_auth_status_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth status requires <provider-id>"))?;
    let store = crate::cli::secret::open_provider_store(options.state_home).await?;
    let provider = store
        .get_provider(&provider_id)
        .await
        .map_err(crate::cli::secret::provider_cli_error)?
        .ok_or_else(|| {
            crate::cli::usage_error(format!("provider {provider_id:?} was not found"))
        })?;
    let status = verlet_metadata::provider_store::llm_provider_auth_status(
        &store,
        &provider,
        &verlet_metadata::provider_store::LlmProviderAuthContext::new(),
    )
    .await
    .map_err(crate::cli::secret::provider_cli_error)?;
    let value = serde_json::json!({
        "provider_id": provider.provider_id,
        "display_name": provider.display_name,
        "configured": status.configured,
        "source": status.source,
        "label": status.label,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to encode auth status: {err}"
            ))
        })?
    );
    Ok(())
}

pub(super) async fn auth_set(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_set_args(args)?;
    if options.help {
        print_auth_set_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth set requires <provider-id>"))?;
    if !options.api_key_stdin {
        return Err(crate::cli::usage_error("auth set requires --api-key-stdin"));
    }
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(crate::cli::io_error)?;
    let value = crate::cli::secret::trim_stdin_secret_value(value);
    if value.is_empty() {
        return Err(crate::cli::usage_error(
            "auth set requires a non-empty API key",
        ));
    }
    let store = crate::cli::secret::open_provider_store(options.state_home).await?;
    if store
        .get_provider(&provider_id)
        .await
        .map_err(crate::cli::secret::provider_cli_error)?
        .is_none()
    {
        return Err(crate::cli::usage_error(format!(
            "provider {provider_id:?} was not found"
        )));
    }
    store
        .set_credential(
            &provider_id,
            verlet_metadata::provider_store::LlmProviderCredential::ApiKey { key: value },
        )
        .await
        .map_err(crate::cli::secret::provider_cli_error)?;
    println!("stored provider credential {provider_id}");
    Ok(())
}

pub(super) async fn auth_delete(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_name_args(args, "auth delete")?;
    if options.help {
        print_auth_delete_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth delete requires <provider-id>"))?;
    let store = crate::cli::secret::open_provider_store(options.state_home).await?;
    store
        .delete_credential(&provider_id)
        .await
        .map_err(crate::cli::secret::provider_cli_error)?;
    println!("deleted provider credential {provider_id}");
    Ok(())
}

#[derive(Debug)]
pub(super) struct AuthSetArgs {
    provider_id: Option<String>,
    api_key_stdin: bool,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(super) struct AuthNameArgs {
    provider_id: Option<String>,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

pub(super) fn parse_auth_set_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<AuthSetArgs> {
    let mut provider_id = None;
    let mut api_key_stdin = false;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--api-key-stdin" => api_key_stdin = true,
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown auth set argument {other:?}"
                )));
            }
            _ => {
                if provider_id.is_some() {
                    return Err(crate::cli::usage_error(
                        "auth set accepts exactly one <provider-id>",
                    ));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthSetArgs {
        provider_id,
        api_key_stdin,
        state_home,
        help,
    })
}

pub(super) fn parse_auth_name_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<AuthNameArgs> {
    let mut provider_id = None;
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
                if provider_id.is_some() {
                    return Err(crate::cli::usage_error(format!(
                        "{command} accepts exactly one <provider-id>"
                    )));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthNameArgs {
        provider_id,
        state_home,
        help,
    })
}

pub(super) fn print_auth_help() {
    println!(
        "verlet auth\n\
\n\
Usage:\n\
  verlet auth status <provider-id> [--state-home ~/.verlet/state]\n\
  verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]\n\
  verlet auth delete <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Manages model-provider credentials in the local metadata store. Values are read\n\
from stdin and never printed.\n"
    );
}

pub(super) fn print_auth_status_help() {
    println!(
        "verlet auth status\n\
\n\
Usage:\n\
  verlet auth status <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Prints redacted model-provider credential status.\n"
    );
}

pub(super) fn print_auth_set_help() {
    println!(
        "verlet auth set\n\
\n\
Usage:\n\
  verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]\n\
\n\
Stores a model-provider API key read from stdin. The stored value is never printed.\n"
    );
}

pub(super) fn print_auth_delete_help() {
    println!(
        "verlet auth delete\n\
\n\
Usage:\n\
  verlet auth delete <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Deletes a stored model-provider credential.\n"
    );
}
