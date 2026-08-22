//! The `auth` subcommand family.

use std::io::Read as _;

pub(crate) async fn run_auth(
    mut args: Vec<std::ffi::OsString>,
    client: Option<crate::cli::InstanceClient>,
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
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "login" => print_auth_login_help(),
            "status" => print_auth_status_help(),
            "set" => print_auth_set_help(),
            "delete" => print_auth_delete_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown auth subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    let mut client = client.ok_or_else(|| {
        crate::cli::usage_error("auth command did not receive an instance connection")
    })?;
    let result = match subcommand.to_string_lossy().as_ref() {
        "login" => auth_login(args, &mut client).await,
        "status" => auth_status(args, &mut client).await,
        "set" => auth_set(args, &mut client).await,
        "delete" => auth_delete(args, &mut client).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown auth subcommand {other:?}; use `verlet auth --help`"
        ))),
    };
    let close = client.close().await;
    result?;
    close
}

pub(crate) async fn auth_login(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_login_args(args)?;
    if options.help {
        print_auth_login_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth login requires <provider-id>"))?;
    if provider_id != verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID {
        return Err(crate::cli::usage_error(format!(
            "provider {provider_id:?} does not support OAuth login; expected openai-codex"
        )));
    }
    let oauth_client =
        crate::openai_codex::OpenAICodexOAuthClient::new().map_err(openai_codex_auth_error)?;
    let credential = if options.device {
        device_login(&oauth_client).await?
    } else {
        let login = oauth_client
            .begin_browser_login()
            .await
            .map_err(openai_codex_auth_error)?;
        let authorization_url = login.authorization_url().to_string();
        println!("OpenAI login URL: {authorization_url}");
        match crate::cli::console::open_browser_url_checked(&authorization_url).await {
            Ok(()) => oauth_client
                .complete_browser_login(login)
                .await
                .map_err(openai_codex_auth_error)?,
            Err(err) => {
                eprintln!("could not open a browser ({err}); falling back to device login");
                drop(login);
                device_login(&oauth_client).await?
            }
        }
    };
    let identity = oauth_identity(&credential);
    let verlet_metadata::provider_store::LlmProviderCredential::OAuth {
        access,
        refresh,
        expires_at_ms,
        account_id,
        email,
    } = credential
    else {
        return Err(crate::cli::usage_error(
            "OpenAI OAuth login returned an API-key credential",
        ));
    };
    client
        .model_provider_auth_set_oauth_typed(
            &provider_id,
            &access,
            &refresh,
            expires_at_ms,
            account_id.as_deref(),
            email.as_deref(),
        )
        .await?;
    match identity.email.as_deref().or(identity.account_id.as_deref()) {
        Some(account) => println!("signed in to {provider_id} as {account}"),
        None => println!("signed in to {provider_id}"),
    }
    Ok(())
}

async fn device_login(
    client: &crate::openai_codex::OpenAICodexOAuthClient,
) -> crate::kernel::runtime_host::VerletResult<verlet_metadata::provider_store::LlmProviderCredential>
{
    let login = client
        .start_device_login()
        .await
        .map_err(openai_codex_auth_error)?;
    println!("Open {}", login.verification_uri);
    println!("Enter code {}", login.user_code);
    client
        .complete_device_login(login)
        .await
        .map_err(openai_codex_auth_error)
}

fn openai_codex_auth_error(
    err: crate::openai_codex::OpenAICodexError,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
}

#[derive(Default)]
struct OAuthIdentity {
    account_id: Option<String>,
    email: Option<String>,
}

fn oauth_identity(
    credential: &verlet_metadata::provider_store::LlmProviderCredential,
) -> OAuthIdentity {
    match credential {
        verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            expires_at_ms: _,
            account_id,
            email,
            ..
        } => OAuthIdentity {
            account_id: account_id.clone(),
            email: email.clone(),
        },
        verlet_metadata::provider_store::LlmProviderCredential::ApiKey { .. } => {
            OAuthIdentity::default()
        }
    }
}

pub(crate) async fn auth_status(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_name_args(args, "auth status")?;
    if options.help {
        print_auth_status_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth status requires <provider-id>"))?;
    let result = client.model_provider_auth_status_for(&provider_id).await?;
    let value = result.get("auth").cloned().ok_or_else(|| {
        crate::cli::usage_error("modelProvider/auth/status response did not include auth")
    })?;
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

pub(crate) async fn auth_set(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_set_args(args)?;
    if options.help {
        print_auth_set_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth set requires <provider-id>"))?;
    validate_auth_set_provider(&provider_id)?;
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
    client
        .model_provider_auth_set_typed(&provider_id, &value)
        .await?;
    println!("stored provider credential {provider_id}");
    Ok(())
}

pub(crate) async fn auth_delete(
    args: Vec<std::ffi::OsString>,
    client: &mut crate::cli::InstanceClient,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_auth_name_args(args, "auth delete")?;
    if options.help {
        print_auth_delete_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| crate::cli::usage_error("auth delete requires <provider-id>"))?;
    client
        .model_provider_auth_delete_typed(&provider_id)
        .await?;
    println!("deleted provider credential {provider_id}");
    Ok(())
}

fn validate_auth_set_provider(provider_id: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    if provider_id == verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID {
        return Err(crate::cli::usage_error(
            "openai-codex uses OAuth; run `verlet auth login openai-codex` instead of `verlet auth set`",
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct AuthSetArgs {
    provider_id: Option<String>,
    api_key_stdin: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct AuthLoginArgs {
    provider_id: Option<String>,
    device: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct AuthNameArgs {
    provider_id: Option<String>,
    help: bool,
}

pub(crate) fn parse_auth_set_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<AuthSetArgs> {
    let mut provider_id = None;
    let mut api_key_stdin = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--api-key-stdin" => api_key_stdin = true,
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
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
        help,
    })
}

pub(crate) fn parse_auth_login_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<AuthLoginArgs> {
    let mut provider_id = None;
    let mut device = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--device" => device = true,
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown auth login argument {other:?}"
                )));
            }
            _ => {
                if provider_id.is_some() {
                    return Err(crate::cli::usage_error(
                        "auth login accepts exactly one <provider-id>",
                    ));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthLoginArgs {
        provider_id,
        device,
        help,
    })
}

pub(crate) fn parse_auth_name_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<AuthNameArgs> {
    let mut provider_id = None;
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
                if provider_id.is_some() {
                    return Err(crate::cli::usage_error(format!(
                        "{command} accepts exactly one <provider-id>"
                    )));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthNameArgs { provider_id, help })
}

pub(crate) fn print_auth_help() {
    println!(
        "verlet auth\n\
\n\
Usage:\n\
  verlet auth login openai-codex [--device] [--state-home ~/.verlet/state]\n\
  verlet auth status <provider-id> [--state-home ~/.verlet/state]\n\
  verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]\n\
  verlet auth delete <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Manages model-provider credentials through the running instance. OAuth and\n\
API-key secret values are never printed.\n"
    );
}

pub(crate) fn print_auth_login_help() {
    println!(
        "verlet auth login\n\
\n\
Usage:\n\
  verlet auth login openai-codex [--device] [--state-home ~/.verlet/state]\n\
\n\
Signs in with an OpenAI ChatGPT plan. The default PKCE flow opens a browser and\n\
listens on 127.0.0.1:1455; --device supports headless environments. Tokens are\n\
sent only to the connected instance and are never printed.\n"
    );
}

pub(crate) fn print_auth_status_help() {
    println!(
        "verlet auth status\n\
\n\
Usage:\n\
  verlet auth status <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Prints redacted model-provider credential status.\n"
    );
}

pub(crate) fn print_auth_set_help() {
    println!(
        "verlet auth set\n\
\n\
Usage:\n\
  verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]\n\
\n\
Stores a model-provider API key read from stdin. The stored value is never printed.\n"
    );
}

pub(crate) fn print_auth_delete_help() {
    println!(
        "verlet auth delete\n\
\n\
Usage:\n\
  verlet auth delete <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Deletes a stored model-provider credential.\n"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn api_key_set_rejects_openai_codex_before_reading_stdin() {
        let error = crate::cli::auth::validate_auth_set_provider("openai-codex").unwrap_err();

        assert!(error.to_string().contains("verlet auth login openai-codex"));
    }

    #[test]
    fn login_args_accept_device_and_state_home() {
        let parsed = crate::cli::auth::parse_auth_login_args(
            [
                "openai-codex",
                "--device",
                "--state-home",
                "/tmp/verlet-auth-state",
            ]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect(),
        )
        .unwrap();

        assert_eq!(parsed.provider_id.as_deref(), Some("openai-codex"));
        assert!(parsed.device);
    }
}
