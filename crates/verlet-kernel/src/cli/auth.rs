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
        "login" => auth_login(args).await,
        "status" => auth_status(args).await,
        "set" => auth_set(args).await,
        "delete" => auth_delete(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown auth subcommand {other:?}; use `verlet auth --help`"
        ))),
    }
}

pub(super) async fn auth_login(
    args: Vec<std::ffi::OsString>,
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
    let store = crate::cli::secret::open_provider_store(options.state_home).await?;
    let client =
        crate::openai_codex::OpenAICodexOAuthClient::new().map_err(openai_codex_auth_error)?;
    let credential = if options.device {
        device_login(&client).await?
    } else {
        let login = client
            .begin_browser_login()
            .await
            .map_err(openai_codex_auth_error)?;
        let authorization_url = login.authorization_url().to_string();
        println!("OpenAI login URL: {authorization_url}");
        match crate::cli::console::open_browser_url(&authorization_url) {
            Ok(()) => client
                .complete_browser_login(login)
                .await
                .map_err(openai_codex_auth_error)?,
            Err(err) => {
                eprintln!("could not open a browser ({err}); falling back to device login");
                drop(login);
                device_login(&client).await?
            }
        }
    };
    let identity = oauth_identity(&credential);
    store
        .set_credential(&provider_id, credential)
        .await
        .map_err(crate::cli::secret::provider_cli_error)?;
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
    expires_at_ms: Option<i64>,
}

fn oauth_identity(
    credential: &verlet_metadata::provider_store::LlmProviderCredential,
) -> OAuthIdentity {
    match credential {
        verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            expires_at_ms,
            account_id,
            email,
            ..
        } => OAuthIdentity {
            account_id: account_id.clone(),
            email: email.clone(),
            expires_at_ms: Some(*expires_at_ms),
        },
        verlet_metadata::provider_store::LlmProviderCredential::ApiKey { .. } => {
            OAuthIdentity::default()
        }
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
    let credential = store
        .get_credential(&provider.provider_id)
        .await
        .map_err(crate::cli::secret::provider_cli_error)?;
    let value = auth_status_value(&provider, &status, credential.as_ref());
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

fn auth_status_value(
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
    status: &verlet_metadata::provider_store::LlmProviderAuthStatus,
    credential: Option<&verlet_metadata::provider_store::LlmProviderCredential>,
) -> serde_json::Value {
    let identity = credential.map(oauth_identity).unwrap_or_default();
    let credential_type = match credential {
        Some(verlet_metadata::provider_store::LlmProviderCredential::OAuth { .. }) => Some("oauth"),
        Some(verlet_metadata::provider_store::LlmProviderCredential::ApiKey { .. }) => {
            Some("api_key")
        }
        None => None,
    };
    serde_json::json!({
        "provider_id": provider.provider_id,
        "display_name": provider.display_name,
        "configured": status.configured,
        "source": status.source,
        "label": status.label,
        "signed_in": credential_type == Some("oauth"),
        "credential_type": credential_type,
        "account_id": identity.account_id,
        "email": identity.email,
        "expires_at_ms": identity.expires_at_ms,
    })
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
pub(super) struct AuthLoginArgs {
    provider_id: Option<String>,
    device: bool,
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

pub(super) fn parse_auth_login_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<AuthLoginArgs> {
    let mut provider_id = None;
    let mut device = false;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--device" => device = true,
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
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
  verlet auth login openai-codex [--device] [--state-home ~/.verlet/state]\n\
  verlet auth status <provider-id> [--state-home ~/.verlet/state]\n\
  verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]\n\
  verlet auth delete <provider-id> [--state-home ~/.verlet/state]\n\
\n\
Manages model-provider credentials in the local metadata store. OAuth and API-key\n\
secret values are never printed.\n"
    );
}

pub(super) fn print_auth_login_help() {
    println!(
        "verlet auth login\n\
\n\
Usage:\n\
  verlet auth login openai-codex [--device] [--state-home ~/.verlet/state]\n\
\n\
Signs in with an OpenAI ChatGPT plan. The default PKCE flow opens a browser and\n\
listens on 127.0.0.1:1455; --device supports headless environments. Tokens are\n\
stored only in the local provider store and are never printed.\n"
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

#[cfg(test)]
mod tests {
    #[test]
    fn login_args_accept_device_and_state_home() {
        let parsed = super::parse_auth_login_args(
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
        assert_eq!(
            parsed.state_home.as_deref(),
            Some(std::path::Path::new("/tmp/verlet-auth-state"))
        );
    }

    #[test]
    fn oauth_status_reports_account_without_tokens() {
        let provider = verlet_metadata::provider_store::default_openai_codex_llm_provider_record();
        let status = verlet_metadata::provider_store::LlmProviderAuthStatus::configured(
            verlet_metadata::provider_store::LlmProviderAuthSourceKind::Stored,
            "stored credential",
        );
        let credential = verlet_metadata::provider_store::LlmProviderCredential::OAuth {
            access: "secret-access".to_string(),
            refresh: "secret-refresh".to_string(),
            expires_at_ms: 1_900_000_000_000,
            account_id: Some("acct-123".to_string()),
            email: Some("user@example.com".to_string()),
        };

        let value = super::auth_status_value(&provider, &status, Some(&credential));
        assert_eq!(value["signed_in"], true);
        assert_eq!(value["credential_type"], "oauth");
        assert_eq!(value["account_id"], "acct-123");
        assert_eq!(value["email"], "user@example.com");
        let json = value.to_string();
        assert!(!json.contains("secret-access"));
        assert!(!json.contains("secret-refresh"));
    }
}
