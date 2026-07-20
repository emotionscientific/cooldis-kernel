//! The offline `identity` subcommand family.

use super::*;
use crate::daemon::identity::{
    IdentityAuthority, PrincipalId, PrincipalKind, SqliteIdentityAuthority,
};

/// Dispatch an offline identity-store command.
pub(super) async fn run_identity(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_identity_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "bootstrap" => print_identity_bootstrap_help(),
            "declare" => print_identity_declare_help(),
            "mint" => print_identity_mint_help(),
            "revoke-credential" => print_identity_revoke_credential_help(),
            "revoke-principal" => print_identity_revoke_principal_help(),
            "list" => print_identity_list_help(),
            other => {
                return Err(usage_error(format!(
                    "unknown identity subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "bootstrap" => identity_bootstrap(args).await,
        "declare" => identity_declare(args).await,
        "mint" => identity_mint(args).await,
        "revoke-credential" => identity_revoke_credential(args).await,
        "revoke-principal" => identity_revoke_principal(args).await,
        "list" => identity_list(args).await,
        other => Err(usage_error(format!(
            "unknown identity subcommand {other:?}; use `cooldis identity --help`"
        ))),
    }
}

/// Declare the first operator and print its credential once.
pub(super) async fn identity_bootstrap(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_bootstrap_args(args)?;
    if options.help {
        print_identity_bootstrap_help();
        return Ok(());
    }
    let principal_id = options
        .principal_id
        .ok_or_else(|| usage_error("identity bootstrap requires <principal-id>"))?;
    let display = options
        .display
        .ok_or_else(|| usage_error("identity bootstrap requires --display <display>"))?;
    let authority = open_identity_authority(options.state_home).await?;
    let (principal, credential, token) = authority
        .bootstrap_operator(&PrincipalId::new(principal_id), &display)
        .await
        .map_err(identity_cli_error)?;
    println!("principal_id {}", principal.principal_id);
    println!("credential_id {}", credential.credential_id);
    eprintln!("WARNING: identity credential token is shown once; store it securely now.");
    println!("token {token}");
    Ok(())
}

/// Declare an adapter principal in the offline identity store.
pub(super) async fn identity_declare(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_declare_args(args)?;
    if options.help {
        print_identity_declare_help();
        return Ok(());
    }
    let principal_id = options
        .principal_id
        .ok_or_else(|| usage_error("identity declare requires <principal-id>"))?;
    let kind = options
        .kind
        .ok_or_else(|| usage_error("identity declare requires --kind adapter"))?;
    let display = options
        .display
        .ok_or_else(|| usage_error("identity declare requires --display <display>"))?;
    let declared_by = options
        .declared_by
        .ok_or_else(|| usage_error("identity declare requires --declared-by <principal-id>"))?;
    let authority = open_identity_authority(options.state_home).await?;
    let record = authority
        .declare_principal(
            &PrincipalId::new(declared_by),
            &PrincipalId::new(principal_id),
            kind,
            &display,
        )
        .await
        .map_err(identity_cli_error)?;
    println!("declared principal {} kind=adapter", record.principal_id);
    Ok(())
}

/// Mint and print one credential for an active principal.
pub(super) async fn identity_mint(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_mint_args(args)?;
    if options.help {
        print_identity_mint_help();
        return Ok(());
    }
    let principal_id = options
        .principal_id
        .ok_or_else(|| usage_error("identity mint requires <principal-id>"))?;
    let minted_by = options
        .minted_by
        .ok_or_else(|| usage_error("identity mint requires --minted-by <principal-id>"))?;
    let authority = open_identity_authority(options.state_home).await?;
    let (credential, token) = authority
        .mint_credential(
            &PrincipalId::new(minted_by),
            &PrincipalId::new(principal_id),
            options.expires_at_ms,
        )
        .await
        .map_err(identity_cli_error)?;
    println!("credential_id {}", credential.credential_id);
    eprintln!("WARNING: identity credential token is shown once; store it securely now.");
    println!("token {token}");
    Ok(())
}

/// Revoke one credential in the offline identity store.
pub(super) async fn identity_revoke_credential(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_revoke_args(args, "identity revoke-credential")?;
    if options.help {
        print_identity_revoke_credential_help();
        return Ok(());
    }
    let credential_id = options
        .subject_id
        .ok_or_else(|| usage_error("identity revoke-credential requires <credential-id>"))?;
    let revoked_by = options.revoked_by.ok_or_else(|| {
        usage_error("identity revoke-credential requires --revoked-by <principal-id>")
    })?;
    let authority = open_identity_authority(options.state_home).await?;
    authority
        .revoke_credential(&PrincipalId::new(revoked_by), &credential_id)
        .await
        .map_err(identity_cli_error)?;
    println!("revoked credential {credential_id}");
    Ok(())
}

/// Revoke one principal in the offline identity store.
pub(super) async fn identity_revoke_principal(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_revoke_args(args, "identity revoke-principal")?;
    if options.help {
        print_identity_revoke_principal_help();
        return Ok(());
    }
    let principal_id = options
        .subject_id
        .ok_or_else(|| usage_error("identity revoke-principal requires <principal-id>"))?;
    let revoked_by = options.revoked_by.ok_or_else(|| {
        usage_error("identity revoke-principal requires --revoked-by <principal-id>")
    })?;
    let authority = open_identity_authority(options.state_home).await?;
    authority
        .revoke_principal(
            &PrincipalId::new(revoked_by),
            &PrincipalId::new(&principal_id),
        )
        .await
        .map_err(identity_cli_error)?;
    println!("revoked principal {principal_id}");
    Ok(())
}

/// Print redacted principal and credential records from the identity store.
pub(super) async fn identity_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_identity_list_args(args)?;
    if options.help {
        print_identity_list_help();
        return Ok(());
    }
    let authority = open_identity_authority(options.state_home).await?;
    let principals = authority
        .list_principals()
        .await
        .map_err(identity_cli_error)?;
    let mut credentials = Vec::new();
    for principal in &principals {
        for credential in authority
            .list_credentials(&principal.principal_id)
            .await
            .map_err(identity_cli_error)?
        {
            credentials.push(json!({
                "credential_id": credential.credential_id,
                "principal_id": credential.principal_id,
                "minted_by": credential.minted_by,
                "minted_at_ms": credential.minted_at_ms,
                "expires_at_ms": credential.expires_at_ms,
                "revoked_at_ms": credential.revoked_at_ms,
            }));
        }
    }
    let principals = principals
        .into_iter()
        .map(|principal| {
            json!({
                "principal_id": principal.principal_id,
                "kind": principal.kind,
                "display": principal.display,
                "declared_by": principal.declared_by,
                "declared_at_ms": principal.declared_at_ms,
                "revoked_at_ms": principal.revoked_at_ms,
            })
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "principals": principals,
            "credentials": credentials,
        }))
        .map_err(|error| {
            CooldisError::RuntimeFactory(format!("failed to encode identity list: {error}"))
        })?
    );
    Ok(())
}

#[derive(Debug)]
struct IdentityBootstrapArgs {
    principal_id: Option<String>,
    display: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct IdentityDeclareArgs {
    principal_id: Option<String>,
    kind: Option<PrincipalKind>,
    display: Option<String>,
    declared_by: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct IdentityMintArgs {
    principal_id: Option<String>,
    minted_by: Option<String>,
    expires_at_ms: Option<i64>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct IdentityRevokeArgs {
    subject_id: Option<String>,
    revoked_by: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct IdentityListArgs {
    state_home: Option<PathBuf>,
    help: bool,
}

fn parse_identity_bootstrap_args(args: Vec<OsString>) -> CooldisResult<IdentityBootstrapArgs> {
    let mut principal_id = None;
    let mut display = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--display" => display = Some(required_string_value(&mut iter, "--display")?),
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown identity bootstrap argument {other:?}"
                )));
            }
            _ => set_identity_subject(&mut principal_id, arg, "identity bootstrap")?,
        }
    }
    Ok(IdentityBootstrapArgs {
        principal_id,
        display,
        state_home,
        help,
    })
}

fn parse_identity_declare_args(args: Vec<OsString>) -> CooldisResult<IdentityDeclareArgs> {
    let mut principal_id = None;
    let mut kind = None;
    let mut display = None;
    let mut declared_by = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--kind" => {
                let value = required_string_value(&mut iter, "--kind")?;
                kind = Some(parse_declarable_cli_kind(&value)?);
            }
            "--display" => display = Some(required_string_value(&mut iter, "--display")?),
            "--declared-by" => {
                declared_by = Some(required_string_value(&mut iter, "--declared-by")?)
            }
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown identity declare argument {other:?}"
                )));
            }
            _ => set_identity_subject(&mut principal_id, arg, "identity declare")?,
        }
    }
    Ok(IdentityDeclareArgs {
        principal_id,
        kind,
        display,
        declared_by,
        state_home,
        help,
    })
}

fn parse_identity_mint_args(args: Vec<OsString>) -> CooldisResult<IdentityMintArgs> {
    let mut principal_id = None;
    let mut minted_by = None;
    let mut expires_at_ms = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--minted-by" => minted_by = Some(required_string_value(&mut iter, "--minted-by")?),
            "--expires-at-ms" => {
                let value = required_string_value(&mut iter, "--expires-at-ms")?;
                expires_at_ms = Some(value.parse::<i64>().map_err(|_| {
                    usage_error("identity mint --expires-at-ms must be an integer")
                })?);
            }
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown identity mint argument {other:?}"
                )));
            }
            _ => set_identity_subject(&mut principal_id, arg, "identity mint")?,
        }
    }
    Ok(IdentityMintArgs {
        principal_id,
        minted_by,
        expires_at_ms,
        state_home,
        help,
    })
}

fn parse_identity_revoke_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<IdentityRevokeArgs> {
    let mut subject_id = None;
    let mut revoked_by = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--revoked-by" => revoked_by = Some(required_string_value(&mut iter, "--revoked-by")?),
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => set_identity_subject(&mut subject_id, arg, command)?,
        }
    }
    Ok(IdentityRevokeArgs {
        subject_id,
        revoked_by,
        state_home,
        help,
    })
}

fn parse_identity_list_args(args: Vec<OsString>) -> CooldisResult<IdentityListArgs> {
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other => {
                return Err(usage_error(format!(
                    "unknown identity list argument {other:?}"
                )));
            }
        }
    }
    Ok(IdentityListArgs { state_home, help })
}

fn set_identity_subject(
    subject: &mut Option<String>,
    value: OsString,
    command: &str,
) -> CooldisResult<()> {
    if subject.is_some() {
        return Err(usage_error(format!(
            "{command} accepts exactly one positional id"
        )));
    }
    *subject = Some(value.to_string_lossy().to_string());
    Ok(())
}

fn required_string_value(
    iter: &mut impl Iterator<Item = OsString>,
    option: &str,
) -> CooldisResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().to_string())
        .ok_or_else(|| usage_error(format!("{option} requires a value")))
}

fn parse_declarable_cli_kind(value: &str) -> CooldisResult<PrincipalKind> {
    match value {
        "adapter" => Ok(PrincipalKind::Adapter),
        "member" => Err(usage_error(
            "member principals are reserved and cannot be declared in identity plane v0",
        )),
        "operator" => Err(usage_error(
            "identity declare only creates adapter principals; use identity bootstrap for the first operator",
        )),
        other => Err(usage_error(format!(
            "unknown identity principal kind {other:?}; expected adapter"
        ))),
    }
}

async fn open_identity_authority(
    state_home: Option<PathBuf>,
) -> CooldisResult<SqliteIdentityAuthority> {
    let state_home = match state_home {
        Some(state_home) => state_home,
        None => default_user_state_home()?,
    };
    let store = SqliteSessionStore::open(state_home.join("session_history.sqlite3"))
        .await
        .map_err(identity_cli_error)?;
    let clock: Arc<dyn crate::DaemonClock> = Arc::new(SystemDaemonClock);
    SqliteIdentityAuthority::new(store, clock, None)
        .await
        .map_err(identity_cli_error)
}

fn identity_cli_error(error: impl std::fmt::Display) -> CooldisError {
    let message = error.to_string();
    if turso_cross_process_lock_error(&message) {
        cross_process_database_guidance("stop the daemon and retry")
    } else {
        CooldisError::RuntimeFactory(format!("identity store failed: {message}"))
    }
}

/// Print help for the identity subcommand family.
pub(super) fn print_identity_help() {
    println!(
        "cooldis identity\n\
\n\
Usage:\n\
  cooldis identity bootstrap <principal-id> --display <display> [--state-home ~/.cooldis/state]\n\
  cooldis identity declare <principal-id> --kind adapter --display <display> --declared-by <principal-id> [--state-home ~/.cooldis/state]\n\
  cooldis identity mint <principal-id> --minted-by <principal-id> [--expires-at-ms <ms>] [--state-home ~/.cooldis/state]\n\
  cooldis identity revoke-credential <credential-id> --revoked-by <principal-id> [--state-home ~/.cooldis/state]\n\
  cooldis identity revoke-principal <principal-id> --revoked-by <principal-id> [--state-home ~/.cooldis/state]\n\
  cooldis identity list [--state-home ~/.cooldis/state]\n\
\n\
Manages daemon identity records directly in the offline session store. Stop the\n\
daemon before running these commands. Credential tokens are printed only when minted.\n"
    );
}

/// Print help for `identity bootstrap`.
pub(super) fn print_identity_bootstrap_help() {
    println!(
        "cooldis identity bootstrap\n\
\n\
Usage:\n\
  cooldis identity bootstrap <principal-id> --display <display> [--state-home ~/.cooldis/state]\n\
\n\
Declares the first operator and mints its credential atomically. The credential\n\
token is shown once. Bootstrap refuses a store with an active operator.\n"
    );
}

/// Print help for `identity declare`.
pub(super) fn print_identity_declare_help() {
    println!(
        "cooldis identity declare\n\
\n\
Usage:\n\
  cooldis identity declare <principal-id> --kind adapter --display <display> --declared-by <principal-id> [--state-home ~/.cooldis/state]\n\
\n\
Declares an adapter principal. Member principals are reserved in v0.\n"
    );
}

/// Print help for `identity mint`.
pub(super) fn print_identity_mint_help() {
    println!(
        "cooldis identity mint\n\
\n\
Usage:\n\
  cooldis identity mint <principal-id> --minted-by <principal-id> [--expires-at-ms <ms>] [--state-home ~/.cooldis/state]\n\
\n\
Mints a credential for an active principal. The credential token is shown once.\n"
    );
}

/// Print help for `identity revoke-credential`.
pub(super) fn print_identity_revoke_credential_help() {
    println!(
        "cooldis identity revoke-credential\n\
\n\
Usage:\n\
  cooldis identity revoke-credential <credential-id> --revoked-by <principal-id> [--state-home ~/.cooldis/state]\n"
    );
}

/// Print help for `identity revoke-principal`.
pub(super) fn print_identity_revoke_principal_help() {
    println!(
        "cooldis identity revoke-principal\n\
\n\
Usage:\n\
  cooldis identity revoke-principal <principal-id> --revoked-by <principal-id> [--state-home ~/.cooldis/state]\n"
    );
}

/// Print help for `identity list`.
pub(super) fn print_identity_list_help() {
    println!(
        "cooldis identity list\n\
\n\
Usage:\n\
  cooldis identity list [--state-home ~/.cooldis/state]\n\
\n\
Prints principals and redacted credential metadata.\n"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declare_parser_rejects_reserved_member_kind() {
        let error = parse_identity_declare_args(vec![
            OsString::from("member:reserved"),
            OsString::from("--kind"),
            OsString::from("member"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("member principals are reserved"));
    }
}
