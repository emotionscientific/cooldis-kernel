use verlet::daemon::identity::IdentityAuthority as _;

#[tokio::test]
async fn bootstrap_prints_one_secret_and_refuses_a_second_root() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let first = run_identity([
        "identity",
        "bootstrap",
        "operator:root",
        "--display",
        "Root operator",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(
        first.status.success(),
        "bootstrap failed\nstderr:\n{}",
        stderr(&first)
    );
    let first_stdout = stdout(&first);
    let first_stderr = stderr(&first);
    let token = first_stdout
        .lines()
        .find_map(|line| line.strip_prefix("token "))
        .expect("bootstrap should print the token once");
    assert!(token.starts_with("verlet_id_"));
    assert_eq!(first_stdout.matches(token).count(), 1);
    assert!(first_stderr.contains("shown once"));

    let store_path = state_home.join("session_history.sqlite3");
    let store = verlet::SqliteSessionStore::open(&store_path).await.unwrap();
    let clock: std::sync::Arc<dyn verlet::DaemonClock> =
        std::sync::Arc::new(verlet::SystemDaemonClock);
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(store, clock, None)
        .await
        .unwrap();
    let principals = authority.list_principals().await.unwrap();
    assert_eq!(principals.len(), 1);
    assert_eq!(
        principals[0].principal_id,
        verlet::daemon::identity::PrincipalId::new("operator:root")
    );
    assert!(authority.verify_token(token).await.unwrap().is_some());
    drop(authority);

    let second = run_identity([
        "identity",
        "bootstrap",
        "operator:other",
        "--display",
        "Other operator",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(!second.status.success());
    assert!(stderr(&second).contains("active operator"));
    assert!(!stdout(&second).contains("verlet_id_"));
    assert!(!stderr(&second).contains("verlet_id_"));

    remove_sqlite_state(&state_home);
}

#[tokio::test]
async fn offline_identity_commands_manage_adapters_without_reprinting_secrets() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let bootstrap = run_identity([
        "identity",
        "bootstrap",
        "operator:root",
        "--display",
        "Root operator",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(bootstrap.status.success(), "{}", stderr(&bootstrap));

    let member = run_identity([
        "identity",
        "declare",
        "member:reserved",
        "--kind",
        "member",
        "--display",
        "Reserved member",
        "--declared-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(!member.status.success());
    assert!(stderr(&member).contains("member principals are reserved"));

    let declare = run_identity([
        "identity",
        "declare",
        "adapter:webhook",
        "--kind",
        "adapter",
        "--display",
        "Webhook adapter",
        "--declared-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(declare.status.success(), "{}", stderr(&declare));

    let mint = run_identity([
        "identity",
        "mint",
        "adapter:webhook",
        "--minted-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(mint.status.success(), "{}", stderr(&mint));
    let mint_stdout = stdout(&mint);
    let credential_id = mint_stdout
        .lines()
        .find_map(|line| line.strip_prefix("credential_id "))
        .unwrap();
    let token = mint_stdout
        .lines()
        .find_map(|line| line.strip_prefix("token "))
        .unwrap();

    let list = run_identity(["identity", "list", "--state-home", &state_home_arg]);
    assert!(list.status.success(), "{}", stderr(&list));
    let list_stdout = stdout(&list);
    assert!(list_stdout.contains("adapter:webhook"));
    assert!(list_stdout.contains(credential_id));
    assert!(!list_stdout.contains(token));
    assert!(!list_stdout.contains("token_digest"));
    assert!(!list_stdout.contains("sha256:"));
    let listed: serde_json::Value = serde_json::from_str(&list_stdout).unwrap();
    let kinds = listed["principals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|principal| principal["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(kinds.len(), 2);
    assert!(kinds.contains(&"operator"));
    assert!(kinds.contains(&"adapter"));
    assert!(!kinds.contains(&"member"));

    let revoke_credential = run_identity([
        "identity",
        "revoke-credential",
        credential_id,
        "--revoked-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(
        revoke_credential.status.success(),
        "{}",
        stderr(&revoke_credential)
    );

    let second_mint = run_identity([
        "identity",
        "mint",
        "adapter:webhook",
        "--minted-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(second_mint.status.success(), "{}", stderr(&second_mint));
    let second_token = stdout(&second_mint)
        .lines()
        .find_map(|line| line.strip_prefix("token "))
        .unwrap()
        .to_string();
    let revoke_principal = run_identity([
        "identity",
        "revoke-principal",
        "adapter:webhook",
        "--revoked-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(
        revoke_principal.status.success(),
        "{}",
        stderr(&revoke_principal)
    );

    let store = verlet::SqliteSessionStore::open(state_home.join("session_history.sqlite3"))
        .await
        .unwrap();
    let clock: std::sync::Arc<dyn verlet::DaemonClock> =
        std::sync::Arc::new(verlet::SystemDaemonClock);
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(store, clock, None)
        .await
        .unwrap();
    assert!(authority.verify_token(token).await.unwrap().is_none());
    assert!(
        authority
            .verify_token(&second_token)
            .await
            .unwrap()
            .is_none()
    );
    drop(authority);

    remove_sqlite_state(&state_home);
}

#[test]
fn revoking_a_bogus_id_fails_without_printing_success() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let revoke = run_identity([
        "identity",
        "revoke-credential",
        "credential_missing",
        "--revoked-by",
        "operator:root",
        "--state-home",
        &state_home_arg,
    ]);

    assert!(!revoke.status.success());
    assert!(stderr(&revoke).contains("credential was not found"));
    assert!(!stdout(&revoke).contains("revoked"));
    assert!(!stderr(&revoke).contains("revoked credential"));

    remove_sqlite_state(&state_home);
}

#[tokio::test]
async fn locked_store_tells_the_user_to_stop_the_daemon() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let store = verlet::SqliteSessionStore::open(state_home.join("session_history.sqlite3"))
        .await
        .unwrap();
    let clock: std::sync::Arc<dyn verlet::DaemonClock> =
        std::sync::Arc::new(verlet::SystemDaemonClock);
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(store, clock, None)
        .await
        .unwrap();

    let list = run_identity(["identity", "list", "--state-home", &state_home_arg]);
    assert!(!list.status.success());
    assert!(stderr(&list).contains("another process holds this database"));
    assert!(stderr(&list).contains("stop the daemon and retry"));

    drop(authority);
    remove_sqlite_state(&state_home);
}

fn run_identity<const N: usize>(args: [&str; N]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args(args)
        .output()
        .expect("failed to run verlet identity command")
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn temp_state_home() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("verlet-identity-cli-{}", uuid::Uuid::now_v7()))
}

fn remove_sqlite_state(state_home: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let path = std::path::PathBuf::from(format!(
            "{}{}",
            state_home.join("session_history.sqlite3").display(),
            suffix
        ));
        let _ = std::fs::remove_file(path);
    }
    let _ = std::fs::remove_dir(state_home);
}
