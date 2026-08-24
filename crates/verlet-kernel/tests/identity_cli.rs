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
    let store = verlet_history_sqlite::SqliteSessionStore::open(&store_path)
        .await
        .unwrap();
    let clock: std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock);
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
async fn identity_clients_manage_adapters_without_reprinting_secrets() {
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
        "--state-home",
        &state_home_arg,
    ]);
    assert!(declare.status.success(), "{}", stderr(&declare));

    let mint = run_identity([
        "identity",
        "mint",
        "adapter:webhook",
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
    let token_digest = mint_stdout
        .lines()
        .find_map(|line| line.strip_prefix("token_digest="))
        .unwrap();
    assert_eq!(
        token_digest,
        verlet::daemon::identity::identity_token_digest(token)
    );

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
        "--state-home",
        &state_home_arg,
    ]);
    assert!(
        revoke_principal.status.success(),
        "{}",
        stderr(&revoke_principal)
    );

    wait_for_endpoint_removal(&state_home).await;
    let store = open_session_store_after_owner_exit(&state_home).await;
    let clock: std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock);
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

#[tokio::test]
async fn revoking_a_bogus_id_fails_without_printing_success() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let revoke = run_identity([
        "identity",
        "revoke-credential",
        "credential_missing",
        "--state-home",
        &state_home_arg,
    ]);

    assert!(!revoke.status.success());
    assert!(stderr(&revoke).contains("credential was not found"));
    assert!(!stdout(&revoke).contains("revoked"));
    assert!(!stderr(&revoke).contains("revoked credential"));

    wait_for_endpoint_removal(&state_home).await;
    remove_sqlite_state(&state_home);
}

#[tokio::test]
async fn bootstrap_on_a_locked_store_tells_the_user_to_stop_the_running_instance() {
    let state_home = temp_state_home();
    std::fs::create_dir_all(&state_home).unwrap();
    let state_home_arg = state_home.to_string_lossy().to_string();
    let store =
        verlet_history_sqlite::SqliteSessionStore::open(state_home.join("session_history.sqlite3"))
            .await
            .unwrap();
    let clock: std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock);
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(store, clock, None)
        .await
        .unwrap();

    let bootstrap = run_identity([
        "identity",
        "bootstrap",
        "operator:blocked",
        "--display",
        "Blocked operator",
        "--state-home",
        &state_home_arg,
    ]);
    assert!(!bootstrap.status.success());
    assert!(stderr(&bootstrap).contains("another process holds this database"));
    assert!(stderr(&bootstrap).contains("stop that instance and retry"));

    drop(authority);
    remove_sqlite_state(&state_home);
}

fn run_identity<const N: usize>(args: [&str; N]) -> std::process::Output {
    let state_home = args
        .iter()
        .position(|arg| *arg == "--state-home")
        .and_then(|index| args.get(index + 1))
        .map(std::path::PathBuf::from)
        .expect("identity CLI test requires --state-home");
    let client_home = state_home.join("client-home");
    std::fs::create_dir_all(&client_home).unwrap();
    std::fs::write(
        client_home.join("config.toml"),
        "[daemon]\nidle_timeout = \"300ms\"\n",
    )
    .unwrap();
    std::process::Command::new(env!("CARGO_BIN_EXE_verlet"))
        .args(args)
        .env("HOME", &client_home)
        .env("VERLET_HOME", &client_home)
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
    let _ = std::fs::remove_dir_all(state_home);
}

async fn wait_for_endpoint_removal(state_home: &std::path::Path) {
    let endpoint = state_home.join("endpoint.json");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if !endpoint.exists() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {} to be removed",
            endpoint.display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn open_session_store_after_owner_exit(
    state_home: &std::path::Path,
) -> verlet_history_sqlite::SqliteSessionStore {
    let path = state_home.join("session_history.sqlite3");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match verlet_history_sqlite::SqliteSessionStore::open(&path).await {
            Ok(store) => return store,
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting to reopen {}: {error}",
                    path.display()
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
}
