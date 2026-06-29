use cooldis::{
    CodexCliRuntimeFactory, CodexRuntimeConfig, CooldisSupervisor, TenantRegistration,
    TenantRuntimeContext, ThreadEvent, ThreadStartRequest, ThreadTopology,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::env::current_dir()?;
    let codex_bin = std::env::var_os("COOLDIS_CODEX_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("codex"));
    let smoke_home = std::env::temp_dir().join("cooldis-live-smoke");
    let codex_home = smoke_home.join("codex-home");
    let sqlite_home = smoke_home.join("sqlite");
    std::fs::create_dir_all(&codex_home)?;
    std::fs::create_dir_all(&sqlite_home)?;

    let supervisor = CooldisSupervisor::new();
    let tenant_context = TenantRuntimeContext::local("local_smoke", &smoke_home, &smoke_home);
    supervisor
        .register_tenant(TenantRegistration {
            context: tenant_context,
            runtime_factory: Arc::new(CodexCliRuntimeFactory::new(
                CodexRuntimeConfig::local(&codex_home, &sqlite_home, cwd)
                    .with_codex_bin(codex_bin)
                    .with_extra_arg("--help"),
            )),
        })
        .await?;

    let thread = supervisor
        .start_thread(ThreadStartRequest {
            tenant_id: "local_smoke".to_string(),
            user_id: "local_user".to_string(),
            session_id: "local_session".to_string(),
            topology: ThreadTopology::root(),
            metadata: Default::default(),
        })
        .await?;
    let mut events = thread.subscribe_events();
    supervisor
        .submit_to(&thread.context().coordinates, "smoke", "print help")
        .await?;

    let saw_help = loop {
        let event = timeout(Duration::from_secs(5), events.recv()).await??;
        match event {
            ThreadEvent::Output { text, .. } => {
                break text.contains("Run Codex non-interactively")
                    || text.contains("Usage: codex exec");
            }
            ThreadEvent::Failed { message, .. } => return Err(message.into()),
            _ => {}
        }
    };

    supervisor
        .shutdown_thread_at(&thread.context().coordinates)
        .await?;

    if saw_help {
        println!("cooldis live smoke ok");
        Ok(())
    } else {
        Err("codex help output did not contain expected text".into())
    }
}
