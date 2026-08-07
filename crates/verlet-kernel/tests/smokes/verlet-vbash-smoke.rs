#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = verlet::kernel::supervisor::VerletSupervisor::new();
    supervisor
        .register_tenant(verlet::kernel::supervisor::TenantRegistration {
            context: verlet::kernel::supervisor::TenantRuntimeContext::local(
                "tenant-smoke",
                "/tmp/verlet-vbash-runtime",
                "/tmp/verlet-vbash-state",
            ),
            runtime_factory: std::sync::Arc::new(
                verlet::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
        })
        .await?;

    let thread = supervisor
        .start_thread(verlet::kernel::supervisor::ThreadStartRequest {
            tenant_id: "tenant-smoke".to_string(),
            user_id: "user-smoke".to_string(),
            session_id: "session-smoke".to_string(),
            topology: verlet_runtime_contracts::ThreadTopology::root(),
            metadata: Default::default(),
        })
        .await?;
    let mut events = thread.subscribe_events();

    supervisor
        .submit(
            "tenant-smoke",
            thread.context().coordinates.thread_id,
            "turn-smoke",
            "echo hi > /workspace/a && cat /workspace/a",
        )
        .await?;

    let output = tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            match events.recv().await? {
                verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                    break Ok::<_, broadcast_error>(text);
                }
                verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                    message, ..
                } => {
                    break Err(broadcast_error::failed(message));
                }
                _ => {}
            }
        }
    })
    .await??;

    if !output.contains("hi") {
        return Err(format!("unexpected virtual bash output: {output:?}").into());
    }

    supervisor.shutdown_all().await?;
    println!("verlet vbash smoke ok: {}", output.trim());
    Ok(())
}

#[allow(non_camel_case_types)]
#[derive(Debug)]
enum broadcast_error {
    Recv(tokio::sync::broadcast::error::RecvError),
    Failed(String),
}

impl broadcast_error {
    fn failed(message: String) -> Self {
        Self::Failed(message)
    }
}

impl std::fmt::Display for broadcast_error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recv(err) => write!(f, "{err}"),
            Self::Failed(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for broadcast_error {}

impl From<tokio::sync::broadcast::error::RecvError> for broadcast_error {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        Self::Recv(err)
    }
}
