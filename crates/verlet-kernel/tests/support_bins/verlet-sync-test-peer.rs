use std::io::Write as _;
use verlet::daemon::remote_store::propagator::StreamPropagator as _;
use verlet_history::EventStore as _;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sync test peer failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("child-once") => {
            let child_db = required_arg(&mut args, "child db path")?;
            let label = args.next().filter(|label| label != "-");
            run_child(child_db, label, false).await
        }
        Some("child-park") => {
            let child_db = required_arg(&mut args, "child db path")?;
            let label = required_arg(&mut args, "child event label")?;
            run_child(child_db, Some(label), true).await
        }
        _ => Err("expected child-once or child-park".into()),
    }
}

async fn run_child(
    child_db: String,
    label: Option<String>,
    park: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint_url = required_env("VERLET_SYNC_TEST_URL")?;
    let bearer_token = required_env("VERLET_SYNC_TEST_TOKEN")?;
    let stream_id = verlet_history::EventStreamId::new(required_env("VERLET_SYNC_TEST_STREAM")?);
    let grant: verlet::daemon::remote_store::lease::StreamLeaseGrantV1 =
        serde_json::from_str(&required_env("VERLET_SYNC_TEST_GRANT")?)?;
    let store =
        verlet_history_sqlite::SqliteSessionStore::open(std::path::PathBuf::from(child_db)).await?;
    if let Some(label) = label {
        store
            .append_events(&stream_id, vec![record(&label)])
            .await?;
    }
    let state_store = std::sync::Arc::new(
        verlet::daemon::remote_store::propagator::SqlitePropagationStateStore::new(store.clone())
            .await?,
    );
    let mut state = match state_store.load(&stream_id).await? {
        Some(mut state) => {
            if state.lease.lease_id != grant.lease_id {
                state.lease = grant;
            }
            state
        }
        None => verlet::daemon::remote_store::propagator::StreamPropagationState {
            stream_id: stream_id.clone(),
            lease: grant,
            pushed_through: None,
        },
    };
    state_store.persist(&state).await?;
    if park {
        println!("READY child tail persisted");
        std::io::stdout().flush()?;
        std::future::pending::<()>().await;
        unreachable!();
    }
    let client = std::sync::Arc::new(
        verlet::daemon::remote_store::endpoint_http::HttpSyncClient::new(endpoint_url)?,
    );
    let propagator = verlet::daemon::remote_store::propagator::LocalFirstStreamPropagator::new(
        store,
        std::sync::Arc::clone(&client)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPushGate>,
        std::sync::Arc::clone(&client)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPullSource>,
        std::sync::Arc::clone(&client)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncLeaseRenewer>,
        state_store,
        bearer_token,
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock),
    );
    let step = propagator.propagate_once(&mut state).await?;
    match step {
        verlet::daemon::remote_store::propagator::PropagationStep::Converged => {
            println!("STEP converged")
        }
        verlet::daemon::remote_store::propagator::PropagationStep::Advanced { pushed_through } => {
            println!("STEP advanced={}", pushed_through.get())
        }
        verlet::daemon::remote_store::propagator::PropagationStep::EndpointUnavailable => {
            println!("STEP endpoint_unavailable")
        }
        verlet::daemon::remote_store::propagator::PropagationStep::LeaseFenced => {
            println!("STEP lease_fenced")
        }
        verlet::daemon::remote_store::propagator::PropagationStep::StreamDiverged {
            actual_next_sequence,
        } => println!("STEP stream_diverged={}", actual_next_sequence.get()),
    }
    std::io::stdout().flush()?;
    Ok(())
}

fn record(label: &str) -> verlet_history::NewEventRecord {
    verlet_history::NewEventRecord::witnessed(
        verlet_runtime_contracts::ThreadCoordinates::new(
            "tenant-process",
            "user-process",
            "session-process",
        ),
        verlet_history::EventKind::SessionEntryAppended,
        serde_json::json!({ "entry_id": label }),
    )
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next().ok_or_else(|| format!("missing {label}").into())
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("missing {name}").into())
}
