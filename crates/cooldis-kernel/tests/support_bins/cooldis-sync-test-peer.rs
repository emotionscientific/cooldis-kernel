use cooldis::daemon::remote_store::endpoint_http::HttpSyncClient;
use cooldis::daemon::remote_store::lease::StreamLeaseGrantV1;
use cooldis::daemon::remote_store::propagator::{
    LocalFirstStreamPropagator, PropagationStep, SqlitePropagationStateStore,
    StreamPropagationState, StreamPropagator,
};
use cooldis::{
    EventKind, EventStore, EventStreamId, NewEventRecord, SqliteSessionStore, SystemDaemonClock,
};
use cooldis_runtime_contracts::ThreadCoordinates;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("sync test peer failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
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
    let endpoint_url = required_env("COOLDIS_SYNC_TEST_URL")?;
    let bearer_token = required_env("COOLDIS_SYNC_TEST_TOKEN")?;
    let stream_id = EventStreamId::new(required_env("COOLDIS_SYNC_TEST_STREAM")?);
    let grant: StreamLeaseGrantV1 =
        serde_json::from_str(&required_env("COOLDIS_SYNC_TEST_GRANT")?)?;
    let store = SqliteSessionStore::open(PathBuf::from(child_db)).await?;
    if let Some(label) = label {
        store
            .append_events(&stream_id, vec![record(&label)])
            .await?;
    }
    let state_store = Arc::new(SqlitePropagationStateStore::new(store.clone()).await?);
    let mut state = match state_store.load(&stream_id).await? {
        Some(mut state) => {
            if state.lease.lease_id != grant.lease_id {
                state.lease = grant;
            }
            state
        }
        None => StreamPropagationState {
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
    let client = Arc::new(HttpSyncClient::new(endpoint_url)?);
    let propagator = LocalFirstStreamPropagator::new(
        store,
        Arc::clone(&client) as Arc<dyn cooldis::daemon::remote_store::endpoint::SyncPushGate>,
        Arc::clone(&client) as Arc<dyn cooldis::daemon::remote_store::endpoint::SyncPullSource>,
        Arc::clone(&client) as Arc<dyn cooldis::daemon::remote_store::endpoint::SyncLeaseRenewer>,
        state_store,
        bearer_token,
        Arc::new(SystemDaemonClock),
    );
    let step = propagator.propagate_once(&mut state).await?;
    match step {
        PropagationStep::Converged => println!("STEP converged"),
        PropagationStep::Advanced { pushed_through } => {
            println!("STEP advanced={}", pushed_through.get())
        }
        PropagationStep::EndpointUnavailable => println!("STEP endpoint_unavailable"),
        PropagationStep::LeaseFenced => println!("STEP lease_fenced"),
        PropagationStep::StreamDiverged {
            actual_next_sequence,
        } => println!("STEP stream_diverged={}", actual_next_sequence.get()),
    }
    std::io::stdout().flush()?;
    Ok(())
}

fn record(label: &str) -> NewEventRecord {
    NewEventRecord::witnessed(
        ThreadCoordinates::new("tenant-process", "user-process", "session-process"),
        EventKind::SessionEntryAppended,
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
    env::var(name).map_err(|_| format!("missing {name}").into())
}
