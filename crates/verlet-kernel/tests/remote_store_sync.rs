use chrono::TimeZone as _;
use tokio::io::AsyncBufReadExt as _;
use verlet::daemon::remote_store::endpoint::SyncPullSource as _;
use verlet::daemon::remote_store::endpoint::SyncPushGate as _;
use verlet::daemon::remote_store::lease::StreamLeaseAuthority as _;
use verlet::daemon::remote_store::lease::SyncCredentialAuthority as _;
use verlet::daemon::remote_store::propagator::StreamPropagator as _;
use verlet_history::EventStore as _;

#[path = "support/model_catalog.rs"]
mod model_catalog_test_support;

const LEASE_RACE_DST_SEED: u64 = 0x4290_0000_0000_0001;
const OFFLINE_WINDOW_DST_SEED: u64 = 0x4290_0000_0000_0002;

#[derive(Clone)]
struct FixedClock(i64);

impl verlet::daemon::clock_route::DaemonClock for FixedClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc
            .timestamp_millis_opt(self.0)
            .single()
            .expect("test timestamp should be representable")
    }
}

struct Fixture {
    parent_path: std::path::PathBuf,
    parent_store: verlet_history_sqlite::SqliteSessionStore,
    authority: std::sync::Arc<verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority>,
    endpoint: std::sync::Arc<verlet::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
    clock: std::sync::Arc<FixedClock>,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let parent_path = temp_db_path(&format!("{name}-parent"));
        let parent_store = verlet_history_sqlite::SqliteSessionStore::open(&parent_path)
            .await
            .unwrap();
        let clock = std::sync::Arc::new(FixedClock(1_700_000_000_000));
        let authority = std::sync::Arc::new(
            verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
                parent_store.clone(),
                verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default(),
                std::sync::Arc::clone(&clock)
                    as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
            )
            .await
            .unwrap(),
        );
        let endpoint = std::sync::Arc::new(
            verlet::daemon::remote_store::endpoint::SqliteSyncEndpoint::new(
                parent_store.clone(),
                std::sync::Arc::clone(&authority),
                std::sync::Arc::clone(&clock)
                    as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
            )
            .await
            .unwrap(),
        );
        Self {
            parent_path,
            parent_store,
            authority,
            endpoint,
            clock,
        }
    }

    async fn credential(
        &self,
        scope: &str,
        dispatch: &str,
        lineage: verlet::daemon::remote_store::lease::StreamLeaseLineage,
    ) -> (
        verlet::daemon::remote_store::lease::StreamLeaseGrantV1,
        String,
    ) {
        let grant = self
            .authority
            .grant_lease(
                &verlet::daemon::remote_store::lease::StreamPrefixScope::new(scope),
                &verlet_runtime_contracts::handle::DispatchId::new(dispatch),
                lineage,
            )
            .await
            .unwrap();
        let (_, token) = self.authority.mint_credential(&grant).await.unwrap();
        (grant, token)
    }
}

#[tokio::test]
async fn scope_rejection_is_witnessed_before_return_and_survives_restart_without_secrets() {
    let fixture = Fixture::new("scope-witness").await;
    let (grant, token) = fixture
        .credential(
            "thread:prefix-a",
            "dispatch-a",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let stream_id = verlet_history::EventStreamId::new("thread:prefix-b");
    let request = request_for(&stream_id, &grant.lease_id, vec![record("outside-scope")]);

    let outcome = fixture.endpoint.push(&token, request).await.unwrap();
    assert!(matches!(
        outcome,
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Rejected {
            rejection: verlet::daemon::remote_store::endpoint::SyncPushRejectionV1 {
                reason: verlet::daemon::remote_store::endpoint::SyncPushRejectionReason::ScopeViolation { .. },
                ..
            }
        }
    ));
    assert!(
        fixture
            .parent_store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty(),
        "scope rejection must not move the protected stream tail"
    );
    fixture
        .parent_store
        .append_events(&stream_id, vec![record("private-to-prefix-b")])
        .await
        .unwrap();
    assert!(matches!(
        fixture.endpoint.pull_after(&token, &stream_id, None).await,
        Err(verlet::kernel::runtime_host::VerletError::History(message)) if message == "sync pull not authorized"
    ));

    let witnessed = fixture
        .endpoint
        .rejection_witnesses(Some(&stream_id))
        .await
        .unwrap();
    assert_eq!(witnessed.len(), 1);
    let encoded = serde_json::to_string(&witnessed).unwrap();
    assert!(!encoded.contains(&token));
    assert!(!encoded.contains(grant.lease_id.as_str()));

    drop(fixture.endpoint);
    drop(fixture.authority);
    drop(fixture.parent_store);
    let reopened_store = verlet_history_sqlite::SqliteSessionStore::open(&fixture.parent_path)
        .await
        .unwrap();
    let reopened_authority = std::sync::Arc::new(
        verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
            reopened_store.clone(),
            verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default(),
            std::sync::Arc::clone(&fixture.clock)
                as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
        )
        .await
        .unwrap(),
    );
    let reopened = verlet::daemon::remote_store::endpoint::SqliteSyncEndpoint::new(
        reopened_store,
        reopened_authority,
        std::sync::Arc::clone(&fixture.clock)
            as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .rejection_witnesses(Some(&stream_id))
            .await
            .unwrap(),
        witnessed,
        "witnessed fence decisions must survive daemon restart unchanged"
    );
}

#[tokio::test]
async fn superseding_propagators_commit_one_batch_and_witness_the_loser() {
    let fixture = Fixture::new(&format!("lease-race-{LEASE_RACE_DST_SEED:016x}")).await;
    let stream_id = verlet_history::EventStreamId::new("thread:racing-child");
    let (first, first_token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-first",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let (successor, successor_token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-successor",
            verlet::daemon::remote_store::lease::StreamLeaseLineage {
                superseded_lease_id: Some(first.lease_id.clone()),
            },
        )
        .await;
    let local = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let event = local
        .append_events(&stream_id, vec![record("raced")])
        .await
        .unwrap()
        .remove(0)
        .to_stream_record_v1();
    let losing = verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
        schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
        stream_id: stream_id.clone(),
        lease_id: first.lease_id,
        expected_next_sequence: verlet_history::EventSequence::new(1),
        records: vec![event.clone()],
    };
    let winning = verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
        lease_id: successor.lease_id,
        ..losing.clone()
    };

    let (loser, winner) = if seeded_bool(LEASE_RACE_DST_SEED) {
        tokio::join!(
            fixture.endpoint.push(&first_token, losing),
            fixture.endpoint.push(&successor_token, winning),
        )
    } else {
        let (winner, loser) = tokio::join!(
            fixture.endpoint.push(&successor_token, winning),
            fixture.endpoint.push(&first_token, losing),
        );
        (loser, winner)
    };
    assert!(matches!(
        loser.unwrap(),
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Rejected {
            rejection: verlet::daemon::remote_store::endpoint::SyncPushRejectionV1 {
                reason: verlet::daemon::remote_store::endpoint::SyncPushRejectionReason::CredentialUnknown,
                ..
            }
        }
    ));
    assert!(matches!(
        winner.unwrap(),
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
    ));
    let parent_events = fixture
        .parent_store
        .read_events(&stream_id, None)
        .await
        .unwrap();
    assert_eq!(parent_events.len(), 1);
    assert_eq!(parent_events[0].id, event.event_id);
    assert_eq!(
        fixture
            .endpoint
            .rejection_witnesses(Some(&stream_id))
            .await
            .unwrap()
            .len(),
        1
    );
}

struct LoseAcceptedResponse {
    inner: std::sync::Arc<verlet::daemon::remote_store::endpoint::SqliteSyncEndpoint>,
    lose_once: std::sync::atomic::AtomicBool,
    offline: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl verlet::daemon::remote_store::endpoint::SyncPushGate for LoseAcceptedResponse {
    async fn push(
        &self,
        bearer_token: &str,
        request: verlet::daemon::remote_store::endpoint::SyncPushRequestV1,
    ) -> verlet::kernel::runtime_host::VerletResult<
        verlet::daemon::remote_store::endpoint::SyncPushOutcome,
    > {
        if self.offline.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(verlet::kernel::runtime_host::VerletError::RuntimeExecution(
                "injected endpoint outage".to_string(),
            ));
        }
        let outcome = self.inner.push(bearer_token, request).await?;
        if matches!(
            outcome,
            verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
        ) && self
            .lose_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(verlet::kernel::runtime_host::VerletError::RuntimeExecution(
                "injected lost response after durable ack".to_string(),
            ));
        }
        Ok(outcome)
    }
}

#[tokio::test]
async fn synthetic_queue_pull_kind_cannot_be_pushed_into_a_real_stream() {
    let fixture = Fixture::new("queue-kind-push-rejected").await;
    let stream_id = verlet_history::EventStreamId::new("thread:queue-kind-push-rejected");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-queue-kind-push-rejected",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let mut request = request_for(
        &stream_id,
        &grant.lease_id,
        vec![record("must-not-enter-history")],
    );
    request.records[0].kind = "sync.ingress.queue.entry".to_string();
    request.records[0].payload_schema = "cooldis.stream.sync_ingress_queue_entry/1".to_string();

    let outcome = fixture.endpoint.push(&token, request).await.unwrap();
    assert!(matches!(
        outcome,
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Rejected {
            rejection: verlet::daemon::remote_store::endpoint::SyncPushRejectionV1 {
                reason: verlet::daemon::remote_store::endpoint::SyncPushRejectionReason::RequestInvalid { detail },
                ..
            }
        } if detail == "record kind is not in the frozen event vocabulary"
    ));
    assert!(
        fixture
            .parent_store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn lost_ack_and_offline_window_reconcile_without_duplicate_or_loss() {
    let fixture = Fixture::new(&format!("lost-ack-offline-{OFFLINE_WINDOW_DST_SEED:016x}")).await;
    let stream_id = verlet_history::EventStreamId::new("thread:offline-child");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-child",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let child_path = temp_db_path(&format!(
        "lost-ack-offline-child-{OFFLINE_WINDOW_DST_SEED:016x}"
    ));
    let child_store = verlet_history_sqlite::SqliteSessionStore::open(&child_path)
        .await
        .unwrap();
    let state_store = std::sync::Arc::new(
        verlet::daemon::remote_store::propagator::SqlitePropagationStateStore::new(
            child_store.clone(),
        )
        .await
        .unwrap(),
    );
    let push = std::sync::Arc::new(LoseAcceptedResponse {
        inner: std::sync::Arc::clone(&fixture.endpoint),
        lose_once: std::sync::atomic::AtomicBool::new(false),
        offline: std::sync::atomic::AtomicBool::new(false),
    });
    let propagator = verlet::daemon::remote_store::propagator::LocalFirstStreamPropagator::new(
        child_store.clone(),
        std::sync::Arc::clone(&push)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPushGate>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPullSource>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncLeaseRenewer>,
        state_store.clone(),
        token.clone(),
        std::sync::Arc::clone(&fixture.clock)
            as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
    );
    let mut state = verlet::daemon::remote_store::propagator::StreamPropagationState {
        stream_id: stream_id.clone(),
        lease: grant,
        pushed_through: None,
    };

    let cuts = if seeded_bool(OFFLINE_WINDOW_DST_SEED) {
        [
            OfflineFaultCut::CommitBeforeResponse,
            OfflineFaultCut::AckBeforeState,
        ]
    } else {
        [
            OfflineFaultCut::AckBeforeState,
            OfflineFaultCut::CommitBeforeResponse,
        ]
    };
    for (index, cut) in cuts.into_iter().enumerate() {
        let sequence = verlet_history::EventSequence::new(index as i64 + 1);
        child_store
            .append_events(
                &stream_id,
                vec![record(&format!("seeded-cut-{}", sequence.get()))],
            )
            .await
            .unwrap();
        match cut {
            OfflineFaultCut::CommitBeforeResponse => {
                push.lose_once
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(
                    propagator.propagate_once(&mut state).await.unwrap(),
                    verlet::daemon::remote_store::propagator::PropagationStep::EndpointUnavailable,
                    "seed {OFFLINE_WINDOW_DST_SEED:#x}: response must be lost after commit"
                );
            }
            OfflineFaultCut::AckBeforeState => {
                let local = child_store
                    .read_events(&stream_id, Some(sequence))
                    .await
                    .unwrap()
                    .remove(0)
                    .to_stream_record_v1();
                let outcome = fixture
                    .endpoint
                    .push(
                        &token,
                        verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
                            schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1
                                .to_string(),
                            stream_id: stream_id.clone(),
                            lease_id: state.lease.lease_id.clone(),
                            expected_next_sequence: sequence,
                            records: vec![local],
                        },
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    outcome,
                    verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
                ));
                // A hard process kill here loses the in-memory ack before the
                // child-side propagation state transaction can begin.
            }
        }
        assert_eq!(
            state.pushed_through.map(|value| value.get()),
            (sequence.get() > 1).then_some(sequence.get() - 1),
            "seed {OFFLINE_WINDOW_DST_SEED:#x}: the injected cut must leave stale local state"
        );
        assert_eq!(
            propagator.propagate_once(&mut state).await.unwrap(),
            verlet::daemon::remote_store::propagator::PropagationStep::Advanced {
                pushed_through: sequence
            },
            "seed {OFFLINE_WINDOW_DST_SEED:#x}: sequence conflict must adopt an identical batch"
        );
    }

    push.offline
        .store(true, std::sync::atomic::Ordering::SeqCst);
    child_store
        .append_events(&stream_id, vec![record("offline-three")])
        .await
        .unwrap();
    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::EndpointUnavailable
    );
    assert_eq!(
        state.pushed_through,
        Some(verlet_history::EventSequence::new(2))
    );
    push.offline
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::Advanced {
            pushed_through: verlet_history::EventSequence::new(3)
        }
    );

    let parent = fixture
        .parent_store
        .read_events(&stream_id, None)
        .await
        .unwrap();
    let child = child_store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(parent, child);
    assert_eq!(
        state_store.load(&stream_id).await.unwrap(),
        Some(state),
        "pushed_through must be durable before progress is returned"
    );
}

#[tokio::test]
async fn reconciliation_advances_one_batch_when_remote_matching_history_is_ahead() {
    let fixture = Fixture::new("remote-matching-ahead").await;
    let stream_id = verlet_history::EventStreamId::new("thread:remote-matching-ahead");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-remote-matching-ahead",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let child_store = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let local = child_store
        .append_events(
            &stream_id,
            vec![record("matching-one"), record("matching-two")],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.to_stream_record_v1())
        .collect::<Vec<_>>();
    assert!(matches!(
        fixture
            .endpoint
            .push(
                &token,
                verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
                    schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
                    stream_id: stream_id.clone(),
                    lease_id: grant.lease_id.clone(),
                    expected_next_sequence: verlet_history::EventSequence::new(1),
                    records: local,
                },
            )
            .await
            .unwrap(),
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
    ));
    let state_store = std::sync::Arc::new(
        verlet::daemon::remote_store::propagator::SqlitePropagationStateStore::new(
            child_store.clone(),
        )
        .await
        .unwrap(),
    );
    let propagator = verlet::daemon::remote_store::propagator::LocalFirstStreamPropagator::new(
        child_store,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPushGate>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPullSource>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncLeaseRenewer>,
        state_store,
        token,
        std::sync::Arc::clone(&fixture.clock)
            as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
    )
    .with_batch_size(1);
    let mut state = verlet::daemon::remote_store::propagator::StreamPropagationState {
        stream_id,
        lease: grant,
        pushed_through: None,
    };
    assert!(!format!("{state:?}").contains(state.lease.lease_id.as_str()));

    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::Advanced {
            pushed_through: verlet_history::EventSequence::new(1)
        }
    );
    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::Advanced {
            pushed_through: verlet_history::EventSequence::new(2)
        }
    );
    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::Converged
    );
}

#[tokio::test]
async fn reconciliation_rejects_remote_records_past_the_local_tail() {
    let fixture = Fixture::new("remote-unmatched-ahead").await;
    let stream_id = verlet_history::EventStreamId::new("thread:remote-unmatched-ahead");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-remote-unmatched-ahead",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let child_store = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let local = child_store
        .append_events(&stream_id, vec![record("local-only-one")])
        .await
        .unwrap()
        .remove(0)
        .to_stream_record_v1();
    assert!(matches!(
        fixture
            .endpoint
            .push(
                &token,
                verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
                    schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
                    stream_id: stream_id.clone(),
                    lease_id: grant.lease_id.clone(),
                    expected_next_sequence: verlet_history::EventSequence::new(1),
                    records: vec![local],
                },
            )
            .await
            .unwrap(),
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
    ));
    fixture
        .parent_store
        .append_events(&stream_id, vec![record("remote-only-two")])
        .await
        .unwrap();
    let state_store = std::sync::Arc::new(
        verlet::daemon::remote_store::propagator::SqlitePropagationStateStore::new(
            child_store.clone(),
        )
        .await
        .unwrap(),
    );
    let propagator = verlet::daemon::remote_store::propagator::LocalFirstStreamPropagator::new(
        child_store,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPushGate>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncPullSource>,
        std::sync::Arc::clone(&fixture.endpoint)
            as std::sync::Arc<dyn verlet::daemon::remote_store::endpoint::SyncLeaseRenewer>,
        state_store,
        token,
        std::sync::Arc::clone(&fixture.clock)
            as std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock>,
    )
    .with_batch_size(1);
    let mut state = verlet::daemon::remote_store::propagator::StreamPropagationState {
        stream_id,
        lease: grant,
        pushed_through: None,
    };

    assert_eq!(
        propagator.propagate_once(&mut state).await.unwrap(),
        verlet::daemon::remote_store::propagator::PropagationStep::StreamDiverged {
            actual_next_sequence: verlet_history::EventSequence::new(3)
        }
    );
    assert_eq!(state.pushed_through, None);
}

#[tokio::test]
async fn localhost_http_projection_preserves_wire_records_and_releases_tasks_and_socket() {
    let fixture = Fixture::new("http-projection").await;
    let stream_id = verlet_history::EventStreamId::new("thread:http-child");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-http-child",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let local = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let event = local
        .append_events(&stream_id, vec![record("over-http")])
        .await
        .unwrap()
        .remove(0)
        .to_stream_record_v1();
    let server = verlet::daemon::remote_store::endpoint_http::DaemonSyncHttpServer::bind(
        verlet::adapters::app_server::AppServerListenAddr::parse("ws://127.0.0.1:0").unwrap(),
        std::sync::Arc::clone(&fixture.endpoint),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap().unwrap();
    let task = tokio::spawn(server.serve());
    let client =
        verlet::daemon::remote_store::endpoint_http::HttpSyncClient::new(format!("http://{addr}"))
            .unwrap();

    let outcome = client
        .push(
            &token,
            verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
                schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
                stream_id: stream_id.clone(),
                lease_id: grant.lease_id,
                expected_next_sequence: verlet_history::EventSequence::new(1),
                records: vec![event.clone()],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
    ));
    assert_eq!(
        client.pull_after(&token, &stream_id, None).await.unwrap(),
        vec![event]
    );

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let rebound = tokio::net::TcpListener::bind(addr).await.unwrap();
    drop(rebound);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_http_projection_removes_socket_when_serve_is_cancelled() {
    let fixture = Fixture::new("unix-http-cleanup").await;
    let stream_id = verlet_history::EventStreamId::new("thread:unix-child");
    let (grant, token) = fixture
        .credential(
            stream_id.as_str(),
            "dispatch-unix-child",
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await;
    let local = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let event = local
        .append_events(&stream_id, vec![record("over-unix-http")])
        .await
        .unwrap()
        .remove(0)
        .to_stream_record_v1();
    let root = temp_root_path("unix-http-cleanup");
    let socket = root.join("run/sync.sock");
    let server = verlet::daemon::remote_store::endpoint_http::DaemonSyncHttpServer::bind(
        verlet::adapters::app_server::AppServerListenAddr::Unix(socket.clone()),
        std::sync::Arc::clone(&fixture.endpoint),
    )
    .await
    .unwrap();
    assert!(socket.exists());

    let task = tokio::spawn(server.serve());
    let client = verlet::daemon::remote_store::endpoint_http::HttpSyncClient::new(format!(
        "unix://{}",
        socket.display()
    ))
    .unwrap();
    assert!(matches!(
        client
            .push(
                &token,
                verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
                    schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
                    stream_id: stream_id.clone(),
                    lease_id: grant.lease_id,
                    expected_next_sequence: verlet_history::EventSequence::new(1),
                    records: vec![event.clone()],
                },
            )
            .await
            .unwrap(),
        verlet::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { .. }
    ));
    assert_eq!(
        client.pull_after(&token, &stream_id, None).await.unwrap(),
        vec![event]
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!socket.exists());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn process_backed_offline_restart_kill_and_lineage_re_lease_converge() {
    let daemon_root = temp_root_path("process-daemon");
    let parent_path = daemon_root.join("state/session_history.sqlite3");
    let child_path = temp_db_path("process-child");
    let stream_id = verlet_history::EventStreamId::new("thread:process-child");
    let store = verlet_history_sqlite::SqliteSessionStore::open(&parent_path)
        .await
        .unwrap();
    let clock: std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock);
    let authority = verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
        store.clone(),
        verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig {
            lease_ttl_secs: 300,
            ..verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default()
        },
        std::sync::Arc::clone(&clock),
    )
    .await
    .unwrap();
    let first = authority
        .grant_lease(
            &verlet::daemon::remote_store::lease::StreamPrefixScope::new(stream_id.as_str()),
            &verlet_runtime_contracts::handle::DispatchId::new("dispatch-process-first"),
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    let (_, first_token) = authority.mint_credential(&first).await.unwrap();
    drop(authority);
    drop(store);

    let (parent, first_url) = start_sync_daemon(&daemon_root).await;
    let first_run = run_sync_child(
        "child-once",
        &child_path,
        Some("one"),
        &first_url,
        &first_token,
        &stream_id,
        &first,
    )
    .await;
    assert_eq!(first_run, "STEP advanced=1");
    let first_converged = run_sync_child(
        "child-once",
        &child_path,
        None,
        &first_url,
        &first_token,
        &stream_id,
        &first,
    )
    .await;
    assert_eq!(first_converged, "STEP converged");
    parent.stop().await;

    let offline = run_sync_child(
        "child-once",
        &child_path,
        Some("offline-two"),
        &first_url,
        &first_token,
        &stream_id,
        &first,
    )
    .await;
    assert_eq!(offline, "STEP endpoint_unavailable");

    let (restarted_parent, restarted_url) = start_sync_daemon(&daemon_root).await;
    let resumed = run_sync_child(
        "child-once",
        &child_path,
        None,
        &restarted_url,
        &first_token,
        &stream_id,
        &first,
    )
    .await;
    assert_eq!(resumed, "STEP advanced=2");
    restarted_parent.stop().await;

    let mut parked = spawn_sync_child(
        "child-park",
        &child_path,
        Some("killed-three"),
        &restarted_url,
        &first_token,
        &stream_id,
        &first,
    );
    let parked_stdout = parked.stdout.take().unwrap();
    let mut parked_lines = tokio::io::BufReader::new(parked_stdout).lines();
    let ready = tokio::time::timeout(
        tokio::time::Duration::from_secs(30),
        parked_lines.next_line(),
    )
    .await
    .expect("child park readiness timed out")
    .unwrap()
    .unwrap();
    assert_eq!(ready, "READY child tail persisted");
    stop_process(&mut parked).await;

    let store = verlet_history_sqlite::SqliteSessionStore::open(&parent_path)
        .await
        .unwrap();
    let authority = verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
        store.clone(),
        verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig {
            lease_ttl_secs: 300,
            ..verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default()
        },
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock),
    )
    .await
    .unwrap();
    let successor = authority
        .grant_lease(
            &verlet::daemon::remote_store::lease::StreamPrefixScope::new(stream_id.as_str()),
            &verlet_runtime_contracts::handle::DispatchId::new("dispatch-process-successor"),
            verlet::daemon::remote_store::lease::StreamLeaseLineage {
                superseded_lease_id: Some(first.lease_id.clone()),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        successor.lineage.superseded_lease_id.as_ref(),
        Some(&first.lease_id)
    );
    let (_, successor_token) = authority.mint_credential(&successor).await.unwrap();
    drop(authority);
    drop(store);

    let (final_parent, final_url) = start_sync_daemon(&daemon_root).await;
    let fenced = run_sync_child(
        "child-once",
        &child_path,
        None,
        &final_url,
        &first_token,
        &stream_id,
        &first,
    )
    .await;
    assert_eq!(fenced, "STEP lease_fenced");
    let re_leased = run_sync_child(
        "child-once",
        &child_path,
        None,
        &final_url,
        &successor_token,
        &stream_id,
        &successor,
    )
    .await;
    assert_eq!(re_leased, "STEP advanced=3");
    let final_converged = run_sync_child(
        "child-once",
        &child_path,
        None,
        &final_url,
        &successor_token,
        &stream_id,
        &successor,
    )
    .await;
    assert_eq!(final_converged, "STEP converged");
    final_parent.stop().await;

    let parent_store = verlet_history_sqlite::SqliteSessionStore::open(&parent_path)
        .await
        .unwrap();
    let child_store = verlet_history_sqlite::SqliteSessionStore::open(&child_path)
        .await
        .unwrap();
    let parent = parent_store.read_events(&stream_id, None).await.unwrap();
    let child = child_store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(parent, child);
    assert_eq!(parent.len(), 3);
    let _ = std::fs::remove_dir_all(daemon_root);
}

#[tokio::test]
async fn remote_thread_spawn_runs_a_separate_child_and_folds_terminal_into_parent_ingress() {
    let root = temp_root_path("remote-placement-e2e");
    let (daemon, _) = start_sync_daemon(&root).await;
    let socket = root.join("run/verlet.sock");
    let mut client = connect_daemon_client(&socket).await;
    let parent = client
        .thread_start(serde_json::json!({"placement": {"target": "local"}}))
        .await
        .unwrap();
    let spawn_params = serde_json::json!({
        "threadId": parent.id,
        "taskName": "remote-worker",
        "message": "remote placement process proof",
        "agentRef": "agent://cooldis/default@latest",
        "placement": {"target": "remote"},
        "dispatchId": "emo-430-e2e-dispatch"
    });
    let spawned = client
        .request("thread/spawn", spawn_params.clone())
        .await
        .unwrap();
    let retried = client.request("thread/spawn", spawn_params).await.unwrap();
    assert_eq!(retried["threadId"], spawned["threadId"]);
    let child_id = spawned["threadId"].as_str().unwrap();
    let child_thread_id = verlet_runtime_contracts::ThreadId::parse_str(child_id).unwrap();
    let parent_thread_id = verlet_runtime_contracts::ThreadId::parse_str(&parent.id).unwrap();
    let store_path = root.join("state/session_history.sqlite3");
    let child_stream = verlet_history::EventStreamId::new(format!("thread:{child_id}"));

    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            let child = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": child_id, "stream": "thread"}),
                )
                .await
                .unwrap();
            let control = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": parent.id, "stream": "control"}),
                )
                .await
                .unwrap();
            let parent_events = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": parent.id, "stream": "thread"}),
                )
                .await
                .unwrap();
            if event_page_has_kind(&child, "turn.completed")
                && control["data"].as_array().unwrap().iter().any(|event| {
                    event["kind"] == "thread.joined"
                        && event["payload"]["child_thread_id"].as_str() == Some(child_id)
                })
                && event_page_has_kind(&parent_events, "turn.completed")
            {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remote child stream and parent envelope-triggered turn did not converge");

    let submit_params = serde_json::json!({
        "threadId": child_id,
        "message": "remote submit retry proof",
        "dispatchId": "emo-430-submit-dispatch"
    });
    let submitted = client
        .request("thread/submit", submit_params.clone())
        .await
        .unwrap();
    let submit_retry = client
        .request("thread/submit", submit_params)
        .await
        .unwrap();
    assert_eq!(submit_retry["turnId"], submitted["turnId"]);
    assert!(
        client
            .request(
                "thread/submit",
                serde_json::json!({
                    "threadId": child_id,
                    "message": "must not replace the first payload",
                    "dispatchId": "emo-430-submit-dispatch"
                }),
            )
            .await
            .is_err(),
        "same dispatch with a different payload must be rejected"
    );
    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            let child = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": child_id, "stream": "thread"}),
                )
                .await
                .unwrap();
            let events = child["data"].as_array().unwrap();
            if events
                .iter()
                .filter(|event| event["kind"] == "turn.completed")
                .count()
                >= 2
            {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("retried remote submit did not converge");

    client.close().await.unwrap();
    daemon.stop().await;
    let child_store_path = root
        .join("state/remote-children")
        .join(child_thread_id.to_string())
        .join("state/session_history.sqlite3");
    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            match verlet_history_sqlite::SqliteSessionStore::open(&child_store_path).await {
                Ok(store) => break store,
                Err(_) => tokio::time::sleep(tokio::time::Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("remote child process retained its SQLite lock after daemon shutdown");
    let store = verlet_history_sqlite::SqliteSessionStore::open(&store_path)
        .await
        .unwrap();
    let child_events = store.read_events(&child_stream, None).await.unwrap();
    assert_eq!(
        child_events
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::TurnSubmitted)
            .count(),
        2,
        "spawn plus one idempotently retried submit must produce two child deliveries"
    );
    assert_eq!(
        child_events
            .iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::TurnSubmitted
                    && event.payload["turn_id"] == "thread-submit-emo-430-submit-dispatch"
            })
            .count(),
        1,
        "same-dispatch submit retry must fold to one child turn"
    );
    assert!(child_events.iter().any(|event| {
        event.kind == verlet_history::EventKind::PlacementDecision
            && event.payload["placement"] == "remote"
    }));
    assert!(
        root.join("state/remote-children")
            .join(child_thread_id.to_string())
            .exists(),
        "the child must own a separate local state root"
    );
    assert_ne!(child_thread_id, parent_thread_id);

    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn remote_child_survives_sync_endpoint_outage_and_converges_after_restore() {
    let root = temp_root_path("remote-placement-endpoint-outage");
    let sync_socket = root.join("run/sync.sock");
    let sync_listen = format!("unix://{}", sync_socket.display());
    let (daemon, endpoint) = start_sync_daemon_with_listen(&root, &sync_listen).await;
    assert_eq!(endpoint, sync_listen);
    let app_socket = root.join("run/verlet.sock");
    let mut client = connect_daemon_client(&app_socket).await;
    let parent = client
        .thread_start(serde_json::json!({"placement": {"target": "local"}}))
        .await
        .unwrap();
    let spawned = client
        .request(
            "thread/spawn",
            serde_json::json!({
                "threadId": parent.id,
                "taskName": "outage-worker",
                "message": "establish the long-lived remote child",
                "agentRef": "agent://cooldis/default@latest",
                "placement": {"target": "remote"},
                "dispatchId": "emo-430-outage-spawn"
            }),
        )
        .await
        .unwrap();
    let child_id = spawned["threadId"].as_str().unwrap().to_string();
    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            let child = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": child_id, "stream": "thread"}),
                )
                .await
                .unwrap();
            if event_page_has_kind(&child, "turn.completed") {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remote child did not establish before the outage");

    let parked_socket = root.join("run/sync.offline.sock");
    std::fs::rename(&sync_socket, &parked_socket).unwrap();
    client
        .request(
            "thread/submit",
            serde_json::json!({
                "threadId": child_id,
                "message": "queued while the endpoint is offline",
                "dispatchId": "emo-430-outage-submit"
            }),
        )
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
    std::fs::rename(&parked_socket, &sync_socket).unwrap();

    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            let child = client
                .request(
                    "thread/events/list",
                    serde_json::json!({"threadId": child_id, "stream": "thread"}),
                )
                .await
                .unwrap();
            if child["data"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["kind"] == "turn.completed")
                .count()
                >= 2
            {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("remote child died instead of converging after endpoint restore");

    let control = client
        .request(
            "thread/events/list",
            serde_json::json!({"threadId": parent.id, "stream": "control"}),
        )
        .await
        .unwrap();
    assert_eq!(
        control["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| {
                event["kind"] == "thread.joined"
                    && event["payload"]["child_thread_id"].as_str() == Some(child_id.as_str())
            })
            .count(),
        1,
        "a later submit turn must never settle the spawn handle again"
    );

    client.close().await.unwrap();
    daemon.stop().await;
    let _ = std::fs::remove_dir_all(root);
}

async fn start_sync_daemon(root: &std::path::Path) -> (DaemonProcess, String) {
    start_sync_daemon_with_listen(root, "ws://127.0.0.1:0").await
}

async fn start_sync_daemon_with_listen(
    root: &std::path::Path,
    sync_listen: &str,
) -> (DaemonProcess, String) {
    std::fs::create_dir_all(root).unwrap();
    let config_path = root.join("verlet.toml");
    let socket_path = root.join("run/verlet.sock");
    let config = format!(
        r#"
[daemon.runtime]
cwd = "{}"
runtime_home = "runtime"
state_home = "state"

[daemon.app_server]
listen = "unix://{}"

[daemon.sync]
listen = "{}"
lease_ttl_secs = 300

[daemon.io.ingress.queue]
sqlite_path = "run/ingress.sqlite"

[daemon.provider]
provider = "local"
"#,
        escape_toml(&std::env::current_dir().unwrap().display().to_string()),
        escape_toml(&socket_path.display().to_string()),
        escape_toml(sync_listen),
    );
    std::fs::write(&config_path, config).unwrap();
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet"));
    model_catalog_test_support::disable_for_tokio_command(&mut command);
    let mut child = command
        .arg("serve")
        .arg("--config")
        .arg(&config_path)
        .env("VERLET_HOME", root.join("user-home"))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let drain = tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(stderr).lines();
        let mut ready_tx = Some(ready_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(url) = line.strip_prefix("verlet daemon sync endpoint listening on ")
                && let Some(tx) = ready_tx.take()
            {
                let _ = tx.send(url.to_string());
            }
        }
    });
    let url = tokio::time::timeout(tokio::time::Duration::from_secs(30), ready_rx)
        .await
        .expect("daemon sync readiness timed out")
        .expect("daemon exited before sync readiness");
    if let Some(address) = url.strip_prefix("http://") {
        let address = address
            .parse::<std::net::SocketAddr>()
            .expect("daemon sync readiness must report a socket address");
        assert!(address.ip().is_loopback());
        assert_ne!(
            address.port(),
            0,
            "port 0 must be resolved before readiness"
        );
    } else {
        assert!(url.starts_with("unix://"));
    }
    (DaemonProcess { child, drain }, url)
}

struct DaemonProcess {
    child: tokio::process::Child,
    drain: tokio::task::JoinHandle<()>,
}

impl DaemonProcess {
    async fn stop(mut self) {
        self.child.start_kill().unwrap();
        let status = tokio::time::timeout(tokio::time::Duration::from_secs(30), self.child.wait())
            .await
            .expect("daemon did not terminate")
            .unwrap();
        assert!(!status.success());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(30), &mut self.drain).await;
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.drain.abort();
    }
}

#[derive(Clone, Copy)]
enum OfflineFaultCut {
    CommitBeforeResponse,
    AckBeforeState,
}

fn seeded_bool(seed: u64) -> bool {
    let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) & 1 == 1
}

fn escape_toml(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

async fn connect_daemon_client(
    socket: &std::path::Path,
) -> verlet::adapters::operator_client::OperatorClient<tokio::net::UnixStream> {
    let mut last_error = None;
    for _ in 0..1_500 {
        if socket.exists() {
            match verlet::adapters::operator_client::OperatorClient::connect_unix(
                socket,
                verlet::adapters::operator_client::OperatorConnectConfig {
                    client_name: "verlet-remote-placement-e2e".to_string(),
                    ..verlet::adapters::operator_client::OperatorConnectConfig::default()
                },
            )
            .await
            {
                Ok(client) => return client,
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    panic!(
        "timed out waiting for daemon socket {}; last error: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "socket did not appear".to_string())
    );
}

fn event_page_has_kind(page: &serde_json::Value, kind: &str) -> bool {
    page["data"]
        .as_array()
        .is_some_and(|events| events.iter().any(|event| event["kind"] == kind))
}

fn temp_root_path(name: &str) -> std::path::PathBuf {
    let id = uuid::Uuid::now_v7().simple().to_string();
    std::path::Path::new("/tmp").join(format!("cdis-{name}-{}", &id[..12]))
}

async fn run_sync_child(
    mode: &str,
    child_path: &std::path::Path,
    label: Option<&str>,
    endpoint_url: &str,
    token: &str,
    stream_id: &verlet_history::EventStreamId,
    grant: &verlet::daemon::remote_store::lease::StreamLeaseGrantV1,
) -> String {
    let output = spawn_sync_child(
        mode,
        child_path,
        label,
        endpoint_url,
        token,
        stream_id,
        grant,
    )
    .wait_with_output()
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let mut steps = stdout.lines().filter(|line| line.starts_with("STEP "));
    let step = steps.next().expect("child did not print a STEP outcome");
    assert!(
        steps.next().is_none(),
        "child printed multiple STEP outcomes"
    );
    step.to_string()
}

fn spawn_sync_child(
    mode: &str,
    child_path: &std::path::Path,
    label: Option<&str>,
    endpoint_url: &str,
    token: &str,
    stream_id: &verlet_history::EventStreamId,
    grant: &verlet::daemon::remote_store::lease::StreamLeaseGrantV1,
) -> tokio::process::Child {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_verlet-sync-test-peer"));
    command
        .arg(mode)
        .arg(child_path)
        .arg(label.unwrap_or("-"))
        .env("VERLET_SYNC_TEST_URL", endpoint_url)
        .env("VERLET_SYNC_TEST_TOKEN", token)
        .env("VERLET_SYNC_TEST_STREAM", stream_id.as_str())
        .env(
            "VERLET_SYNC_TEST_GRANT",
            serde_json::to_string(grant).unwrap(),
        )
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command.spawn().unwrap()
}

async fn stop_process(child: &mut tokio::process::Child) {
    child.start_kill().unwrap();
    let status = tokio::time::timeout(tokio::time::Duration::from_secs(30), child.wait())
        .await
        .expect("process did not terminate")
        .unwrap();
    assert!(!status.success());
}

fn request_for(
    stream_id: &verlet_history::EventStreamId,
    lease_id: &verlet::daemon::remote_store::lease::StreamLeaseId,
    records: Vec<verlet_history::NewEventRecord>,
) -> verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
    let envelopes = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            verlet_history::EventRecord::from_new(
                stream_id.clone(),
                verlet_history::EventSequence::new(index as i64 + 1),
                record,
            )
            .to_stream_record_v1()
        })
        .collect();
    verlet::daemon::remote_store::endpoint::SyncPushRequestV1 {
        schema: verlet::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1.to_string(),
        stream_id: stream_id.clone(),
        lease_id: lease_id.clone(),
        expected_next_sequence: verlet_history::EventSequence::new(1),
        records: envelopes,
    }
}

fn record(entry_id: &str) -> verlet_history::NewEventRecord {
    verlet_history::NewEventRecord::witnessed(
        verlet_runtime_contracts::ThreadCoordinates::new("tenant-a", "user-a", "session-a"),
        verlet_history::EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": entry_id}),
    )
}

fn temp_db_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("verlet-{name}-{}.sqlite3", uuid::Uuid::now_v7()))
}
