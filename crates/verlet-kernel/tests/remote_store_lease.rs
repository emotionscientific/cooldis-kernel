use chrono::{TimeZone, Utc};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier as ThreadBarrier, Mutex};
use tokio::sync::Barrier;
use uuid::Uuid;
use verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig;
use verlet::daemon::remote_store::lease::{
    LeaseFenceDecision, LeaseFencedAppendOutcome, SqliteStreamLeaseAuthority, StreamLeaseAuthority,
    StreamLeaseLineage, StreamPrefixScope, SyncCredentialAuthority,
};
use verlet::{
    DaemonClock, EventKind, EventSequence, EventStore, EventStreamId, NewEventRecord,
    SqliteSessionStore, VerletError,
};
use verlet_runtime_contracts::{DispatchId, ThreadCoordinates};
use verlet_sqlite::{Db, DbConfig, TransactionBehavior, params};

struct ClockReadGate {
    entered: ThreadBarrier,
    release: ThreadBarrier,
}

impl ClockReadGate {
    fn new() -> Self {
        Self {
            entered: ThreadBarrier::new(2),
            release: ThreadBarrier::new(2),
        }
    }

    async fn wait_until_entered(self: &Arc<Self>) {
        let gate = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            gate.entered.wait();
        })
        .await
        .unwrap();
    }

    async fn release(self: &Arc<Self>) {
        let gate = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            gate.release.wait();
        })
        .await
        .unwrap();
    }
}

struct TestClock {
    now_ms: AtomicI64,
    next_read_gate: Mutex<Option<Arc<ClockReadGate>>>,
}

impl TestClock {
    fn new(now_ms: i64) -> Self {
        Self {
            now_ms: AtomicI64::new(now_ms),
            next_read_gate: Mutex::new(None),
        }
    }

    fn set(&self, now_ms: i64) {
        self.now_ms.store(now_ms, Ordering::SeqCst);
    }

    fn gate_next_read(&self) -> Arc<ClockReadGate> {
        let gate = Arc::new(ClockReadGate::new());
        let mut next = self.next_read_gate.lock().unwrap();
        assert!(next.replace(Arc::clone(&gate)).is_none());
        gate
    }
}

impl DaemonClock for TestClock {
    fn now(&self) -> chrono::DateTime<Utc> {
        let gate = self.next_read_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.wait();
            gate.release.wait();
        }
        Utc.timestamp_millis_opt(self.now_ms.load(Ordering::SeqCst))
            .single()
            .expect("test timestamp should be representable")
    }
}

struct Fixture {
    path: PathBuf,
    store: SqliteSessionStore,
    authority: SqliteStreamLeaseAuthority,
    clock: Arc<TestClock>,
}

impl Fixture {
    async fn new(test_name: &str, now_ms: i64, lease_ttl_secs: u32) -> Self {
        let path = temp_db_path(test_name);
        let store = SqliteSessionStore::open(&path).await.unwrap();
        let clock = Arc::new(TestClock::new(now_ms));
        let config = VerletDaemonSyncConfig {
            lease_ttl_secs,
            ..VerletDaemonSyncConfig::default()
        };
        let authority = SqliteStreamLeaseAuthority::new(
            store.clone(),
            config,
            Arc::clone(&clock) as Arc<dyn DaemonClock>,
        )
        .await
        .unwrap();
        Self {
            path,
            store,
            authority,
            clock,
        }
    }
}

#[tokio::test]
async fn first_grant_overlap_and_racing_releases_fail_closed() {
    let fixture = Fixture::new("grant-race", 1_000, 60).await;

    let occupied = StreamPrefixScope::new("thread:child-7");
    let first = fixture
        .authority
        .grant_lease(
            &occupied,
            &DispatchId::new("dispatch-first"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();

    assert!(
        fixture
            .authority
            .grant_lease(
                &occupied,
                &DispatchId::new("dispatch-empty-lineage-loser"),
                StreamLeaseLineage::default(),
            )
            .await
            .is_err(),
        "only the first-ever exact-scope grant may carry empty lineage"
    );
    for overlapping in ["thread", "thread:child-7:trace"] {
        assert!(
            fixture
                .authority
                .grant_lease(
                    &StreamPrefixScope::new(overlapping),
                    &DispatchId::new(format!("dispatch-overlap-{overlapping}")),
                    StreamLeaseLineage::default(),
                )
                .await
                .is_err(),
            "ancestor and descendant live scopes must overlap fail-closed"
        );
    }
    let sibling = fixture
        .authority
        .grant_lease(
            &StreamPrefixScope::new("thread:child-8"),
            &DispatchId::new("dispatch-sibling"),
            StreamLeaseLineage::default(),
        )
        .await
        .expect("sibling scopes do not overlap");
    assert!(
        fixture
            .authority
            .grant_lease(
                &occupied,
                &DispatchId::new("dispatch-wrong-scope-predecessor"),
                StreamLeaseLineage {
                    superseded_lease_id: Some(sibling.lease_id),
                },
            )
            .await
            .is_err(),
        "a predecessor from a different scope cannot satisfy lineage"
    );

    let barrier = Arc::new(Barrier::new(3));
    let authority = Arc::new(fixture.authority.clone());
    let race = |dispatch: &'static str| {
        let barrier = Arc::clone(&barrier);
        let authority = Arc::clone(&authority);
        let scope = occupied.clone();
        let predecessor = first.lease_id.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            authority
                .grant_lease(
                    &scope,
                    &DispatchId::new(dispatch),
                    StreamLeaseLineage {
                        superseded_lease_id: Some(predecessor),
                    },
                )
                .await
        })
    };
    let racer_a = race("dispatch-racer-a");
    let racer_b = race("dispatch-racer-b");
    barrier.wait().await;
    let result_a = racer_a.await.unwrap();
    let result_b = racer_b.await.unwrap();
    let (winner, loser_error) = match (result_a, result_b) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        (result_a, result_b) => panic!(
            "two replacements naming one predecessor must commit exactly one grant: {result_a:?}, {result_b:?}"
        ),
    };
    assert!(
        matches!(
            loser_error,
            VerletError::History(ref message)
                if message == "lease lineage does not name the immediately preceding grant"
        ),
        "the serialized loser must observe the winning generation, not reach the UNIQUE fallback: {loser_error}"
    );
    assert!(
        authority
            .grant_lease(
                &occupied,
                &DispatchId::new("dispatch-superseded-predecessor"),
                StreamLeaseLineage {
                    superseded_lease_id: Some(first.lease_id.clone()),
                },
            )
            .await
            .is_err(),
        "a superseded non-latest lease cannot satisfy lineage"
    );
    authority.release_lease(&first.lease_id).await.unwrap();
    // The authority release above is an intentional no-op for a non-latest
    // lease. Also seed the durable edge state directly to prove that a legacy
    // late-release marker cannot hide or retire the latest generation.
    let database = fixture.store.sqlite_database();
    let mut connection = database.connect().await.unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .unwrap();
    transaction
        .execute(
            "UPDATE cooldis_stream_leases
             SET released_at_ms = ?2
             WHERE lease_id = ?1",
            params![first.lease_id.as_str(), 1_001_i64],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    drop(connection);
    drop(database);

    drop(authority);
    drop(fixture.authority);
    drop(fixture.store);

    let reopened_store = SqliteSessionStore::open(&fixture.path).await.unwrap();
    let reopened = SqliteStreamLeaseAuthority::new(
        reopened_store.clone(),
        VerletDaemonSyncConfig {
            lease_ttl_secs: 60,
            ..VerletDaemonSyncConfig::default()
        },
        Arc::clone(&fixture.clock) as Arc<dyn DaemonClock>,
    )
    .await
    .unwrap();
    let stream_id = EventStreamId::new("thread:child-7");
    assert_eq!(
        reopened
            .check_fence(&stream_id, &first.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Superseded,
        "a fresh authority must re-derive supersession without disclosing the winner id"
    );
    assert_eq!(
        reopened
            .check_fence(&stream_id, &winner.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Current
    );
}

#[tokio::test]
async fn expiry_rejects_append_but_latest_lease_renews_and_resumes() {
    let fixture = Fixture::new("expiry-recovery", 1_000, 1).await;
    let scope = StreamPrefixScope::new("thread:offline-child");
    let stream_id = EventStreamId::new(scope.as_str());
    let grant = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-offline"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    assert_eq!(grant.expires_at_ms, 2_000);

    fixture.clock.set(2_000);
    let expired = fixture
        .authority
        .append_if_current(
            &stream_id,
            &grant.lease_id,
            EventSequence::new(1),
            vec![record("rejected-while-expired")],
        )
        .await
        .unwrap();
    assert_eq!(
        expired,
        LeaseFencedAppendOutcome::LeaseRejected {
            fence: LeaseFenceDecision::Expired
        }
    );
    assert!(
        fixture
            .store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    let renewed = fixture
        .authority
        .renew_lease(&grant.lease_id)
        .await
        .expect("an expired latest unreleased grant must renew");
    assert_eq!(renewed.lease_id, grant.lease_id);
    assert_eq!(renewed.expires_at_ms, 3_000);
    let appended = fixture
        .authority
        .append_if_current(
            &stream_id,
            &grant.lease_id,
            EventSequence::new(1),
            vec![record("accepted-after-renewal")],
        )
        .await
        .unwrap();
    assert!(matches!(
        appended,
        LeaseFencedAppendOutcome::Appended { ack }
            if ack.start_sequence == EventSequence::new(1)
                && ack.end_sequence == EventSequence::new(1)
    ));

    let successor = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-successor"),
            StreamLeaseLineage {
                superseded_lease_id: Some(grant.lease_id.clone()),
            },
        )
        .await
        .unwrap();
    assert!(
        fixture
            .authority
            .renew_lease(&grant.lease_id)
            .await
            .is_err()
    );
    fixture
        .authority
        .release_lease(&successor.lease_id)
        .await
        .unwrap();
    assert!(
        fixture
            .authority
            .renew_lease(&successor.lease_id)
            .await
            .is_err()
    );
    assert!(
        fixture
            .authority
            .grant_lease(
                &scope,
                &DispatchId::new("dispatch-after-release-without-lineage"),
                StreamLeaseLineage::default(),
            )
            .await
            .is_err(),
        "release does not reset a scope to first-ever grant state"
    );
    fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-after-release"),
            StreamLeaseLineage {
                superseded_lease_id: Some(successor.lease_id),
            },
        )
        .await
        .expect("replacement after release must name the released latest grant");
}

#[tokio::test]
async fn takeover_and_expired_comeback_serialize_to_one_current_lease() {
    let fixture = Fixture::new("takeover-comeback-race", 10_000, 1).await;
    let scope = StreamPrefixScope::new("thread:takeover-race");
    let stream_id = EventStreamId::new(scope.as_str());
    let predecessor = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-predecessor"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    fixture.clock.set(11_000);

    let barrier = Arc::new(Barrier::new(3));
    let renew_authority = fixture.authority.clone();
    let renew_barrier = Arc::clone(&barrier);
    let renew_id = predecessor.lease_id.clone();
    let renew = tokio::spawn(async move {
        renew_barrier.wait().await;
        renew_authority.renew_lease(&renew_id).await
    });
    let grant_authority = fixture.authority.clone();
    let grant_barrier = Arc::clone(&barrier);
    let grant_scope = scope.clone();
    let grant_predecessor = predecessor.lease_id.clone();
    let takeover = tokio::spawn(async move {
        grant_barrier.wait().await;
        grant_authority
            .grant_lease(
                &grant_scope,
                &DispatchId::new("dispatch-takeover"),
                StreamLeaseLineage {
                    superseded_lease_id: Some(grant_predecessor),
                },
            )
            .await
    });
    barrier.wait().await;
    let renew_result = renew.await.unwrap();
    let successor = takeover
        .await
        .unwrap()
        .expect("valid lineage remains legal whether renewal serialized first or second");

    assert_eq!(
        fixture
            .authority
            .check_fence(&stream_id, &predecessor.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Superseded
    );
    assert_eq!(
        fixture
            .authority
            .check_fence(&stream_id, &successor.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Current,
        "regardless of which transaction acquired the writer lock first, one lease is current"
    );
    if let Ok(renewed) = renew_result {
        assert_eq!(renewed.lease_id, predecessor.lease_id);
        assert!(renewed.expires_at_ms > fixture.clock.now().timestamp_millis());
    }

    let renew_first_scope = StreamPrefixScope::new("thread:renew-first");
    let renew_first_stream = EventStreamId::new(renew_first_scope.as_str());
    let renew_first = fixture
        .authority
        .grant_lease(
            &renew_first_scope,
            &DispatchId::new("dispatch-renew-first-predecessor"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    fixture.clock.set(12_000);
    fixture
        .authority
        .renew_lease(&renew_first.lease_id)
        .await
        .expect("comeback wins the first serialization point");
    let after_renew = fixture
        .authority
        .grant_lease(
            &renew_first_scope,
            &DispatchId::new("dispatch-after-renew"),
            StreamLeaseLineage {
                superseded_lease_id: Some(renew_first.lease_id.clone()),
            },
        )
        .await
        .expect("grant-with-lineage remains legal after renewal");
    assert_eq!(
        fixture
            .authority
            .check_fence(&renew_first_stream, &after_renew.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Current
    );

    let grant_first_scope = StreamPrefixScope::new("thread:grant-first");
    let grant_first = fixture
        .authority
        .grant_lease(
            &grant_first_scope,
            &DispatchId::new("dispatch-grant-first-predecessor"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    fixture.clock.set(13_000);
    fixture
        .authority
        .grant_lease(
            &grant_first_scope,
            &DispatchId::new("dispatch-grant-first-successor"),
            StreamLeaseLineage {
                superseded_lease_id: Some(grant_first.lease_id.clone()),
            },
        )
        .await
        .expect("takeover wins the first serialization point");
    assert!(
        fixture
            .authority
            .renew_lease(&grant_first.lease_id)
            .await
            .is_err(),
        "renewal serialized after takeover must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_if_current_closes_diagnostic_check_then_supersede_interleaving() {
    let fixture = Fixture::new("atomic-append", 1_000, 60).await;
    let scope = StreamPrefixScope::new("thread:atomic-child");
    let stream_id = EventStreamId::new(scope.as_str());
    let first = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-first"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .authority
            .check_fence(&stream_id, &first.lease_id)
            .await
            .unwrap(),
        LeaseFenceDecision::Current
    );

    let replacement = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-replacement"),
            StreamLeaseLineage {
                superseded_lease_id: Some(first.lease_id.clone()),
            },
        )
        .await
        .unwrap();
    let clock_gate = fixture.clock.gate_next_read();
    let append_authority = fixture.authority.clone();
    let append_stream_id = stream_id.clone();
    let stale_lease_id = first.lease_id.clone();
    let stale_append = tokio::spawn(async move {
        append_authority
            .append_if_current(
                &append_stream_id,
                &stale_lease_id,
                EventSequence::new(1),
                vec![record("must-not-land")],
            )
            .await
    });
    clock_gate.wait_until_entered().await;

    let database = fixture.store.sqlite_database();
    let mut probe = database.connect().await.unwrap();
    probe.busy_timeout(std::time::Duration::ZERO).unwrap();
    let competing_writer = probe
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await;
    let writer_was_blocked = match competing_writer {
        Ok(transaction) => {
            transaction.rollback().await.unwrap();
            false
        }
        Err(_) => true,
    };
    clock_gate.release().await;
    assert!(
        writer_was_blocked,
        "append_if_current must reserve the writer before reading its clock or authority rows"
    );

    let stale_append = stale_append.await.unwrap().unwrap();
    assert_eq!(
        stale_append,
        LeaseFencedAppendOutcome::LeaseRejected {
            fence: LeaseFenceDecision::Superseded
        }
    );
    assert!(
        fixture
            .store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    fixture
        .authority
        .append_if_current(
            &stream_id,
            &replacement.lease_id,
            EventSequence::new(1),
            vec![record("winner")],
        )
        .await
        .unwrap();
    let conflict = fixture
        .authority
        .append_if_current(
            &stream_id,
            &replacement.lease_id,
            EventSequence::new(1),
            vec![record("wrong-tail")],
        )
        .await
        .unwrap();
    assert_eq!(
        conflict,
        LeaseFencedAppendOutcome::SequenceFenceConflict {
            actual_next_sequence: EventSequence::new(2)
        }
    );
    let events = fixture.store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload["entry_id"], "winner");
}

#[tokio::test]
async fn credentials_verify_revoke_and_never_persist_or_render_the_bearer() {
    let fixture = Fixture::new("credential-hygiene", 1_000, 60).await;
    let scope = StreamPrefixScope::new("thread:credential-child");
    let grant = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-credential"),
            StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    let (credential, token) = fixture.authority.mint_credential(&grant).await.unwrap();
    let identity = fixture
        .authority
        .verify_token(&token)
        .await
        .unwrap()
        .expect("fresh credential should verify");
    assert_eq!(identity.credential_id, credential.credential_id);
    assert_eq!(identity.scope, grant.scope);
    assert_eq!(identity.lease_id, grant.lease_id);
    assert_eq!(
        fixture.authority.verify_token("unknown").await.unwrap(),
        None
    );
    assert!(!format!("{credential:?}").contains(&token));
    assert!(!format!("{identity:?}").contains(&token));

    fixture
        .authority
        .revoke_credential(&credential.credential_id)
        .await
        .unwrap();
    assert_eq!(fixture.authority.verify_token(&token).await.unwrap(), None);

    let (_, superseded_token) = fixture.authority.mint_credential(&grant).await.unwrap();
    let successor = fixture
        .authority
        .grant_lease(
            &scope,
            &DispatchId::new("dispatch-credential-successor"),
            StreamLeaseLineage {
                superseded_lease_id: Some(grant.lease_id.clone()),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .authority
            .verify_token(&superseded_token)
            .await
            .unwrap(),
        None,
        "supersession must revoke every credential bound to the predecessor"
    );
    assert!(
        fixture.authority.mint_credential(&grant).await.is_err(),
        "a stale grant value cannot mint a credential after supersession"
    );

    let (_, release_token) = fixture.authority.mint_credential(&successor).await.unwrap();
    fixture
        .authority
        .release_lease(&successor.lease_id)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .authority
            .verify_token(&release_token)
            .await
            .unwrap(),
        None,
        "release must revoke every credential bound to the lease"
    );

    let path = fixture.path.clone();
    drop(fixture.authority);
    drop(fixture.store);
    let db = Db::open(&path, DbConfig::default()).await.unwrap();
    let connection = db.connect().await.unwrap();
    let mut rows = connection
        .query(
            "SELECT credential_id, token_digest, scope, lease_id
             FROM cooldis_stream_write_credentials
             ORDER BY credential_id",
            (),
        )
        .await
        .unwrap();
    let mut durable_text = Vec::new();
    let mut token_digests = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        token_digests.push(row.get::<String>(1).unwrap());
        for column in 0..4 {
            durable_text.push(row.get::<String>(column).unwrap());
        }
    }
    assert!(durable_text.iter().all(|value| value != &token));
    assert!(durable_text.iter().all(|value| value != &superseded_token));
    assert!(durable_text.iter().all(|value| value != &release_token));
    for digest in token_digests {
        let hex = digest
            .strip_prefix("sha256:")
            .expect("credential digests must carry an explicit stable algorithm tag");
        assert_eq!(hex.len(), 64);
        assert!(
            hex.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "credential digests must use lowercase SHA-256 hex"
        );
    }
    drop(rows);
    drop(connection);
    drop(db);
    assert_file_family_excludes(&path, token.as_bytes());
    assert_file_family_excludes(&path, superseded_token.as_bytes());
    assert_file_family_excludes(&path, release_token.as_bytes());
}

fn record(entry_id: &str) -> NewEventRecord {
    NewEventRecord::witnessed(
        ThreadCoordinates::new("tenant-a", "user-a", "session-a"),
        EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": entry_id}),
    )
}

fn temp_db_path(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("verlet-{test_name}-{}.sqlite3", Uuid::now_v7()))
}

fn assert_file_family_excludes(path: &Path, needle: &[u8]) {
    for candidate in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let Ok(bytes) = std::fs::read(&candidate) else {
            continue;
        };
        assert!(
            !bytes.windows(needle.len()).any(|window| window == needle),
            "bearer token leaked into {}",
            candidate.display()
        );
    }
}
