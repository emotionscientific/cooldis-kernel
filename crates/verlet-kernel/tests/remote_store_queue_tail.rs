use chrono::TimeZone as _;
use verlet::daemon::remote_store::endpoint::SyncPullSource as _;
use verlet::daemon::remote_store::lease::StreamLeaseAuthority as _;
use verlet::daemon::remote_store::lease::SyncCredentialAuthority as _;
use verlet::daemon::remote_store::queue::RemoteIngressQueue as _;
use verlet::daemon::remote_store::tail::RemoteStreamTail as _;
use verlet_history::EventStore as _;

const QUEUE_CRASH_WINDOW_DST_SEED: u64 = 0x4300_0000_0000_0001;

#[derive(Clone)]
struct FixedClock(i64);

impl verlet::daemon::clock_route::DaemonClock for FixedClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.timestamp_millis_opt(self.0).single().unwrap()
    }
}

fn entry(
    thread_id: verlet_runtime_contracts::ThreadId,
    dispatch: &str,
    text: &str,
) -> verlet::daemon::remote_store::queue::RemoteIngressQueueEntryV1 {
    let dispatch_id = verlet_runtime_contracts::handle::DispatchId::new(dispatch);
    verlet::daemon::remote_store::queue::RemoteIngressQueueEntryV1 {
        schema: verlet::daemon::remote_store::queue::SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1.to_string(),
        dispatch_id: dispatch_id.clone(),
        target_thread_id: thread_id,
        envelope: verlet_io_core::IngressEnvelope::new(
            verlet_io_core::IoSource::new("cooldis.remote", "parent"),
            verlet_io_core::IoConversation::new(
                format!("thread:{thread_id}"),
                verlet_io_core::ConversationKind::System,
            ),
            verlet_io_core::IngressContent::text(text),
            1_700_000_000_000,
        )
        .with_dedupe_key(verlet_io_core::IoDedupeKey::new(
            "cooldis.remote.dispatch",
            dispatch_id.to_string(),
        ))
        .with_delivery(verlet_io_core::IoDelivery::new(dispatch_id.to_string()))
        .with_principal(verlet_io_core::IoPrincipal::new(
            "remote-tenant",
            "remote-user",
            format!("remote:{dispatch_id}"),
        )),
        enqueued_at_ms: 0,
    }
}

#[tokio::test]
async fn concurrent_same_dispatch_folds_atomically_and_conflict_never_replaces() {
    let queue = std::sync::Arc::new(
        verlet::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(
            verlet_history_sqlite::SqliteSessionStore::in_memory()
                .await
                .unwrap(),
        )
        .await
        .unwrap(),
    );
    let thread_id = verlet_runtime_contracts::ThreadId::new();
    let expected = entry(thread_id, "dispatch-race", "one payload");
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let queue = std::sync::Arc::clone(&queue);
        let barrier = std::sync::Arc::clone(&barrier);
        let expected = expected.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            queue.enqueue(expected).await.unwrap()
        }));
    }
    let mut enqueued = 0;
    let mut folded = 0;
    for task in tasks {
        match task.await.unwrap().disposition {
            verlet::daemon::remote_store::queue::RemoteEnqueueDisposition::Enqueued => {
                enqueued += 1
            }
            verlet::daemon::remote_store::queue::RemoteEnqueueDisposition::FoldedToExisting => {
                folded += 1
            }
        }
    }
    assert_eq!((enqueued, folded), (1, 15));

    let page = queue.tail_pending(thread_id, None).await.unwrap();
    assert_eq!(page.entries, vec![expected.clone()]);
    assert_eq!(page.next, Some(verlet_history::EventSequence::new(1)));

    let err = queue
        .enqueue(entry(thread_id, "dispatch-race", "different payload"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, verlet::kernel::runtime_host::VerletError::History(message) if message.contains("different payload"))
    );
    assert_eq!(
        queue.tail_pending(thread_id, None).await.unwrap().entries,
        vec![expected]
    );
}

#[tokio::test]
async fn queue_rejects_missing_or_mismatched_dispatch_dedupe_before_mutation() {
    let queue = verlet::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(
        verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let thread_id = verlet_runtime_contracts::ThreadId::new();
    let mut missing = entry(thread_id, "dispatch-key", "payload");
    missing.envelope.dedupe_key = None;
    assert!(queue.enqueue(missing).await.is_err());
    let mut mismatched = entry(thread_id, "dispatch-key", "payload");
    mismatched.envelope.dedupe_key.as_mut().unwrap().key = "other".to_string();
    assert!(queue.enqueue(mismatched).await.is_err());
    let mut unstable_timestamp = entry(thread_id, "dispatch-key", "payload");
    unstable_timestamp.enqueued_at_ms = 1_700_000_000_000;
    let err = queue.enqueue(unstable_timestamp).await.unwrap_err();
    assert!(
        matches!(err, verlet::kernel::runtime_host::VerletError::History(message) if message.contains("enqueued_at_ms must be zero"))
    );
    assert!(
        queue
            .tail_pending(thread_id, None)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
}

#[tokio::test]
async fn acknowledge_is_noop_safe_and_lost_ack_redelivery_keeps_dispatch_identity() {
    let queue = verlet::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(
        verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let thread_id = verlet_runtime_contracts::ThreadId::new();
    let first = entry(thread_id, "dispatch-one", "one");
    let second = entry(thread_id, "dispatch-two", "two");
    queue.enqueue(first.clone()).await.unwrap();
    queue.enqueue(second.clone()).await.unwrap();

    let first_delivery = queue.tail_pending(thread_id, None).await.unwrap();
    assert_eq!(first_delivery.entries, vec![first.clone(), second.clone()]);
    let replay_after_crash = if seeded_bool(QUEUE_CRASH_WINDOW_DST_SEED) {
        queue.tail_pending(thread_id, None).await.unwrap()
    } else {
        queue.tail_pending(thread_id, None).await.unwrap()
    };
    assert_eq!(replay_after_crash.entries, first_delivery.entries);
    assert_eq!(
        replay_after_crash.entries[0].envelope.dedupe_key,
        first.envelope.dedupe_key
    );

    queue
        .acknowledge(
            thread_id,
            &verlet_runtime_contracts::handle::DispatchId::new("dispatch-one"),
        )
        .await
        .unwrap();
    queue
        .acknowledge(
            thread_id,
            &verlet_runtime_contracts::handle::DispatchId::new("dispatch-one"),
        )
        .await
        .unwrap();
    queue
        .acknowledge(
            thread_id,
            &verlet_runtime_contracts::handle::DispatchId::new("unknown"),
        )
        .await
        .unwrap();
    assert_eq!(
        queue.tail_pending(thread_id, None).await.unwrap().entries,
        vec![second]
    );
}

#[tokio::test]
async fn queue_pull_credential_is_confined_to_its_child_prefix() {
    let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
        .await
        .unwrap();
    let queue = verlet::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(store.clone())
        .await
        .unwrap();
    let own = verlet_runtime_contracts::ThreadId::new();
    let other = verlet_runtime_contracts::ThreadId::new();
    queue
        .enqueue(entry(own, "own-dispatch", "own"))
        .await
        .unwrap();
    queue
        .enqueue(entry(other, "other-dispatch", "other"))
        .await
        .unwrap();
    let config = verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig {
        lease_ttl_secs: 300,
        ..verlet::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default()
    };
    let clock: std::sync::Arc<dyn verlet::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(FixedClock(1_700_000_000_000));
    let authority = std::sync::Arc::new(
        verlet::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
            store.clone(),
            config,
            std::sync::Arc::clone(&clock),
        )
        .await
        .unwrap(),
    );
    let own_stream = verlet::daemon::remote_store::queue::remote_ingress_queue_stream_id(own);
    let grant = authority
        .grant_lease(
            &verlet::daemon::remote_store::lease::StreamPrefixScope::new(own_stream.as_str()),
            &verlet_runtime_contracts::handle::DispatchId::new("queue-reader"),
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    let (_, token) = authority.mint_credential(&grant).await.unwrap();
    let ordinary_stream = verlet_history::EventStreamId::new(format!("thread:{own}"));
    let ordinary_grant = authority
        .grant_lease(
            &verlet::daemon::remote_store::lease::StreamPrefixScope::new(ordinary_stream.as_str()),
            &verlet_runtime_contracts::handle::DispatchId::new("ordinary-stream-reader"),
            verlet::daemon::remote_store::lease::StreamLeaseLineage::default(),
        )
        .await
        .unwrap();
    let (_, ordinary_token) = authority.mint_credential(&ordinary_grant).await.unwrap();
    let endpoint = verlet::daemon::remote_store::endpoint::SqliteSyncEndpoint::new(
        store.clone(),
        authority,
        clock,
    )
    .await
    .unwrap();

    let records = endpoint
        .pull_after(&token, &own_stream, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload["dispatch_id"], "own-dispatch");
    let err = endpoint
        .pull_after(
            &token,
            &verlet::daemon::remote_store::queue::remote_ingress_queue_stream_id(other),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, verlet::kernel::runtime_host::VerletError::History(message) if message == "sync pull not authorized")
    );
    let err = endpoint
        .pull_after(&ordinary_token, &own_stream, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, verlet::kernel::runtime_host::VerletError::History(message) if message == "sync pull not authorized")
    );

    let colon_descendant = verlet_history::EventStreamId::new(format!("{own_stream}:ordinary"));
    store
        .append_events(
            &colon_descendant,
            vec![verlet_history::NewEventRecord::witnessed(
                verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id":"must-stay-hidden"}),
            )],
        )
        .await
        .unwrap();
    let err = endpoint
        .pull_after(&token, &colon_descendant, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, verlet::kernel::runtime_host::VerletError::History(message) if message == "invalid remote ingress queue stream")
    );

    assert_eq!(
        verlet::daemon::remote_store::queue::remote_ingress_queue_target(&own_stream),
        Some(own)
    );
    assert_eq!(
        verlet::daemon::remote_store::queue::remote_ingress_queue_target(
            &verlet_history::EventStreamId::new(format!("{own_stream}:suffix"))
        ),
        None
    );
    assert_eq!(
        verlet::daemon::remote_store::queue::remote_ingress_queue_target(
            &verlet_history::EventStreamId::new(format!("sync-ingressx:{own}"))
        ),
        None
    );
}

#[tokio::test]
async fn parent_tail_refreshes_snapshot_and_cursor_replay_is_verified() {
    let path = temp_db_path("tail-refresh");
    let store = verlet_history_sqlite::SqliteSessionStore::open(&path)
        .await
        .unwrap();
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let tail = verlet::daemon::remote_store::tail::SqliteRemoteStreamTail::new(store.clone());
    let start = verlet::daemon::remote_store::tail::RemoteStreamTailCursor {
        stream_id: stream_id.clone(),
        cursor: None,
    };
    let empty = tail.poll(&start).await.unwrap();
    assert!(empty.records.is_empty());
    assert_eq!(empty.next, start);

    let database = store.sqlite_database();
    let mut held_connection = database.connect().await.unwrap();
    let held_snapshot = held_connection
        .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Deferred)
        .await
        .unwrap();
    let mut before_rows = held_snapshot
        .query(
            "SELECT COUNT(*) FROM event_records WHERE stream_id = ?1",
            verlet_sqlite::params![stream_id.as_str()],
        )
        .await
        .unwrap();
    assert_eq!(
        before_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        0
    );
    drop(before_rows);

    store
        .append_events(
            &stream_id,
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id":"one"}),
            )],
        )
        .await
        .unwrap();
    let mut stale_rows = held_snapshot
        .query(
            "SELECT COUNT(*) FROM event_records WHERE stream_id = ?1",
            verlet_sqlite::params![stream_id.as_str()],
        )
        .await
        .unwrap();
    assert_eq!(
        stale_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        0,
        "the regression fixture must retain its pre-pull snapshot"
    );
    drop(stale_rows);
    let first = tail.poll(&empty.next).await.unwrap();
    assert_eq!(
        first.records.len(),
        1,
        "a poll after a held pre-append snapshot must reopen"
    );
    held_snapshot.rollback().await.unwrap();

    store
        .append_events(
            &stream_id,
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates,
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id":"two"}),
            )],
        )
        .await
        .unwrap();
    let second = tail.poll(&first.next).await.unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(
        second.records[0].sequence,
        verlet_history::EventSequence::new(2)
    );

    let mut poisoned = first.next;
    poisoned.cursor.as_mut().unwrap().event_id =
        verlet_history::EventRecordId::from_uuid(uuid::Uuid::now_v7());
    assert!(tail.poll(&poisoned).await.is_err());
    let healthy_other = verlet::daemon::remote_store::tail::RemoteStreamTailCursor {
        stream_id: verlet_history::EventStreamId::new("thread:healthy-other"),
        cursor: None,
    };
    assert!(tail.poll(&healthy_other).await.unwrap().records.is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn raw_queue_entry_fixture_is_forward_decodable_and_canonically_reencodes() {
    let thread_id = verlet_runtime_contracts::ThreadId::new();
    let raw = serde_json::json!({
        "schema": verlet::daemon::remote_store::queue::SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1,
        "dispatch_id": "fixture-dispatch",
        "target_thread_id": thread_id,
        "envelope": {
            "id": "ing-fixture",
            "source": {"protocol":"cooldis.remote","instance_id":"parent","future_source":true},
            "conversation": {"external_conversation_id":"thread:fixture","kind":"system","future_conversation":true},
            "content": {"type":"text","text":"fixture","future_content":true},
            "dedupe_key": {"scope":"cooldis.remote.dispatch","key":"fixture-dispatch","future_key":true},
            "received_at_ms": 1_700_000_000_000_u64,
            "future_envelope": true
        },
        "enqueued_at_ms": 0,
        "future_entry": true
    });
    let decoded: verlet::daemon::remote_store::queue::RemoteIngressQueueEntryV1 =
        serde_json::from_value(raw).unwrap();
    assert_eq!(
        decoded.schema,
        verlet::daemon::remote_store::queue::SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1
    );
    assert_eq!(
        decoded.dispatch_id,
        verlet_runtime_contracts::handle::DispatchId::new("fixture-dispatch")
    );
    assert_eq!(decoded.target_thread_id, thread_id);
    let canonical = serde_json::to_value(&decoded).unwrap();
    assert!(canonical.get("future_entry").is_none());
    assert_eq!(
        serde_json::from_value::<verlet::daemon::remote_store::queue::RemoteIngressQueueEntryV1>(
            canonical
        )
        .unwrap(),
        decoded
    );
}

fn seeded_bool(seed: u64) -> bool {
    let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) & 1 == 1
}

fn temp_db_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "verlet-emo430-{name}-{}.sqlite3",
        uuid::Uuid::now_v7()
    ))
}
