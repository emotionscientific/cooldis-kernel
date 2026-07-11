use async_trait::async_trait;
use cooldis_history::{
    EventKind, EventOrigin, EventProvenance, EventRecord, EventRecordId, EventSequence, EventStore,
    EventStreamId, HistoryError, HistoryResult, NewEventRecord, NewObservationRecord,
    ObservationId, ObservationRecord, ObservationStore, STREAM_RECORD_SCHEMA_V1, SessionContext,
    SessionContextSourceCut, SessionEntry, SessionEntryId, SessionEntryKind, SessionStore,
    ThreadBaseRef, ThreadBranchSelectedPayload, ThreadForkReason, append_model_visible_messages,
    codec_error, coordinates_with_thread_id, decode_entry, parse_event_origin, parse_thread_id,
    parse_uuid, session_entry_event, session_entry_event_with_provenance,
    session_entry_is_user_authored, storage_error, validate_entry_coordinates, validate_new_event,
    validate_thread_base_ref,
};
use cooldis_runtime_contracts::{ThreadCheckpointId, ThreadCoordinates, ThreadId};
use rusqlite::{OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SqliteSessionStore {
    inner: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteSessionStore {
    pub fn open(path: impl AsRef<Path>) -> HistoryResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(storage_error)?;
        }
        let connection = rusqlite::Connection::open(path).map_err(storage_error)?;
        Self::from_connection(connection)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> HistoryResult<Self> {
        let connection = rusqlite::Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(storage_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage_error)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn in_memory() -> HistoryResult<Self> {
        let connection = rusqlite::Connection::open_in_memory().map_err(storage_error)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: rusqlite::Connection) -> HistoryResult<Self> {
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(storage_error)?;
        init_sqlite_schema(&connection)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    fn lock_connection(&self) -> HistoryResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.inner
            .lock()
            .map_err(|err| HistoryError::Storage(format!("sqlite connection lock poisoned: {err}")))
    }

    pub fn list_control_stream_coordinates(&self) -> HistoryResult<Vec<ThreadCoordinates>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT DISTINCT tenant_id, user_id, session_id, thread_id
                 FROM event_records
                 WHERE stream_id LIKE 'control:%'
                 ORDER BY tenant_id, user_id, session_id, thread_id",
            )
            .map_err(storage_error)?;
        let mut rows = statement.query([]).map_err(storage_error)?;
        let mut coordinates = Vec::new();
        while let Some(row) = rows.next().map_err(storage_error)? {
            coordinates.push(ThreadCoordinates {
                tenant_id: row.get(0).map_err(storage_error)?,
                user_id: row.get(1).map_err(storage_error)?,
                session_id: row.get(2).map_err(storage_error)?,
                thread_id: parse_thread_id(&row.get::<_, String>(3).map_err(storage_error)?)?,
            });
        }
        Ok(coordinates)
    }

    pub fn list_thread_events(&self, thread_id: ThreadId) -> HistoryResult<Vec<EventRecord>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                        tenant_id, user_id, session_id, created_at_ms, kind, origin,
                        provenance_json, payload_json
                 FROM event_records
                 WHERE thread_id = ?1
                 ORDER BY rowid",
            )
            .map_err(storage_error)?;
        let mut rows = statement
            .query(params![thread_id.to_string()])
            .map_err(storage_error)?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().map_err(storage_error)? {
            events.push(sqlite_event_from_row(row)?);
        }
        Ok(events)
    }
}

#[async_trait]
impl SessionStore for SqliteSessionStore {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, None)
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, Some(provenance))
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        let connection = self.lock_connection()?;
        let thread_id = coordinates.thread_id.to_string();
        sqlite_active_leaf_entry(&connection, &thread_id)?
            .map(|entry| {
                validate_entry_coordinates(coordinates, &entry)?;
                Ok(entry.entry_id)
            })
            .transpose()
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        let thread_id = coordinates.thread_id.to_string();
        let prior_entry_id = sqlite_active_leaf_entry(&tx, &thread_id)?
            .map(|entry| {
                validate_entry_coordinates(coordinates, &entry)?;
                Ok(entry.entry_id)
            })
            .transpose()?;
        if let Some(leaf_entry_id) = leaf_entry_id {
            sqlite_branch_path(&tx, coordinates, leaf_entry_id)?;
        }
        let payload = serde_json::to_value(ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: leaf_entry_id,
            prior_entry_id,
        })
        .map_err(codec_error)?;
        sqlite_insert_event(
            &tx,
            &EventStreamId::for_thread(coordinates),
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::ThreadBranchSelected,
                payload,
            ),
        )?;
        match leaf_entry_id {
            Some(leaf_entry_id) => {
                tx.execute(
                    "INSERT INTO active_leaves (thread_id, entry_id)
                     VALUES (?1, ?2)
                     ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                    params![thread_id, leaf_entry_id.to_string()],
                )
                .map_err(storage_error)?;
            }
            None => {
                tx.execute(
                    "DELETE FROM active_leaves WHERE thread_id = ?1",
                    params![thread_id],
                )
                .map_err(storage_error)?;
            }
        }
        tx.commit().map_err(storage_error)?;
        Ok(())
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        let connection = self.lock_connection()?;
        sqlite_build_context(&connection, coordinates, None, false, &mut HashSet::new())
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        tx.execute(
            "DELETE FROM thread_bases WHERE child_thread_id = ?1",
            params![target_coordinates.thread_id.to_string()],
        )
        .map_err(storage_error)?;
        let Some(source_leaf) = source_leaf else {
            tx.execute(
                "DELETE FROM active_leaves WHERE thread_id = ?1",
                params![target_coordinates.thread_id.to_string()],
            )
            .map_err(storage_error)?;
            tx.commit().map_err(storage_error)?;
            return Ok(None);
        };
        let entries = sqlite_branch_path(&tx, source_coordinates, source_leaf)?;
        let mut parent_entry_id = None;
        let mut latest_entry_id = None;
        for source_entry in entries {
            let entry = SessionEntry::new(
                target_coordinates.clone(),
                parent_entry_id,
                source_entry.kind.clone(),
            );
            sqlite_insert_entry(&tx, &entry)?;
            parent_entry_id = Some(entry.entry_id);
            latest_entry_id = Some(entry.entry_id);
        }
        if let Some(entry_id) = latest_entry_id {
            tx.execute(
                "INSERT INTO active_leaves (thread_id, entry_id)
                 VALUES (?1, ?2)
                 ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                params![
                    target_coordinates.thread_id.to_string(),
                    entry_id.to_string()
                ],
            )
            .map_err(storage_error)?;
        }
        tx.commit().map_err(storage_error)?;
        Ok(latest_entry_id)
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        validate_thread_base_ref(source_coordinates, target_coordinates, &base)?;
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        validate_sqlite_base_cycle(
            &tx,
            target_coordinates.thread_id,
            source_coordinates.thread_id,
        )?;
        if let Some(parent_leaf) = base.parent_leaf_entry_id {
            sqlite_build_context(
                &tx,
                source_coordinates,
                Some(parent_leaf),
                false,
                &mut HashSet::new(),
            )?;
        }
        tx.execute(
            "DELETE FROM active_leaves WHERE thread_id = ?1",
            params![target_coordinates.thread_id.to_string()],
        )
        .map_err(storage_error)?;
        sqlite_insert_thread_base(&tx, &base)?;
        tx.commit().map_err(storage_error)?;
        Ok(())
    }
}

impl SqliteSessionStore {
    async fn append_inner(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: Option<EventProvenance>,
    ) -> HistoryResult<SessionEntry> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        let thread_id = coordinates.thread_id.to_string();
        let parent_entry_id = match parent_entry_id {
            Some(parent) => {
                let parent_entry = sqlite_load_entry(&tx, &thread_id, parent)?
                    .ok_or(HistoryError::EntryNotFound(parent))?;
                validate_entry_coordinates(coordinates, &parent_entry)?;
                Some(parent)
            }
            None => sqlite_active_leaf_entry(&tx, &thread_id)?
                .map(|entry| {
                    validate_entry_coordinates(coordinates, &entry)?;
                    Ok(entry.entry_id)
                })
                .transpose()?,
        };

        let entry = SessionEntry::new(coordinates.clone(), parent_entry_id, kind);
        sqlite_insert_entry_with_optional_provenance(&tx, &entry, provenance)?;
        tx.execute(
            "INSERT INTO active_leaves (thread_id, entry_id)
             VALUES (?1, ?2)
             ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
            params![
                entry.coordinates.thread_id.to_string(),
                entry.entry_id.to_string()
            ],
        )
        .map_err(storage_error)?;
        tx.commit().map_err(storage_error)?;
        Ok(entry)
    }
}

#[async_trait]
impl EventStore for SqliteSessionStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let mut connection = self.lock_connection()?;
        let tx = connection.transaction().map_err(storage_error)?;
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            appended.push(sqlite_insert_event(&tx, stream_id, record)?);
        }
        tx.commit().map_err(storage_error)?;
        Ok(appended)
    }

    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let mut connection = self.lock_connection()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let actual_next_sequence = sqlite_next_event_sequence(&tx, stream_id)?;
        if actual_next_sequence != expected_next_sequence.get() {
            return Err(HistoryError::AppendFenceConflict {
                stream_id: stream_id.clone(),
                expected_next_sequence: expected_next_sequence.get(),
                actual_next_sequence,
            });
        }
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            appended.push(sqlite_insert_event(&tx, stream_id, record)?);
        }
        tx.commit().map_err(storage_error)?;
        Ok(appended)
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let connection = self.lock_connection()?;
        sqlite_read_events(&connection, stream_id, from_sequence)
    }
}

#[async_trait]
impl ObservationStore for SqliteSessionStore {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        let connection = self.lock_connection()?;
        sqlite_insert_observation(&connection, record)
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        let connection = self.lock_connection()?;
        sqlite_list_observations(&connection, scope, kind)
    }
}

fn init_sqlite_schema(connection: &rusqlite::Connection) -> HistoryResult<()> {
    connection
        .execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS session_entries (
                entry_id TEXT PRIMARY KEY NOT NULL,
                parent_entry_id TEXT REFERENCES session_entries(entry_id),
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                entry_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_session_entries_scope
                ON session_entries(tenant_id, user_id, session_id, thread_id, created_at_ms);

            CREATE INDEX IF NOT EXISTS idx_session_entries_parent
                ON session_entries(thread_id, parent_entry_id);

            CREATE TABLE IF NOT EXISTS active_leaves (
                thread_id TEXT PRIMARY KEY NOT NULL,
                entry_id TEXT NOT NULL REFERENCES session_entries(entry_id)
            );

            CREATE TABLE IF NOT EXISTS thread_bases (
                child_thread_id TEXT PRIMARY KEY NOT NULL,
                parent_thread_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                parent_leaf_entry_id TEXT REFERENCES session_entries(entry_id),
                parent_stream_id TEXT NOT NULL,
                parent_stream_to_sequence INTEGER,
                parent_binding_snapshot_id TEXT,
                reason TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_thread_bases_parent
                ON thread_bases(parent_thread_id);

            CREATE TABLE IF NOT EXISTS event_records (
                event_id TEXT PRIMARY KEY NOT NULL,
                schema TEXT NOT NULL DEFAULT 'cooldis.stream.record/1',
                payload_schema TEXT NOT NULL DEFAULT '',
                stream_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                origin TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(stream_id, sequence)
            );

            CREATE INDEX IF NOT EXISTS idx_event_records_stream
                ON event_records(stream_id, sequence);

            CREATE INDEX IF NOT EXISTS idx_event_records_scope
                ON event_records(tenant_id, user_id, session_id, thread_id, created_at_ms);

            CREATE TABLE IF NOT EXISTS observation_records (
                observation_id TEXT PRIMARY KEY NOT NULL,
                kind TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                supersedes_observation_id TEXT,
                confidence REAL
            );

            CREATE INDEX IF NOT EXISTS idx_observation_records_scope
                ON observation_records(tenant_id, user_id, session_id, thread_id, kind, created_at_ms);
            "#,
        )
        .map_err(storage_error)?;
    sqlite_migrate_event_records_schema(connection)?;
    sqlite_rebuild_active_leaves_from_events(connection)
}

fn sqlite_rebuild_active_leaves_from_events(
    connection: &rusqlite::Connection,
) -> HistoryResult<()> {
    let tx = connection.unchecked_transaction().map_err(storage_error)?;
    let mut selected_threads = HashSet::new();
    {
        let mut statement = tx
            .prepare(
                "SELECT DISTINCT thread_id
                 FROM event_records
                 WHERE kind = ?1",
            )
            .map_err(storage_error)?;
        let mut rows = statement
            .query(params![EventKind::ThreadBranchSelected.as_str()])
            .map_err(storage_error)?;
        while let Some(row) = rows.next().map_err(storage_error)? {
            selected_threads.insert(row.get::<_, String>(0).map_err(storage_error)?);
        }
    }
    if selected_threads.is_empty() {
        return tx.commit().map_err(storage_error);
    }
    for thread_id in &selected_threads {
        tx.execute(
            "DELETE FROM active_leaves WHERE thread_id = ?1",
            params![thread_id],
        )
        .map_err(storage_error)?;
    }

    let mut journal_entries = Vec::new();
    {
        let mut statement = tx
            .prepare(
                "SELECT kind, thread_id, payload_json
                 FROM event_records
                 WHERE kind IN (?1, ?2)
                 ORDER BY rowid",
            )
            .map_err(storage_error)?;
        let mut rows = statement
            .query(params![
                EventKind::SessionEntryAppended.as_str(),
                EventKind::ThreadBranchSelected.as_str(),
            ])
            .map_err(storage_error)?;
        while let Some(row) = rows.next().map_err(storage_error)? {
            journal_entries.push((
                row.get::<_, String>(0).map_err(storage_error)?,
                row.get::<_, String>(1).map_err(storage_error)?,
                row.get::<_, String>(2).map_err(storage_error)?,
            ));
        }
    }

    for (kind, stored_thread_id, payload_json) in journal_entries {
        if !selected_threads.contains(&stored_thread_id) {
            continue;
        }
        let selected_entry_id = if kind == EventKind::ThreadBranchSelected.as_str() {
            let payload: ThreadBranchSelectedPayload =
                serde_json::from_str(&payload_json).map_err(codec_error)?;
            if payload.thread_id.to_string() != stored_thread_id {
                return Err(HistoryError::Codec(format!(
                    "thread.branch.selected payload thread {} does not match event thread {stored_thread_id}",
                    payload.thread_id
                )));
            }
            payload.selected_entry_id
        } else {
            let payload: serde_json::Value =
                serde_json::from_str(&payload_json).map_err(codec_error)?;
            serde_json::from_value(payload["entry_id"].clone()).map_err(codec_error)?
        };
        match selected_entry_id {
            Some(entry_id) => {
                if sqlite_load_entry(&tx, &stored_thread_id, entry_id)?.is_none() {
                    return Err(HistoryError::EntryNotFound(entry_id));
                }
                tx.execute(
                    "INSERT INTO active_leaves (thread_id, entry_id)
                         VALUES (?1, ?2)
                         ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                    params![stored_thread_id, entry_id.to_string()],
                )
                .map_err(storage_error)?;
            }
            None => {
                tx.execute(
                    "DELETE FROM active_leaves WHERE thread_id = ?1",
                    params![stored_thread_id],
                )
                .map_err(storage_error)?;
            }
        }
    }
    tx.commit().map_err(storage_error)
}

/// Migrates legacy event rows honestly: reconstructed provenance names this
/// migration and does not impersonate the runtime component that may have
/// produced the original unversioned row.
fn sqlite_migrate_event_records_schema(connection: &rusqlite::Connection) -> HistoryResult<()> {
    let mut added_identity_column = false;
    let mut added_origin_column = false;
    if !sqlite_table_has_column(connection, "event_records", "schema")? {
        connection
            .execute(
                "ALTER TABLE event_records
                 ADD COLUMN schema TEXT NOT NULL DEFAULT 'cooldis.stream.record/1'",
                [],
            )
            .map_err(storage_error)?;
        added_identity_column = true;
    }
    if !sqlite_table_has_column(connection, "event_records", "payload_schema")? {
        connection
            .execute(
                "ALTER TABLE event_records
                 ADD COLUMN payload_schema TEXT NOT NULL DEFAULT ''",
                [],
            )
            .map_err(storage_error)?;
        added_identity_column = true;
    }
    if !sqlite_table_has_column(connection, "event_records", "origin")? {
        connection
            .execute(
                "ALTER TABLE event_records
                 ADD COLUMN origin TEXT NOT NULL DEFAULT 'witnessed'",
                [],
            )
            .map_err(storage_error)?;
        added_origin_column = true;
    }
    if !sqlite_table_has_column(connection, "event_records", "provenance_json")? {
        connection
            .execute(
                "ALTER TABLE event_records
                 ADD COLUMN provenance_json TEXT NOT NULL DEFAULT '{}'",
                [],
            )
            .map_err(storage_error)?;
        added_origin_column = true;
    }
    if added_identity_column {
        let mut identity_rows = Vec::new();
        {
            let mut statement = connection
                .prepare("SELECT event_id, kind FROM event_records ORDER BY stream_id, sequence")
                .map_err(storage_error)?;
            let mut rows = statement.query([]).map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                identity_rows.push((
                    row.get::<_, String>(0).map_err(storage_error)?,
                    row.get::<_, String>(1).map_err(storage_error)?,
                ));
            }
        }
        for (event_id, kind) in identity_rows {
            let payload_schema = kind
                .parse::<EventKind>()
                .map(|kind| kind.payload_schema_id().to_string())
                .unwrap_or_default();
            connection
                .execute(
                    "UPDATE event_records
                     SET schema = ?2,
                         payload_schema = ?3
                     WHERE event_id = ?1",
                    params![event_id, STREAM_RECORD_SCHEMA_V1, payload_schema],
                )
                .map_err(storage_error)?;
        }
    }
    if !added_origin_column {
        return Ok(());
    }

    const ORIGIN_BACKFILL_MIGRATION: &str = "migration:origin-backfill@v1";

    let mut rows_to_backfill = Vec::new();
    {
        let mut statement = connection
            .prepare(
                "SELECT event_id, stream_id, kind, payload_json
                 FROM event_records
                 WHERE kind IN (?1, ?2)
                 ORDER BY stream_id, sequence",
            )
            .map_err(storage_error)?;
        let mut rows = statement
            .query(params![
                EventKind::SessionEntryAppended.as_str(),
                EventKind::ContextCompileCompleted.as_str(),
            ])
            .map_err(storage_error)?;
        while let Some(row) = rows.next().map_err(storage_error)? {
            rows_to_backfill.push((
                row.get::<_, String>(0).map_err(storage_error)?,
                row.get::<_, String>(1).map_err(storage_error)?,
                row.get::<_, String>(2).map_err(storage_error)?,
                row.get::<_, String>(3).map_err(storage_error)?,
            ));
        }
    }

    for (event_id, _stream_id, kind, payload_json) in rows_to_backfill {
        let (origin, provenance) = if kind == EventKind::SessionEntryAppended.as_str() {
            match serde_json::from_str::<SessionEntry>(&payload_json) {
                Ok(entry) if session_entry_is_user_authored(&entry.kind) => {
                    (EventOrigin::Witnessed, EventProvenance::default())
                }
                _ => (
                    EventOrigin::Discharged,
                    EventProvenance {
                        discharged_by: Some(ORIGIN_BACKFILL_MIGRATION.to_string()),
                        ..EventProvenance::default()
                    },
                ),
            }
        } else {
            (
                EventOrigin::Discharged,
                EventProvenance {
                    discharged_by: Some(ORIGIN_BACKFILL_MIGRATION.to_string()),
                    ..EventProvenance::default()
                },
            )
        };
        let provenance_json = serde_json::to_string(&provenance).map_err(codec_error)?;
        connection
            .execute(
                "UPDATE event_records
                 SET origin = ?1, provenance_json = ?2
                 WHERE event_id = ?3",
                params![origin.as_str(), provenance_json, event_id],
            )
            .map_err(storage_error)?;
    }
    Ok(())
}

fn sqlite_table_has_column(
    connection: &rusqlite::Connection,
    table: &str,
    column: &str,
) -> HistoryResult<bool> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(storage_error)?;
    let mut rows = statement.query([]).map_err(storage_error)?;
    while let Some(row) = rows.next().map_err(storage_error)? {
        let name: String = row.get(1).map_err(storage_error)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn sqlite_load_entry(
    connection: &rusqlite::Connection,
    thread_id: &str,
    entry_id: SessionEntryId,
) -> HistoryResult<Option<SessionEntry>> {
    let entry_json = connection
        .query_row(
            "SELECT entry_json FROM session_entries WHERE thread_id = ?1 AND entry_id = ?2",
            params![thread_id, entry_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage_error)?;
    entry_json.map(|json| decode_entry(&json)).transpose()
}

fn sqlite_active_leaf_entry(
    connection: &rusqlite::Connection,
    thread_id: &str,
) -> HistoryResult<Option<SessionEntry>> {
    let entry_json = connection
        .query_row(
            "SELECT e.entry_json
             FROM active_leaves a
             JOIN session_entries e ON e.thread_id = a.thread_id AND e.entry_id = a.entry_id
             WHERE a.thread_id = ?1",
            params![thread_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage_error)?;
    entry_json.map(|json| decode_entry(&json)).transpose()
}

fn sqlite_insert_entry(tx: &rusqlite::Transaction<'_>, entry: &SessionEntry) -> HistoryResult<()> {
    sqlite_insert_entry_with_optional_provenance(tx, entry, None)
}

fn sqlite_insert_entry_with_optional_provenance(
    tx: &rusqlite::Transaction<'_>,
    entry: &SessionEntry,
    provenance: Option<EventProvenance>,
) -> HistoryResult<()> {
    let entry_json = serde_json::to_string(entry).map_err(codec_error)?;
    tx.execute(
        "INSERT INTO session_entries (
            entry_id,
            parent_entry_id,
            thread_id,
            tenant_id,
            user_id,
            session_id,
            created_at_ms,
            entry_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            entry.entry_id.to_string(),
            entry.parent_entry_id.map(|id| id.to_string()),
            entry.coordinates.thread_id.to_string(),
            entry.coordinates.tenant_id.as_str(),
            entry.coordinates.user_id.as_str(),
            entry.coordinates.session_id.as_str(),
            entry.created_at_ms,
            entry_json,
        ],
    )
    .map_err(storage_error)?;
    sqlite_insert_event(
        tx,
        &EventStreamId::for_thread(&entry.coordinates),
        match provenance {
            Some(provenance) => session_entry_event_with_provenance(entry, provenance),
            None => session_entry_event(entry),
        },
    )?;
    Ok(())
}

fn sqlite_insert_event(
    tx: &rusqlite::Transaction<'_>,
    stream_id: &EventStreamId,
    record: NewEventRecord,
) -> HistoryResult<EventRecord> {
    validate_new_event(&record)?;
    let event_id = record.id;
    let event_id_exists = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM event_records WHERE event_id = ?1)",
            params![event_id.to_string()],
            |row| row.get::<_, bool>(0),
        )
        .map_err(storage_error)?;
    if event_id_exists {
        return Err(HistoryError::DuplicateEventId(event_id));
    }
    let next_sequence = sqlite_next_event_sequence(tx, stream_id)?;
    let event = EventRecord::from_new(stream_id.clone(), EventSequence::new(next_sequence), record);
    event.validate_stream_record_v1()?;
    let payload_json = serde_json::to_string(&event.payload).map_err(codec_error)?;
    let provenance_json = serde_json::to_string(&event.provenance).map_err(codec_error)?;
    tx.execute(
        "INSERT INTO event_records (
            event_id,
            schema,
            payload_schema,
            stream_id,
            sequence,
            thread_id,
            tenant_id,
            user_id,
            session_id,
            created_at_ms,
            kind,
            origin,
            provenance_json,
            payload_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            event.id.to_string(),
            STREAM_RECORD_SCHEMA_V1,
            event.kind.payload_schema_id(),
            event.stream_id.as_str(),
            event.sequence.get(),
            event.coordinates.thread_id.to_string(),
            event.coordinates.tenant_id.as_str(),
            event.coordinates.user_id.as_str(),
            event.coordinates.session_id.as_str(),
            event.created_at_ms,
            event.kind.as_str(),
            event.origin.as_str(),
            provenance_json,
            payload_json,
        ],
    )
    .map_err(storage_error)?;
    Ok(event)
}

fn sqlite_next_event_sequence(
    connection: &rusqlite::Connection,
    stream_id: &EventStreamId,
) -> HistoryResult<i64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM event_records WHERE stream_id = ?1",
            params![stream_id.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(storage_error)
}

fn sqlite_read_events(
    connection: &rusqlite::Connection,
    stream_id: &EventStreamId,
    from_sequence: Option<EventSequence>,
) -> HistoryResult<Vec<EventRecord>> {
    let mut events = Vec::new();
    match from_sequence {
        Some(sequence) => {
            let mut statement = connection
                .prepare(
                    "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                            tenant_id, user_id, session_id, created_at_ms, kind, origin,
                            provenance_json, payload_json
                     FROM event_records
                     WHERE stream_id = ?1 AND sequence >= ?2
                     ORDER BY sequence",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![stream_id.as_str(), sequence.get()])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                events.push(sqlite_event_from_row(row)?);
            }
        }
        None => {
            let mut statement = connection
                .prepare(
                    "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                            tenant_id, user_id, session_id, created_at_ms, kind, origin,
                            provenance_json, payload_json
                     FROM event_records
                     WHERE stream_id = ?1
                     ORDER BY sequence",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![stream_id.as_str()])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                events.push(sqlite_event_from_row(row)?);
            }
        }
    }
    Ok(events)
}

fn sqlite_event_from_row(row: &rusqlite::Row<'_>) -> HistoryResult<EventRecord> {
    let event_id: String = row.get(0).map_err(storage_error)?;
    let schema: String = row.get(1).map_err(storage_error)?;
    if schema != STREAM_RECORD_SCHEMA_V1 {
        return Err(codec_error(format!(
            "event record {event_id} has unsupported stream schema {schema:?}"
        )));
    }
    let payload_schema: String = row.get(2).map_err(storage_error)?;
    let kind: String = row.get(10).map_err(storage_error)?;
    let kind = kind.parse::<EventKind>()?;
    let expected_payload_schema = kind.payload_schema_id();
    if payload_schema != expected_payload_schema {
        return Err(codec_error(format!(
            "event record {event_id} kind {kind} has payload_schema {payload_schema:?}, expected {expected_payload_schema:?}"
        )));
    }
    let origin: String = row.get(11).map_err(storage_error)?;
    let provenance_json: String = row.get(12).map_err(storage_error)?;
    let payload_json: String = row.get(13).map_err(storage_error)?;
    let event = EventRecord {
        id: EventRecordId::from_uuid(parse_uuid(&event_id)?),
        stream_id: EventStreamId::new(row.get::<_, String>(3).map_err(storage_error)?),
        sequence: EventSequence::new(row.get(4).map_err(storage_error)?),
        coordinates: ThreadCoordinates {
            thread_id: parse_thread_id(&row.get::<_, String>(5).map_err(storage_error)?)?,
            tenant_id: row.get(6).map_err(storage_error)?,
            user_id: row.get(7).map_err(storage_error)?,
            session_id: row.get(8).map_err(storage_error)?,
        },
        created_at_ms: row.get(9).map_err(storage_error)?,
        kind,
        origin: parse_event_origin(&origin)?,
        provenance: serde_json::from_str(&provenance_json).map_err(codec_error)?,
        payload: serde_json::from_str(&payload_json).map_err(codec_error)?,
    };
    event.validate_stream_record_v1()?;
    Ok(event)
}

fn sqlite_insert_observation(
    connection: &rusqlite::Connection,
    record: NewObservationRecord,
) -> HistoryResult<ObservationRecord> {
    let record = ObservationRecord::from(record);
    let payload_json = serde_json::to_string(&record.payload).map_err(codec_error)?;
    let provenance_json = serde_json::to_string(&record.provenance).map_err(codec_error)?;
    connection
        .execute(
            "INSERT INTO observation_records (
                observation_id,
                kind,
                thread_id,
                tenant_id,
                user_id,
                session_id,
                created_at_ms,
                payload_json,
                provenance_json,
                supersedes_observation_id,
                confidence
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                record.id.to_string(),
                record.kind.as_str(),
                record.scope.thread_id.to_string(),
                record.scope.tenant_id.as_str(),
                record.scope.user_id.as_str(),
                record.scope.session_id.as_str(),
                record.created_at_ms,
                payload_json,
                provenance_json,
                record.supersedes.map(|id| id.to_string()),
                record.confidence,
            ],
        )
        .map_err(storage_error)?;
    Ok(record)
}

fn sqlite_list_observations(
    connection: &rusqlite::Connection,
    scope: &ThreadCoordinates,
    kind: Option<&str>,
) -> HistoryResult<Vec<ObservationRecord>> {
    let mut observations = Vec::new();
    match kind {
        Some(kind) => {
            let mut statement = connection
                .prepare(
                    "SELECT observation_id, kind, thread_id, tenant_id, user_id, session_id,
                            created_at_ms, payload_json, provenance_json,
                            supersedes_observation_id, confidence
                     FROM observation_records
                     WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
                       AND thread_id = ?4 AND kind = ?5
                     ORDER BY created_at_ms, observation_id",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![
                    scope.tenant_id.as_str(),
                    scope.user_id.as_str(),
                    scope.session_id.as_str(),
                    scope.thread_id.to_string(),
                    kind,
                ])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                observations.push(sqlite_observation_from_row(row)?);
            }
        }
        None => {
            let mut statement = connection
                .prepare(
                    "SELECT observation_id, kind, thread_id, tenant_id, user_id, session_id,
                            created_at_ms, payload_json, provenance_json,
                            supersedes_observation_id, confidence
                     FROM observation_records
                     WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
                       AND thread_id = ?4
                     ORDER BY created_at_ms, observation_id",
                )
                .map_err(storage_error)?;
            let mut rows = statement
                .query(params![
                    scope.tenant_id.as_str(),
                    scope.user_id.as_str(),
                    scope.session_id.as_str(),
                    scope.thread_id.to_string(),
                ])
                .map_err(storage_error)?;
            while let Some(row) = rows.next().map_err(storage_error)? {
                observations.push(sqlite_observation_from_row(row)?);
            }
        }
    }
    Ok(observations)
}

fn sqlite_observation_from_row(row: &rusqlite::Row<'_>) -> HistoryResult<ObservationRecord> {
    let observation_id: String = row.get(0).map_err(storage_error)?;
    let payload_json: String = row.get(7).map_err(storage_error)?;
    let provenance_json: String = row.get(8).map_err(storage_error)?;
    let supersedes: Option<String> = row.get(9).map_err(storage_error)?;
    Ok(ObservationRecord {
        id: ObservationId::from_uuid(parse_uuid(&observation_id)?),
        kind: row.get(1).map_err(storage_error)?,
        scope: ThreadCoordinates {
            thread_id: parse_thread_id(&row.get::<_, String>(2).map_err(storage_error)?)?,
            tenant_id: row.get(3).map_err(storage_error)?,
            user_id: row.get(4).map_err(storage_error)?,
            session_id: row.get(5).map_err(storage_error)?,
        },
        created_at_ms: row.get(6).map_err(storage_error)?,
        payload: serde_json::from_str(&payload_json).map_err(codec_error)?,
        provenance: serde_json::from_str(&provenance_json).map_err(codec_error)?,
        supersedes: supersedes
            .map(|id| parse_uuid(&id).map(ObservationId::from_uuid))
            .transpose()?,
        confidence: row.get(10).map_err(storage_error)?,
    })
}

fn sqlite_branch_path(
    connection: &rusqlite::Connection,
    coordinates: &ThreadCoordinates,
    leaf_entry_id: SessionEntryId,
) -> HistoryResult<Vec<SessionEntry>> {
    let thread_id = coordinates.thread_id.to_string();
    let mut path = Vec::new();
    let mut cursor = Some(leaf_entry_id);
    while let Some(entry_id) = cursor {
        let entry = sqlite_load_entry(connection, &thread_id, entry_id)?
            .ok_or(HistoryError::EntryNotFound(entry_id))?;
        validate_entry_coordinates(coordinates, &entry)?;
        cursor = entry.parent_entry_id;
        path.push(entry);
    }
    path.reverse();
    Ok(path)
}

fn sqlite_build_context(
    connection: &rusqlite::Connection,
    coordinates: &ThreadCoordinates,
    local_leaf_override: Option<SessionEntryId>,
    inherited: bool,
    visiting: &mut HashSet<ThreadId>,
) -> HistoryResult<SessionContext> {
    if !visiting.insert(coordinates.thread_id) {
        return Err(HistoryError::ThreadBaseCycle {
            child_thread_id: coordinates.thread_id,
            ancestor_thread_id: coordinates.thread_id,
        });
    }

    let mut entries = Vec::new();
    let mut source_cuts = Vec::new();
    if let Some(base) = sqlite_load_thread_base(connection, coordinates.thread_id)? {
        if base.child_thread_id != coordinates.thread_id {
            return Err(HistoryError::Storage(format!(
                "thread base child id {} does not match requested thread {}",
                base.child_thread_id, coordinates.thread_id
            )));
        }
        let parent_coordinates = coordinates_with_thread_id(coordinates, base.parent_thread_id);
        let parent_context = sqlite_build_context(
            connection,
            &parent_coordinates,
            base.parent_leaf_entry_id,
            true,
            visiting,
        )?;
        entries.extend(parent_context.entries);
        source_cuts.extend(parent_context.source_cuts);
    }

    let thread_id = coordinates.thread_id.to_string();
    let local_leaf = match local_leaf_override {
        Some(leaf) => Some(leaf),
        None => sqlite_active_leaf_entry(connection, &thread_id)?.map(|entry| entry.entry_id),
    };
    if let Some(local_leaf) = local_leaf {
        let local_path = sqlite_branch_path(connection, coordinates, local_leaf)?;
        if !local_path.is_empty() {
            source_cuts.push(SessionContextSourceCut {
                coordinates: coordinates.clone(),
                stream_id: EventStreamId::for_thread(coordinates),
                inherited,
                entry_ids: local_path.iter().map(|entry| entry.entry_id).collect(),
            });
        }
        entries.extend(local_path);
    }

    visiting.remove(&coordinates.thread_id);
    let mut messages = Vec::new();
    append_model_visible_messages(&entries, &mut messages);
    Ok(SessionContext {
        entries,
        messages,
        source_cuts,
    })
}

fn sqlite_insert_thread_base(
    tx: &rusqlite::Transaction<'_>,
    base: &ThreadBaseRef,
) -> HistoryResult<()> {
    tx.execute(
        "INSERT INTO thread_bases (
            child_thread_id,
            parent_thread_id,
            parent_checkpoint_id,
            parent_leaf_entry_id,
            parent_stream_id,
            parent_stream_to_sequence,
            parent_binding_snapshot_id,
            reason,
            created_at_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(child_thread_id) DO UPDATE SET
            parent_thread_id = excluded.parent_thread_id,
            parent_checkpoint_id = excluded.parent_checkpoint_id,
            parent_leaf_entry_id = excluded.parent_leaf_entry_id,
            parent_stream_id = excluded.parent_stream_id,
            parent_stream_to_sequence = excluded.parent_stream_to_sequence,
            parent_binding_snapshot_id = excluded.parent_binding_snapshot_id,
            reason = excluded.reason,
            created_at_ms = excluded.created_at_ms",
        params![
            base.child_thread_id.to_string(),
            base.parent_thread_id.to_string(),
            base.parent_checkpoint_id.map(|id| id.to_string()),
            base.parent_leaf_entry_id.map(|id| id.to_string()),
            base.parent_stream_id.as_str(),
            base.parent_stream_to_sequence
                .map(|sequence| sequence.get()),
            base.parent_binding_snapshot_id.as_deref(),
            encode_thread_fork_reason(&base.reason)?,
            base.created_at_ms,
        ],
    )
    .map_err(storage_error)?;
    Ok(())
}

fn sqlite_load_thread_base(
    connection: &rusqlite::Connection,
    child_thread_id: ThreadId,
) -> HistoryResult<Option<ThreadBaseRef>> {
    connection
        .query_row(
            "SELECT child_thread_id, parent_thread_id, parent_checkpoint_id,
                    parent_leaf_entry_id, parent_stream_id, parent_stream_to_sequence,
                    parent_binding_snapshot_id, reason, created_at_ms
             FROM thread_bases
             WHERE child_thread_id = ?1",
            params![child_thread_id.to_string()],
            |row| sqlite_thread_base_from_row(row),
        )
        .optional()
        .map_err(storage_error)?
        .transpose()
}

fn sqlite_thread_base_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<HistoryResult<ThreadBaseRef>> {
    let child_thread_id: String = row.get(0)?;
    let parent_thread_id: String = row.get(1)?;
    let parent_checkpoint_id: Option<String> = row.get(2)?;
    let parent_leaf_entry_id: Option<String> = row.get(3)?;
    let parent_stream_to_sequence: Option<i64> = row.get(5)?;
    let reason: String = row.get(7)?;
    Ok((|| {
        Ok(ThreadBaseRef {
            child_thread_id: parse_thread_id(&child_thread_id)?,
            parent_thread_id: parse_thread_id(&parent_thread_id)?,
            parent_checkpoint_id: parent_checkpoint_id
                .map(|id| ThreadCheckpointId::parse_str(&id).map_err(codec_error))
                .transpose()?,
            parent_leaf_entry_id: parent_leaf_entry_id
                .map(|id| parse_uuid(&id).map(SessionEntryId::from_uuid))
                .transpose()?,
            parent_stream_id: EventStreamId::new(row.get::<_, String>(4).map_err(storage_error)?),
            parent_stream_to_sequence: parent_stream_to_sequence.map(EventSequence::new),
            parent_binding_snapshot_id: row.get(6).map_err(storage_error)?,
            reason: decode_thread_fork_reason(&reason)?,
            created_at_ms: row.get(8).map_err(storage_error)?,
        })
    })())
}

fn validate_sqlite_base_cycle(
    connection: &rusqlite::Connection,
    child_thread_id: ThreadId,
    parent_thread_id: ThreadId,
) -> HistoryResult<()> {
    let mut cursor = Some(parent_thread_id);
    let mut visited = HashSet::new();
    while let Some(thread_id) = cursor {
        if thread_id == child_thread_id || !visited.insert(thread_id) {
            return Err(HistoryError::ThreadBaseCycle {
                child_thread_id,
                ancestor_thread_id: thread_id,
            });
        }
        cursor = sqlite_load_thread_base(connection, thread_id)?.map(|base| base.parent_thread_id);
    }
    Ok(())
}

fn encode_thread_fork_reason(reason: &ThreadForkReason) -> HistoryResult<String> {
    let value = serde_json::to_value(reason).map_err(codec_error)?;
    value.as_str().map(str::to_string).ok_or_else(|| {
        HistoryError::Codec("thread fork reason did not encode as string".to_string())
    })
}

fn decode_thread_fork_reason(value: &str) -> HistoryResult<ThreadForkReason> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(codec_error)
}

#[cfg(test)]
mod tests;
