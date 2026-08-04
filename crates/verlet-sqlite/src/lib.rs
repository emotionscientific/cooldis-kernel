//! Engine-owner crate for every first-party verlet SQLite store (ADR 0005).
//!
//! This crate is the only place in the workspace that depends on a SQLite
//! engine. Store crates depend on `verlet-sqlite` and receive configured
//! connections; they never name an engine directly. The crate is named for
//! the file format, not the engine — the current engine is Turso
//! (tursodatabase/turso, pinned exactly), and backing it out is an internal
//! change here, not a store migration.
//!
//! Rules carried by this crate (ADR 0005 plus the EMO-427 transaction policy):
//! - WAL is the default journal mode; `JournalMode::Mvcc` requires a
//!   per-store justification because it makes the file a Turso extension
//!   until checkpointed back.
//! - One engine per database file: never open a file owned by a running
//!   verlet process with stock SQLite tooling (`sqlite3` CLI, rusqlite
//!   builds). Post-migration debugging goes through `tursodb` or daemon RPC.
//! - Production write transactions use [`TransactionBehavior::Immediate`],
//!   allowing competing writers to serialize through the configured busy
//!   timeout. A deferred transaction that reads before writing can instead
//!   receive `BusySnapshot` when a peer commits after its snapshot read; such
//!   callers must roll back the whole transaction and retry with a bound.
//! - The DST seam is [`Db::open_with_io`]: scenario harnesses drive the
//!   engine through simulated, fault-injectable, deterministic IO.

use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

pub use turso::{
    params, transaction::TransactionBehavior, Connection, IntoParams, Row, Rows, Statement, Value,
};

/// Turso IO contracts re-exported by the engine owner for deterministic test
/// harnesses. Callers of [`Db::open_with_io`] should not need a second direct
/// engine dependency merely to implement the seam it exposes.
pub mod io {
    pub use turso_core::io::{FileId, FileSyncType};
    pub use turso_core::{
        Buffer, Clock, Completion, CompletionError, File, LimboError, MemoryIO, MonotonicInstant,
        OpenFlags, WallClockInstant, IO,
    };
}

/// Errors surfaced by the engine-owner layer.
///
/// Store crates map this into their own error vocabulary at the trait
/// boundary (e.g. `HistoryError::Storage`), the same way they map
/// `rusqlite::Error` today.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    #[error("sqlite engine error: {0}")]
    Engine(#[from] turso::Error),
    #[error("sqlite io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type SqliteResult<T> = Result<T, SqliteError>;

/// Drive a future to completion with a minimal thread park/unpark poll loop.
///
/// This exists for synchronous store surfaces layered over the async SQLite
/// engine. It is reentrant-safe where `futures_executor::block_on` panics when
/// nested because it has no executor-specific nesting guard. Do not call this
/// from an async context: pending work parks the current thread and can block
/// the executor running it.
pub fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker {
        thread: std::thread::Thread,
        notified: AtomicBool,
    }

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.notified.store(true, Ordering::Release);
            self.thread.unpark();
        }
    }

    let mut future = std::pin::pin!(future);
    let thread_waker = Arc::new(ThreadWaker {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&thread_waker));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                while !thread_waker.notified.swap(false, Ordering::AcqRel) {
                    std::thread::park();
                }
            }
        }
    }
}

/// Journal mode policy for a database file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalMode {
    /// The workspace default; format-compatible with stock SQLite in both
    /// directions, which is what keeps rollback a dependency flip.
    #[default]
    Wal,
    /// Turso's MVCC mode (`BEGIN CONCURRENT`, concurrent writers). Opt-in
    /// per store with a recorded justification (ADR 0005 §3): a file in
    /// this mode is not freely openable by stock SQLite until checkpointed
    /// back.
    Mvcc,
}

/// Open-time configuration applied to every connection handed out by [`Db`].
///
/// Defaults reproduce what every current store sets by hand: WAL,
/// `foreign_keys = ON`, 5s busy timeout, read-write.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub journal_mode: JournalMode,
    pub foreign_keys: bool,
    pub busy_timeout: Duration,
    /// Applied per connection with `PRAGMA query_only`. ADVISORY, not
    /// enforced: the pinned Turso 0.7 pre-releases have no local read-only
    /// open flag (checked on pre.18; pin now pre.19), and a
    /// caller can flip the pragma back on a connection it holds. This catches
    /// accidental writes (they error), which is the contract our read-only
    /// paths need today.
    pub read_only: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            journal_mode: JournalMode::Wal,
            foreign_keys: true,
            busy_timeout: Duration::from_secs(5),
            read_only: false,
        }
    }
}

/// A configured database handle. Cheap to clone; a store holds one and
/// acquires connections per operation (Turso connections are inexpensive,
/// and a fresh connection is also what picks up externally applied
/// changes — a held connection reads a stable snapshot).
#[derive(Clone)]
pub struct Db {
    inner: turso::Database,
    config: DbConfig,
}

impl Db {
    /// Open the database at `path`, creating parent directories, and apply
    /// `config`. The file is created if missing (unless `read_only`).
    pub async fn open(path: impl AsRef<Path>, config: DbConfig) -> SqliteResult<Self> {
        let path = path.as_ref();
        let path_str = sqlite_path(path)?;
        prepare_path(path, config.read_only)?;
        let inner = turso::Builder::new_local(path_str).build().await?;
        Self::from_database(inner, config).await
    }

    /// Open with a caller-supplied IO implementation. This is the DST seam:
    /// scenario harnesses pass simulated IO here so fault plans can act on
    /// the engine's reads, writes, and syncs — below the store traits that
    /// `FaultingRuntimeStore` wraps. Path creation and existence checks are
    /// delegated to that IO implementation rather than the host filesystem.
    pub async fn open_with_io(
        path: impl AsRef<Path>,
        config: DbConfig,
        io: Arc<dyn io::IO>,
    ) -> SqliteResult<Self> {
        let path = path.as_ref();
        let path_str = sqlite_path(path)?;
        let inner = turso::Builder::new_local(path_str)
            .with_io_impl(io)
            .build()
            .await?;
        Self::from_database(inner, config).await
    }

    /// In-memory database for tests; same pragma treatment as [`Db::open`].
    pub async fn in_memory(config: DbConfig) -> SqliteResult<Self> {
        let inner = turso::Builder::new_local(":memory:").build().await?;
        Self::from_database(inner, config).await
    }

    /// A new connection with this database's pragmas applied
    /// (`busy_timeout`, `foreign_keys`, `query_only` when read-only).
    pub async fn connect(&self) -> SqliteResult<Connection> {
        let conn = self.inner.connect()?;
        apply_connection_config(&conn, &self.config).await?;
        Ok(conn)
    }

    /// The configuration this handle was opened with.
    pub fn config(&self) -> &DbConfig {
        &self.config
    }

    /// Construct a handle and apply file-level journal policy once before
    /// handing out independently configured connections.
    async fn from_database(inner: turso::Database, config: DbConfig) -> SqliteResult<Self> {
        let db = Self { inner, config };
        let conn = db.connect().await?;
        if !db.config.read_only {
            let journal_mode = match db.config.journal_mode {
                JournalMode::Wal => "'wal'",
                JournalMode::Mvcc => "'mvcc'",
            };
            conn.pragma_update("journal_mode", journal_mode).await?;
        }
        Ok(db)
    }
}

fn sqlite_path(path: &Path) -> SqliteResult<&str> {
    path.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "sqlite database path is not valid UTF-8",
        )
        .into()
    })
}

/// Create a writable database's parent directory, or reject a missing
/// read-only database before Turso can create it as a side effect of open.
fn prepare_path(path: &Path, read_only: bool) -> SqliteResult<()> {
    if read_only {
        if !path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "read-only sqlite database does not exist: {}",
                    path.display()
                ),
            )
            .into());
        }
    } else if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Apply connection-local policy to a newly acquired Turso connection.
async fn apply_connection_config(conn: &Connection, config: &DbConfig) -> SqliteResult<()> {
    conn.busy_timeout(config.busy_timeout)?;
    conn.pragma_update(
        "foreign_keys",
        if config.foreign_keys { "ON" } else { "OFF" },
    )
    .await?;
    if config.read_only {
        conn.pragma_update("query_only", 1).await?;
    }
    Ok(())
}

/// Quote a SQLite identifier by doubling embedded quote characters.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Column names of `table` via `PRAGMA table_info`, in declaration order.
/// Empty when the table does not exist. Replaces the hand-rolled
/// `table_info` probes in the current stores' migration paths.
pub async fn table_columns(conn: &Connection, table: &str) -> SqliteResult<Vec<String>> {
    let mut rows = conn
        .query(
            format!("PRAGMA table_info({})", quote_identifier(table)),
            (),
        )
        .await?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().await? {
        columns.push(row.get(1)?);
    }
    Ok(columns)
}

/// Add `column` (full `ALTER TABLE ... ADD COLUMN` tail in `ddl`) when it
/// is absent — the additive-migration idiom every store repeats today.
pub async fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> SqliteResult<()> {
    let result = conn
        .execute(
            format!("ALTER TABLE {} ADD COLUMN {ddl}", quote_identifier(table)),
            (),
        )
        .await;
    if let Err(error) = result {
        if table_columns(conn, table)
            .await?
            .iter()
            .any(|existing| existing == column)
        {
            return Ok(());
        }
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn nested_block_on_preserves_the_outer_wake() {
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let outer_polls = Arc::new(AtomicUsize::new(0));
            let result = block_on(std::future::poll_fn({
                let outer_polls = Arc::clone(&outer_polls);
                move |outer_context| {
                    if outer_polls.fetch_add(1, Ordering::SeqCst) > 0 {
                        return Poll::Ready("completed");
                    }

                    let inner_polls = Arc::new(AtomicUsize::new(0));
                    let inner_waker = Arc::new(std::sync::Mutex::new(None::<Waker>));
                    let delayed_wake = {
                        let inner_polls = Arc::clone(&inner_polls);
                        let inner_waker = Arc::clone(&inner_waker);
                        std::thread::spawn(move || {
                            std::thread::sleep(Duration::from_millis(50));
                            if inner_polls.load(Ordering::SeqCst) == 1 {
                                if let Some(waker) = inner_waker.lock().unwrap().take() {
                                    waker.wake();
                                }
                            }
                        })
                    };
                    block_on(std::future::poll_fn({
                        let inner_polls = Arc::clone(&inner_polls);
                        let inner_waker = Arc::clone(&inner_waker);
                        let outer_waker = outer_context.waker().clone();
                        move |inner_context| match inner_polls.fetch_add(1, Ordering::SeqCst) {
                            0 => {
                                *inner_waker.lock().unwrap() = Some(inner_context.waker().clone());
                                outer_waker.wake_by_ref();
                                Poll::Pending
                            }
                            1 => {
                                inner_context.waker().wake_by_ref();
                                Poll::Pending
                            }
                            _ => Poll::Ready(()),
                        }
                    }));
                    delayed_wake.join().unwrap();
                    Poll::Pending
                }
            }));
            completed_tx.send(result).unwrap();
        });

        assert_eq!(
            completed_rx.recv_timeout(Duration::from_secs(30)),
            Ok("completed"),
            "nested block_on consumed the outer future's wake"
        );
    }

    async fn pragma_value(conn: &Connection, name: &str) -> Value {
        let mut value = None;
        conn.pragma_query(name, |row| {
            value = Some(row.get_value(0).expect("pragma value"));
            Ok(())
        })
        .await
        .expect("pragma query");
        value.expect("pragma returned one row")
    }

    #[tokio::test]
    async fn tempfile_open_creates_parents_and_applies_connection_pragmas() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("nested/state/metadata.sqlite3");
        let config = DbConfig {
            busy_timeout: Duration::from_millis(1_234),
            ..DbConfig::default()
        };

        let db = Db::open(&path, config).await.unwrap();
        assert!(path.exists());
        let conn = db.connect().await.unwrap();

        assert_eq!(
            pragma_value(&conn, "journal_mode").await,
            Value::Text("wal".into())
        );
        assert_eq!(pragma_value(&conn, "foreign_keys").await, Value::Integer(1));
        assert_eq!(
            pragma_value(&conn, "busy_timeout").await,
            Value::Integer(1_234)
        );
        assert_eq!(pragma_value(&conn, "query_only").await, Value::Integer(0));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_utf8_path_is_rejected_without_creating_parents() {
        use std::os::unix::ffi::OsStringExt;

        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("must-not-exist");
        let path = parent.join(std::ffi::OsString::from_vec(vec![0xff]));

        assert!(Db::open(path, DbConfig::default()).await.is_err());
        assert!(!parent.exists());
    }

    #[tokio::test]
    async fn custom_io_open_does_not_touch_the_host_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("owned-by-custom-io");
        let path = parent.join("store.sqlite3");
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());

        let db = Db::open_with_io(&path, DbConfig::default(), io)
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE records (value TEXT)", ())
            .await
            .unwrap();

        assert!(!parent.exists());
    }

    #[tokio::test]
    async fn in_memory_applies_pragmas_and_migrates_columns_idempotently() {
        let db = Db::in_memory(DbConfig::default()).await.unwrap();
        let conn = db.connect().await.unwrap();
        assert_eq!(pragma_value(&conn, "foreign_keys").await, Value::Integer(1));
        assert_eq!(
            pragma_value(&conn, "busy_timeout").await,
            Value::Integer(5_000)
        );

        conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();
        assert_eq!(table_columns(&conn, "widgets").await.unwrap(), vec!["id"]);
        assert!(table_columns(&conn, "missing_table")
            .await
            .unwrap()
            .is_empty());

        ensure_column(&conn, "widgets", "label", "label TEXT NOT NULL DEFAULT ''")
            .await
            .unwrap();
        ensure_column(&conn, "widgets", "label", "label TEXT NOT NULL DEFAULT ''")
            .await
            .unwrap();
        assert_eq!(
            table_columns(&conn, "widgets").await.unwrap(),
            vec!["id", "label"]
        );
    }

    #[tokio::test]
    async fn tempfile_table_columns_and_ensure_column_survive_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let db = Db::open(&path, DbConfig::default()).await.unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute("CREATE TABLE records (key TEXT PRIMARY KEY)", ())
            .await
            .unwrap();
        ensure_column(&conn, "records", "payload", "payload BLOB")
            .await
            .unwrap();
        drop(conn);
        drop(db);

        let reopened = Db::open(&path, DbConfig::default()).await.unwrap();
        let conn = reopened.connect().await.unwrap();
        assert_eq!(
            table_columns(&conn, "records").await.unwrap(),
            vec!["key", "payload"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_ensure_column_calls_are_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let db = Db::open(&path, DbConfig::default()).await.unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE records (key TEXT PRIMARY KEY)", ())
            .await
            .unwrap();

        let workers = 8;
        let barrier = Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for _ in 0..workers {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let conn = db.connect().await.unwrap();
                barrier.wait().await;
                ensure_column(&conn, "records", "payload", "payload BLOB").await
            }));
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert_eq!(
            table_columns(&db.connect().await.unwrap(), "records")
                .await
                .unwrap(),
            vec!["key", "payload"]
        );
    }

    #[tokio::test]
    async fn read_only_connections_can_read_and_reject_writes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let writable = Db::open(&path, DbConfig::default()).await.unwrap();
        let conn = writable.connect().await.unwrap();
        conn.execute_batch(
            "CREATE TABLE records (value TEXT); INSERT INTO records VALUES ('seed');",
        )
        .await
        .unwrap();
        drop(conn);
        drop(writable);

        let read_only = Db::open(
            &path,
            DbConfig {
                read_only: true,
                ..DbConfig::default()
            },
        )
        .await
        .unwrap();
        let conn = read_only.connect().await.unwrap();
        assert_eq!(pragma_value(&conn, "query_only").await, Value::Integer(1));
        let mut rows = conn.query("SELECT value FROM records", ()).await.unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "seed"
        );
        assert!(conn
            .execute("INSERT INTO records VALUES ('denied')", ())
            .await
            .is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_transaction_commit_regression() {
        // Turso 0.6.1 returned `cannot commit - no transaction is active` from
        // Transaction::commit under concurrent read-then-upsert traffic; fixed
        // in the 0.7 line (verified on 0.7.0-pre.18, EMO-412). Turso 0.7 can
        // legitimately reject a deferred write upgrade with BusySnapshot, so
        // this guard rolls back and retries only that typed error. Every other
        // error, including the 0.6.1 commit failure, remains fatal.
        const MAX_ATTEMPTS_PER_WORKER: usize = 4;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("transaction-repro.sqlite3");
        let db = Db::open(&path, DbConfig::default()).await.unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute(
            "CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                committed_workers INTEGER NOT NULL
            )",
            (),
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO records VALUES (1, 0)", ())
            .await
            .unwrap();
        drop(conn);

        let workers = 2;
        let start_barrier = Arc::new(tokio::sync::Barrier::new(workers));
        let first_snapshot_barrier = Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for worker in 0..workers {
            let db = db.clone();
            let start_barrier = start_barrier.clone();
            let first_snapshot_barrier = first_snapshot_barrier.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = db.connect().await.unwrap();
                start_barrier.wait().await;
                let mut busy_snapshot_retries = 0;

                for attempt in 1..=MAX_ATTEMPTS_PER_WORKER {
                    let tx = conn
                        .transaction_with_behavior(TransactionBehavior::Deferred)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} begin failed: {error:?}")
                        });
                    let mut rows = tx
                        .query("SELECT committed_workers FROM records WHERE id = 1", ())
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} read failed: {error:?}")
                        });
                    let committed_workers = rows
                        .next()
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} row read failed: {error:?}")
                        })
                        .expect("records row must exist")
                        .get::<i64>(0)
                        .unwrap();
                    drop(rows);

                    if attempt == 1 {
                        // Both workers now own the same deferred snapshot. One
                        // write upgrade must lose the race and retry.
                        first_snapshot_barrier.wait().await;
                    }

                    let worker_bit = 1_i64 << worker;
                    match tx
                        .execute(
                            "INSERT INTO records VALUES (1, ?1)
                             ON CONFLICT(id) DO UPDATE
                             SET committed_workers = excluded.committed_workers",
                            [committed_workers | worker_bit],
                        )
                        .await
                    {
                        Ok(1) => {}
                        Ok(changed) => panic!(
                            "worker {worker} attempt {attempt} updated {changed} rows, expected one"
                        ),
                        Err(turso::Error::BusySnapshot(_)) => {
                            busy_snapshot_retries += 1;
                            tx.rollback().await.unwrap_or_else(|error| {
                                panic!(
                                    "worker {worker} attempt {attempt} BusySnapshot rollback failed: {error:?}"
                                )
                            });
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Err(error) => {
                            panic!("worker {worker} attempt {attempt} write failed: {error:?}")
                        }
                    }

                    // In pinned Turso, BusySnapshot is produced only while a
                    // statement upgrades a stale read snapshot to a writer.
                    // A successful write already owns the WAL write lock, so
                    // COMMIT has no BusySnapshot path to retry here.
                    tx.commit().await.unwrap_or_else(|error| {
                        panic!("worker {worker} attempt {attempt} commit failed: {error:?}")
                    });
                    return (attempt, busy_snapshot_retries);
                }

                panic!("worker {worker} exhausted {MAX_ATTEMPTS_PER_WORKER} attempts")
            }));
        }

        let mut total_attempts = 0;
        let mut total_busy_snapshot_retries = 0;
        for handle in handles {
            let (attempts, busy_snapshot_retries) = handle.await.unwrap();
            total_attempts += attempts;
            total_busy_snapshot_retries += busy_snapshot_retries;
        }
        assert_eq!(
            total_busy_snapshot_retries, 1,
            "forced same-snapshot race must produce exactly one BusySnapshot"
        );
        assert_eq!(
            total_attempts,
            workers + total_busy_snapshot_retries,
            "each BusySnapshot must account for exactly one whole-transaction retry"
        );
        assert!(
            total_attempts <= workers * MAX_ATTEMPTS_PER_WORKER,
            "deferred retry bound exceeded: {total_attempts} total attempts"
        );

        let conn = db.connect().await.unwrap();
        let mut rows = conn
            .query("SELECT committed_workers FROM records WHERE id = 1", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0b11,
            "both workers' effects must survive their successful commits"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn immediate_transactions_serialize_without_busy_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("immediate-serialization.sqlite3");
        let db = Db::open(&path, DbConfig::default()).await.unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE counter (value INTEGER NOT NULL)", ())
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("INSERT INTO counter VALUES (0)", ())
            .await
            .unwrap();

        let workers = 2;
        let begin_barrier = Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for worker in 0..workers {
            let db = db.clone();
            let begin_barrier = begin_barrier.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = db.connect().await.unwrap();
                // Force both workers to contend at BEGIN IMMEDIATE. The loser
                // must busy-wait under DbConfig::default(), not take a stale
                // deferred snapshot.
                begin_barrier.wait().await;
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("worker {worker} BEGIN IMMEDIATE failed: {error:?}")
                    });
                let mut rows = tx.query("SELECT value FROM counter", ()).await.unwrap();
                let value = rows
                    .next()
                    .await
                    .unwrap()
                    .expect("counter row must exist")
                    .get::<i64>(0)
                    .unwrap();
                drop(rows);
                tx.execute("UPDATE counter SET value = ?1", [value + 1])
                    .await
                    .unwrap_or_else(|error| {
                        panic!("worker {worker} Immediate write failed: {error:?}")
                    });
                tx.commit().await.unwrap_or_else(|error| {
                    panic!("worker {worker} Immediate commit failed: {error:?}")
                });
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let conn = db.connect().await.unwrap();
        let mut rows = conn.query("SELECT value FROM counter", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            2,
            "Immediate transactions must serialize both increments"
        );
    }
}
