//! Engine-owner crate for every first-party cooldis SQLite store (ADR 0005).
//!
//! This crate is the only place in the workspace that depends on a SQLite
//! engine. Store crates depend on `cooldis-sqlite` and receive configured
//! connections; they never name an engine directly. The crate is named for
//! the file format, not the engine — the current engine is Turso
//! (tursodatabase/turso, pinned exactly), and backing it out is an internal
//! change here, not a store migration.
//!
//! Rules carried by this crate (see ADR 0005):
//! - WAL is the default journal mode; `JournalMode::Mvcc` requires a
//!   per-store justification because it makes the file a Turso extension
//!   until checkpointed back.
//! - One engine per database file: never open a file owned by a running
//!   cooldis process with stock SQLite tooling (`sqlite3` CLI, rusqlite
//!   builds). Post-migration debugging goes through `tursodb` or daemon RPC.
//! - The DST seam is [`Db::open_with_io`]: scenario harnesses drive the
//!   engine through simulated, fault-injectable, deterministic IO.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub use turso::{Connection, Row, Rows, Statement, Value};

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
    /// Enforced per connection with `PRAGMA query_only`; replaces
    /// rusqlite's `SQLITE_OPEN_READ_ONLY` open flag.
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
        let _ = (path.as_ref(), &config);
        todo!("EMO-411 wave 1: Builder::new_local + journal/foreign_keys setup")
    }

    /// Open with a caller-supplied IO implementation. This is the DST seam:
    /// scenario harnesses pass simulated IO here so fault plans can act on
    /// the engine's reads, writes, and syncs — below the store traits that
    /// `FaultingRuntimeStore` wraps.
    pub async fn open_with_io(
        path: impl AsRef<Path>,
        config: DbConfig,
        io: Arc<dyn turso_core::IO>,
    ) -> SqliteResult<Self> {
        let _ = (path.as_ref(), &config, &io);
        todo!("EMO-411 wave 1: Builder::new_local + with_io_impl")
    }

    /// In-memory database for tests; same pragma treatment as [`Db::open`].
    pub async fn in_memory(config: DbConfig) -> SqliteResult<Self> {
        let _ = &config;
        todo!("EMO-411 wave 1: Builder::new_local(\":memory:\")")
    }

    /// A new connection with this database's pragmas applied
    /// (`busy_timeout`, `foreign_keys`, `query_only` when read-only).
    pub async fn connect(&self) -> SqliteResult<Connection> {
        todo!("EMO-411 wave 1: connect + per-connection pragmas")
    }

    /// The configuration this handle was opened with.
    pub fn config(&self) -> &DbConfig {
        &self.config
    }
}

/// Column names of `table` via `PRAGMA table_info`, in declaration order.
/// Empty when the table does not exist. Replaces the hand-rolled
/// `table_info` probes in the current stores' migration paths.
pub async fn table_columns(conn: &Connection, table: &str) -> SqliteResult<Vec<String>> {
    let _ = (conn, table);
    todo!("EMO-411 wave 1: PRAGMA table_info projection")
}

/// Add `column` (full `ALTER TABLE ... ADD COLUMN` tail in `ddl`) when it
/// is absent — the additive-migration idiom every store repeats today.
pub async fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> SqliteResult<()> {
    let _ = (conn, table, column, ddl);
    todo!("EMO-411 wave 1: table_columns + conditional ALTER TABLE")
}
