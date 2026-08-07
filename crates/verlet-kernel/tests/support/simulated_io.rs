#![allow(dead_code)]

//! Deterministic, fault-plan-driven Turso IO for seeded storage scenarios.
//!
//! The rule shape intentionally matches `FaultScript`: a named operation, a
//! one-based occurrence, before/after timing, and an action. Engine-native
//! actions extend the action slot only where the IO surface exposes a real
//! behavior: short completion, successful sync without persistence, and a
//! crash that optionally persists a torn prefix of the triggering write.
//!
//! Writes and truncates update a volatile file image. A truthful `sync`
//! atomically copies that image to the durable image. A crash drops every
//! unsynced change; `TornWrite` first copies a seed-selected prefix of the
//! triggering write into the durable image. Recovery clears in-process locks
//! and creates a fresh IO generation over those durable images.

pub const IO_READ: &str = "read";
pub const IO_WRITE: &str = "write";
pub const IO_SYNC: &str = "sync";
pub const IO_TRUNCATE: &str = "truncate";
pub const IO_OPEN: &str = "open";
pub const IO_REMOVE: &str = "remove";
pub const IO_LOCK: &str = "lock";
pub const IO_UNLOCK: &str = "unlock";

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum CrashSurvival {
    /// Only writes covered by the last truthful sync survive.
    DiscardUnsynced,
    /// Persist a deterministic non-empty prefix of the triggering write,
    /// bounded by `max_bytes`, before discarding every other unsynced byte.
    TornWrite { max_bytes: usize },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum IoFaultAction {
    Fail,
    /// Present only to fail plan validation explicitly. `IO` methods are
    /// synchronous, so the async FaultScript delay vocabulary cannot be
    /// projected without blocking or wall-clock dependence.
    Delay(std::time::Duration),
    Short {
        max_bytes: usize,
    },
    SyncLie,
    Crash(CrashSurvival),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct IoFaultRule {
    pub operation: &'static str,
    pub nth: usize,
    pub timing: crate::support::fault_plan::FaultTiming,
    pub action: IoFaultAction,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct IoFaultPlan {
    pub seed: u64,
    pub rules: Vec<IoFaultRule>,
}

impl IoFaultPlan {
    pub fn new(seed: u64, rules: Vec<IoFaultRule>) -> Result<Self, String> {
        let plan = Self { seed, rules };
        plan.validate()?;
        Ok(plan)
    }

    pub fn crash_after_write(seed: u64, nth: usize, survival: CrashSurvival) -> Self {
        Self::new(
            seed,
            vec![IoFaultRule {
                operation: IO_WRITE,
                nth,
                timing: crate::support::fault_plan::FaultTiming::After,
                action: IoFaultAction::Crash(survival),
            }],
        )
        .expect("the crash-after-write constructor is valid")
    }

    fn validate(&self) -> Result<(), String> {
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.nth == 0 {
                return Err(format!("IO fault rule {index} has zero occurrence"));
            }
            if ![
                IO_READ,
                IO_WRITE,
                IO_SYNC,
                IO_TRUNCATE,
                IO_OPEN,
                IO_REMOVE,
                IO_LOCK,
                IO_UNLOCK,
            ]
            .contains(&rule.operation)
            {
                return Err(format!(
                    "IO fault rule {index} names unknown operation {:?}",
                    rule.operation
                ));
            }
            if matches!(rule.action, IoFaultAction::Delay(_)) {
                return Err(format!(
                    "IO fault rule {index} requests delay, but turso_core::IO operations are synchronous"
                ));
            }
            match &rule.action {
                IoFaultAction::Short { max_bytes } => {
                    if !matches!(rule.operation, IO_READ | IO_WRITE)
                        || rule.timing != crate::support::fault_plan::FaultTiming::After
                        || *max_bytes == 0
                    {
                        return Err(format!(
                            "IO fault rule {index} short completion requires read/write, after timing, and max_bytes > 0"
                        ));
                    }
                }
                IoFaultAction::SyncLie
                    if rule.operation != IO_SYNC
                        || rule.timing != crate::support::fault_plan::FaultTiming::After =>
                {
                    return Err(format!(
                        "IO fault rule {index} sync lie must target sync after the call"
                    ));
                }
                IoFaultAction::Crash(CrashSurvival::TornWrite { max_bytes }) => {
                    if rule.operation != IO_WRITE
                        || rule.timing != crate::support::fault_plan::FaultTiming::After
                        || *max_bytes == 0
                    {
                        return Err(format!(
                            "IO fault rule {index} torn crash requires write/after and max_bytes > 0"
                        ));
                    }
                }
                IoFaultAction::Crash(CrashSurvival::DiscardUnsynced)
                    if rule.operation != IO_WRITE
                        || rule.timing != crate::support::fault_plan::FaultTiming::After =>
                {
                    return Err(format!(
                        "IO fault rule {index} crash must fire after a write"
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct IoTranscriptEntry {
    pub ordinal: usize,
    pub generation: usize,
    pub operation: &'static str,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<usize>,
    pub outcome: String,
}

#[derive(Default)]
struct FileState {
    durable: Vec<u8>,
    volatile: Vec<u8>,
    lock_owner: Option<u64>,
}

struct SimulatedState {
    files: std::collections::BTreeMap<String, FileState>,
    calls: std::collections::BTreeMap<&'static str, usize>,
    rules: Vec<IoFaultRule>,
    transcript: Vec<IoTranscriptEntry>,
    ordinal: usize,
    generation: usize,
    next_handle: u64,
    crashed: bool,
    rng: crate::support::fault_plan::SplitMix64,
    clock_tick: u128,
}

impl SimulatedState {
    fn record(
        &mut self,
        operation: &'static str,
        path: &str,
        offset: Option<u64>,
        len: Option<usize>,
        outcome: impl Into<String>,
    ) {
        self.ordinal += 1;
        self.transcript.push(IoTranscriptEntry {
            ordinal: self.ordinal,
            generation: self.generation,
            operation,
            path: path.to_string(),
            offset,
            len,
            outcome: outcome.into(),
        });
    }

    fn take_rule(&mut self, operation: &'static str) -> Option<IoFaultRule> {
        let call = self.calls.entry(operation).or_default();
        *call += 1;
        self.rules
            .iter()
            .position(|rule| rule.operation == operation && rule.nth == *call)
            .map(|index| self.rules.remove(index))
    }

    fn ensure_live(
        &mut self,
        generation: usize,
        operation: &'static str,
        path: &str,
    ) -> Result<(), verlet_sqlite::io::CompletionError> {
        if generation != self.generation {
            self.record(operation, path, None, None, "rejected-stale-generation");
            return Err(completion_failure(
                operation,
                std::io::ErrorKind::ConnectionReset,
            ));
        }
        if self.crashed {
            self.record(operation, path, None, None, "rejected-after-crash");
            return Err(completion_failure(
                operation,
                std::io::ErrorKind::ConnectionReset,
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SimulatedIo {
    seed: u64,
    generation: usize,
    state: std::sync::Arc<std::sync::Mutex<SimulatedState>>,
}

impl SimulatedIo {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            generation: 0,
            state: std::sync::Arc::new(std::sync::Mutex::new(SimulatedState {
                files: std::collections::BTreeMap::new(),
                calls: std::collections::BTreeMap::new(),
                rules: Vec::new(),
                transcript: Vec::new(),
                ordinal: 0,
                generation: 0,
                next_handle: 1,
                crashed: false,
                rng: crate::support::fault_plan::SplitMix64::new(seed),
                clock_tick: u128::from(seed),
            })),
        }
    }

    /// Arm a new plan relative to subsequent IO occurrences. This lets a
    /// scenario finish schema setup before placing the cut inside its append.
    pub fn arm(&self, plan: IoFaultPlan) -> Result<(), String> {
        plan.validate()?;
        if plan.seed != self.seed {
            return Err(format!(
                "IO plan seed {} does not match simulated disk seed {}",
                plan.seed, self.seed
            ));
        }
        let mut state = self.state.lock().unwrap();
        if self.generation != state.generation {
            return Err("cannot arm a stale simulated IO generation".to_string());
        }
        if state.crashed {
            return Err("cannot arm a crashed simulated IO generation".to_string());
        }
        state.calls.clear();
        state.rules = plan.rules;
        Ok(())
    }

    pub fn transcript(&self) -> Vec<IoTranscriptEntry> {
        self.state.lock().unwrap().transcript.clone()
    }

    pub fn crashed(&self) -> bool {
        let state = self.state.lock().unwrap();
        self.generation == state.generation && state.crashed
    }

    /// Start a fresh IO generation over only the bytes that survived the
    /// latched crash. No old connection or file handle is reused.
    pub fn recover(&self) -> Result<Self, String> {
        let mut state = self.state.lock().unwrap();
        if self.generation != state.generation {
            return Err("simulated IO recovery requires the current generation".to_string());
        }
        if !state.crashed {
            return Err("simulated IO recovery requires a latched crash".to_string());
        }
        for file in state.files.values_mut() {
            file.volatile = file.durable.clone();
            file.lock_owner = None;
        }
        state.calls.clear();
        state.rules.clear();
        state.crashed = false;
        state.generation += 1;
        state.record("recover", "*", None, None, "durable-images-restored");
        let generation = state.generation;
        drop(state);
        Ok(Self {
            seed: self.seed,
            generation,
            state: std::sync::Arc::clone(&self.state),
        })
    }
}

impl verlet_sqlite::io::Clock for SimulatedIo {
    fn current_time_monotonic(&self) -> verlet_sqlite::io::MonotonicInstant {
        let mut state = self.state.lock().unwrap();
        state.clock_tick = state.clock_tick.wrapping_add(1_000_000);
        verlet_sqlite::io::MonotonicInstant::from_nanos(state.clock_tick)
    }

    fn current_time_wall_clock(&self) -> verlet_sqlite::io::WallClockInstant {
        verlet_sqlite::io::WallClockInstant {
            secs: (self.seed & i64::MAX as u64) as i64,
            micros: (self.seed % 1_000_000) as u32,
        }
    }
}

impl verlet_sqlite::io::IO for SimulatedIo {
    fn open_file(
        &self,
        path: &str,
        flags: verlet_sqlite::io::OpenFlags,
        _direct: bool,
    ) -> Result<std::sync::Arc<dyn verlet_sqlite::io::File>, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_live(self.generation, IO_OPEN, path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let rule = state.take_rule(IO_OPEN);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_OPEN, path, None, None, "fail-before");
            return Err(io_failure(IO_OPEN, std::io::ErrorKind::Other));
        }
        if !state.files.contains_key(path) && !flags.contains(verlet_sqlite::io::OpenFlags::Create)
        {
            state.record(IO_OPEN, path, None, None, "not-found");
            return Err(io_failure(IO_OPEN, std::io::ErrorKind::NotFound));
        }
        state.files.entry(path.to_string()).or_default();
        let owner = state.next_handle;
        state.next_handle += 1;
        if !flags.intersects(
            verlet_sqlite::io::OpenFlags::ReadOnly | verlet_sqlite::io::OpenFlags::NoLock,
        ) {
            let file = state.files.get_mut(path).unwrap();
            if file.lock_owner.is_some() {
                state.record(IO_OPEN, path, None, None, "lock-busy");
                return Err(verlet_sqlite::io::LimboError::LockingError(format!(
                    "simulated file {path:?} is already exclusively locked"
                )));
            }
            file.lock_owner = Some(owner);
        }
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            if let Some(file) = state.files.get_mut(path)
                && file.lock_owner == Some(owner)
            {
                file.lock_owner = None;
            }
            state.record(IO_OPEN, path, None, None, "fail-after");
            return Err(io_failure(IO_OPEN, std::io::ErrorKind::Other));
        }
        state.record(IO_OPEN, path, None, None, "ok");
        Ok(std::sync::Arc::new(SimulatedFile {
            path: path.to_string(),
            owner,
            generation: self.generation,
            state: std::sync::Arc::clone(&self.state),
        }))
    }

    fn remove_file(&self, path: &str) -> Result<(), verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_live(self.generation, IO_REMOVE, path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let rule = state.take_rule(IO_REMOVE);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_REMOVE, path, None, None, "fail-before");
            return Err(io_failure(IO_REMOVE, std::io::ErrorKind::Other));
        }
        state.files.remove(path);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_REMOVE, path, None, None, "fail-after");
            return Err(io_failure(IO_REMOVE, std::io::ErrorKind::Other));
        }
        state.record(IO_REMOVE, path, None, None, "ok");
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.state.lock().unwrap().rng.next_u64() as i64
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        let mut state = self.state.lock().unwrap();
        for chunk in dest.chunks_mut(8) {
            let bytes = state.rng.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    fn file_id(
        &self,
        path: &str,
    ) -> Result<verlet_sqlite::io::FileId, verlet_sqlite::io::LimboError> {
        self.state
            .lock()
            .unwrap()
            .ensure_live(self.generation, "file-id", path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in path.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Ok(verlet_sqlite::io::FileId {
            dev: self.seed,
            ino: hash,
        })
    }
}

struct SimulatedFile {
    path: String,
    owner: u64,
    generation: usize,
    state: std::sync::Arc<std::sync::Mutex<SimulatedState>>,
}

impl SimulatedFile {
    fn apply_write(
        state: &mut SimulatedState,
        path: &str,
        pos: u64,
        bytes: &[u8],
    ) -> Result<usize, verlet_sqlite::io::CompletionError> {
        let start = usize::try_from(pos)
            .map_err(|_| completion_failure(IO_WRITE, std::io::ErrorKind::InvalidInput))?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| completion_failure(IO_WRITE, std::io::ErrorKind::InvalidInput))?;
        let file = state
            .files
            .get_mut(path)
            .ok_or_else(|| completion_failure(IO_WRITE, std::io::ErrorKind::NotFound))?;
        if file.volatile.len() < end {
            file.volatile.resize(end, 0);
        }
        file.volatile[start..end].copy_from_slice(bytes);
        Ok(bytes.len())
    }

    fn persist_torn_prefix(
        state: &mut SimulatedState,
        path: &str,
        pos: u64,
        bytes: &[u8],
        max_bytes: usize,
    ) -> Result<usize, verlet_sqlite::io::CompletionError> {
        let bound = bytes.len().min(max_bytes);
        if bound == 0 {
            return Ok(0);
        }
        let prefix = 1 + state.rng.next_below(bound as u64) as usize;
        let start = usize::try_from(pos)
            .map_err(|_| completion_failure(IO_WRITE, std::io::ErrorKind::InvalidInput))?;
        let end = start
            .checked_add(prefix)
            .ok_or_else(|| completion_failure(IO_WRITE, std::io::ErrorKind::InvalidInput))?;
        let file = state
            .files
            .get_mut(path)
            .ok_or_else(|| completion_failure(IO_WRITE, std::io::ErrorKind::NotFound))?;
        if file.durable.len() < end {
            file.durable.resize(end, 0);
        }
        file.durable[start..end].copy_from_slice(&bytes[..prefix]);
        Ok(prefix)
    }

    fn write(
        &self,
        pos: u64,
        bytes: &[u8],
        c: verlet_sqlite::io::Completion,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = state.ensure_live(self.generation, IO_WRITE, &self.path) {
            c.error(error);
            return Ok(c);
        }
        let rule = state.take_rule(IO_WRITE);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(
                IO_WRITE,
                &self.path,
                Some(pos),
                Some(bytes.len()),
                "fail-before",
            );
            c.error(completion_failure(IO_WRITE, std::io::ErrorKind::Other));
            return Ok(c);
        }
        let write_len = match rule.as_ref().map(|rule| &rule.action) {
            Some(IoFaultAction::Short { max_bytes }) => bytes.len().min(*max_bytes),
            _ => bytes.len(),
        };
        if let Err(error) = Self::apply_write(&mut state, &self.path, pos, &bytes[..write_len]) {
            state.record(IO_WRITE, &self.path, Some(pos), Some(bytes.len()), "error");
            c.error(error);
            return Ok(c);
        }
        match rule.as_ref().map(|rule| (&rule.timing, &rule.action)) {
            Some((crate::support::fault_plan::FaultTiming::After, IoFaultAction::Fail)) => {
                state.record(
                    IO_WRITE,
                    &self.path,
                    Some(pos),
                    Some(bytes.len()),
                    "fail-after",
                );
                c.error(verlet_sqlite::io::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    IO_WRITE,
                ));
            }
            Some((_, IoFaultAction::Short { .. })) => {
                state.record(
                    IO_WRITE,
                    &self.path,
                    Some(pos),
                    Some(bytes.len()),
                    format!("short:{write_len}"),
                );
                c.complete(write_len as i32);
            }
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Crash(survival),
            )) => {
                let outcome = match survival {
                    CrashSurvival::DiscardUnsynced => "crash:discard-unsynced".to_string(),
                    CrashSurvival::TornWrite { max_bytes } => {
                        let prefix = match Self::persist_torn_prefix(
                            &mut state, &self.path, pos, bytes, *max_bytes,
                        ) {
                            Ok(prefix) => prefix,
                            Err(error) => {
                                state.record(
                                    IO_WRITE,
                                    &self.path,
                                    Some(pos),
                                    Some(bytes.len()),
                                    "error-persisting-torn-prefix",
                                );
                                c.error(error);
                                return Ok(c);
                            }
                        };
                        format!("crash:torn-prefix:{prefix}")
                    }
                };
                state.crashed = true;
                state.record(IO_WRITE, &self.path, Some(pos), Some(bytes.len()), outcome);
                c.error(verlet_sqlite::io::CompletionError::IOError(
                    std::io::ErrorKind::ConnectionReset,
                    IO_WRITE,
                ));
            }
            _ => {
                state.record(IO_WRITE, &self.path, Some(pos), Some(bytes.len()), "ok");
                c.complete(write_len as i32);
            }
        }
        Ok(c)
    }
}

impl verlet_sqlite::io::File for SimulatedFile {
    fn lock_file(&self, _exclusive: bool) -> Result<(), verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_live(self.generation, IO_LOCK, &self.path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let rule = state.take_rule(IO_LOCK);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_LOCK, &self.path, None, None, "fail-before");
            return Err(io_failure(IO_LOCK, std::io::ErrorKind::Other));
        }
        let newly_acquired = {
            let file = state
                .files
                .get_mut(&self.path)
                .ok_or_else(|| io_failure(IO_LOCK, std::io::ErrorKind::NotFound))?;
            match file.lock_owner {
                None => {
                    file.lock_owner = Some(self.owner);
                    Some(true)
                }
                Some(owner) if owner == self.owner => Some(false),
                Some(_) => None,
            }
        };
        let Some(newly_acquired) = newly_acquired else {
            state.record(IO_LOCK, &self.path, None, None, "lock-busy");
            return Err(verlet_sqlite::io::LimboError::LockingError(format!(
                "simulated file {:?} is already locked",
                self.path
            )));
        };
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            if newly_acquired
                && let Some(file) = state.files.get_mut(&self.path)
                && file.lock_owner == Some(self.owner)
            {
                file.lock_owner = None;
            }
            state.record(IO_LOCK, &self.path, None, None, "fail-after");
            return Err(io_failure(IO_LOCK, std::io::ErrorKind::Other));
        }
        state.record(IO_LOCK, &self.path, None, None, "ok");
        Ok(())
    }

    fn unlock_file(&self) -> Result<(), verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_live(self.generation, IO_UNLOCK, &self.path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let rule = state.take_rule(IO_UNLOCK);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_UNLOCK, &self.path, None, None, "fail-before");
            return Err(io_failure(IO_UNLOCK, std::io::ErrorKind::Other));
        }
        if let Some(file) = state.files.get_mut(&self.path)
            && file.lock_owner == Some(self.owner)
        {
            file.lock_owner = None;
        }
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_UNLOCK, &self.path, None, None, "fail-after");
            return Err(io_failure(IO_UNLOCK, std::io::ErrorKind::Other));
        }
        state.record(IO_UNLOCK, &self.path, None, None, "ok");
        Ok(())
    }

    fn pread(
        &self,
        pos: u64,
        c: verlet_sqlite::io::Completion,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = state.ensure_live(self.generation, IO_READ, &self.path) {
            c.error(error);
            return Ok(c);
        }
        let rule = state.take_rule(IO_READ);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(
                IO_READ,
                &self.path,
                Some(pos),
                Some(c.as_read().buf().len()),
                "fail-before",
            );
            c.error(completion_failure(IO_READ, std::io::ErrorKind::Other));
            return Ok(c);
        }
        let buffer = c.as_read().buf();
        let requested = buffer.len();
        let start = match usize::try_from(pos) {
            Ok(start) => start,
            Err(_) => {
                c.error(completion_failure(
                    IO_READ,
                    std::io::ErrorKind::InvalidInput,
                ));
                return Ok(c);
            }
        };
        let Some(file) = state.files.get(&self.path) else {
            c.error(completion_failure(IO_READ, std::io::ErrorKind::NotFound));
            return Ok(c);
        };
        let available = file.volatile.len().saturating_sub(start);
        let mut read_len = requested.min(available);
        if let Some(IoFaultAction::Short { max_bytes }) = rule.as_ref().map(|rule| &rule.action) {
            read_len = read_len.min(*max_bytes);
        }
        if read_len > 0 {
            let file = state.files.get(&self.path).unwrap();
            buffer.as_mut_slice()[..read_len]
                .copy_from_slice(&file.volatile[start..start + read_len]);
        }
        match rule.as_ref().map(|rule| (&rule.timing, &rule.action)) {
            Some((crate::support::fault_plan::FaultTiming::After, IoFaultAction::Fail)) => {
                state.record(
                    IO_READ,
                    &self.path,
                    Some(pos),
                    Some(requested),
                    "fail-after",
                );
                c.error(verlet_sqlite::io::CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    IO_READ,
                ));
            }
            Some((_, IoFaultAction::Short { .. })) => {
                state.record(
                    IO_READ,
                    &self.path,
                    Some(pos),
                    Some(requested),
                    format!("short:{read_len}"),
                );
                c.complete(read_len as i32);
            }
            _ => {
                state.record(
                    IO_READ,
                    &self.path,
                    Some(pos),
                    Some(requested),
                    format!("ok:{read_len}"),
                );
                c.complete(read_len as i32);
            }
        }
        Ok(c)
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: std::sync::Arc<verlet_sqlite::io::Buffer>,
        c: verlet_sqlite::io::Completion,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        self.write(pos, buffer.as_slice(), c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<std::sync::Arc<verlet_sqlite::io::Buffer>>,
        c: verlet_sqlite::io::Completion,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        let Some(total) = buffers
            .iter()
            .try_fold(0usize, |total, buffer| total.checked_add(buffer.len()))
        else {
            c.error(completion_failure(
                IO_WRITE,
                std::io::ErrorKind::InvalidInput,
            ));
            return Ok(c);
        };
        let mut bytes = Vec::new();
        if bytes.try_reserve_exact(total).is_err() {
            c.error(completion_failure(IO_WRITE, std::io::ErrorKind::Other));
            return Ok(c);
        }
        for buffer in buffers {
            bytes.extend_from_slice(buffer.as_slice());
        }
        self.write(pos, &bytes, c)
    }

    fn sync(
        &self,
        c: verlet_sqlite::io::Completion,
        _sync_type: verlet_sqlite::io::FileSyncType,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = state.ensure_live(self.generation, IO_SYNC, &self.path) {
            c.error(error);
            return Ok(c);
        }
        let rule = state.take_rule(IO_SYNC);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_SYNC, &self.path, None, None, "fail-before");
            c.error(completion_failure(IO_SYNC, std::io::ErrorKind::Other));
            return Ok(c);
        }
        if matches!(
            rule.as_ref().map(|rule| &rule.action),
            Some(IoFaultAction::SyncLie)
        ) {
            state.record(IO_SYNC, &self.path, None, None, "lie-success");
            c.complete(0);
            return Ok(c);
        }
        let Some(file) = state.files.get_mut(&self.path) else {
            c.error(completion_failure(IO_SYNC, std::io::ErrorKind::NotFound));
            return Ok(c);
        };
        file.durable = file.volatile.clone();
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_SYNC, &self.path, None, None, "fail-after-durable");
            c.error(verlet_sqlite::io::CompletionError::IOError(
                std::io::ErrorKind::Other,
                IO_SYNC,
            ));
        } else {
            state.record(IO_SYNC, &self.path, None, None, "ok");
            c.complete(0);
        }
        Ok(c)
    }

    fn size(&self) -> Result<u64, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        state
            .ensure_live(self.generation, "size", &self.path)
            .map_err(verlet_sqlite::io::LimboError::from)?;
        let size = state
            .files
            .get(&self.path)
            .ok_or_else(|| io_failure("size", std::io::ErrorKind::NotFound))?
            .volatile
            .len() as u64;
        state.record("size", &self.path, None, None, format!("ok:{size}"));
        Ok(size)
    }

    fn truncate(
        &self,
        len: u64,
        c: verlet_sqlite::io::Completion,
    ) -> Result<verlet_sqlite::io::Completion, verlet_sqlite::io::LimboError> {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = state.ensure_live(self.generation, IO_TRUNCATE, &self.path) {
            c.error(error);
            return Ok(c);
        }
        let rule = state.take_rule(IO_TRUNCATE);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::Before,
                IoFaultAction::Fail
            ))
        ) {
            state.record(
                IO_TRUNCATE,
                &self.path,
                None,
                Some(len as usize),
                "fail-before",
            );
            c.error(completion_failure(IO_TRUNCATE, std::io::ErrorKind::Other));
            return Ok(c);
        }
        let len = match usize::try_from(len) {
            Ok(len) => len,
            Err(_) => {
                c.error(completion_failure(
                    IO_TRUNCATE,
                    std::io::ErrorKind::InvalidInput,
                ));
                return Ok(c);
            }
        };
        let Some(file) = state.files.get_mut(&self.path) else {
            c.error(completion_failure(
                IO_TRUNCATE,
                std::io::ErrorKind::NotFound,
            ));
            return Ok(c);
        };
        file.volatile.resize(len, 0);
        if matches!(
            rule.as_ref().map(|rule| (&rule.timing, &rule.action)),
            Some((
                crate::support::fault_plan::FaultTiming::After,
                IoFaultAction::Fail
            ))
        ) {
            state.record(IO_TRUNCATE, &self.path, None, Some(len), "fail-after");
            c.error(verlet_sqlite::io::CompletionError::IOError(
                std::io::ErrorKind::Other,
                IO_TRUNCATE,
            ));
        } else {
            state.record(IO_TRUNCATE, &self.path, None, Some(len), "ok");
            c.complete(0);
        }
        Ok(c)
    }
}

impl Drop for SimulatedFile {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock()
            && state.generation == self.generation
            && !state.crashed
            && let Some(file) = state.files.get_mut(&self.path)
            && file.lock_owner == Some(self.owner)
        {
            file.lock_owner = None;
        }
    }
}

fn io_failure(operation: &'static str, kind: std::io::ErrorKind) -> verlet_sqlite::io::LimboError {
    completion_failure(operation, kind).into()
}

fn completion_failure(
    operation: &'static str,
    kind: std::io::ErrorKind,
) -> verlet_sqlite::io::CompletionError {
    verlet_sqlite::io::CompletionError::IOError(kind, operation)
}

#[cfg(test)]
mod tests {
    use verlet_sqlite::io::IO as _;

    fn write_completion() -> verlet_sqlite::io::Completion {
        verlet_sqlite::io::Completion::new_write(|_| {})
    }

    fn sync_completion() -> verlet_sqlite::io::Completion {
        verlet_sqlite::io::Completion::new_sync(|_| {})
    }

    fn counted_write_completion(
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> verlet_sqlite::io::Completion {
        verlet_sqlite::io::Completion::new_write(move |result| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(result.is_err());
        })
    }

    #[test]
    fn delay_rule_is_rejected_instead_of_using_wall_clock_time() {
        let error = crate::support::simulated_io::IoFaultPlan::new(
            1,
            vec![crate::support::simulated_io::IoFaultRule {
                operation: "typo-operation",
                nth: 1,
                timing: crate::support::fault_plan::FaultTiming::Before,
                action: crate::support::simulated_io::IoFaultAction::Fail,
            }],
        )
        .unwrap_err();
        assert!(error.contains("unknown operation"));

        let error = crate::support::simulated_io::IoFaultPlan::new(
            1,
            vec![crate::support::simulated_io::IoFaultRule {
                operation: crate::support::simulated_io::IO_WRITE,
                nth: 1,
                timing: crate::support::fault_plan::FaultTiming::Before,
                action: crate::support::simulated_io::IoFaultAction::Delay(
                    std::time::Duration::from_millis(1),
                ),
            }],
        )
        .unwrap_err();
        assert!(error.contains("operations are synchronous"));

        let seed = 0x4150_1000;
        let io = crate::support::simulated_io::SimulatedIo::new(seed);
        let file = io
            .open_file(
                "completion-error.sqlite3",
                verlet_sqlite::io::OpenFlags::Create,
                false,
            )
            .unwrap();
        io.arm(
            crate::support::simulated_io::IoFaultPlan::new(
                seed,
                vec![crate::support::simulated_io::IoFaultRule {
                    operation: crate::support::simulated_io::IO_WRITE,
                    nth: 1,
                    timing: crate::support::fault_plan::FaultTiming::Before,
                    action: crate::support::simulated_io::IoFaultAction::Fail,
                }],
            )
            .unwrap(),
        )
        .unwrap();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let completion = file
            .pwrite(
                0,
                std::sync::Arc::new(verlet_sqlite::io::Buffer::new(vec![1])),
                counted_write_completion(std::sync::Arc::clone(&calls)),
            )
            .expect("injected IO failures are delivered through the completion");
        assert!(completion.finished() && completion.failed());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn sync_lie_reports_success_without_promoting_volatile_bytes() {
        fn run(
            truthful_sync_after_lie: bool,
        ) -> (u64, Vec<crate::support::simulated_io::IoTranscriptEntry>) {
            let seed = 0x4150_1001;
            let io = crate::support::simulated_io::SimulatedIo::new(seed);
            let stale_file = io
                .open_file(
                    "sync-lie.sqlite3",
                    verlet_sqlite::io::OpenFlags::Create,
                    false,
                )
                .unwrap();
            io.arm(
                crate::support::simulated_io::IoFaultPlan::new(
                    seed,
                    vec![
                        crate::support::simulated_io::IoFaultRule {
                            operation: crate::support::simulated_io::IO_SYNC,
                            nth: 1,
                            timing: crate::support::fault_plan::FaultTiming::After,
                            action: crate::support::simulated_io::IoFaultAction::SyncLie,
                        },
                        crate::support::simulated_io::IoFaultRule {
                            operation: crate::support::simulated_io::IO_WRITE,
                            nth: 2,
                            timing: crate::support::fault_plan::FaultTiming::After,
                            action: crate::support::simulated_io::IoFaultAction::Crash(
                                crate::support::simulated_io::CrashSurvival::DiscardUnsynced,
                            ),
                        },
                    ],
                )
                .unwrap(),
            )
            .unwrap();

            drop(
                stale_file
                    .pwrite(
                        0,
                        std::sync::Arc::new(verlet_sqlite::io::Buffer::new(vec![1, 2, 3])),
                        write_completion(),
                    )
                    .unwrap(),
            );
            let sync = stale_file
                .sync(sync_completion(), verlet_sqlite::io::FileSyncType::Fsync)
                .unwrap();
            assert!(
                sync.succeeded(),
                "the injected fsync lie must report success"
            );
            if truthful_sync_after_lie {
                let sync = stale_file
                    .sync(sync_completion(), verlet_sqlite::io::FileSyncType::Fsync)
                    .unwrap();
                assert!(sync.succeeded());
            }
            drop(
                stale_file
                    .pwrite(
                        3,
                        std::sync::Arc::new(verlet_sqlite::io::Buffer::new(vec![4])),
                        write_completion(),
                    )
                    .unwrap(),
            );
            assert!(io.crashed());

            let recovered = io.recover().unwrap();
            let stale_completion = stale_file
                .truncate(
                    0,
                    verlet_sqlite::io::Completion::new_trunc(|result| assert!(result.is_err())),
                )
                .expect("a stale handle reports generation failure through its completion");
            assert!(stale_completion.finished() && stale_completion.failed());
            drop(stale_file);
            let file = recovered
                .open_file(
                    "sync-lie.sqlite3",
                    verlet_sqlite::io::OpenFlags::Create,
                    false,
                )
                .unwrap();
            (file.size().unwrap(), recovered.transcript())
        }

        let without_repair = run(false);
        assert_eq!(without_repair.0, 0);
        assert!(without_repair.1.iter().any(|entry| entry.operation
            == crate::support::simulated_io::IO_SYNC
            && entry.outcome == "lie-success"));

        let with_repair = run(true);
        assert_eq!(with_repair.0, 3);
        assert!(with_repair.1.iter().any(|entry| entry.operation
            == crate::support::simulated_io::IO_SYNC
            && entry.outcome == "ok"));
    }

    #[test]
    fn torn_crash_persists_only_a_seeded_bounded_write_prefix() {
        fn run(
            seed: u64,
            bytes: Vec<u8>,
            max_bytes: usize,
        ) -> (usize, Vec<crate::support::simulated_io::IoTranscriptEntry>) {
            let io = crate::support::simulated_io::SimulatedIo::new(seed);
            let file = io
                .open_file("torn.sqlite3", verlet_sqlite::io::OpenFlags::Create, false)
                .unwrap();
            drop(
                file.pwrite(
                    0,
                    std::sync::Arc::new(verlet_sqlite::io::Buffer::new(vec![1, 2, 3])),
                    write_completion(),
                )
                .unwrap(),
            );
            drop(
                file.sync(sync_completion(), verlet_sqlite::io::FileSyncType::Fsync)
                    .unwrap(),
            );
            io.arm(
                crate::support::simulated_io::IoFaultPlan::crash_after_write(
                    seed,
                    1,
                    crate::support::simulated_io::CrashSurvival::TornWrite { max_bytes },
                ),
            )
            .unwrap();
            drop(
                file.pwrite(
                    3,
                    std::sync::Arc::new(verlet_sqlite::io::Buffer::new(bytes)),
                    write_completion(),
                )
                .unwrap(),
            );
            drop(file);

            let recovered = io.recover().unwrap();
            let file = recovered
                .open_file("torn.sqlite3", verlet_sqlite::io::OpenFlags::Create, false)
                .unwrap();
            let size = file.size().unwrap() as usize;
            (size, recovered.transcript())
        }

        let first = run(0x4150_1002, vec![4, 5, 6, 7], 2);
        let second = run(0x4150_1002, vec![4, 5, 6, 7], 2);
        assert_eq!(first, second);
        assert!((4..=5).contains(&first.0));
        assert!(first.1.iter().any(|entry| {
            entry.operation == crate::support::simulated_io::IO_WRITE
                && entry.outcome.starts_with("crash:torn-prefix:")
        }));

        let empty = run(0x4150_1003, Vec::new(), 4);
        assert_eq!(empty.0, 3);
        assert!(empty.1.iter().any(|entry| {
            entry.operation == crate::support::simulated_io::IO_WRITE
                && entry.outcome == "crash:torn-prefix:0"
        }));

        let full = run(0x4150_1004, vec![4], 1);
        assert_eq!(full.0, 4);
        assert!(full.1.iter().any(|entry| {
            entry.operation == crate::support::simulated_io::IO_WRITE
                && entry.outcome == "crash:torn-prefix:1"
        }));
    }
}
