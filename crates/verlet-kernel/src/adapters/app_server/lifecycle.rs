//! Instance-owned lifecycle (EMO-551): the cancellation and task-ownership
//! boundary that lets one process construct, run, and tear down N kernel
//! instances independently.
//!
//! Today background work an instance starts (subscription watchers,
//! daemon-I/O workers, connection tasks) is spawned detached: dropping the
//! instance leaks the tasks, and a live watcher holding an app clone keeps
//! the whole instance alive. Every such spawn moves into an
//! [`InstanceTaskSet`] owned by the instance; `VerletAppServer::shutdown`
//! cancels the set and awaits every task before releasing the instance.
//! `Drop` never constructs a runtime (the console-credential retirement in
//! `VerletAppServerInner::drop` moves here; the Drop impl becomes a
//! best-effort warning for instances dropped without shutdown).

/// The background tasks and cancellation token owned by one kernel
/// instance. Tasks spawned through this set observe the instance's
/// cancellation token and are awaited by [`InstanceTaskSet::shutdown`];
/// nothing an instance starts may outlive it.
pub struct InstanceTaskSet {
    cancellation: tokio_util::sync::CancellationToken,
    tasks: tokio::sync::Mutex<tokio::task::JoinSet<()>>,
}

impl Default for InstanceTaskSet {
    fn default() -> Self {
        Self::new()
    }
}

impl InstanceTaskSet {
    pub fn new() -> Self {
        Self {
            cancellation: tokio_util::sync::CancellationToken::new(),
            tasks: tokio::sync::Mutex::new(tokio::task::JoinSet::new()),
        }
    }

    /// A child token for a task that needs to select on cancellation
    /// alongside its own work (watchers, accept loops).
    pub fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation.child_token()
    }

    /// Spawn a task owned by this instance. The future must exit promptly
    /// once the cancellation token fires; shutdown awaits it.
    pub async fn spawn<F>(&self, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _ = task;
        unimplemented!("EMO-551: instance-owned task spawn")
    }

    /// Cancel every owned task and await them all. Idempotent; concurrent
    /// callers all observe completion.
    pub async fn shutdown(&self) {
        unimplemented!("EMO-551: instance-owned task shutdown")
    }
}
