//! Instance-owned lifecycle (EMO-551): the cancellation and task-ownership
//! boundary that lets one process construct, run, and tear down N kernel
//! instances independently.
//!
//! Today background work an instance starts (subscription watchers,
//! connection tasks, websocket writers, process-settlement monitors, and
//! persistence one-shots) is
//! spawned detached: dropping the instance leaks the tasks, and a live
//! watcher holding an app clone keeps the whole instance alive. Every such
//! spawn moves into an [`InstanceTaskSet`] owned by the instance;
//! `VerletAppServer::shutdown` cancels the set and awaits every task before
//! releasing the instance.
//! `Drop` never constructs a runtime (the console-credential retirement in
//! `VerletAppServerInner::drop` moves here; the Drop impl becomes a
//! best-effort warning for instances dropped without shutdown).

/// The background tasks and cancellation token owned by one kernel
/// instance. Tasks spawned through this set observe the instance's
/// cancellation token and are awaited by [`InstanceTaskSet::shutdown`];
/// nothing an instance starts may outlive it.
pub struct InstanceTaskSet {
    cancellation: tokio_util::sync::CancellationToken,
    tasks: std::sync::Mutex<Option<tokio::task::JoinSet<()>>>,
    shutdown_drain: tokio::sync::Mutex<()>,
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
            tasks: std::sync::Mutex::new(Some(tokio::task::JoinSet::new())),
            shutdown_drain: tokio::sync::Mutex::new(()),
        }
    }

    /// A child token for a task that needs to select on cancellation
    /// alongside its own work (watchers, accept loops).
    pub fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation.child_token()
    }

    /// Spawn a task owned by this instance. Long-lived work must exit promptly
    /// once the cancellation token fires; bounded atomic one-shots may run to
    /// completion because shutdown awaits them. A spawn after shutdown has
    /// begun drops the supplied future without polling it. Returns whether the
    /// task was accepted.
    pub fn spawn<F>(&self, task: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        if self.cancellation.is_cancelled() {
            return false;
        }
        let Some(tasks) = tasks.as_mut() else {
            return false;
        };
        while let Some(result) = tasks.try_join_next() {
            log_task_result(result);
        }
        tasks.spawn(task);
        true
    }

    /// Spawn work that may be abandoned at the instance cancellation
    /// boundary. Use [`InstanceTaskSet::spawn`] directly for atomic one-shots
    /// that must run to completion once they have started.
    pub fn spawn_cancellable<F>(&self, task: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let cancellation = self.cancellation();
        self.spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {},
                _ = task => {},
            }
        })
    }

    /// Cancel every owned task and await them all. Idempotent; concurrent
    /// callers all observe completion.
    pub async fn shutdown(&self) {
        self.cancel();
        let _drain = self.shutdown_drain.lock().await;
        loop {
            let result = std::future::poll_fn(|context| {
                let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
                let Some(tasks) = tasks.as_mut() else {
                    return std::task::Poll::Ready(None);
                };
                tasks.poll_join_next(context)
            })
            .await;
            let Some(result) = result else {
                break;
            };
            log_task_result(result);
        }
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    /// Signal cancellation without waiting for the owned task set to drain.
    /// Instance shutdown uses this before closing its dispatch gate so an
    /// active request cannot hold the gate while waiting for cancellation that
    /// would otherwise begin only after the gate closed.
    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(crate) fn spawn_from_drop<F>(&self, task: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_err() || self.cancellation.is_cancelled() {
            return false;
        }
        self.spawn(task)
    }

    #[cfg(test)]
    pub(crate) fn task_count(&self) -> usize {
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(tokio::task::JoinSet::len)
            .unwrap_or(0)
    }
}

fn log_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        eprintln!("instance-owned Verlet app-server task failed: {error}");
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn finished_tasks_are_reaped_before_the_next_spawn() {
        let tasks = crate::adapters::app_server::lifecycle::InstanceTaskSet::new();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            let _ = finished_tx.send(());
        });
        finished_rx.await.unwrap();
        tokio::task::yield_now().await;

        let cancellation = tasks.cancellation();
        tasks.spawn(async move {
            cancellation.cancelled().await;
        });

        assert_eq!(tasks.task_count(), 1);
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_can_resume_after_the_draining_caller_is_cancelled() {
        let tasks =
            std::sync::Arc::new(crate::adapters::app_server::lifecycle::InstanceTaskSet::new());
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_started = std::sync::Arc::clone(&started);
        let task_release = std::sync::Arc::clone(&release);
        tasks.spawn(async move {
            task_started.notify_one();
            task_release.notified().await;
        });
        started.notified().await;

        let first = {
            let tasks = std::sync::Arc::clone(&tasks);
            tokio::spawn(async move { tasks.shutdown().await })
        };
        while !tasks.is_shutdown() {
            tokio::task::yield_now().await;
        }
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        release.notify_one();

        // tight-timeout: a retry must not wait for the abandoned shutdown caller
        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("shutdown retry remained stuck after its first caller was cancelled");
        assert_eq!(tasks.task_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_shutdown_cancels_and_drains_every_owned_task() {
        let tasks =
            std::sync::Arc::new(crate::adapters::app_server::lifecycle::InstanceTaskSet::new());
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_started = std::sync::Arc::clone(&started);
        let cancellation = tasks.cancellation();
        tasks.spawn(async move {
            task_started.notify_one();
            cancellation.cancelled().await;
        });
        started.notified().await;

        let first = {
            let tasks = std::sync::Arc::clone(&tasks);
            async move { tasks.shutdown().await }
        };
        let second = {
            let tasks = std::sync::Arc::clone(&tasks);
            async move { tasks.shutdown().await }
        };
        tokio::join!(first, second);

        assert_eq!(tasks.task_count(), 0);
        tasks.shutdown().await;

        let polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_polled = std::sync::Arc::clone(&polled);
        tasks.spawn(async move {
            task_polled.store(true, std::sync::atomic::Ordering::Release);
        });
        assert!(!polled.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(tasks.task_count(), 0);
    }
}
