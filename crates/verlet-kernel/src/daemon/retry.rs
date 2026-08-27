pub(crate) const RETRY_CAP: std::time::Duration = std::time::Duration::from_secs(60);
const RETRY_MIN: std::time::Duration = std::time::Duration::from_millis(1);
const DEGRADED_THRESHOLD: u64 = 5;
const RECOVERY_THRESHOLD: u64 = 2;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RetryDecision {
    pub(crate) delay: std::time::Duration,
    pub(crate) log: Option<RetryLog>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryLog {
    Failed {
        error: String,
    },
    BackoffIncreased {
        failed_polls: u64,
        delay: std::time::Duration,
    },
    ErrorChanged {
        failed_polls: u64,
        delay: std::time::Duration,
        error: String,
    },
    Degraded {
        failed_polls: u64,
        delay: std::time::Duration,
        error: String,
    },
    Recovered {
        failed_polls: u64,
    },
}

impl RetryLog {
    pub(crate) fn message(&self, component: &str, retry_delay: std::time::Duration) -> String {
        match self {
            Self::Failed { error } => {
                format!("{component} failed: {error}; retrying in {retry_delay:?}")
            }
            Self::BackoffIncreased {
                failed_polls,
                delay,
            } => format!(
                "{component} still failing after {failed_polls} failed polls; retrying in {delay:?}"
            ),
            Self::ErrorChanged {
                failed_polls,
                delay,
                error,
            } => format!(
                "{component} failure changed after {failed_polls} failed polls: {error}; retrying in {delay:?}"
            ),
            Self::Degraded {
                failed_polls,
                delay,
                error,
            } => format!(
                "{component} degraded after {failed_polls} failed polls: {error}; will keep retrying at capped interval {delay:?}"
            ),
            Self::Recovered { failed_polls } => {
                format!("{component} recovered after {failed_polls} failed polls")
            }
        }
    }
}

pub(crate) struct RetryState {
    failed_polls: u64,
    successful_polls: u64,
    current_delay: std::time::Duration,
    last_error_key: Option<String>,
}

impl RetryState {
    pub(crate) fn new(poll_interval: std::time::Duration) -> Self {
        Self {
            failed_polls: 0,
            successful_polls: 0,
            current_delay: poll_delay(poll_interval),
            last_error_key: None,
        }
    }

    pub(crate) fn on_failure(
        &mut self,
        error: &str,
        poll_interval: std::time::Duration,
    ) -> RetryDecision {
        self.on_failure_with_key(&stable_error_key(error), error, poll_interval)
    }

    pub(crate) fn on_failure_with_key(
        &mut self,
        error_key: &str,
        error: &str,
        poll_interval: std::time::Duration,
    ) -> RetryDecision {
        let same_error = self.last_error_key.as_deref() == Some(error_key);
        let previous_delay = self.current_delay;
        self.failed_polls = self.failed_polls.saturating_add(1);
        self.successful_polls = 0;
        self.current_delay = if self.failed_polls >= DEGRADED_THRESHOLD {
            RETRY_CAP
        } else if self.failed_polls == 1 {
            retry_base(poll_interval)
        } else {
            self.current_delay.saturating_mul(2).min(RETRY_CAP)
        };
        self.last_error_key = Some(error_key.to_string());

        let log = if self.failed_polls == 1 {
            Some(RetryLog::Failed {
                error: error.to_string(),
            })
        } else if self.failed_polls == DEGRADED_THRESHOLD {
            Some(RetryLog::Degraded {
                failed_polls: self.failed_polls,
                delay: self.current_delay,
                error: error.to_string(),
            })
        } else if !same_error {
            Some(RetryLog::ErrorChanged {
                failed_polls: self.failed_polls,
                delay: self.current_delay,
                error: error.to_string(),
            })
        } else if self.current_delay > previous_delay {
            Some(RetryLog::BackoffIncreased {
                failed_polls: self.failed_polls,
                delay: self.current_delay,
            })
        } else {
            None
        };
        RetryDecision {
            delay: self.current_delay,
            log,
        }
    }

    pub(crate) fn on_success(&mut self, poll_interval: std::time::Duration) -> RetryDecision {
        let poll_delay = poll_delay(poll_interval);
        if self.failed_polls == 0 {
            self.current_delay = poll_delay;
            return RetryDecision {
                delay: self.current_delay,
                log: None,
            };
        }
        self.successful_polls = self.successful_polls.saturating_add(1);
        if self.successful_polls < RECOVERY_THRESHOLD {
            return RetryDecision {
                delay: self.current_delay,
                log: None,
            };
        }
        let failed_polls = self.failed_polls;
        self.failed_polls = 0;
        self.successful_polls = 0;
        self.current_delay = poll_delay;
        self.last_error_key = None;
        RetryDecision {
            delay: self.current_delay,
            log: Some(RetryLog::Recovered { failed_polls }),
        }
    }
}

fn retry_base(poll_interval: std::time::Duration) -> std::time::Duration {
    poll_delay(poll_interval).min(RETRY_CAP)
}

fn poll_delay(poll_interval: std::time::Duration) -> std::time::Duration {
    poll_interval.max(RETRY_MIN)
}

pub(crate) fn stable_error_key(error: &str) -> String {
    let Some(start) = error.find("short read on WAL frame") else {
        return error.to_string();
    };
    let kind = if error[start..].starts_with("short read on WAL frame validation") {
        "short read on WAL frame validation"
    } else {
        "short read on WAL frame"
    };
    format!("{}{kind}", &error[..start])
}
