use std::error::Error;
use std::fmt;
use std::future::Future;

pub const LIVE_SMOKE_MAX_ATTEMPTS: usize = 3;

pub type LiveSmokeResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveSmokeModelMisbehavior {
    message: String,
}

impl LiveSmokeModelMisbehavior {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for LiveSmokeModelMisbehavior {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for LiveSmokeModelMisbehavior {}

pub fn model_misbehavior(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(LiveSmokeModelMisbehavior::new(message))
}

pub fn is_model_misbehavior(error: &(dyn Error + 'static)) -> bool {
    error.downcast_ref::<LiveSmokeModelMisbehavior>().is_some()
}

/// Runs a live provider smoke with bounded model-misbehavior retries.
///
/// The attempt closure must create a fresh conversation/thread, and any state
/// root it owns should be attempt-unique. Only failures marked with
/// [`LiveSmokeModelMisbehavior`] are retried: wrong tool input, missing expected
/// tool calls, empty/whitespace assistant text, and tool/final-output content
/// that fails the smoke's marker/profile assertions. Infra, config, registry,
/// Wasm load, store, connect, and kernel errors are returned immediately. The
/// cap is fixed at three attempts; the final model-misbehavior failure reports
/// the last failure and how many attempts were made.
pub async fn retry_model_misbehavior<F, Fut, T>(
    smoke_name: &str,
    mut attempt: F,
) -> LiveSmokeResult<T>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = LiveSmokeResult<T>>,
{
    for attempt_number in 1..=LIVE_SMOKE_MAX_ATTEMPTS {
        match attempt(attempt_number).await {
            Ok(output) => return Ok(output),
            Err(error) if is_model_misbehavior(error.as_ref()) => {
                let message = error.to_string();
                if attempt_number == LIVE_SMOKE_MAX_ATTEMPTS {
                    return Err(model_misbehavior(format!(
                        "{smoke_name} failed after {attempt_number} attempts; last model misbehavior: {message}"
                    )));
                }
                eprintln!(
                    "LIVE SMOKE RETRY {smoke_name}: attempt {attempt_number}/{LIVE_SMOKE_MAX_ATTEMPTS} failed with model misbehavior; retrying fresh attempt {}: {message}",
                    attempt_number + 1
                );
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("live smoke retry loop always returns inside the fixed attempt cap")
}

#[cfg(test)]
mod tests;
