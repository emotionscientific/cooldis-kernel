mod bridge;
mod execution;
mod live;
mod process;

pub use bridge::*;
pub use execution::*;
pub use live::*;
pub use process::*;

pub type CooldisProcessResult<T> = Result<T, CooldisProcessError>;

#[derive(Debug, thiserror::Error)]
pub enum CooldisProcessError {
    #[error("process execution failed: {0}")]
    Execution(String),
}

pub(crate) fn process_error(err: impl std::fmt::Display) -> CooldisProcessError {
    CooldisProcessError::Execution(err.to_string())
}
