pub mod bridge;
pub mod execution;
pub mod live;
pub mod process;

pub type VerletProcessResult<T> = Result<T, VerletProcessError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletProcessError {
    #[error("process execution failed: {0}")]
    Execution(String),
}

pub(crate) fn process_error(err: impl std::fmt::Display) -> VerletProcessError {
    VerletProcessError::Execution(err.to_string())
}
