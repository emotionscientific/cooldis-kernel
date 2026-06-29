pub mod contracts;
pub mod manifest_schema;
pub mod tool_ref;

pub use contracts::*;
pub use manifest_schema::*;
pub use tool_ref::*;

pub type CooldisResult<T> = Result<T, CooldisAgentError>;

#[derive(Debug, thiserror::Error)]
pub enum CooldisAgentError {
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error(transparent)]
    Operations(#[from] cooldis_operations::CooldisOperationsError),
}
