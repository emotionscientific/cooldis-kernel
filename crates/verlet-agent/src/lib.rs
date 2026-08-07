pub mod contracts;
pub mod manifest_schema;
pub mod tool_ref;

pub type VerletResult<T> = Result<T, VerletAgentError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletAgentError {
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error(transparent)]
    Operations(#[from] verlet_operations::VerletOperationsError),
}
