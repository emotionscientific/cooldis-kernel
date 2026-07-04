use super::stdlib_couplings::StdlibCouplingExecutor;

pub(crate) fn registered_coupling_executor_supports_template(id: &str) -> bool {
    StdlibCouplingExecutor::supports_template(id)
}
