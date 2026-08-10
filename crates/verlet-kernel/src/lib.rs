//! Verlet is a small multi-tenant host boundary for agent runtime loops.
//!
//! The crate intentionally starts above provider, shell, sandbox, and product
//! concerns. A concrete runtime implementation can be a provider loop, a test
//! runtime, or a later virtual-shell/procedure backend, but the host owns
//! tenancy, lifecycle, cancellation, and event routing.

#[cfg(test)]
mod test_abort_tripwire;

#[cfg(test)]
extern crate self as verlet;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/support/lib_mount.rs"]
mod support;

pub mod adapters {
    pub mod acp_agent;
    pub mod agent_loop;
    pub mod app_server;
    pub mod host;
    pub mod mcp_client;
    pub mod mcp_server;
    pub mod operator_client;
}

mod openai_codex;

pub mod agent {
    pub mod agent_process;
    pub mod agent_tool_router;
    pub mod coupling_templates;
    pub mod hooks;
    pub mod manifest;
    pub mod manifest_bind;
    pub mod tool_interceptor;
    pub mod tool_universe;
}

pub mod capabilities {
    pub mod execution;
    pub mod wasm_runner;
}

#[doc(hidden)]
pub mod cli;

pub mod daemon {
    pub mod clock_route;
    pub mod daemon_config;
    pub mod daemon_io;
    pub(crate) mod handle_ingress;
    pub mod identity;
    pub(crate) mod recovery_sweep;
    pub mod remote_store;
}

pub mod kernel {
    pub(crate) mod admission;
    pub mod compaction;
    pub mod context_compiler;
    pub mod control_decision;
    pub mod coupling_executor_registry;
    pub mod coupling_scheduler;
    pub mod mandate_lifecycle;
    pub mod process_handle_dispatch;
    pub mod runtime_host;
    pub mod stdlib_couplings;
    pub mod supervisor;
    pub mod thread_spawn_projector;
    pub mod wasm_couplings;
}

#[doc(hidden)]
pub mod live_smoke_support;

pub mod operations {
    pub mod kernel_packages;
    pub mod openapi_import;
    pub mod operation_builder;
    pub mod operation_registry;
    pub mod plugins;
}
