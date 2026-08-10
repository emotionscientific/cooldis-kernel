//! Hermetic model-catalog setup shared by integration and smoke harnesses.

#![allow(dead_code)]

pub(crate) const MODEL_CATALOG_URL_ENV: &str = "VERLET_MODEL_CATALOG_URL";

pub(crate) fn disable_in_process_refresh() {
    static DISABLED: std::sync::Once = std::sync::Once::new();
    DISABLED.call_once(|| {
        // SAFETY: every app-server constructor in the mounting test process
        // calls this one-time setup before it can read the catalog variable.
        unsafe { std::env::set_var(MODEL_CATALOG_URL_ENV, "") };
    });
}

pub(crate) fn disable_for_std_command(command: &mut std::process::Command) {
    command.env(MODEL_CATALOG_URL_ENV, "");
}

pub(crate) fn disable_for_tokio_command(command: &mut tokio::process::Command) {
    command.env(MODEL_CATALOG_URL_ENV, "");
}
