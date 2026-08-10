//! EMO-564 acceptance-shaped unit tests for the host CLI. Config
//! parse/validate cases run here; the end-to-end boot test lives in
//! `tests/host_facade.rs` beside the facade suite.

// EMO-564: implement against the stubs in `cli/host.rs`:
// - parse_minimal_config_with_defaults (allow_non_loopback defaults false)
// - reject_duplicate_instance_ids
// - reject_duplicate_route_digest_across_instances
// - reject_relative_root_cwd_or_hook_shell
// - reject_blank_tenant_or_console_principal
// - bifrost_provider_requires_base_url_key_env_and_model
// - bifrost_key_env_must_resolve_at_load_time
// - unknown_provider_name_is_an_error
// - unknown_toml_field_is_an_error (deny_unknown_fields)
