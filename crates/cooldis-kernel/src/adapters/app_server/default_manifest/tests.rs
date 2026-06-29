use super::*;
use crate::AppServerListenAddr;

#[test]
fn patch_bump_version_rejects_overflow() {
    let version = format!("1.0.{}", u64::MAX);
    let err = patch_bump_version(&version).unwrap_err();
    assert!(err.to_string().contains("not a patch-bumpable semver"));
}

#[test]
fn synthesized_default_manifest_preserves_slash_bearing_model_ids() {
    let mut config = CooldisAppServerConfig::local(
        AppServerListenAddr::Unix(std::env::temp_dir().join("cooldis-test.sock")),
        std::env::temp_dir(),
    );
    config.model_provider = "anthropic".to_string();
    config.model = "bedrock/global.anthropic.claude-sonnet-4-5-20250929-v1:0".to_string();

    let manifest = synthesize_default_manifest_with_version(&config, false, "0.1.0").unwrap();
    let profile = &manifest.model_profiles[0];

    assert_eq!(profile.provider_ref, "provider://anthropic");
    assert_eq!(
        profile.model_ref,
        "model://anthropic/bedrock/global.anthropic.claude-sonnet-4-5-20250929-v1:0"
    );
}
