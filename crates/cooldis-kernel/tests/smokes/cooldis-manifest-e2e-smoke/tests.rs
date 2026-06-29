#[tokio::test]
async fn researcher_manifest_bash_tool_e2e_smoke() {
    super::run_researcher().await.unwrap();
}
