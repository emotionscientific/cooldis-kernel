#[tokio::test]
async fn researcher_manifest_bash_tool_e2e_smoke() {
    crate::run_researcher().await.unwrap();
}
