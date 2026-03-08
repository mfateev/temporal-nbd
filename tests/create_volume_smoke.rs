use anyhow::Context;

#[tokio::test]
async fn create_volume_duplicate_returns_already_exists() -> anyhow::Result<()> {
    if std::env::var("TEMPORAL_NAMESPACE").is_err() {
        eprintln!(
            "Skipping smoke test: set TEMPORAL_NAMESPACE (and optional TEMPORAL_FRONTEND_ENDPOINT) to run"
        );
        return Ok(());
    }

    let config =
        temporal_nbd::SmokeConfig::from_env().context("failed to load config from environment")?;

    temporal_nbd::run_phase2_workflowservice_smoke(&config)
        .await
        .context("smoke test failed")
}
