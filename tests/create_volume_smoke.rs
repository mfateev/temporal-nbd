use anyhow::Context;

#[tokio::test]
async fn create_volume_duplicate_returns_already_exists() -> anyhow::Result<()> {
    if std::env::var("TEMPORAL_HISTORY_ENDPOINT").is_err()
        || std::env::var("TEMPORAL_NAMESPACE_ID").is_err()
    {
        eprintln!(
            "Skipping smoke test: set TEMPORAL_HISTORY_ENDPOINT and TEMPORAL_NAMESPACE_ID to run"
        );
        return Ok(());
    }

    let config =
        temporal_nbd::SmokeConfig::from_env().context("failed to load config from environment")?;

    temporal_nbd::run_phase1_create_volume_smoke(&config)
        .await
        .context("smoke test failed")
}
