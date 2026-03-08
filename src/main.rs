use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = temporal_nbd::SmokeConfig::from_env()
        .context("failed to load smoke-test configuration from environment")?;

    temporal_nbd::run_phase2_workflowservice_smoke(&config)
        .await
        .context("phase2 smoke test failed")?;

    println!(
        "Phase 2 smoke test passed. Stored volume_id={} in {}",
        config.volume_id,
        config.volume_id_file.display()
    );

    Ok(())
}
