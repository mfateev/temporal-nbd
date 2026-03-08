use crate::bridge;
use crate::engine::{CachingBlockEngine, EngineConfig};
use crate::nbd::{self, NbdConfig};
use crate::session::{RetryConfig, VolumeSession, VolumeSessionConfig};
use anyhow::Context;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct AttachConfig {
    pub frontend_endpoint: String,
    pub namespace: String,
    pub volume_id: String,
    pub device_path: PathBuf,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub retry_max_attempts: usize,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub dirty_high_watermark_blocks: usize,
    pub flush_retry_deadline: Duration,
    pub flush_retry_interval: Duration,
    pub request_queue_capacity: usize,
    pub nbd_timeout_secs: u64,
}

pub async fn run_attach(config: AttachConfig) -> anyhow::Result<()> {
    nbd::preflight(&config.device_path)?;

    let retry = RetryConfig {
        max_attempts: config.retry_max_attempts,
        initial_backoff: config.retry_initial_backoff,
        max_backoff: config.retry_max_backoff,
        jitter_ratio: 0.2,
    };

    let session = VolumeSession::connect_and_open(VolumeSessionConfig {
        frontend_endpoint: config.frontend_endpoint.clone(),
        namespace: config.namespace.clone(),
        volume_id: config.volume_id.clone(),
        connect_timeout: config.connect_timeout,
        rpc_timeout: config.rpc_timeout,
        retry,
    })
    .await
    .context("failed to open volume for attach")?;

    let geometry = session.geometry();
    let engine = CachingBlockEngine::new(
        geometry.clone(),
        session,
        EngineConfig {
            dirty_high_watermark_blocks: config.dirty_high_watermark_blocks,
            flush_retry_deadline: config.flush_retry_deadline,
            flush_retry_interval: config.flush_retry_interval,
        },
    );

    let shutdown = CancellationToken::new();
    let (requests_tx, requests_rx) = bridge::channel(config.request_queue_capacity);

    let engine_task = tokio::spawn(bridge::run_engine_loop(
        engine,
        geometry.block_size_bytes,
        requests_rx,
        shutdown.child_token(),
    ));

    let mut nbd_task = tokio::spawn(nbd::serve(
        NbdConfig {
            device_path: config.device_path.clone(),
            timeout_secs: config.nbd_timeout_secs,
        },
        geometry,
        requests_tx,
        shutdown.child_token(),
    ));

    let nbd_result = tokio::select! {
        result = &mut nbd_task => {
            result.context("nbd task join failed")?
        }
        signal_result = wait_for_shutdown_signal() => {
            let signal_name = signal_result?;
            eprintln!("received {signal_name}, starting graceful detach");
            shutdown.cancel();
            nbd_task.await.context("nbd task join failed after signal")?
        }
    };

    shutdown.cancel();
    engine_task.await.context("engine task join failure")?;

    nbd_result
}

async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigint =
            signal(SignalKind::interrupt()).context("failed to install SIGINT handler")?;
        let mut sigterm =
            signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;

        tokio::select! {
            _ = sigint.recv() => Ok("SIGINT"),
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("failed to wait for ctrl-c signal")?;
        Ok("CTRL-C")
    }
}
