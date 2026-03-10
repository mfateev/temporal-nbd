use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;
use temporal_nbd::session::{RetryConfig, VolumeSession, VolumeSessionConfig};
use temporal_nbd::ublk::runtime::{self, DeviceStartConfig};
use temporal_nbd::ublk::server::{run_serve, ServeConfig};
use temporal_nbd::ublk::signal::wait_for_shutdown_signal;
use tokio::time::timeout;

#[derive(Debug, Parser)]
#[command(name = "temporal-ublk")]
#[command(about = "Temporal UBLK manager (Phase 1)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Attach one volume with Phase 1 ublk lifecycle supervision
    Attach(AttachArgs),

    /// Run multi-device control-plane manager on a Unix socket
    Serve(ServeArgs),
}

#[derive(Debug, Args)]
struct AttachArgs {
    #[arg(
        long,
        env = "TEMPORAL_FRONTEND_ENDPOINT",
        default_value = "127.0.0.1:7233"
    )]
    frontend_endpoint: String,

    #[arg(long, env = "TEMPORAL_NAMESPACE")]
    namespace: String,

    #[arg(long, env = "TEMPORAL_VOLUME_ID")]
    volume_id: String,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_CONTROL_DEVICE",
        default_value = "/dev/ublk-control"
    )]
    ublk_control_device: PathBuf,

    #[arg(long, env = "TEMPORAL_UBLK_DEVICE_ID")]
    ublk_device_id: Option<u32>,

    #[arg(long, env = "TEMPORAL_UBLK_QUEUES", default_value_t = 1)]
    ublk_queues: u16,

    #[arg(long, env = "TEMPORAL_UBLK_QUEUE_DEPTH", default_value_t = 128)]
    ublk_queue_depth: u16,

    #[arg(long, env = "TEMPORAL_UBLK_TIMEOUT_SECS", default_value_t = 30)]
    ublk_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_CONNECT_TIMEOUT_SECS", default_value_t = 5)]
    connect_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_RPC_TIMEOUT_SECS", default_value_t = 5)]
    rpc_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_RETRY_MAX_ATTEMPTS", default_value_t = 8)]
    retry_max_attempts: usize,

    #[arg(long, env = "TEMPORAL_RETRY_INITIAL_BACKOFF_MS", default_value_t = 150)]
    retry_initial_backoff_ms: u64,

    #[arg(long, env = "TEMPORAL_RETRY_MAX_BACKOFF_MS", default_value_t = 2000)]
    retry_max_backoff_ms: u64,

    #[arg(long, env = "TEMPORAL_RETRY_JITTER_RATIO", default_value_t = 0.2)]
    retry_jitter_ratio: f64,
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(
        long,
        env = "TEMPORAL_FRONTEND_ENDPOINT",
        default_value = "127.0.0.1:7233"
    )]
    frontend_endpoint: String,

    #[arg(long, env = "TEMPORAL_NAMESPACE")]
    namespace: String,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_CONTROL_DEVICE",
        default_value = "/dev/ublk-control"
    )]
    ublk_control_device: PathBuf,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_CONTROL_SOCKET",
        default_value = "/tmp/temporal-ublk.sock"
    )]
    control_socket: PathBuf,

    #[arg(long, env = "TEMPORAL_UBLK_MAX_DEVICES", default_value_t = 64)]
    max_devices: usize,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_TERMINAL_HISTORY_LIMIT",
        default_value_t = 256
    )]
    terminal_history_limit: usize,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_IDEMPOTENCY_CACHE_LIMIT",
        default_value_t = 2048
    )]
    idempotency_cache_limit: usize,

    #[arg(long, env = "TEMPORAL_UBLK_DEFAULT_QUEUES", default_value_t = 1)]
    default_ublk_queues: u16,

    #[arg(long, env = "TEMPORAL_UBLK_DEFAULT_QUEUE_DEPTH", default_value_t = 128)]
    default_ublk_queue_depth: u16,

    #[arg(long, env = "TEMPORAL_UBLK_DEFAULT_TIMEOUT_SECS", default_value_t = 30)]
    default_ublk_timeout_secs: u64,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_GRACEFUL_DRAIN_TIMEOUT_SECS",
        default_value_t = 20
    )]
    graceful_drain_timeout_secs: u64,

    #[arg(
        long,
        env = "TEMPORAL_UBLK_FORCE_DETACH_TIMEOUT_SECS",
        default_value_t = 10
    )]
    force_detach_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_UBLK_METRICS_LISTEN")]
    metrics_listen: Option<String>,

    #[arg(long, env = "TEMPORAL_UBLK_READY_FILE")]
    ready_file: Option<PathBuf>,

    #[arg(long, env = "TEMPORAL_CONNECT_TIMEOUT_SECS", default_value_t = 5)]
    connect_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_RPC_TIMEOUT_SECS", default_value_t = 5)]
    rpc_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_RETRY_MAX_ATTEMPTS", default_value_t = 8)]
    retry_max_attempts: usize,

    #[arg(long, env = "TEMPORAL_RETRY_INITIAL_BACKOFF_MS", default_value_t = 150)]
    retry_initial_backoff_ms: u64,

    #[arg(long, env = "TEMPORAL_RETRY_MAX_BACKOFF_MS", default_value_t = 2000)]
    retry_max_backoff_ms: u64,

    #[arg(long, env = "TEMPORAL_RETRY_JITTER_RATIO", default_value_t = 0.2)]
    retry_jitter_ratio: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Attach(args) => run_attach(args).await,
        Command::Serve(args) => run_serve_mode(args).await,
    }
}

async fn run_attach(args: AttachArgs) -> anyhow::Result<()> {
    runtime::preflight(&args.ublk_control_device).map_err(|err| anyhow::anyhow!(err.message))?;
    runtime::validate_queue_model(args.ublk_queues, args.ublk_queue_depth)
        .map_err(|err| anyhow::anyhow!(err.message))?;

    let session = VolumeSession::connect_and_open(VolumeSessionConfig {
        frontend_endpoint: args.frontend_endpoint.clone(),
        namespace: args.namespace.clone(),
        volume_id: args.volume_id.clone(),
        connect_timeout: Duration::from_secs(args.connect_timeout_secs),
        rpc_timeout: Duration::from_secs(args.rpc_timeout_secs),
        retry: RetryConfig {
            max_attempts: args.retry_max_attempts,
            initial_backoff: Duration::from_millis(args.retry_initial_backoff_ms),
            max_backoff: Duration::from_millis(args.retry_max_backoff_ms),
            jitter_ratio: args.retry_jitter_ratio,
        },
    })
    .await
    .map_err(|err| anyhow::anyhow!("failed to open volume '{}': {}", args.volume_id, err))?;
    let geometry = session.geometry();
    let _session = session;

    let runtime = runtime::start_device(DeviceStartConfig {
        control_device: args.ublk_control_device.clone(),
        volume_id: args.volume_id.clone(),
        requested_device_id: args.ublk_device_id,
        size_bytes: geometry.size_bytes,
        block_size_bytes: geometry.block_size_bytes,
        queues: args.ublk_queues,
        queue_depth: args.ublk_queue_depth,
    })
    .await
    .map_err(|err| anyhow::anyhow!("failed to start ublk runtime: {}", err.message))?;

    eprintln!(
        "temporal-ublk attach ready: volume_id={} size_bytes={} block_size_bytes={} device_id={} device_path={} control_device={}",
        args.volume_id,
        geometry.size_bytes,
        geometry.block_size_bytes,
        runtime.device_id,
        runtime.device_path,
        args.ublk_control_device.display(),
    );
    eprintln!("waiting for shutdown signal");
    let _ = wait_for_shutdown_signal().await?;
    runtime.request_stop();
    timeout(Duration::from_secs(args.ublk_timeout_secs), runtime.wait())
        .await
        .context("timed out waiting for ublk detach")?
        .map_err(|err| anyhow::anyhow!("ublk runtime shutdown failed: {}", err.message))?;
    Ok(())
}

async fn run_serve_mode(args: ServeArgs) -> anyhow::Result<()> {
    run_serve(ServeConfig {
        frontend_endpoint: args.frontend_endpoint,
        namespace: args.namespace,
        ublk_control_device: args.ublk_control_device,
        control_socket: args.control_socket,
        max_devices: args.max_devices,
        terminal_history_limit: args.terminal_history_limit,
        idempotency_cache_limit: args.idempotency_cache_limit,
        default_ublk_queues: args.default_ublk_queues,
        default_ublk_queue_depth: args.default_ublk_queue_depth,
        default_ublk_timeout: Duration::from_secs(args.default_ublk_timeout_secs),
        metrics_listen: args.metrics_listen,
        ready_file: args.ready_file,
        connect_timeout: Duration::from_secs(args.connect_timeout_secs),
        rpc_timeout: Duration::from_secs(args.rpc_timeout_secs),
        retry_max_attempts: args.retry_max_attempts,
        retry_initial_backoff: Duration::from_millis(args.retry_initial_backoff_ms),
        retry_max_backoff: Duration::from_millis(args.retry_max_backoff_ms),
        retry_jitter_ratio: args.retry_jitter_ratio,
        graceful_drain_timeout: Duration::from_secs(args.graceful_drain_timeout_secs),
        force_detach_timeout: Duration::from_secs(args.force_detach_timeout_secs),
    })
    .await
    .context("temporal-ublk serve failed")
}
