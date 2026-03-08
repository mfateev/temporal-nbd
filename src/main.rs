use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "temporal-nbd")]
#[command(about = "Temporal blockdevice client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create one Temporal volume
    CreateVolume(CreateVolumeArgs),

    /// Attach one Temporal volume to one Linux NBD device
    Attach(AttachArgs),
}

#[derive(Debug, Args)]
struct CreateVolumeArgs {
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

    #[arg(long, env = "TEMPORAL_VOLUME_SIZE_BYTES", default_value_t = 1 << 30)]
    size_bytes: u64,

    #[arg(long, env = "TEMPORAL_VOLUME_BLOCK_SIZE_BYTES", default_value_t = 0)]
    block_size_bytes: u32,

    #[arg(long, env = "TEMPORAL_CONNECT_TIMEOUT_SECS", default_value_t = 5)]
    connect_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_RPC_TIMEOUT_SECS", default_value_t = 5)]
    rpc_timeout_secs: u64,

    #[arg(long, env = "TEMPORAL_CREATE_REQUEST_ID")]
    request_id: Option<String>,

    #[arg(long, env = "TEMPORAL_CREATE_IF_NOT_EXISTS", default_value_t = false)]
    if_not_exists: bool,
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

    #[arg(long, env = "TEMPORAL_NBD_DEVICE", default_value = "/dev/nbd0")]
    nbd_device: PathBuf,

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

    #[arg(long, env = "TEMPORAL_DIRTY_HIGH_WATER_BLOCKS", default_value_t = 4096)]
    dirty_high_water_blocks: usize,

    #[arg(long, env = "TEMPORAL_FLUSH_RETRY_DEADLINE_SECS", default_value_t = 20)]
    flush_retry_deadline_secs: u64,

    #[arg(long, env = "TEMPORAL_FLUSH_RETRY_INTERVAL_MS", default_value_t = 200)]
    flush_retry_interval_ms: u64,

    #[arg(long, env = "TEMPORAL_ENGINE_QUEUE_CAPACITY", default_value_t = 1024)]
    engine_queue_capacity: usize,

    #[arg(long, env = "TEMPORAL_NBD_TIMEOUT_SECS", default_value_t = 30)]
    nbd_timeout_secs: u64,
}

impl From<AttachArgs> for temporal_nbd::attach::AttachConfig {
    fn from(value: AttachArgs) -> Self {
        Self {
            frontend_endpoint: value.frontend_endpoint,
            namespace: value.namespace,
            volume_id: value.volume_id,
            device_path: value.nbd_device,
            connect_timeout: Duration::from_secs(value.connect_timeout_secs),
            rpc_timeout: Duration::from_secs(value.rpc_timeout_secs),
            retry_max_attempts: value.retry_max_attempts,
            retry_initial_backoff: Duration::from_millis(value.retry_initial_backoff_ms),
            retry_max_backoff: Duration::from_millis(value.retry_max_backoff_ms),
            retry_jitter_ratio: value.retry_jitter_ratio,
            dirty_high_watermark_blocks: value.dirty_high_water_blocks,
            flush_retry_deadline: Duration::from_secs(value.flush_retry_deadline_secs),
            flush_retry_interval: Duration::from_millis(value.flush_retry_interval_ms),
            request_queue_capacity: value.engine_queue_capacity,
            nbd_timeout_secs: value.nbd_timeout_secs,
        }
    }
}

impl From<CreateVolumeArgs> for temporal_nbd::create::CreateVolumeConfig {
    fn from(value: CreateVolumeArgs) -> Self {
        Self {
            frontend_endpoint: value.frontend_endpoint,
            namespace: value.namespace,
            volume_id: value.volume_id,
            size_bytes: value.size_bytes,
            block_size_bytes: value.block_size_bytes,
            connect_timeout: Duration::from_secs(value.connect_timeout_secs),
            rpc_timeout: Duration::from_secs(value.rpc_timeout_secs),
            request_id: value.request_id,
            if_not_exists: value.if_not_exists,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::CreateVolume(args) => {
            let namespace = args.namespace.clone();
            let volume_id = args.volume_id.clone();
            let outcome = temporal_nbd::create::run_create_volume(args.into())
                .await
                .context("create-volume failed")?;
            match outcome {
                temporal_nbd::create::CreateVolumeOutcome::Created { run_id } => {
                    println!(
                        "created volume '{}' in namespace '{}' (run_id={run_id})",
                        volume_id, namespace
                    );
                }
                temporal_nbd::create::CreateVolumeOutcome::AlreadyExists => {
                    println!(
                        "volume '{}' already exists in namespace '{}'",
                        volume_id, namespace
                    );
                }
            }
            Ok(())
        }
        Command::Attach(args) => {
            temporal_nbd::attach::run_attach(args.into())
                .await
                .context("attach mode failed")?;
            Ok(())
        }
    }
}
