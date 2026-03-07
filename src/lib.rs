use anyhow::{anyhow, Context};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;
use tonic::transport::Endpoint;
use tonic::Code;
use uuid::Uuid;

pub mod blockdevicepb {
    tonic::include_proto!("temporal.server.chasm.lib.blockdevice.proto.v1");
}

use blockdevicepb::block_device_service_client::BlockDeviceServiceClient;
use blockdevicepb::CreateVolumeRequest;

#[derive(Clone, Debug)]
pub struct SmokeConfig {
    pub history_endpoint: String,
    pub namespace_id: String,
    pub volume_id: String,
    pub size_bytes: u64,
    pub block_size_bytes: u32,
    pub volume_id_file: PathBuf,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
}

impl SmokeConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let history_endpoint = env::var("TEMPORAL_HISTORY_ENDPOINT")
            .context("TEMPORAL_HISTORY_ENDPOINT is required (e.g. 127.0.0.1:7234)")?;
        let namespace_id =
            env::var("TEMPORAL_NAMESPACE_ID").context("TEMPORAL_NAMESPACE_ID is required")?;

        let volume_id = env::var("TEMPORAL_VOLUME_ID")
            .unwrap_or_else(|_| format!("phase1-volume-{}", Uuid::new_v4().simple()));

        let size_bytes = env::var("TEMPORAL_VOLUME_SIZE_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1 << 30);

        let block_size_bytes = env::var("TEMPORAL_VOLUME_BLOCK_SIZE_BYTES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let volume_id_file = env::var("TEMPORAL_VOLUME_ID_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./phase1-volume-id.txt"));

        let connect_timeout = Duration::from_secs(
            env::var("TEMPORAL_CONNECT_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5),
        );

        let rpc_timeout = Duration::from_secs(
            env::var("TEMPORAL_RPC_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5),
        );

        Ok(Self {
            history_endpoint,
            namespace_id,
            volume_id,
            size_bytes,
            block_size_bytes,
            volume_id_file,
            connect_timeout,
            rpc_timeout,
        })
    }
}

pub async fn run_phase1_create_volume_smoke(config: &SmokeConfig) -> anyhow::Result<()> {
    let endpoint_url = normalize_endpoint(&config.history_endpoint);
    let endpoint = Endpoint::from_shared(endpoint_url.clone())
        .with_context(|| format!("invalid endpoint: {endpoint_url}"))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.rpc_timeout);
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to history endpoint: {endpoint_url}"))?;

    let mut client = BlockDeviceServiceClient::new(channel);

    let first_request = CreateVolumeRequest {
        namespace_id: config.namespace_id.clone(),
        volume_id: config.volume_id.clone(),
        size_bytes: config.size_bytes,
        block_size_bytes: config.block_size_bytes,
        request_id: format!("req-{}", Uuid::new_v4().simple()),
    };

    let first_response = timeout(config.rpc_timeout, client.create_volume(first_request))
        .await
        .with_context(|| {
            format!(
                "CreateVolume timed out after {}s on first call",
                config.rpc_timeout.as_secs()
            )
        })?
        .context("first CreateVolume call failed")?
        .into_inner();

    if first_response.volume_id != config.volume_id {
        return Err(anyhow!(
            "CreateVolume returned unexpected volume_id: got {}, want {}",
            first_response.volume_id,
            config.volume_id
        ));
    }
    if first_response.run_id.is_empty() {
        return Err(anyhow!("CreateVolume returned empty run_id"));
    }

    let second_request = CreateVolumeRequest {
        namespace_id: config.namespace_id.clone(),
        volume_id: config.volume_id.clone(),
        size_bytes: config.size_bytes,
        block_size_bytes: config.block_size_bytes,
        request_id: format!("req-{}", Uuid::new_v4().simple()),
    };

    let duplicate_result = timeout(config.rpc_timeout, client.create_volume(second_request))
        .await
        .with_context(|| {
            format!(
                "CreateVolume timed out after {}s on duplicate call",
                config.rpc_timeout.as_secs()
            )
        })?;
    match duplicate_result {
        Ok(_) => {
            return Err(anyhow!(
                "expected duplicate CreateVolume to fail with AlreadyExists, but call succeeded"
            ));
        }
        Err(status) if status.code() == Code::AlreadyExists => {}
        Err(status) => {
            return Err(anyhow!(
                "expected AlreadyExists on duplicate CreateVolume, got {} ({})",
                status.code(),
                status.message()
            ));
        }
    }

    persist_volume_id(&config.volume_id_file, &config.volume_id)?;
    Ok(())
}

fn normalize_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

fn persist_volume_id(path: &Path, volume_id: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
    }
    fs::write(path, format!("{volume_id}\n"))
        .with_context(|| format!("write volume id file {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_endpoint_adds_http_when_missing() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:7234"),
            "http://127.0.0.1:7234"
        );
    }

    #[test]
    fn normalize_endpoint_keeps_existing_scheme() {
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:7234"),
            "http://127.0.0.1:7234"
        );
        assert_eq!(
            normalize_endpoint("https://example.com:443"),
            "https://example.com:443"
        );
    }

    #[test]
    fn persist_volume_id_writes_expected_content() {
        let out = PathBuf::from(format!(
            "/tmp/temporal-nbd-test-{}.txt",
            Uuid::new_v4().simple()
        ));
        persist_volume_id(&out, "volume-abc").expect("persist should succeed");
        let content = fs::read_to_string(&out).expect("must read written file");
        assert_eq!(content, "volume-abc\n");
        fs::remove_file(out).ok();
    }
}
