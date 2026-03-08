use anyhow::{anyhow, bail, Context};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use temporal_nbd::{workflowservicepb, WorkflowServiceClient};
use tokio::time::timeout;
use tonic::transport::Endpoint;
use tonic::Code;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct SmokeConfig {
    pub frontend_endpoint: String,
    pub namespace: String,
    pub volume_id: String,
    pub size_bytes: u64,
    pub block_size_bytes: u32,
    #[allow(dead_code)]
    pub volume_id_file: PathBuf,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct BlockWriteInput {
    pub lba: u64,
    pub data: Vec<u8>,
}

impl SmokeConfig {
    #[allow(dead_code)]
    pub fn from_env() -> anyhow::Result<Self> {
        let frontend_endpoint =
            env::var("TEMPORAL_FRONTEND_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7233".to_string());
        let namespace = env::var("TEMPORAL_NAMESPACE")
            .context("TEMPORAL_NAMESPACE is required (namespace name, not namespace ID)")?;

        let volume_id = env::var("TEMPORAL_VOLUME_ID")
            .unwrap_or_else(|_| format!("phase2-volume-{}", Uuid::new_v4().simple()));

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
            .unwrap_or_else(|_| PathBuf::from("./phase2-volume-id.txt"));

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
            frontend_endpoint,
            namespace,
            volume_id,
            size_bytes,
            block_size_bytes,
            volume_id_file,
            connect_timeout,
            rpc_timeout,
        })
    }
}

#[allow(dead_code)]
pub async fn run_phase2_workflowservice_smoke(config: &SmokeConfig) -> anyhow::Result<()> {
    let mut client = connect_workflow_client(config).await?;

    let first_response = create_volume(
        &mut client,
        config,
        format!("req-{}", Uuid::new_v4().simple()),
    )
    .await
    .context("first CreateVolume call failed")?;
    validate_create_volume_response(&first_response, &config.volume_id)?;

    let duplicate = create_volume(
        &mut client,
        config,
        format!("req-{}", Uuid::new_v4().simple()),
    )
    .await;
    match duplicate {
        Ok(_) => {
            return Err(anyhow!(
                "expected duplicate CreateVolume to fail with AlreadyExists, but call succeeded"
            ));
        }
        Err(err) => {
            let status = err.downcast_ref::<tonic::Status>().ok_or_else(|| {
                anyhow!("expected tonic::Status for duplicate create, got: {err}")
            })?;
            if status.code() != Code::AlreadyExists {
                return Err(anyhow!(
                    "expected AlreadyExists on duplicate CreateVolume, got {} ({})",
                    status.code(),
                    status.message()
                ));
            }
        }
    }

    let open_response = open_volume(
        &mut client,
        &config.namespace,
        &config.volume_id,
        config.rpc_timeout,
    )
    .await
    .context("OpenVolume failed")?;

    if open_response.volume_id != config.volume_id {
        bail!(
            "OpenVolume returned unexpected volume_id: got {}, want {}",
            open_response.volume_id,
            config.volume_id
        );
    }
    if open_response.size_bytes != i64::try_from(config.size_bytes)? {
        bail!(
            "OpenVolume returned unexpected size_bytes: got {}, want {}",
            open_response.size_bytes,
            config.size_bytes
        );
    }
    if open_response.block_size_bytes <= 0 {
        bail!(
            "OpenVolume returned invalid block_size_bytes: {}",
            open_response.block_size_bytes
        );
    }
    let block_size_bytes = u32::try_from(open_response.block_size_bytes)
        .context("OpenVolume returned block_size_bytes that cannot fit into u32")?;

    let first_block = vec![0xAA; usize::try_from(block_size_bytes)?];
    let second_block = vec![0xBB; usize::try_from(block_size_bytes)?];
    let third_block = vec![0xCC; usize::try_from(block_size_bytes)?];
    let zero_block = vec![0x00; usize::try_from(block_size_bytes)?];

    let write_response = write_batch(
        &mut client,
        &config.namespace,
        &config.volume_id,
        block_size_bytes,
        &[
            BlockWriteInput {
                lba: 1,
                data: first_block.clone(),
            },
            BlockWriteInput {
                lba: 1,
                data: second_block.clone(),
            },
            BlockWriteInput {
                lba: 3,
                data: third_block.clone(),
            },
        ],
        config.rpc_timeout,
    )
    .await
    .context("WriteBatch failed")?;
    if write_response.writes_applied != 3 {
        bail!(
            "WriteBatch returned unexpected writes_applied: got {}, want 3",
            write_response.writes_applied
        );
    }

    let read_response = read_blocks(
        &mut client,
        &config.namespace,
        &config.volume_id,
        1,
        3,
        config.rpc_timeout,
    )
    .await
    .context("ReadBlocks failed")?;

    let expected = [
        second_block.as_slice(),
        zero_block.as_slice(),
        third_block.as_slice(),
    ]
    .concat();
    if read_response.data != expected {
        bail!("ReadBlocks returned unexpected bytes for written/unwritten range");
    }

    let total_blocks = u64::try_from(open_response.size_bytes)?
        .checked_div(u64::from(block_size_bytes))
        .ok_or_else(|| anyhow!("invalid block_size_bytes returned by OpenVolume"))?;
    let out_of_range_lba = total_blocks;

    let out_of_range_read = read_blocks(
        &mut client,
        &config.namespace,
        &config.volume_id,
        out_of_range_lba,
        1,
        config.rpc_timeout,
    )
    .await;
    match out_of_range_read {
        Ok(_) => return Err(anyhow!("expected out-of-range ReadBlocks to fail")),
        Err(err) => {
            let status = err.downcast_ref::<tonic::Status>().ok_or_else(|| {
                anyhow!("expected tonic::Status for out-of-range read, got: {err}")
            })?;
            if status.code() != Code::InvalidArgument {
                return Err(anyhow!(
                    "expected InvalidArgument for out-of-range ReadBlocks, got {} ({})",
                    status.code(),
                    status.message()
                ));
            }
        }
    }

    let out_of_range_write = write_batch(
        &mut client,
        &config.namespace,
        &config.volume_id,
        block_size_bytes,
        &[BlockWriteInput {
            lba: out_of_range_lba,
            data: vec![0xEE; usize::try_from(block_size_bytes)?],
        }],
        config.rpc_timeout,
    )
    .await;
    match out_of_range_write {
        Ok(_) => return Err(anyhow!("expected out-of-range WriteBatch to fail")),
        Err(err) => {
            let status = err.downcast_ref::<tonic::Status>().ok_or_else(|| {
                anyhow!("expected tonic::Status for out-of-range write, got: {err}")
            })?;
            if status.code() != Code::InvalidArgument {
                return Err(anyhow!(
                    "expected InvalidArgument for out-of-range WriteBatch, got {} ({})",
                    status.code(),
                    status.message()
                ));
            }
        }
    }

    persist_volume_id(&config.volume_id_file, &config.volume_id)?;
    Ok(())
}

pub async fn connect_workflow_client(
    config: &SmokeConfig,
) -> anyhow::Result<WorkflowServiceClient> {
    let endpoint_url = normalize_endpoint(&config.frontend_endpoint);
    let endpoint = Endpoint::from_shared(endpoint_url.clone())
        .with_context(|| format!("invalid endpoint: {endpoint_url}"))?
        .connect_timeout(config.connect_timeout)
        .timeout(config.rpc_timeout);
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to frontend endpoint: {endpoint_url}"))?;

    Ok(WorkflowServiceClient::new(channel))
}

pub async fn create_volume(
    client: &mut WorkflowServiceClient,
    config: &SmokeConfig,
    request_id: String,
) -> anyhow::Result<workflowservicepb::CreateVolumeResponse> {
    let request = build_create_volume_request(config, request_id)?;
    let response = timeout(config.rpc_timeout, client.create_volume(request))
        .await
        .with_context(|| {
            format!(
                "CreateVolume timed out after {}s",
                config.rpc_timeout.as_secs()
            )
        })?;
    Ok(response?.into_inner())
}

#[allow(dead_code)]
pub async fn open_volume(
    client: &mut WorkflowServiceClient,
    namespace: &str,
    volume_id: &str,
    rpc_timeout: Duration,
) -> anyhow::Result<workflowservicepb::OpenVolumeResponse> {
    let response = timeout(
        rpc_timeout,
        client.open_volume(workflowservicepb::OpenVolumeRequest {
            namespace: namespace.to_string(),
            volume_id: volume_id.to_string(),
        }),
    )
    .await
    .with_context(|| format!("OpenVolume timed out after {}s", rpc_timeout.as_secs()))?;
    Ok(response?.into_inner())
}

#[allow(dead_code)]
pub async fn write_batch(
    client: &mut WorkflowServiceClient,
    namespace: &str,
    volume_id: &str,
    block_size_bytes: u32,
    writes: &[BlockWriteInput],
    rpc_timeout: Duration,
) -> anyhow::Result<workflowservicepb::WriteBatchResponse> {
    if writes.is_empty() {
        bail!("write_batch requires at least one write");
    }
    let block_size = usize::try_from(block_size_bytes)
        .context("block_size_bytes must fit into usize for client-side validation")?;

    let mut request_writes = Vec::with_capacity(writes.len());
    for (index, write) in writes.iter().enumerate() {
        if write.data.len() != block_size {
            bail!(
                "write {} has invalid data length: got {}, want {}",
                index,
                write.data.len(),
                block_size
            );
        }
        request_writes.push(workflowservicepb::BlockWrite {
            lba: write.lba,
            data: write.data.clone(),
        });
    }

    let response = timeout(
        rpc_timeout,
        client.write_batch(workflowservicepb::WriteBatchRequest {
            namespace: namespace.to_string(),
            volume_id: volume_id.to_string(),
            writes: request_writes,
        }),
    )
    .await
    .with_context(|| format!("WriteBatch timed out after {}s", rpc_timeout.as_secs()))?;
    Ok(response?.into_inner())
}

#[allow(dead_code)]
pub async fn read_blocks(
    client: &mut WorkflowServiceClient,
    namespace: &str,
    volume_id: &str,
    start_lba: u64,
    block_count: u32,
    rpc_timeout: Duration,
) -> anyhow::Result<workflowservicepb::ReadBlocksResponse> {
    if block_count == 0 {
        bail!("read_blocks requires block_count > 0");
    }

    let response = timeout(
        rpc_timeout,
        client.read_blocks(workflowservicepb::ReadBlocksRequest {
            namespace: namespace.to_string(),
            volume_id: volume_id.to_string(),
            start_lba,
            block_count,
        }),
    )
    .await
    .with_context(|| format!("ReadBlocks timed out after {}s", rpc_timeout.as_secs()))?;
    Ok(response?.into_inner())
}

fn normalize_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

fn build_create_volume_request(
    config: &SmokeConfig,
    request_id: String,
) -> anyhow::Result<workflowservicepb::CreateVolumeRequest> {
    let size_bytes = i64::try_from(config.size_bytes)
        .context("TEMPORAL_VOLUME_SIZE_BYTES must be <= i64::MAX")?;
    let block_size_bytes = i32::try_from(config.block_size_bytes)
        .context("TEMPORAL_VOLUME_BLOCK_SIZE_BYTES must be <= i32::MAX")?;

    Ok(workflowservicepb::CreateVolumeRequest {
        namespace: config.namespace.clone(),
        volume_id: config.volume_id.clone(),
        size_bytes,
        block_size_bytes,
        request_id,
        ..Default::default()
    })
}

fn validate_create_volume_response(
    response: &workflowservicepb::CreateVolumeResponse,
    expected_volume_id: &str,
) -> anyhow::Result<()> {
    if response.volume_id != expected_volume_id {
        return Err(anyhow!(
            "CreateVolume returned unexpected volume_id: got {}, want {}",
            response.volume_id,
            expected_volume_id
        ));
    }
    if response.run_id.is_empty() {
        return Err(anyhow!("CreateVolume returned empty run_id"));
    }
    Ok(())
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

    fn sample_config() -> SmokeConfig {
        SmokeConfig {
            frontend_endpoint: "127.0.0.1:7233".to_string(),
            namespace: "default".to_string(),
            volume_id: "volume-id".to_string(),
            size_bytes: 1 << 30,
            block_size_bytes: 4096,
            volume_id_file: PathBuf::from("/tmp/volume-id.txt"),
            connect_timeout: Duration::from_secs(3),
            rpc_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn normalize_endpoint_adds_http_when_missing() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:7233"),
            "http://127.0.0.1:7233"
        );
    }

    #[test]
    fn normalize_endpoint_keeps_existing_scheme() {
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:7233"),
            "http://127.0.0.1:7233"
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

    #[test]
    fn build_create_volume_request_sets_workflowservice_fields() {
        let config = sample_config();
        let request = build_create_volume_request(&config, "req-123".to_string())
            .expect("request should build");

        assert_eq!(request.namespace, config.namespace);
        assert_eq!(request.volume_id, config.volume_id);
        assert_eq!(request.size_bytes, config.size_bytes as i64);
        assert_eq!(request.block_size_bytes, config.block_size_bytes as i32);
        assert_eq!(request.request_id, "req-123");
    }

    #[test]
    fn validate_create_volume_response_rejects_empty_run_id() {
        let response = workflowservicepb::CreateVolumeResponse {
            volume_id: "volume-id".to_string(),
            run_id: String::new(),
        };

        let err = validate_create_volume_response(&response, "volume-id")
            .expect_err("empty run id must fail");
        assert!(err.to_string().contains("empty run_id"));
    }
}
