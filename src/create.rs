use crate::workflowservicepb;
use crate::WorkflowServiceClient;
use anyhow::{anyhow, Context};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tonic::transport::Endpoint;
use tonic::Code;

#[derive(Clone, Debug)]
pub struct CreateVolumeConfig {
    pub frontend_endpoint: String,
    pub namespace: String,
    pub volume_id: String,
    pub size_bytes: u64,
    pub block_size_bytes: u32,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub request_id: Option<String>,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateVolumeOutcome {
    Created { run_id: String },
    AlreadyExists,
}

pub async fn run_create_volume(config: CreateVolumeConfig) -> anyhow::Result<CreateVolumeOutcome> {
    let CreateVolumeConfig {
        frontend_endpoint,
        namespace,
        volume_id,
        size_bytes,
        block_size_bytes,
        connect_timeout,
        rpc_timeout,
        request_id,
        if_not_exists,
    } = config;

    if size_bytes == 0 {
        return Err(anyhow!("size_bytes must be > 0"));
    }

    let endpoint_url = normalize_endpoint(&frontend_endpoint);
    let endpoint = Endpoint::from_shared(endpoint_url.clone())
        .with_context(|| format!("invalid frontend endpoint: {endpoint_url}"))?
        .connect_timeout(connect_timeout)
        .timeout(rpc_timeout);
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to frontend endpoint: {endpoint_url}"))?;
    let mut client = WorkflowServiceClient::new(channel);

    let size_bytes_i64 = i64::try_from(size_bytes).context("size_bytes must be <= i64::MAX")?;
    let block_size_i32 =
        i32::try_from(block_size_bytes).context("block_size_bytes must be <= i32::MAX")?;

    let request = workflowservicepb::CreateVolumeRequest {
        namespace: namespace.clone(),
        volume_id: volume_id.clone(),
        size_bytes: size_bytes_i64,
        block_size_bytes: block_size_i32,
        request_id: request_id.unwrap_or_else(default_request_id),
        ..Default::default()
    };

    let response = timeout(rpc_timeout, client.create_volume(request))
        .await
        .with_context(|| format!("CreateVolume timed out after {}s", rpc_timeout.as_secs()))?;

    match response {
        Ok(response) => {
            let created = response.into_inner();
            if created.volume_id != volume_id {
                return Err(anyhow!(
                    "CreateVolume returned unexpected volume_id: got {}, want {}",
                    created.volume_id,
                    volume_id
                ));
            }
            if created.run_id.is_empty() {
                return Err(anyhow!("CreateVolume returned empty run_id"));
            }
            Ok(CreateVolumeOutcome::Created {
                run_id: created.run_id,
            })
        }
        Err(status) => {
            if status.code() == Code::AlreadyExists && if_not_exists {
                return Ok(CreateVolumeOutcome::AlreadyExists);
            }
            Err(anyhow!(
                "CreateVolume failed with {} ({})",
                status.code(),
                status.message()
            ))
        }
    }
}

fn normalize_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

fn default_request_id() -> String {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("temporal-nbd-create-{}-{since_epoch}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn default_request_id_has_expected_prefix() {
        let request_id = default_request_id();
        assert!(request_id.starts_with("temporal-nbd-create-"));
        assert!(request_id.len() > "temporal-nbd-create-".len());
    }
}
