use anyhow::{anyhow, Context};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;
use tonic::transport::Endpoint;
use tonic::Code;
use uuid::Uuid;

pub mod temporal {
    pub mod api {
        pub mod activity {
            pub mod v1 {
                tonic::include_proto!("temporal.api.activity.v1");
            }
        }
        pub mod batch {
            pub mod v1 {
                tonic::include_proto!("temporal.api.batch.v1");
            }
        }
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");
            }
        }
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("temporal.api.common.v1");
            }
        }
        pub mod deployment {
            pub mod v1 {
                tonic::include_proto!("temporal.api.deployment.v1");
            }
        }
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }
        pub mod failure {
            pub mod v1 {
                tonic::include_proto!("temporal.api.failure.v1");
            }
        }
        pub mod filter {
            pub mod v1 {
                tonic::include_proto!("temporal.api.filter.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                tonic::include_proto!("temporal.api.history.v1");
            }
        }
        pub mod namespace {
            pub mod v1 {
                tonic::include_proto!("temporal.api.namespace.v1");
            }
        }
        pub mod nexus {
            pub mod v1 {
                tonic::include_proto!("temporal.api.nexus.v1");
            }
        }
        pub mod protocol {
            pub mod v1 {
                tonic::include_proto!("temporal.api.protocol.v1");
            }
        }
        pub mod query {
            pub mod v1 {
                tonic::include_proto!("temporal.api.query.v1");
            }
        }
        pub mod replication {
            pub mod v1 {
                tonic::include_proto!("temporal.api.replication.v1");
            }
        }
        pub mod rules {
            pub mod v1 {
                tonic::include_proto!("temporal.api.rules.v1");
            }
        }
        pub mod schedule {
            pub mod v1 {
                tonic::include_proto!("temporal.api.schedule.v1");
            }
        }
        pub mod sdk {
            pub mod v1 {
                tonic::include_proto!("temporal.api.sdk.v1");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                tonic::include_proto!("temporal.api.taskqueue.v1");
            }
        }
        pub mod update {
            pub mod v1 {
                tonic::include_proto!("temporal.api.update.v1");
            }
        }
        pub mod version {
            pub mod v1 {
                tonic::include_proto!("temporal.api.version.v1");
            }
        }
        pub mod worker {
            pub mod v1 {
                tonic::include_proto!("temporal.api.worker.v1");
            }
        }
        pub mod workflow {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflow.v1");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflowservice.v1");
            }
        }
    }
    pub mod server {
        pub mod chasm {
            pub mod lib {
                pub mod blockdevice {
                    pub mod proto {
                        pub mod v1 {
                            tonic::include_proto!("temporal.server.chasm.lib.blockdevice.proto.v1");
                        }
                    }
                }
            }
        }
    }
}

pub mod blockdevicepb {
    pub use crate::temporal::server::chasm::lib::blockdevice::proto::v1::*;
}

pub mod workflowservicepb {
    pub use crate::temporal::api::workflowservice::v1::*;
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

    let first_request =
        build_create_volume_request(config, format!("req-{}", Uuid::new_v4().simple()))?;

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

    validate_first_create_volume_response(&first_response, &config.volume_id)?;

    let second_request =
        build_create_volume_request(config, format!("req-{}", Uuid::new_v4().simple()))?;

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

fn build_create_volume_request(
    config: &SmokeConfig,
    request_id: String,
) -> anyhow::Result<CreateVolumeRequest> {
    let size_bytes = i64::try_from(config.size_bytes)
        .context("TEMPORAL_VOLUME_SIZE_BYTES must be <= i64::MAX")?;
    let block_size_bytes = i32::try_from(config.block_size_bytes)
        .context("TEMPORAL_VOLUME_BLOCK_SIZE_BYTES must be <= i32::MAX")?;

    Ok(CreateVolumeRequest {
        namespace_id: config.namespace_id.clone(),
        frontend_request: Some(workflowservicepb::CreateVolumeRequest {
            volume_id: config.volume_id.clone(),
            size_bytes,
            block_size_bytes,
            request_id,
            ..Default::default()
        }),
    })
}

fn validate_first_create_volume_response(
    response: &blockdevicepb::CreateVolumeResponse,
    expected_volume_id: &str,
) -> anyhow::Result<()> {
    let frontend_response = response
        .frontend_response
        .as_ref()
        .ok_or_else(|| anyhow!("CreateVolume returned empty frontend_response"))?;

    if frontend_response.volume_id != expected_volume_id {
        return Err(anyhow!(
            "CreateVolume returned unexpected volume_id: got {}, want {}",
            frontend_response.volume_id,
            expected_volume_id
        ));
    }
    if frontend_response.run_id.is_empty() {
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
            history_endpoint: "127.0.0.1:7234".to_string(),
            namespace_id: "namespace-id".to_string(),
            volume_id: "volume-id".to_string(),
            size_bytes: 1 << 30,
            block_size_bytes: 4096,
            volume_id_file: PathBuf::from("/tmp/volume-id.txt"),
            connect_timeout: Duration::from_secs(3),
            rpc_timeout: Duration::from_secs(5),
        }
    }

    fn parse_go_directive(contents: &str) -> Option<String> {
        contents
            .lines()
            .map(str::trim)
            .find_map(|line| line.strip_prefix("go ").map(str::trim))
            .map(ToString::to_string)
    }

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

    #[test]
    fn build_create_volume_request_wraps_frontend_request() {
        let config = sample_config();
        let request = build_create_volume_request(&config, "req-123".to_string())
            .expect("request should build");

        assert_eq!(request.namespace_id, config.namespace_id);
        let frontend = request
            .frontend_request
            .expect("frontend request should be present");
        assert_eq!(frontend.volume_id, config.volume_id);
        assert_eq!(frontend.size_bytes, config.size_bytes as i64);
        assert_eq!(frontend.block_size_bytes, config.block_size_bytes as i32);
        assert_eq!(frontend.request_id, "req-123");
    }

    #[test]
    fn validate_first_create_volume_response_rejects_missing_frontend_response() {
        let response = blockdevicepb::CreateVolumeResponse {
            frontend_response: None,
        };

        let err = validate_first_create_volume_response(&response, "volume-id")
            .expect_err("missing frontend response must fail");
        assert!(err.to_string().contains("empty frontend_response"));
    }

    #[test]
    fn workspace_go_version_matches_temporal_go_mod() {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .canonicalize()
            .expect("repo root should resolve");

        let go_work_contents =
            fs::read_to_string(repo_root.join("go.work")).expect("go.work should exist");
        let temporal_go_mod_contents = fs::read_to_string(repo_root.join("temporal/go.mod"))
            .expect("temporal/go.mod should exist");

        let workspace_go =
            parse_go_directive(&go_work_contents).expect("go.work must define a go directive");
        let module_go = parse_go_directive(&temporal_go_mod_contents)
            .expect("temporal/go.mod must define a go directive");

        assert_eq!(
            workspace_go, module_go,
            "go.work and temporal/go.mod must use the same go directive",
        );
    }

    #[test]
    fn generated_proto_set_excludes_unneeded_api_packages() {
        let out_dir = PathBuf::from(env!("OUT_DIR"));

        assert!(out_dir.join("temporal.api.workflowservice.v1.rs").exists());
        assert!(out_dir
            .join("temporal.server.chasm.lib.blockdevice.proto.v1.rs")
            .exists());

        assert!(
            !out_dir.join("temporal.api.operatorservice.v1.rs").exists(),
            "operatorservice proto should not be generated for phase1 client",
        );
        assert!(
            !out_dir.join("temporal.api.export.v1.rs").exists(),
            "export proto should not be generated for phase1 client",
        );
        assert!(
            !out_dir.join("temporal.api.errordetails.v1.rs").exists(),
            "errordetails proto should not be generated for phase1 client",
        );
    }
}
