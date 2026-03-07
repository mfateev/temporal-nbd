use anyhow::{anyhow, bail, Context};
use prost_types::Duration as ProstDuration;
use std::env;
use std::fs::{self, File};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use temporal_nbd::workflowservicepb::{
    workflow_service_client::WorkflowServiceClient, DescribeNamespaceRequest,
    RegisterNamespaceRequest,
};
use temporal_nbd::{run_phase1_create_volume_smoke, SmokeConfig};
use tokio::time::{sleep, timeout};
use tonic::transport::Endpoint;
use tonic::Code;
use uuid::Uuid;

#[tokio::test]
#[ignore = "builds and runs source temporal-server with development-sqlite"]
async fn phase1_e2e_sqlite_source_server() -> anyhow::Result<()> {
    timeout(Duration::from_secs(240), run_phase1_e2e())
        .await
        .context("phase1 e2e test timed out")?
}

async fn run_phase1_e2e() -> anyhow::Result<()> {
    let temporal_repo = temporal_repo_root()?;

    let frontend_endpoint =
        env::var("TEMPORAL_FRONTEND_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7233".to_string());
    let history_endpoint =
        env::var("TEMPORAL_HISTORY_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7234".to_string());
    let server_env =
        env::var("TEMPORAL_SERVER_ENV").unwrap_or_else(|_| "development-sqlite".to_string());

    ensure_endpoint_free(&frontend_endpoint, "frontend")?;
    ensure_endpoint_free(&history_endpoint, "history")?;

    let run_suffix = Uuid::new_v4().simple().to_string();
    let server_bin = env::temp_dir().join(format!("temporal-server-rust-e2e-{run_suffix}"));
    let server_log = env::temp_dir().join(format!("temporal-server-rust-e2e-{run_suffix}.log"));

    build_temporal_server(&temporal_repo, &server_bin)?;

    let mut server = ServerProcess::start(&server_bin, &server_log, &server_env, &temporal_repo)?;

    wait_for_endpoint(
        &mut server,
        &frontend_endpoint,
        "frontend",
        Duration::from_secs(45),
    )
    .await?;
    wait_for_endpoint(
        &mut server,
        &history_endpoint,
        "history",
        Duration::from_secs(45),
    )
    .await?;

    let namespace_name = format!("blockdevice-rust-e2e-{run_suffix}");
    let namespace_id = register_and_describe_namespace(&frontend_endpoint, &namespace_name).await?;

    let volume_id = format!("phase1-e2e-sqlite-{run_suffix}");
    let volume_id_file = env::temp_dir().join(format!("phase1-e2e-volume-id-{run_suffix}.txt"));

    let config = SmokeConfig {
        history_endpoint,
        namespace_id,
        volume_id: volume_id.clone(),
        size_bytes: 1 << 30,
        block_size_bytes: 0,
        volume_id_file: volume_id_file.clone(),
        connect_timeout: Duration::from_secs(3),
        rpc_timeout: Duration::from_secs(5),
    };

    run_phase1_create_volume_smoke(&config)
        .await
        .context("phase1 create-volume smoke flow failed")?;

    let persisted = fs::read_to_string(&volume_id_file)
        .with_context(|| format!("failed to read {}", volume_id_file.display()))?;
    if persisted.trim() != volume_id {
        bail!(
            "persisted volume id mismatch: got {}, want {}",
            persisted.trim(),
            volume_id
        );
    }

    let _ = fs::remove_file(&volume_id_file);
    let _ = fs::remove_file(&server_bin);
    let _ = fs::remove_file(&server_log);
    Ok(())
}

fn temporal_repo_root() -> anyhow::Result<PathBuf> {
    if let Ok(path) = env::var("TEMPORAL_REPO") {
        let root = PathBuf::from(path);
        if root.exists() {
            return Ok(root);
        }
        bail!("TEMPORAL_REPO points to a non-existent path");
    }

    let default = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../temporal");
    if default.exists() {
        return Ok(default);
    }

    bail!("Temporal repo not found; set TEMPORAL_REPO to a Temporal checkout")
}

fn build_temporal_server(temporal_repo: &Path, output_bin: &Path) -> anyhow::Result<()> {
    let go_bin = resolve_go_bin();
    run_command(
        temporal_server_build_command(&go_bin, temporal_repo, output_bin),
        "build temporal-server",
    )
}

fn resolve_go_bin() -> String {
    env::var("GO_BIN").unwrap_or_else(|_| {
        if Path::new("/usr/local/go/bin/go").exists() {
            "/usr/local/go/bin/go".to_string()
        } else {
            "go".to_string()
        }
    })
}

fn temporal_server_build_command(go_bin: &str, temporal_repo: &Path, output_bin: &Path) -> Command {
    let mut cmd = Command::new(go_bin);
    cmd.arg("build")
        .arg("-tags")
        .arg("disable_grpc_modules")
        .arg("-o")
        .arg(output_bin)
        .arg("./cmd/server")
        .current_dir(temporal_repo);
    cmd
}

fn run_command(mut command: Command, context_msg: &str) -> anyhow::Result<()> {
    let output = command
        .output()
        .with_context(|| format!("failed to execute command: {context_msg}"))?;

    if output.status.success() {
        return Ok(());
    }

    bail!(
        "{context_msg} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn ensure_endpoint_free(endpoint: &str, label: &str) -> anyhow::Result<()> {
    if endpoint_open(endpoint, Duration::from_millis(250))? {
        bail!("{label} endpoint is already in use ({endpoint}); stop conflicting process first");
    }
    Ok(())
}

async fn wait_for_endpoint(
    server: &mut ServerProcess,
    endpoint: &str,
    label: &str,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout_after;
    while Instant::now() < deadline {
        if endpoint_open(endpoint, Duration::from_millis(250))? {
            return Ok(());
        }
        server.ensure_running()?;
        sleep(Duration::from_millis(250)).await;
    }

    bail!(
        "{label} endpoint {endpoint} did not become ready within {}s\nlog tail:\n{}",
        timeout_after.as_secs(),
        server.log_tail(120)
    )
}

fn endpoint_open(endpoint: &str, connect_timeout: Duration) -> anyhow::Result<bool> {
    let addr = resolve_endpoint(endpoint)?;
    Ok(TcpStream::connect_timeout(&addr, connect_timeout).is_ok())
}

fn resolve_endpoint(endpoint: &str) -> anyhow::Result<SocketAddr> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    without_scheme
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve endpoint {endpoint}"))?
        .next()
        .ok_or_else(|| anyhow!("endpoint {endpoint} resolved to no addresses"))
}

fn normalize_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

async fn register_and_describe_namespace(
    frontend_endpoint: &str,
    namespace_name: &str,
) -> anyhow::Result<String> {
    let endpoint_url = normalize_endpoint(frontend_endpoint);
    let endpoint = Endpoint::from_shared(endpoint_url.clone())
        .with_context(|| format!("invalid frontend endpoint: {endpoint_url}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5));

    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect to frontend endpoint: {endpoint_url}"))?;

    let mut client = WorkflowServiceClient::new(channel);

    let register_request = RegisterNamespaceRequest {
        namespace: namespace_name.to_string(),
        workflow_execution_retention_period: Some(ProstDuration {
            seconds: 24 * 60 * 60,
            nanos: 0,
        }),
        ..Default::default()
    };

    match client.register_namespace(register_request).await {
        Ok(_) => {}
        Err(status) if status.code() == Code::AlreadyExists => {}
        Err(status) => {
            bail!(
                "register namespace failed: {} ({})",
                status.code(),
                status.message()
            )
        }
    }

    let describe_response = client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: namespace_name.to_string(),
            id: String::new(),
        })
        .await
        .with_context(|| format!("describe namespace failed for {namespace_name}"))?
        .into_inner();

    let namespace_id = describe_response
        .namespace_info
        .map(|info| info.id)
        .unwrap_or_default();

    if namespace_id.is_empty() {
        bail!("describe namespace returned empty namespace id");
    }

    Ok(namespace_id)
}

struct ServerProcess {
    child: Child,
    log_path: PathBuf,
}

impl ServerProcess {
    fn start(
        server_bin: &Path,
        log_path: &Path,
        server_env: &str,
        working_dir: &Path,
    ) -> anyhow::Result<Self> {
        let stdout = File::create(log_path)
            .with_context(|| format!("failed to create server log file {}", log_path.display()))?;
        let stderr = stdout
            .try_clone()
            .with_context(|| format!("failed to clone log file handle {}", log_path.display()))?;

        let child = Command::new(server_bin)
            .arg("--env")
            .arg(server_env)
            .arg("--allow-no-auth")
            .arg("start")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .current_dir(working_dir)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start temporal-server binary {}",
                    server_bin.display()
                )
            })?;

        Ok(Self {
            child,
            log_path: log_path.to_path_buf(),
        })
    }

    fn ensure_running(&mut self) -> anyhow::Result<()> {
        if let Some(status) = self
            .child
            .try_wait()
            .context("failed to check temporal-server process status")?
        {
            bail!(
                "temporal-server exited early with status {status}\nlog tail:\n{}",
                self.log_tail(120)
            );
        }
        Ok(())
    }

    fn log_tail(&self, max_lines: usize) -> String {
        let content = match fs::read_to_string(&self.log_path) {
            Ok(content) => content,
            Err(err) => {
                return format!(
                    "failed to read server log {}: {err}",
                    self.log_path.display()
                )
            }
        };

        let lines: Vec<&str> = content.lines().collect();
        let start = lines.len().saturating_sub(max_lines);
        lines[start..].join("\n")
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            Err(_) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn temporal_server_build_command_avoids_modfile_and_api_rewrites() {
        let cmd = temporal_server_build_command(
            "go",
            Path::new("/tmp/temporal"),
            Path::new("/tmp/temporal-server"),
        );

        assert_eq!(cmd.get_program(), OsStr::new("go"));

        let args: Vec<String> = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args.first().map(String::as_str), Some("build"));
        assert!(args.iter().any(|arg| arg == "./cmd/server"));
        assert!(args.iter().all(|arg| !arg.contains("-modfile")));
        assert!(args
            .iter()
            .all(|arg| !arg.starts_with("go.temporal.io/api@")));

        assert!(cmd.get_envs().all(|(key, _)| key != OsStr::new("GOFLAGS")));
    }
}
