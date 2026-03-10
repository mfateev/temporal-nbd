use crate::control::protocol::{
    decode_request, read_frame, write_frame, AddDeviceRequest, ControlRequest, ErrorCode,
    Operation, RemoveDeviceRequest, RequestBody, ResponseEnvelope, DEFAULT_MAX_PAYLOAD_BYTES,
};
use crate::errors::TransportError;
use crate::session::{RetryConfig, VolumeSession, VolumeSessionConfig};
use crate::ublk::manager::{ControlManager, RemovePreparation};
use crate::ublk::signal::wait_for_shutdown_signal;
use anyhow::Context;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, Mutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct ServeConfig {
    pub control_socket: PathBuf,
    pub max_devices: usize,
    pub terminal_history_limit: usize,
    pub idempotency_cache_limit: usize,
    pub ready_file: Option<PathBuf>,
    pub frontend_endpoint: String,
    pub namespace: String,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub retry_max_attempts: usize,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub retry_jitter_ratio: f64,
    pub graceful_drain_timeout: Duration,
    pub force_detach_timeout: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            control_socket: PathBuf::from("/tmp/temporal-ublk.sock"),
            max_devices: 64,
            terminal_history_limit: 256,
            idempotency_cache_limit: 2048,
            ready_file: None,
            frontend_endpoint: "127.0.0.1:7233".to_string(),
            namespace: "default".to_string(),
            connect_timeout: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(5),
            retry_max_attempts: 8,
            retry_initial_backoff: Duration::from_millis(150),
            retry_max_backoff: Duration::from_millis(2000),
            retry_jitter_ratio: 0.2,
            graceful_drain_timeout: Duration::from_secs(20),
            force_detach_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeConfig {
    frontend_endpoint: String,
    namespace: String,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    retry_max_attempts: usize,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    retry_jitter_ratio: f64,
    graceful_drain_timeout: Duration,
    force_detach_timeout: Duration,
}

#[derive(Debug)]
struct RuntimeDevice {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

type RuntimeDevices = Arc<Mutex<HashMap<u32, RuntimeDevice>>>;

pub async fn run_serve(config: ServeConfig) -> anyhow::Result<()> {
    #[cfg(not(unix))]
    {
        let _ = config;
        anyhow::bail!("temporal-ublk serve requires a Unix platform");
    }

    #[cfg(unix)]
    {
        prepare_socket_path(&config.control_socket)?;

        let listener = UnixListener::bind(&config.control_socket).with_context(|| {
            format!(
                "failed to bind control socket at {}",
                config.control_socket.display()
            )
        })?;

        if let Some(path) = &config.ready_file {
            write_ready_file(path)?;
        }

        log_event(
            "INFO",
            "manager_listening",
            &[(
                "control_socket",
                config.control_socket.display().to_string(),
            )],
        );

        let manager = Arc::new(Mutex::new(ControlManager::new(
            config.max_devices,
            config.terminal_history_limit,
            config.idempotency_cache_limit,
        )));
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let runtime_config = Arc::new(RuntimeConfig {
            frontend_endpoint: config.frontend_endpoint,
            namespace: config.namespace,
            connect_timeout: config.connect_timeout,
            rpc_timeout: config.rpc_timeout,
            retry_max_attempts: config.retry_max_attempts,
            retry_initial_backoff: config.retry_initial_backoff,
            retry_max_backoff: config.retry_max_backoff,
            retry_jitter_ratio: config.retry_jitter_ratio,
            graceful_drain_timeout: config.graceful_drain_timeout,
            force_detach_timeout: config.force_detach_timeout,
        });
        let mutating_serial = Arc::new(Mutex::new(()));

        let mut tasks = JoinSet::new();
        let shutdown_signal = wait_for_shutdown_signal();
        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, _addr) = match accept {
                        Ok(values) => values,
                        Err(err) => {
                            log_event("ERROR", "control_accept_failed", &[("error", err.to_string())]);
                            continue;
                        }
                    };
                    let manager = manager.clone();
                    let runtime_devices = runtime_devices.clone();
                    let runtime_config = runtime_config.clone();
                    let mutating_serial = mutating_serial.clone();
                    tasks.spawn(async move {
                        if let Err(err) = handle_connection(
                            stream,
                            manager,
                            runtime_devices,
                            runtime_config,
                            mutating_serial,
                        )
                        .await
                        {
                            log_event("ERROR", "control_connection_error", &[("error", err.to_string())]);
                        }
                    });
                }
                signal_name = &mut shutdown_signal => {
                    let signal_name = signal_name?;
                    log_event("INFO", "manager_shutdown_signal", &[("signal", signal_name.to_string())]);
                    break;
                }
            }
        }

        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                log_event("ERROR", "control_join_error", &[("error", err.to_string())]);
            }
        }

        shutdown_runtime_devices(&runtime_devices, &runtime_config).await;
        drop(listener);
        cleanup_path_if_exists(&config.control_socket)?;
        if let Some(path) = &config.ready_file {
            cleanup_path_if_exists(path)?;
        }
        Ok(())
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    manager: Arc<Mutex<ControlManager>>,
    runtime_devices: RuntimeDevices,
    runtime_config: Arc<RuntimeConfig>,
    mutating_serial: Arc<Mutex<()>>,
) -> anyhow::Result<()> {
    loop {
        let payload = match read_frame(&mut stream, DEFAULT_MAX_PAYLOAD_BYTES).await {
            Ok(Some(payload)) => payload,
            Ok(None) => return Ok(()),
            Err(err) => {
                return Err(anyhow::anyhow!("frame read failed: {err}"));
            }
        };

        let response = match decode_request(&payload) {
            Ok(request) => {
                process_request(
                    request,
                    manager.clone(),
                    runtime_devices.clone(),
                    runtime_config.clone(),
                    mutating_serial.clone(),
                )
                .await
            }
            Err(err) => ResponseEnvelope::from_protocol_error(err),
        };

        let response_payload = serde_json::to_vec(&response)
            .context("failed to serialize control response as JSON")?;
        write_frame(&mut stream, &response_payload)
            .await
            .context("failed to write control response frame")?;
    }
}

async fn process_request(
    request: ControlRequest,
    manager: Arc<Mutex<ControlManager>>,
    runtime_devices: RuntimeDevices,
    runtime_config: Arc<RuntimeConfig>,
    mutating_serial: Arc<Mutex<()>>,
) -> ResponseEnvelope {
    let request_id = request.request_id.clone();
    let idempotency_key = request.idempotency_key.clone().unwrap_or_default();
    let fingerprint = request.body_fingerprint().unwrap_or_default();

    match request.body {
        RequestBody::AddDevice(body) => {
            process_add_device(
                request_id,
                idempotency_key,
                fingerprint,
                body,
                manager,
                runtime_devices,
                runtime_config,
                mutating_serial,
            )
            .await
        }
        RequestBody::RemoveDevice(body) => {
            process_remove_device(
                request_id,
                idempotency_key,
                fingerprint,
                body,
                manager,
                runtime_devices,
                runtime_config,
                mutating_serial,
            )
            .await
        }
        RequestBody::ListDevices(body) => {
            let guard = manager.lock().await;
            guard.list_devices(request_id, body)
        }
        RequestBody::Health(_) => {
            let guard = manager.lock().await;
            guard.health(request_id)
        }
    }
}

async fn process_add_device(
    request_id: String,
    idempotency_key: String,
    fingerprint: Vec<u8>,
    body: AddDeviceRequest,
    manager: Arc<Mutex<ControlManager>>,
    runtime_devices: RuntimeDevices,
    runtime_config: Arc<RuntimeConfig>,
    mutating_serial: Arc<Mutex<()>>,
) -> ResponseEnvelope {
    let _serial_guard = mutating_serial.lock().await;

    let reservation = {
        let mut guard = manager.lock().await;
        if let Some(cached) = guard.lookup_cached_mutating_outcome(
            request_id.clone(),
            &idempotency_key,
            Operation::AddDevice,
            &fingerprint,
        ) {
            return cached;
        }

        match guard.reserve_add_device(request_id.clone(), &body) {
            Ok(reservation) => {
                if let Err(response) = guard.mark_device_opening(reservation.device_id) {
                    guard.cache_mutating_outcome(
                        idempotency_key,
                        Operation::AddDevice,
                        fingerprint,
                        &response,
                    );
                    return response;
                }
                reservation
            }
            Err(response) => {
                guard.cache_mutating_outcome(
                    idempotency_key,
                    Operation::AddDevice,
                    fingerprint,
                    &response,
                );
                return response;
            }
        }
    };

    let runtime_device =
        match start_runtime_task(reservation.volume_id.clone(), runtime_config.clone()).await {
            Ok(device) => device,
            Err(err) => {
                let mut guard = manager.lock().await;
                let response = guard.fail_add_device(
                    request_id.clone(),
                    reservation.device_id,
                    err.code,
                    err.message,
                );
                guard.cache_mutating_outcome(
                    idempotency_key,
                    Operation::AddDevice,
                    fingerprint,
                    &response,
                );
                return response;
            }
        };

    runtime_devices
        .lock()
        .await
        .insert(reservation.device_id, runtime_device);

    let mut guard = manager.lock().await;
    let response = guard.finalize_add_success(request_id.clone(), reservation.device_id);
    if !response.ok {
        let _ = stop_runtime_device(
            reservation.device_id,
            &runtime_devices,
            &runtime_config,
            true,
        )
        .await;
    }
    guard.cache_mutating_outcome(
        idempotency_key,
        Operation::AddDevice,
        fingerprint,
        &response,
    );
    response
}

async fn process_remove_device(
    request_id: String,
    idempotency_key: String,
    fingerprint: Vec<u8>,
    body: RemoveDeviceRequest,
    manager: Arc<Mutex<ControlManager>>,
    runtime_devices: RuntimeDevices,
    runtime_config: Arc<RuntimeConfig>,
    mutating_serial: Arc<Mutex<()>>,
) -> ResponseEnvelope {
    let _serial_guard = mutating_serial.lock().await;

    let prep = {
        let mut guard = manager.lock().await;
        if let Some(cached) = guard.lookup_cached_mutating_outcome(
            request_id.clone(),
            &idempotency_key,
            Operation::RemoveDevice,
            &fingerprint,
        ) {
            return cached;
        }
        guard.prepare_remove_device(request_id.clone(), &body)
    };

    let (device_id, mut force_detach) = match prep {
        RemovePreparation::Immediate(response) => {
            let mut guard = manager.lock().await;
            guard.cache_mutating_outcome(
                idempotency_key,
                Operation::RemoveDevice,
                fingerprint,
                &response,
            );
            return response;
        }
        RemovePreparation::Draining {
            device_id,
            volume_id: _volume_id,
            force,
        } => (device_id, force),
    };

    let timed_out =
        stop_runtime_device(device_id, &runtime_devices, &runtime_config, force_detach).await;
    if timed_out {
        force_detach = true;
    }

    let mut guard = manager.lock().await;
    let response = guard.complete_remove_device(request_id, device_id, force_detach);
    guard.cache_mutating_outcome(
        idempotency_key,
        Operation::RemoveDevice,
        fingerprint,
        &response,
    );
    response
}

struct RuntimeStartError {
    code: ErrorCode,
    message: String,
}

async fn start_runtime_task(
    volume_id: String,
    config: Arc<RuntimeConfig>,
) -> Result<RuntimeDevice, RuntimeStartError> {
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.child_token();
    let (ready_tx, ready_rx) = oneshot::channel();
    let task_config = config.clone();
    let task = tokio::spawn(async move {
        run_runtime_device_task(volume_id, task_config, task_shutdown, ready_tx).await;
    });

    match ready_rx.await {
        Ok(Ok(())) => Ok(RuntimeDevice { shutdown, task }),
        Ok(Err(err)) => {
            let _ = task.await;
            Err(err)
        }
        Err(err) => {
            let _ = task.await;
            Err(RuntimeStartError {
                code: ErrorCode::Internal,
                message: format!("runtime startup channel dropped: {err}"),
            })
        }
    }
}

async fn run_runtime_device_task(
    volume_id: String,
    config: Arc<RuntimeConfig>,
    shutdown: CancellationToken,
    ready_tx: oneshot::Sender<Result<(), RuntimeStartError>>,
) {
    let session_config = build_session_config(&config, &volume_id);
    let open = VolumeSession::connect_and_open(session_config).await;
    let session = match open {
        Ok(session) => session,
        Err(err) => {
            let _ = ready_tx.send(Err(map_open_error(&volume_id, err)));
            return;
        }
    };

    let _ = ready_tx.send(Ok(()));
    let _session = session;
    shutdown.cancelled().await;
}

fn build_session_config(config: &RuntimeConfig, volume_id: &str) -> VolumeSessionConfig {
    VolumeSessionConfig {
        frontend_endpoint: config.frontend_endpoint.clone(),
        namespace: config.namespace.clone(),
        volume_id: volume_id.to_string(),
        connect_timeout: config.connect_timeout,
        rpc_timeout: config.rpc_timeout,
        retry: RetryConfig {
            max_attempts: config.retry_max_attempts,
            initial_backoff: config.retry_initial_backoff,
            max_backoff: config.retry_max_backoff,
            jitter_ratio: config.retry_jitter_ratio,
        },
    }
}

fn map_open_error(volume_id: &str, error: TransportError) -> RuntimeStartError {
    match error {
        TransportError::Retryable { message } => RuntimeStartError {
            code: ErrorCode::Unavailable,
            message: format!(
                "failed to open volume '{}' (retryable): {}",
                volume_id, message
            ),
        },
        TransportError::Terminal { message } => RuntimeStartError {
            code: ErrorCode::Internal,
            message: format!(
                "failed to open volume '{}' (terminal): {}",
                volume_id, message
            ),
        },
    }
}

async fn stop_runtime_device(
    device_id: u32,
    runtime_devices: &RuntimeDevices,
    runtime_config: &RuntimeConfig,
    force: bool,
) -> bool {
    let runtime = runtime_devices.lock().await.remove(&device_id);
    let Some(runtime) = runtime else {
        return false;
    };

    runtime.shutdown.cancel();
    let wait_duration = if force {
        runtime_config.force_detach_timeout
    } else {
        runtime_config.graceful_drain_timeout
    };
    match timeout(wait_duration, runtime.task).await {
        Ok(join_result) => {
            if let Err(err) = join_result {
                log_event(
                    "ERROR",
                    "runtime_task_join_error",
                    &[
                        ("device_id", device_id.to_string()),
                        ("error", err.to_string()),
                    ],
                );
            }
            false
        }
        Err(_) => {
            log_event(
                "WARN",
                "runtime_task_stop_timeout",
                &[
                    ("device_id", device_id.to_string()),
                    ("force", force.to_string()),
                ],
            );
            true
        }
    }
}

async fn shutdown_runtime_devices(
    runtime_devices: &RuntimeDevices,
    runtime_config: &RuntimeConfig,
) {
    let devices = std::mem::take(&mut *runtime_devices.lock().await);
    for (device_id, runtime) in devices {
        runtime.shutdown.cancel();
        if timeout(runtime_config.graceful_drain_timeout, runtime.task)
            .await
            .is_err()
        {
            log_event(
                "WARN",
                "runtime_shutdown_timeout",
                &[("device_id", device_id.to_string())],
            );
        }
    }
}

#[cfg(unix)]
fn prepare_socket_path(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create control socket parent directory {}",
                parent.display()
            )
        })?;
    }
    cleanup_path_if_exists(path)
}

fn cleanup_path_if_exists(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

fn write_ready_file(path: &Path) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create ready-file parent {}", parent.display()))?;
    }
    std::fs::write(path, b"ready\n")
        .with_context(|| format!("failed to write ready file {}", path.display()))
}

fn log_event(level: &str, event: &str, fields: &[(&str, String)]) {
    let mut parts = Vec::with_capacity(fields.len() + 2);
    parts.push(format!("level={level}"));
    parts.push(format!("event={event}"));
    for (key, value) in fields {
        parts.push(format!("{key}={value}"));
    }
    eprintln!("{}", parts.join(" "));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::protocol::{read_frame, write_frame};
    use serde_json::json;

    async fn request(
        stream: &mut UnixStream,
        request: serde_json::Value,
    ) -> anyhow::Result<ResponseEnvelope> {
        let payload = serde_json::to_vec(&request).context("serialize request JSON")?;
        write_frame(stream, &payload)
            .await
            .context("write request frame")?;
        let response_payload = read_frame(stream, DEFAULT_MAX_PAYLOAD_BYTES)
            .await
            .context("read response frame")?
            .context("response EOF")?;
        serde_json::from_slice(&response_payload).context("decode response envelope")
    }

    fn test_runtime_config() -> Arc<RuntimeConfig> {
        Arc::new(RuntimeConfig {
            frontend_endpoint: "127.0.0.1:1".to_string(),
            namespace: "default".to_string(),
            connect_timeout: Duration::from_millis(20),
            rpc_timeout: Duration::from_millis(20),
            retry_max_attempts: 1,
            retry_initial_backoff: Duration::from_millis(5),
            retry_max_backoff: Duration::from_millis(5),
            retry_jitter_ratio: 0.0,
            graceful_drain_timeout: Duration::from_millis(20),
            force_detach_timeout: Duration::from_millis(20),
        })
    }

    #[tokio::test]
    async fn add_device_returns_unavailable_when_open_volume_fails() {
        let manager = Arc::new(Mutex::new(ControlManager::new(8, 128, 128)));
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let mutating_serial = Arc::new(Mutex::new(()));
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager.clone(),
            runtime_devices.clone(),
            test_runtime_config(),
            mutating_serial,
        ));

        let response = request(
            &mut client,
            json!({
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }),
        )
        .await
        .expect("response should parse");
        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|value| &value.code),
            Some(&ErrorCode::Unavailable)
        );

        drop(client);
        task.await
            .expect("connection task should join")
            .expect("ok");
    }

    #[tokio::test]
    async fn add_device_failure_is_idempotent() {
        let manager = Arc::new(Mutex::new(ControlManager::new(8, 128, 128)));
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let mutating_serial = Arc::new(Mutex::new(()));
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager.clone(),
            runtime_devices.clone(),
            test_runtime_config(),
            mutating_serial,
        ));

        let first = request(
            &mut client,
            json!({
                "version":"v1",
                "request_id":"req-1",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }),
        )
        .await
        .expect("first response should parse");
        let second = request(
            &mut client,
            json!({
                "version":"v1",
                "request_id":"req-2",
                "idempotency_key":"key-1",
                "op":"AddDevice",
                "body":{"volume_id":"vol-a"}
            }),
        )
        .await
        .expect("second response should parse");

        assert!(!first.ok);
        assert!(!second.ok);
        assert_eq!(first.error, second.error);

        drop(client);
        task.await
            .expect("connection task should join")
            .expect("ok");
    }
}
