use crate::control::protocol::{
    decode_request, read_frame, write_frame, AddDeviceRequest, ControlRequest, ErrorCode,
    Operation, RemoveDeviceRequest, RequestBody, ResponseEnvelope, DEFAULT_MAX_PAYLOAD_BYTES,
};
use crate::errors::TransportError;
use crate::session::{RetryConfig, VolumeSession, VolumeSessionConfig};
use crate::ublk::manager::{ControlManager, RemovePreparation};
use crate::ublk::metrics::UblkMetrics;
use crate::ublk::runtime::{self, DeviceStartConfig, RuntimeErrorKind};
use crate::ublk::signal::wait_for_shutdown_signal;
use anyhow::Context;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{oneshot, Mutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct ServeConfig {
    pub ublk_control_device: PathBuf,
    pub control_socket: PathBuf,
    pub max_devices: usize,
    pub terminal_history_limit: usize,
    pub idempotency_cache_limit: usize,
    pub default_ublk_queues: u16,
    pub default_ublk_queue_depth: u16,
    pub default_ublk_timeout: Duration,
    pub metrics_listen: Option<String>,
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
            ublk_control_device: PathBuf::from(runtime::DEFAULT_CONTROL_DEVICE),
            control_socket: PathBuf::from("/tmp/temporal-ublk.sock"),
            max_devices: 64,
            terminal_history_limit: 256,
            idempotency_cache_limit: 2048,
            default_ublk_queues: 1,
            default_ublk_queue_depth: 128,
            default_ublk_timeout: Duration::from_secs(30),
            metrics_listen: None,
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
    ublk_control_device: PathBuf,
    default_ublk_queues: u16,
    default_ublk_queue_depth: u16,
    default_ublk_timeout: Duration,
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

#[derive(Clone, Copy, Debug)]
struct DeviceRuntimeSettings {
    queues: u16,
    queue_depth: u16,
    timeout: Duration,
}

pub async fn run_serve(config: ServeConfig) -> anyhow::Result<()> {
    #[cfg(not(unix))]
    {
        let _ = config;
        anyhow::bail!("temporal-ublk serve requires a Unix platform");
    }

    #[cfg(unix)]
    {
        runtime::preflight(&config.ublk_control_device)
            .map_err(|err| anyhow::anyhow!(err.message))?;
        runtime::validate_queue_model(config.default_ublk_queues, config.default_ublk_queue_depth)
            .map_err(|err| anyhow::anyhow!(err.message))?;

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
        let metrics = Arc::new(UblkMetrics::default());
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let runtime_config = Arc::new(RuntimeConfig {
            frontend_endpoint: config.frontend_endpoint,
            namespace: config.namespace,
            ublk_control_device: config.ublk_control_device,
            default_ublk_queues: config.default_ublk_queues,
            default_ublk_queue_depth: config.default_ublk_queue_depth,
            default_ublk_timeout: config.default_ublk_timeout,
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
        let metrics_shutdown = CancellationToken::new();

        let mut tasks = JoinSet::new();
        if let Some(metrics_addr) = config.metrics_listen.clone() {
            let metrics = metrics.clone();
            let shutdown = metrics_shutdown.child_token();
            tasks.spawn(async move {
                if let Err(err) = run_metrics_server(metrics_addr, metrics, shutdown).await {
                    log_event(
                        "ERROR",
                        "metrics_server_error",
                        &[("error", err.to_string())],
                    );
                }
            });
        }
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
                    let metrics = metrics.clone();
                    let runtime_devices = runtime_devices.clone();
                    let runtime_config = runtime_config.clone();
                    let mutating_serial = mutating_serial.clone();
                    tasks.spawn(async move {
                        if let Err(err) = handle_connection(
                            stream,
                            manager,
                            metrics,
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

        metrics_shutdown.cancel();
        while let Some(result) = tasks.join_next().await {
            if let Err(err) = result {
                log_event("ERROR", "control_join_error", &[("error", err.to_string())]);
            }
        }

        shutdown_runtime_devices(&runtime_devices, &runtime_config, &metrics).await;
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
    metrics: Arc<UblkMetrics>,
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
                    metrics.clone(),
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
    metrics: Arc<UblkMetrics>,
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
                metrics,
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
                metrics,
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
    metrics: Arc<UblkMetrics>,
    runtime_devices: RuntimeDevices,
    runtime_config: Arc<RuntimeConfig>,
    mutating_serial: Arc<Mutex<()>>,
) -> ResponseEnvelope {
    let _serial_guard = mutating_serial.lock().await;

    let (reservation, runtime_settings) = {
        let mut guard = manager.lock().await;
        if let Some(cached) = guard.lookup_cached_mutating_outcome(
            request_id.clone(),
            &idempotency_key,
            Operation::AddDevice,
            &fingerprint,
        ) {
            return cached;
        }

        let runtime_settings =
            match resolve_runtime_settings(&request_id, &body, runtime_config.as_ref()) {
                Ok(settings) => settings,
                Err(response) => {
                    guard.cache_mutating_outcome(
                        idempotency_key,
                        Operation::AddDevice,
                        fingerprint,
                        &response,
                    );
                    return response;
                }
            };

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
                (reservation, runtime_settings)
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

    let runtime_device = match start_runtime_task(
        reservation.device_id,
        reservation.volume_id.clone(),
        runtime_settings,
        runtime_config.clone(),
    )
    .await
    {
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
                idempotency_key.clone(),
                Operation::AddDevice,
                fingerprint,
                &response,
            );
            metrics.record_add_result(false);
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
            &metrics,
        )
        .await;
    }
    metrics.record_add_result(response.ok);
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
    metrics: Arc<UblkMetrics>,
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
            metrics.record_remove_result(response.ok, response_detached_device(&response));
            return response;
        }
        RemovePreparation::Draining {
            device_id,
            volume_id: _volume_id,
            force,
        } => (device_id, force),
    };

    let timed_out = stop_runtime_device(
        device_id,
        &runtime_devices,
        &runtime_config,
        force_detach,
        &metrics,
    )
    .await;
    if timed_out {
        force_detach = true;
    }

    let mut guard = manager.lock().await;
    let response = guard.complete_remove_device(request_id, device_id, force_detach);
    metrics.record_remove_result(response.ok, response_detached_device(&response));
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
    expected_device_id: u32,
    volume_id: String,
    settings: DeviceRuntimeSettings,
    config: Arc<RuntimeConfig>,
) -> Result<RuntimeDevice, RuntimeStartError> {
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.child_token();
    let (ready_tx, ready_rx) = oneshot::channel();
    let task_config = config.clone();
    let task = tokio::spawn(async move {
        run_runtime_device_task(
            expected_device_id,
            volume_id,
            settings,
            task_config,
            task_shutdown,
            ready_tx,
        )
        .await;
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
    expected_device_id: u32,
    volume_id: String,
    settings: DeviceRuntimeSettings,
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
    let geometry = session.geometry();
    let runtime = match runtime::start_device(DeviceStartConfig {
        control_device: config.ublk_control_device.clone(),
        volume_id: volume_id.clone(),
        requested_device_id: Some(expected_device_id),
        size_bytes: geometry.size_bytes,
        block_size_bytes: geometry.block_size_bytes,
        queues: settings.queues,
        queue_depth: settings.queue_depth,
    })
    .await
    {
        Ok(runtime) => runtime,
        Err(err) => {
            let _ = ready_tx.send(Err(map_runtime_error(&volume_id, err)));
            return;
        }
    };

    if runtime.device_id != expected_device_id {
        let _ = ready_tx.send(Err(RuntimeStartError {
            code: ErrorCode::Internal,
            message: format!(
                "runtime started unexpected device_id '{}' for volume '{}' (expected '{}')",
                runtime.device_id, volume_id, expected_device_id
            ),
        }));
        runtime.request_stop();
        let _ = runtime.wait().await;
        return;
    }

    let _ = ready_tx.send(Ok(()));
    let _session = session;
    shutdown.cancelled().await;
    runtime.request_stop();
    match timeout(settings.timeout, runtime.wait()).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => log_event(
            "ERROR",
            "runtime_shutdown_failed",
            &[
                ("device_id", expected_device_id.to_string()),
                ("volume_id", volume_id),
                ("error", err.to_string()),
            ],
        ),
        Err(_) => {
            log_event(
                "WARN",
                "runtime_wait_timeout",
                &[
                    ("device_id", expected_device_id.to_string()),
                    ("volume_id", volume_id),
                ],
            );
            if let Err(err) = runtime::force_detach_device(expected_device_id).await {
                log_event(
                    "ERROR",
                    "runtime_wait_force_detach_failed",
                    &[
                        ("device_id", expected_device_id.to_string()),
                        ("error", err.to_string()),
                    ],
                );
            }
        }
    }
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

fn map_runtime_error(volume_id: &str, error: runtime::RuntimeError) -> RuntimeStartError {
    let code = match error.kind {
        RuntimeErrorKind::InvalidArgument => ErrorCode::InvalidArgument,
        RuntimeErrorKind::Unavailable => ErrorCode::Unavailable,
        RuntimeErrorKind::Internal => ErrorCode::Internal,
    };
    RuntimeStartError {
        code,
        message: format!(
            "failed to start ublk runtime for volume '{}': {}",
            volume_id, error
        ),
    }
}

async fn stop_runtime_device(
    device_id: u32,
    runtime_devices: &RuntimeDevices,
    runtime_config: &RuntimeConfig,
    force: bool,
    metrics: &Arc<UblkMetrics>,
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
            metrics.record_drain_timeout();
            log_event(
                "WARN",
                "runtime_task_stop_timeout",
                &[
                    ("device_id", device_id.to_string()),
                    ("force", force.to_string()),
                ],
            );
            match runtime::force_detach_device(device_id).await {
                Ok(()) => metrics.record_force_detach(),
                Err(err) => log_event(
                    "ERROR",
                    "runtime_force_detach_failed",
                    &[
                        ("device_id", device_id.to_string()),
                        ("error", err.to_string()),
                    ],
                ),
            }
            true
        }
    }
}

async fn shutdown_runtime_devices(
    runtime_devices: &RuntimeDevices,
    runtime_config: &RuntimeConfig,
    metrics: &Arc<UblkMetrics>,
) {
    let devices = std::mem::take(&mut *runtime_devices.lock().await);
    for (device_id, runtime) in devices {
        runtime.shutdown.cancel();
        if timeout(runtime_config.graceful_drain_timeout, runtime.task)
            .await
            .is_err()
        {
            metrics.record_drain_timeout();
            log_event(
                "WARN",
                "runtime_shutdown_timeout",
                &[("device_id", device_id.to_string())],
            );
            match runtime::force_detach_device(device_id).await {
                Ok(()) => metrics.record_force_detach(),
                Err(err) => log_event(
                    "ERROR",
                    "runtime_shutdown_force_detach_failed",
                    &[
                        ("device_id", device_id.to_string()),
                        ("error", err.to_string()),
                    ],
                ),
            }
        }
    }
}

fn resolve_runtime_settings(
    request_id: &str,
    body: &AddDeviceRequest,
    config: &RuntimeConfig,
) -> Result<DeviceRuntimeSettings, ResponseEnvelope> {
    let mut queues = config.default_ublk_queues;
    let mut queue_depth = config.default_ublk_queue_depth;
    let mut timeout = config.default_ublk_timeout;

    if let Some(overrides) = &body.overrides {
        let Some(map) = overrides.as_object() else {
            return Err(ResponseEnvelope::error(
                request_id.to_string(),
                ErrorCode::InvalidArgument,
                "AddDevice body.overrides must be a JSON object",
            ));
        };

        if let Some(value) = map.get("ublk_queues") {
            let parsed = parse_u64_override(request_id, value, "overrides.ublk_queues")?;
            queues = u16::try_from(parsed).map_err(|_| {
                ResponseEnvelope::error(
                    request_id.to_string(),
                    ErrorCode::InvalidArgument,
                    "overrides.ublk_queues must fit in u16",
                )
            })?;
        }

        if let Some(value) = map.get("ublk_queue_depth") {
            let parsed = parse_u64_override(request_id, value, "overrides.ublk_queue_depth")?;
            queue_depth = u16::try_from(parsed).map_err(|_| {
                ResponseEnvelope::error(
                    request_id.to_string(),
                    ErrorCode::InvalidArgument,
                    "overrides.ublk_queue_depth must fit in u16",
                )
            })?;
        }

        if let Some(value) = map.get("ublk_timeout_secs") {
            let parsed = parse_u64_override(request_id, value, "overrides.ublk_timeout_secs")?;
            timeout = Duration::from_secs(parsed);
        }
    }

    runtime::validate_queue_model(queues, queue_depth).map_err(|err| {
        ResponseEnvelope::error(
            request_id.to_string(),
            ErrorCode::InvalidArgument,
            format!("invalid ublk runtime settings: {}", err.message),
        )
    })?;

    Ok(DeviceRuntimeSettings {
        queues,
        queue_depth,
        timeout,
    })
}

fn parse_u64_override(
    request_id: &str,
    value: &Value,
    field_name: &str,
) -> Result<u64, ResponseEnvelope> {
    value.as_u64().ok_or_else(|| {
        ResponseEnvelope::error(
            request_id.to_string(),
            ErrorCode::InvalidArgument,
            format!("{field_name} must be a positive integer"),
        )
    })
}

fn response_detached_device(response: &ResponseEnvelope) -> bool {
    if !response.ok {
        return false;
    }

    response
        .result
        .as_ref()
        .and_then(|value| value.get("noop"))
        .and_then(Value::as_bool)
        .map(|noop| !noop)
        .unwrap_or(false)
}

async fn run_metrics_server(
    listen: String,
    metrics: Arc<UblkMetrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("failed to bind metrics listener on {listen}"))?;
    log_event("INFO", "metrics_listening", &[("listen", listen)]);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            accept = listener.accept() => {
                let (mut stream, _) = accept.context("metrics listener accept failed")?;
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_metrics_connection(&mut stream, metrics).await {
                        log_event("WARN", "metrics_connection_failed", &[("error", err.to_string())]);
                    }
                });
            }
        }
    }
}

async fn handle_metrics_connection(
    stream: &mut tokio::net::TcpStream,
    metrics: Arc<UblkMetrics>,
) -> anyhow::Result<()> {
    let mut buf = [0_u8; 1024];
    let read = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .context("metrics request read timeout")?
        .context("metrics request read failed")?;
    if read == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buf[..read]);
    let first_line = request.lines().next().unwrap_or_default();
    let path = first_line.split_whitespace().nth(1).unwrap_or("/");
    let (content_type, body) = if path == "/ready" {
        ("text/plain; charset=utf-8", "ready\n".to_string())
    } else {
        (
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.render_prometheus(),
        )
    };

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("metrics response write failed")
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
            ublk_control_device: PathBuf::from(runtime::DEFAULT_CONTROL_DEVICE),
            default_ublk_queues: 1,
            default_ublk_queue_depth: 128,
            default_ublk_timeout: Duration::from_millis(20),
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
        let metrics = Arc::new(UblkMetrics::default());
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager.clone(),
            metrics,
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
        let metrics = Arc::new(UblkMetrics::default());
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager.clone(),
            metrics,
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

    #[tokio::test]
    async fn add_device_rejects_non_object_overrides() {
        let manager = Arc::new(Mutex::new(ControlManager::new(8, 128, 128)));
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let mutating_serial = Arc::new(Mutex::new(()));
        let metrics = Arc::new(UblkMetrics::default());
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager,
            metrics,
            runtime_devices,
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
                "body":{"volume_id":"vol-a","overrides":[]}
            }),
        )
        .await
        .expect("response should parse");

        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|value| &value.code),
            Some(&ErrorCode::InvalidArgument)
        );

        drop(client);
        task.await
            .expect("connection task should join")
            .expect("ok");
    }

    #[tokio::test]
    async fn add_device_rejects_invalid_queue_override() {
        let manager = Arc::new(Mutex::new(ControlManager::new(8, 128, 128)));
        let runtime_devices: RuntimeDevices = Arc::new(Mutex::new(HashMap::new()));
        let mutating_serial = Arc::new(Mutex::new(()));
        let metrics = Arc::new(UblkMetrics::default());
        let (mut client, server) = UnixStream::pair().expect("pair should work");
        let task = tokio::spawn(handle_connection(
            server,
            manager,
            metrics,
            runtime_devices,
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
                "body":{"volume_id":"vol-a","overrides":{"ublk_queues":0}}
            }),
        )
        .await
        .expect("response should parse");

        assert!(!response.ok);
        assert_eq!(
            response.error.as_ref().map(|value| &value.code),
            Some(&ErrorCode::InvalidArgument)
        );

        drop(client);
        task.await
            .expect("connection task should join")
            .expect("ok");
    }
}
