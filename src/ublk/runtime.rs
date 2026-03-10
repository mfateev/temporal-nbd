use anyhow::Context;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use tokio::sync::oneshot;

// Phase 1 runtime keeps lifecycle supervision deterministic without shipping
// full kernel request handling. Device IDs are claimed with local lock files.
pub const DEFAULT_CONTROL_DEVICE: &str = "/dev/ublk-control";
const UBLK_MAX_QUEUE_DEPTH: u16 = 4096;
const UBLK_RUNTIME_STATE_DIR: &str = "/tmp/temporal-ublk/devices";
const UBLK_MAX_DEVICE_ID: u32 = 65_535;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeErrorKind {
    InvalidArgument,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeError {
    pub kind: RuntimeErrorKind,
    pub message: String,
}

impl RuntimeError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            kind: RuntimeErrorKind::InvalidArgument,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: RuntimeErrorKind::Unavailable,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: RuntimeErrorKind::Internal,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Clone, Debug)]
pub struct DeviceStartConfig {
    pub control_device: PathBuf,
    pub volume_id: String,
    pub requested_device_id: Option<u32>,
    pub size_bytes: u64,
    pub block_size_bytes: u32,
    pub queues: u16,
    pub queue_depth: u16,
}

#[derive(Debug)]
pub struct RunningDevice {
    pub device_id: u32,
    pub device_path: String,
    stop_tx: mpsc::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), RuntimeError>>,
}

impl RunningDevice {
    pub fn request_stop(&self) {
        let _ = self.stop_tx.send(());
    }

    pub async fn wait(self) -> Result<(), RuntimeError> {
        self.task
            .await
            .map_err(|err| RuntimeError::internal(format!("ublk runtime task join error: {err}")))?
    }
}

#[derive(Debug)]
struct RuntimeReady {
    device_id: u32,
    device_path: String,
}

pub fn validate_queue_model(queues: u16, queue_depth: u16) -> Result<(), RuntimeError> {
    if queues == 0 {
        return Err(RuntimeError::invalid_argument(
            "ublk_queues must be greater than zero",
        ));
    }
    if queue_depth == 0 {
        return Err(RuntimeError::invalid_argument(
            "ublk_queue_depth must be greater than zero",
        ));
    }
    if queue_depth > UBLK_MAX_QUEUE_DEPTH {
        return Err(RuntimeError::invalid_argument(format!(
            "ublk_queue_depth must be <= {UBLK_MAX_QUEUE_DEPTH}"
        )));
    }
    Ok(())
}

pub fn preflight(control_device: &Path) -> Result<(), RuntimeError> {
    if !cfg!(target_os = "linux") {
        return Err(RuntimeError::invalid_argument(
            "ublk runtime is only supported on Linux hosts",
        ));
    }

    if !control_device.exists() {
        return Err(RuntimeError::unavailable(format!(
            "UBLK control device {} does not exist",
            control_device.display()
        )));
    }

    let module_loaded =
        Path::new("/sys/module/ublk_drv").exists() || Path::new("/sys/module/ublk").exists();
    if !module_loaded {
        return Err(RuntimeError::unavailable(
            "ublk kernel module is not loaded (missing /sys/module/ublk_drv)",
        ));
    }

    OpenOptions::new()
        .read(true)
        .write(true)
        .open(control_device)
        .with_context(|| format!("failed to open {}", control_device.display()))
        .map_err(|err| RuntimeError::unavailable(err.to_string()))?;

    Ok(())
}

pub async fn start_device(config: DeviceStartConfig) -> Result<RunningDevice, RuntimeError> {
    validate_queue_model(config.queues, config.queue_depth)?;
    if config.size_bytes == 0 {
        return Err(RuntimeError::invalid_argument(
            "volume size must be greater than zero",
        ));
    }
    if config.block_size_bytes == 0 {
        return Err(RuntimeError::invalid_argument(
            "block size must be greater than zero",
        ));
    }

    let (ready_tx, ready_rx) = oneshot::channel::<Result<RuntimeReady, RuntimeError>>();
    let (stop_tx, stop_rx) = mpsc::channel();
    let task = tokio::task::spawn_blocking(move || run_device_blocking(config, stop_rx, ready_tx));

    match ready_rx.await {
        Ok(Ok(ready)) => Ok(RunningDevice {
            device_id: ready.device_id,
            device_path: ready.device_path,
            stop_tx,
            task,
        }),
        Ok(Err(err)) => {
            let _ = task.await;
            Err(err)
        }
        Err(_) => {
            let task_outcome = task
                .await
                .map_err(|err| RuntimeError::internal(format!("ublk runtime join error: {err}")))?;
            match task_outcome {
                Ok(()) => Err(RuntimeError::internal(
                    "ublk runtime exited before signaling readiness",
                )),
                Err(err) => Err(err),
            }
        }
    }
}

pub async fn force_detach_device(device_id: u32) -> Result<(), RuntimeError> {
    tokio::task::spawn_blocking(move || {
        let lock = device_lock_path(device_id);
        match std::fs::remove_file(&lock) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(RuntimeError::internal(format!(
                "failed to remove runtime lock {}: {err}",
                lock.display()
            ))),
        }
    })
    .await
    .map_err(|err| RuntimeError::internal(format!("forced detach join error: {err}")))?
}

fn run_device_blocking(
    config: DeviceStartConfig,
    stop_rx: mpsc::Receiver<()>,
    ready_tx: oneshot::Sender<Result<RuntimeReady, RuntimeError>>,
) -> Result<(), RuntimeError> {
    let device_id = claim_device_id(config.requested_device_id, &config.volume_id)?;
    let device_path = format!("/dev/ublkb{device_id}");

    let _ = ready_tx.send(Ok(RuntimeReady {
        device_id,
        device_path,
    }));

    let _ = stop_rx.recv();
    cleanup_device_claim(device_id)?;
    Ok(())
}

fn claim_device_id(requested: Option<u32>, volume_id: &str) -> Result<u32, RuntimeError> {
    std::fs::create_dir_all(UBLK_RUNTIME_STATE_DIR).map_err(|err| {
        RuntimeError::internal(format!(
            "failed to create ublk runtime state dir {}: {err}",
            UBLK_RUNTIME_STATE_DIR
        ))
    })?;

    match requested {
        Some(device_id) => {
            if device_id > UBLK_MAX_DEVICE_ID {
                return Err(RuntimeError::invalid_argument(format!(
                    "requested device_id '{}' is out of supported range (max {})",
                    device_id, UBLK_MAX_DEVICE_ID
                )));
            }
            claim_specific_device(device_id, volume_id)?;
            Ok(device_id)
        }
        None => {
            for device_id in 0..=UBLK_MAX_DEVICE_ID {
                match claim_specific_device(device_id, volume_id) {
                    Ok(()) => return Ok(device_id),
                    Err(err) if err.kind == RuntimeErrorKind::Unavailable => continue,
                    Err(err) => return Err(err),
                }
            }
            Err(RuntimeError::unavailable(
                "no free ublk device id available in local runtime allocator",
            ))
        }
    }
}

fn claim_specific_device(device_id: u32, volume_id: &str) -> Result<(), RuntimeError> {
    let path = device_lock_path(device_id);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| match err.kind() {
            std::io::ErrorKind::AlreadyExists => {
                RuntimeError::unavailable(format!("device_id '{}' is already in use", device_id))
            }
            _ => RuntimeError::internal(format!(
                "failed to claim device_id '{}' via {}: {err}",
                device_id,
                path.display()
            )),
        })?;

    file.write_all(format!("volume_id={volume_id}\n").as_bytes())
        .map_err(|err| RuntimeError::internal(format!("failed to write {}: {err}", path.display())))
}

fn cleanup_device_claim(device_id: u32) -> Result<(), RuntimeError> {
    let path = device_lock_path(device_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(RuntimeError::internal(format!(
            "failed to release device_id '{}' at {}: {err}",
            device_id,
            path.display()
        ))),
    }
}

fn device_lock_path(device_id: u32) -> PathBuf {
    Path::new(UBLK_RUNTIME_STATE_DIR).join(format!("ublkb{device_id}.lock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_queue_model_rejects_zero() {
        assert!(validate_queue_model(0, 128).is_err());
        assert!(validate_queue_model(1, 0).is_err());
    }

    #[test]
    fn validate_queue_model_rejects_too_deep() {
        assert!(validate_queue_model(1, UBLK_MAX_QUEUE_DEPTH + 1).is_err());
    }

    #[test]
    fn lock_path_uses_expected_layout() {
        let path = device_lock_path(7);
        assert!(path.ends_with("ublkb7.lock"));
    }
}
