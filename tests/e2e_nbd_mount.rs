use anyhow::{bail, Context};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use temporal_nbd::{connect_workflow_client, create_volume, SmokeConfig};
use tonic::Code;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires Linux NBD device + root privileges + running Temporal frontend"]
async fn phase2_e2e_nbd_attach_mount_roundtrip() -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(420),
        run_phase2_e2e_nbd_attach_mount_roundtrip(),
    )
    .await
    .context("nbd mount e2e test timed out")?
}

async fn run_phase2_e2e_nbd_attach_mount_roundtrip() -> anyhow::Result<()> {
    if !cfg!(target_os = "linux") {
        eprintln!("Skipping nbd mount test: Linux required");
        return Ok(());
    }

    let frontend_endpoint =
        env::var("TEMPORAL_FRONTEND_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7233".to_string());
    let namespace = env::var("TEMPORAL_NAMESPACE")
        .context("TEMPORAL_NAMESPACE is required for nbd mount e2e test")?;
    let volume_id = env::var("TEMPORAL_VOLUME_ID")
        .unwrap_or_else(|_| format!("phaseb-nbd-{}", Uuid::new_v4().simple()));
    let device_path =
        PathBuf::from(env::var("TEMPORAL_NBD_DEVICE").unwrap_or_else(|_| "/dev/nbd0".to_string()));

    if !Path::new("/sys/module/nbd").exists() {
        bail!("nbd kernel module is missing (/sys/module/nbd)");
    }
    if !device_path.exists() {
        bail!("NBD device {} does not exist", device_path.display());
    }

    let device_name = device_path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow::anyhow!("invalid NBD device path {}", device_path.display()))?
        .to_string();
    if nbd_device_pid(&device_name)?.is_some() {
        bail!(
            "NBD device {} is already attached; choose a free device with TEMPORAL_NBD_DEVICE",
            device_path.display()
        );
    }

    let privilege = PrivilegeMode::detect()?;
    assert_program_exists_for_mode(&privilege, "mkfs.ext4")?;
    assert_program_exists_for_mode(&privilege, "mount")?;
    assert_program_exists_for_mode(&privilege, "umount")?;
    assert_program_exists_for_mode(&privilege, "blockdev")?;

    let volume_size_bytes = env::var("TEMPORAL_VOLUME_SIZE_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(256 * 1024 * 1024);

    let create_config = SmokeConfig {
        frontend_endpoint: frontend_endpoint.clone(),
        namespace: namespace.clone(),
        volume_id: volume_id.clone(),
        size_bytes: volume_size_bytes,
        block_size_bytes: 4096,
        volume_id_file: env::temp_dir()
            .join(format!("phaseb-nbd-volume-{}.txt", Uuid::new_v4().simple())),
        connect_timeout: Duration::from_secs(3),
        rpc_timeout: Duration::from_secs(8),
    };
    ensure_volume_exists(&create_config).await?;

    let mount_dir = env::var("TEMPORAL_NBD_MOUNT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            env::temp_dir().join(format!("temporal-nbd-mount-{}", Uuid::new_v4().simple()))
        });
    fs::create_dir_all(&mount_dir)
        .with_context(|| format!("failed to create mount directory {}", mount_dir.display()))?;

    let run_suffix = Uuid::new_v4().simple().to_string();
    let attach_log = env::temp_dir().join(format!("temporal-nbd-attach-{run_suffix}.log"));
    let mut attach_child = spawn_attach_process(
        &privilege,
        &attach_log,
        &frontend_endpoint,
        &namespace,
        &volume_id,
        &device_path,
    )?;

    let mut mounted = false;
    let result = (|| -> anyhow::Result<()> {
        wait_for_device_ready(
            &privilege,
            &device_path,
            &mut attach_child,
            &attach_log,
            Duration::from_secs(30),
        )?;

        let mkfs_result = run_privileged_command(
            &privilege,
            "mkfs.ext4",
            |cmd| {
                cmd.arg("-F").arg(&device_path);
            },
            "mkfs.ext4 on attached nbd device",
        )?;
        eprintln!(
            "mkfs.ext4 output:\n{}",
            String::from_utf8_lossy(&mkfs_result.stdout)
        );

        mount_device(&privilege, &device_path, &mount_dir)?;
        mounted = true;

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        run_privileged_command(
            &privilege,
            "chown",
            |cmd| {
                cmd.arg(format!("{uid}:{gid}")).arg(&mount_dir);
            },
            "chown mount root for test user",
        )?;

        let payload_path = mount_dir.join("roundtrip.bin");
        let payload = deterministic_payload(512 * 1024);
        fs::write(&payload_path, &payload)
            .with_context(|| format!("failed to write payload to {}", payload_path.display()))?;

        run_privileged_command(
            &privilege,
            "sync",
            |_| {},
            "sync filesystem data before unmount",
        )?;

        unmount_device(&privilege, &mount_dir)?;
        mounted = false;

        mount_device(&privilege, &device_path, &mount_dir)?;
        mounted = true;

        let roundtrip = fs::read(&payload_path)
            .with_context(|| format!("failed to read payload from {}", payload_path.display()))?;
        if roundtrip != payload {
            bail!(
                "payload mismatch after unmount/remount roundtrip ({} bytes)",
                payload.len()
            );
        }

        unmount_device(&privilege, &mount_dir)?;
        mounted = false;

        Ok(())
    })();

    if mounted {
        let _ = unmount_device(&privilege, &mount_dir);
    }

    let stop_result = stop_attach_process(&mut attach_child, &attach_log, Duration::from_secs(15));
    let _ = fs::remove_dir_all(&mount_dir);

    match (result, stop_result) {
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Ok(_), Ok(_)) => Ok(()),
    }
}

fn deterministic_payload(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        out.push(((i * 31) % 251) as u8);
    }
    out
}

async fn ensure_volume_exists(config: &SmokeConfig) -> anyhow::Result<()> {
    let mut client = connect_workflow_client(config).await?;
    let create_result = create_volume(
        &mut client,
        config,
        format!("nbd-e2e-req-{}", Uuid::new_v4().simple()),
    )
    .await;

    match create_result {
        Ok(response) => {
            if response.volume_id != config.volume_id {
                bail!(
                    "CreateVolume returned unexpected volume_id: got {}, want {}",
                    response.volume_id,
                    config.volume_id
                );
            }
            Ok(())
        }
        Err(err) => {
            if let Some(status) = err.downcast_ref::<tonic::Status>() {
                if status.code() == Code::AlreadyExists {
                    return Ok(());
                }
            }
            Err(err).context("CreateVolume failed for nbd mount test")
        }
    }
}

#[derive(Clone, Copy)]
enum PrivilegeMode {
    Direct,
    Sudo,
}

impl PrivilegeMode {
    fn detect() -> anyhow::Result<Self> {
        if unsafe { libc::geteuid() } == 0 {
            return Ok(Self::Direct);
        }

        let status = Command::new("sudo")
            .arg("-n")
            .arg("true")
            .status()
            .context("failed to execute sudo -n true")?;
        if status.success() {
            Ok(Self::Sudo)
        } else {
            bail!("test requires root or passwordless sudo")
        }
    }

    fn command(self, program: &str) -> Command {
        match self {
            Self::Direct => Command::new(program),
            Self::Sudo => {
                let mut cmd = Command::new("sudo");
                cmd.arg("-n").arg(program);
                cmd
            }
        }
    }
}

fn assert_program_exists_for_mode(privilege: &PrivilegeMode, program: &str) -> anyhow::Result<()> {
    let mut cmd = match privilege {
        PrivilegeMode::Direct => Command::new("sh"),
        PrivilegeMode::Sudo => {
            let mut cmd = Command::new("sudo");
            cmd.arg("-n").arg("sh");
            cmd
        }
    };

    let status = cmd
        .arg("-lc")
        .arg(format!(
            "command -v {program} >/dev/null || [ -x /sbin/{program} ] || [ -x /usr/sbin/{program} ]"
        ))
        .status()
        .with_context(|| format!("failed checking command availability for {program}"))?;
    if !status.success() {
        bail!("required command not found: {program}");
    }
    Ok(())
}

fn spawn_attach_process(
    privilege: &PrivilegeMode,
    log_path: &Path,
    frontend_endpoint: &str,
    namespace: &str,
    volume_id: &str,
    device_path: &Path,
) -> anyhow::Result<Child> {
    let stdout = File::create(log_path)
        .with_context(|| format!("failed to create attach log file {}", log_path.display()))?;
    let stderr = stdout.try_clone().with_context(|| {
        format!(
            "failed to clone attach log file handle {}",
            log_path.display()
        )
    })?;

    let nbd_bin = resolve_temporal_nbd_bin();
    let mut cmd = privilege.command(&nbd_bin);
    cmd.arg("attach")
        .arg("--frontend-endpoint")
        .arg(frontend_endpoint)
        .arg("--namespace")
        .arg(namespace)
        .arg("--volume-id")
        .arg(volume_id)
        .arg("--nbd-device")
        .arg(device_path)
        .arg("--retry-max-attempts")
        .arg("12")
        .arg("--flush-retry-deadline-secs")
        .arg("30")
        .arg("--engine-queue-capacity")
        .arg("256")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    cmd.spawn().with_context(|| {
        format!(
            "failed to start attach process, log path: {}",
            log_path.display()
        )
    })
}

fn resolve_temporal_nbd_bin() -> String {
    env::var("TEMPORAL_NBD_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_temporal-nbd").to_string())
}

fn wait_for_device_ready(
    privilege: &PrivilegeMode,
    device_path: &Path,
    attach_child: &mut Child,
    attach_log_path: &Path,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout_after;
    while Instant::now() < deadline {
        if let Some(status) = attach_child
            .try_wait()
            .context("failed to poll attach process status")?
        {
            bail!(
                "attach process exited early with status {status}\nlog tail:\n{}",
                log_tail(attach_log_path, 120)
            );
        }

        if device_size_bytes(privilege, device_path)? > 0 {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(200));
    }

    bail!(
        "device {} did not become ready within {}s\nlog tail:\n{}",
        device_path.display(),
        timeout_after.as_secs(),
        log_tail(attach_log_path, 120)
    )
}

fn mount_device(
    privilege: &PrivilegeMode,
    device_path: &Path,
    mount_dir: &Path,
) -> anyhow::Result<()> {
    run_privileged_command(
        privilege,
        "mount",
        |cmd| {
            cmd.arg(device_path).arg(mount_dir);
        },
        "mount nbd device",
    )?;
    Ok(())
}

fn unmount_device(privilege: &PrivilegeMode, mount_dir: &Path) -> anyhow::Result<()> {
    run_privileged_command(
        privilege,
        "umount",
        |cmd| {
            cmd.arg(mount_dir);
        },
        "unmount nbd mountpoint",
    )?;
    Ok(())
}

fn stop_attach_process(
    attach_child: &mut Child,
    attach_log_path: &Path,
    timeout_after: Duration,
) -> anyhow::Result<()> {
    let pid = i32::try_from(attach_child.id()).context("attach process PID does not fit i32")?;
    let _ = unsafe { libc::kill(pid, libc::SIGINT) };

    let deadline = Instant::now() + timeout_after;
    while Instant::now() < deadline {
        if let Some(status) = attach_child
            .try_wait()
            .context("failed while waiting for attach shutdown")?
        {
            if status.success() {
                return Ok(());
            }
            let signal_suffix = status
                .signal()
                .map(|sig| format!(", signal {sig}"))
                .unwrap_or_default();
            bail!(
                "attach exited non-zero: status {}{}\nlog tail:\n{}",
                status,
                signal_suffix,
                log_tail(attach_log_path, 120)
            );
        }
        thread::sleep(Duration::from_millis(100));
    }

    let _ = attach_child.kill();
    let status = attach_child
        .wait()
        .context("failed to wait for forced attach process kill")?;
    bail!(
        "attach did not stop after SIGINT; forced kill status {}\nlog tail:\n{}",
        status,
        log_tail(attach_log_path, 120)
    )
}

fn device_size_bytes(privilege: &PrivilegeMode, device_path: &Path) -> anyhow::Result<u64> {
    let output = run_privileged_command(
        privilege,
        "blockdev",
        |cmd| {
            cmd.arg("--getsize64").arg(device_path);
        },
        "read nbd size with blockdev",
    )?;

    let raw = String::from_utf8(output.stdout).context("blockdev output is not utf-8")?;
    raw.trim()
        .parse::<u64>()
        .with_context(|| format!("invalid blockdev size output: {raw:?}"))
}

fn run_privileged_command<F>(
    privilege: &PrivilegeMode,
    program: &str,
    args_builder: F,
    context_msg: &str,
) -> anyhow::Result<Output>
where
    F: FnOnce(&mut Command),
{
    let mut cmd = privilege.command(program);
    args_builder(&mut cmd);

    let output = cmd
        .output()
        .with_context(|| format!("failed to execute command: {context_msg}"))?;

    if output.status.success() {
        return Ok(output);
    }

    bail!(
        "{context_msg} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn nbd_device_pid(device_name: &str) -> anyhow::Result<Option<u32>> {
    let pid_path = Path::new("/sys/block").join(device_name).join("pid");
    if !pid_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&pid_path)
        .with_context(|| format!("failed to read {}", pid_path.display()))?;
    let pid = raw
        .trim()
        .parse::<u32>()
        .with_context(|| format!("invalid pid value in {}", pid_path.display()))?;

    if pid == 0 {
        Ok(None)
    } else {
        Ok(Some(pid))
    }
}

fn log_tail(path: &Path, max_lines: usize) -> String {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) => {
            return format!("failed to read log {}: {err}", path.display());
        }
    };

    let lines: Vec<&str> = content.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].join("\n")
}
