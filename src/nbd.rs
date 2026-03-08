use crate::bridge::{request, BridgeCommand, BridgeRequestTx};
use crate::engine::VolumeGeometry;
use anyhow::{anyhow, bail, Context};
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct NbdConfig {
    pub device_path: PathBuf,
    pub timeout_secs: u64,
}

pub fn preflight(device_path: &Path) -> anyhow::Result<()> {
    if !cfg!(target_os = "linux") {
        bail!("attach mode is only supported on Linux hosts");
    }

    if !device_path.exists() {
        bail!("NBD device {} does not exist", device_path.display());
    }

    let module_path = Path::new("/sys/module/nbd");
    if !module_path.exists() {
        bail!(
            "nbd kernel module is not loaded (missing {}). Run: sudo modprobe nbd max_part=0",
            module_path.display()
        );
    }

    Ok(())
}

#[cfg(target_os = "linux")]
pub async fn serve(
    config: NbdConfig,
    geometry: VolumeGeometry,
    requests: BridgeRequestTx,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        linux::serve_blocking(config, geometry, requests, shutdown, handle)
    })
    .await
    .context("nbd blocking task join failure")?
}

#[cfg(not(target_os = "linux"))]
pub async fn serve(
    _config: NbdConfig,
    _geometry: VolumeGeometry,
    _requests: BridgeRequestTx,
    _shutdown: CancellationToken,
) -> anyhow::Result<()> {
    Err(anyhow!("attach mode is only supported on Linux hosts"))
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{self, Read, Write};
    use std::os::fd::{AsRawFd, RawFd};
    use std::os::unix::net::UnixStream;
    use std::thread;
    use std::time::Duration;

    const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
    const NBD_REPLY_MAGIC: u32 = 0x6744_6698;

    const NBD_CMD_READ: u32 = 0;
    const NBD_CMD_WRITE: u32 = 1;
    const NBD_CMD_DISC: u32 = 2;
    const NBD_CMD_FLUSH: u32 = 3;
    const NBD_CMD_MASK_COMMAND: u32 = 0x0000_FFFF;

    const NBD_SET_SOCK: libc::c_ulong = 0xAB00;
    const NBD_SET_BLKSIZE: libc::c_ulong = 0xAB01;
    const NBD_SET_SIZE_BLOCKS: libc::c_ulong = 0xAB07;
    const NBD_DO_IT: libc::c_ulong = 0xAB03;
    const NBD_CLEAR_SOCK: libc::c_ulong = 0xAB04;
    const NBD_CLEAR_QUE: libc::c_ulong = 0xAB05;
    const NBD_DISCONNECT: libc::c_ulong = 0xAB08;
    const NBD_SET_TIMEOUT: libc::c_ulong = 0xAB09;
    const NBD_SET_FLAGS: libc::c_ulong = 0xAB0A;

    const NBD_FLAG_HAS_FLAGS: u64 = 1 << 0;
    const NBD_FLAG_SEND_FLUSH: u64 = 1 << 2;

    #[derive(Debug)]
    struct RequestHeader {
        cmd_type: u32,
        handle: [u8; 8],
        offset_bytes: u64,
        length_bytes: u32,
    }

    pub fn serve_blocking(
        config: NbdConfig,
        geometry: VolumeGeometry,
        requests: BridgeRequestTx,
        shutdown: CancellationToken,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<()> {
        let device = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.device_path)
            .with_context(|| format!("failed to open {}", config.device_path.display()))?;

        let (kernel_sock, mut userspace_sock) =
            UnixStream::pair().context("failed to create nbd socketpair")?;

        configure_device(
            device.as_raw_fd(),
            kernel_sock.as_raw_fd(),
            &geometry,
            config.timeout_secs,
        )
        .context("failed to configure nbd device")?;

        let do_it_fd = dup_fd(device.as_raw_fd()).context("failed to dup nbd fd for NBD_DO_IT")?;
        let do_it_thread = thread::spawn(move || run_nbd_do_it(do_it_fd));

        let disconnect_fd =
            dup_fd(device.as_raw_fd()).context("failed to dup nbd fd for disconnect watcher")?;
        let disconnect_shutdown = shutdown.clone();
        let disconnect_watcher = thread::spawn(move || {
            while !disconnect_shutdown.is_cancelled() {
                thread::sleep(Duration::from_millis(100));
            }
            let _ = ioctl_noarg(disconnect_fd, NBD_DISCONNECT);
            close_fd(disconnect_fd);
        });

        let mut request_loop_error: Option<anyhow::Error> = None;
        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let header = match read_request_header(&mut userspace_sock) {
                Ok(Some(header)) => header,
                Ok(None) => break,
                Err(err) => {
                    request_loop_error = Some(anyhow!("failed to read nbd request header: {err}"));
                    break;
                }
            };

            let command = header.cmd_type & NBD_CMD_MASK_COMMAND;
            match command {
                NBD_CMD_READ => {
                    if let Err(errno) =
                        validate_range(&geometry, header.offset_bytes, header.length_bytes)
                    {
                        if let Err(err) =
                            write_reply(&mut userspace_sock, header.handle, errno, &[])
                        {
                            request_loop_error =
                                Some(anyhow!("failed to write read error reply: {err}"));
                            break;
                        }
                        continue;
                    }

                    let response = runtime.block_on(request(
                        &requests,
                        BridgeCommand::Read {
                            offset_bytes: header.offset_bytes,
                            length_bytes: header.length_bytes,
                        },
                    ));
                    let expected = usize::try_from(header.length_bytes)
                        .expect("u32 length should fit into usize");
                    if response.errno == 0 && response.data.len() != expected {
                        if let Err(err) =
                            write_reply(&mut userspace_sock, header.handle, libc::EIO, &[])
                        {
                            request_loop_error =
                                Some(anyhow!("failed to write read-size error reply: {err}"));
                            break;
                        }
                        continue;
                    }

                    if let Err(err) = write_reply(
                        &mut userspace_sock,
                        header.handle,
                        response.errno,
                        &response.data,
                    ) {
                        request_loop_error = Some(anyhow!("failed to write read reply: {err}"));
                        break;
                    }
                }
                NBD_CMD_WRITE => {
                    let mut payload = vec![
                        0_u8;
                        usize::try_from(header.length_bytes)
                            .expect("u32 length should fit")
                    ];
                    if let Err(err) = userspace_sock.read_exact(&mut payload) {
                        request_loop_error = Some(anyhow!("failed to read write payload: {err}"));
                        break;
                    }

                    if let Err(errno) =
                        validate_range(&geometry, header.offset_bytes, header.length_bytes)
                    {
                        if let Err(err) =
                            write_reply(&mut userspace_sock, header.handle, errno, &[])
                        {
                            request_loop_error =
                                Some(anyhow!("failed to write write error reply: {err}"));
                            break;
                        }
                        continue;
                    }

                    let response = runtime.block_on(request(
                        &requests,
                        BridgeCommand::Write {
                            offset_bytes: header.offset_bytes,
                            data: payload,
                        },
                    ));
                    if let Err(err) =
                        write_reply(&mut userspace_sock, header.handle, response.errno, &[])
                    {
                        request_loop_error = Some(anyhow!("failed to write write reply: {err}"));
                        break;
                    }
                }
                NBD_CMD_FLUSH => {
                    let response = runtime.block_on(request(&requests, BridgeCommand::Flush));
                    if let Err(err) =
                        write_reply(&mut userspace_sock, header.handle, response.errno, &[])
                    {
                        request_loop_error = Some(anyhow!("failed to write flush reply: {err}"));
                        break;
                    }
                }
                NBD_CMD_DISC => {
                    let _ = runtime.block_on(request(&requests, BridgeCommand::Disconnect));
                    break;
                }
                _ => {
                    if let Err(err) =
                        write_reply(&mut userspace_sock, header.handle, libc::EINVAL, &[])
                    {
                        request_loop_error =
                            Some(anyhow!("failed to write unsupported-cmd reply: {err}"));
                        break;
                    }
                }
            }
        }

        shutdown.cancel();
        let _ = runtime.block_on(request(&requests, BridgeCommand::Disconnect));

        let _ = ioctl_noarg(device.as_raw_fd(), NBD_DISCONNECT);
        let _ = ioctl_noarg(device.as_raw_fd(), NBD_CLEAR_QUE);
        let _ = ioctl_noarg(device.as_raw_fd(), NBD_CLEAR_SOCK);

        let _ = userspace_sock.flush();
        drop(userspace_sock);
        drop(kernel_sock);
        drop(device);

        let _ = disconnect_watcher.join();

        match do_it_thread.join() {
            Ok(do_it_result) => {
                if let Err(err) = do_it_result {
                    if let Some(loop_err) = request_loop_error {
                        return Err(loop_err.context(format!("NBD_DO_IT also failed: {err}")));
                    }
                    return Err(err);
                }
            }
            Err(_) => {
                return Err(anyhow!("NBD_DO_IT thread panicked"));
            }
        }

        if let Some(err) = request_loop_error {
            return Err(err);
        }

        Ok(())
    }

    fn configure_device(
        nbd_fd: RawFd,
        sock_fd: RawFd,
        geometry: &VolumeGeometry,
        timeout_secs: u64,
    ) -> anyhow::Result<()> {
        let block_size = libc::c_ulong::try_from(geometry.block_size_bytes)
            .context("block size does not fit ioctl arg")?;
        let total_blocks = libc::c_ulong::try_from(geometry.total_blocks)
            .context("total blocks does not fit ioctl arg")?;
        let timeout =
            libc::c_ulong::try_from(timeout_secs).context("timeout does not fit ioctl arg")?;

        ioctl_with_arg(nbd_fd, NBD_SET_BLKSIZE, block_size).context("NBD_SET_BLKSIZE failed")?;
        ioctl_with_arg(nbd_fd, NBD_SET_SIZE_BLOCKS, total_blocks)
            .context("NBD_SET_SIZE_BLOCKS failed")?;

        if timeout_secs > 0 {
            ioctl_with_arg(nbd_fd, NBD_SET_TIMEOUT, timeout).context("NBD_SET_TIMEOUT failed")?;
        }

        let flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH;
        ioctl_with_arg(nbd_fd, NBD_SET_FLAGS, flags).context("NBD_SET_FLAGS failed")?;
        ioctl_with_arg(
            nbd_fd,
            NBD_SET_SOCK,
            libc::c_ulong::try_from(sock_fd).context("socket fd does not fit ioctl arg")?,
        )
        .context("NBD_SET_SOCK failed")?;

        Ok(())
    }

    fn validate_range(
        geometry: &VolumeGeometry,
        offset_bytes: u64,
        length_bytes: u32,
    ) -> Result<(), i32> {
        if length_bytes == 0 {
            return Err(libc::EINVAL);
        }

        let block_size = u64::from(geometry.block_size_bytes);
        if !offset_bytes.is_multiple_of(block_size) {
            return Err(libc::EINVAL);
        }
        if !u64::from(length_bytes).is_multiple_of(block_size) {
            return Err(libc::EINVAL);
        }

        let end = offset_bytes
            .checked_add(u64::from(length_bytes))
            .ok_or(libc::EINVAL)?;
        if end > geometry.size_bytes {
            return Err(libc::EINVAL);
        }

        Ok(())
    }

    fn read_request_header(stream: &mut UnixStream) -> io::Result<Option<RequestHeader>> {
        let mut raw = [0_u8; 28];
        match stream.read_exact(&mut raw) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        }

        let magic = u32::from_be_bytes(raw[0..4].try_into().expect("slice length"));
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid request magic: 0x{magic:08x}"),
            ));
        }

        let cmd_type = u32::from_be_bytes(raw[4..8].try_into().expect("slice length"));
        let mut handle = [0_u8; 8];
        handle.copy_from_slice(&raw[8..16]);
        let offset_bytes = u64::from_be_bytes(raw[16..24].try_into().expect("slice length"));
        let length_bytes = u32::from_be_bytes(raw[24..28].try_into().expect("slice length"));

        Ok(Some(RequestHeader {
            cmd_type,
            handle,
            offset_bytes,
            length_bytes,
        }))
    }

    fn write_reply(
        stream: &mut UnixStream,
        handle: [u8; 8],
        errno: i32,
        payload: &[u8],
    ) -> io::Result<()> {
        debug_assert!(
            errno >= 0,
            "NBD reply errno should be non-negative, got {}",
            errno
        );
        let sanitized_errno = if errno < 0 { libc::EIO } else { errno };

        let mut header = [0_u8; 16];
        header[0..4].copy_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
        header[4..8]
            .copy_from_slice(&(u32::try_from(sanitized_errno).unwrap_or(u32::MAX)).to_be_bytes());
        header[8..16].copy_from_slice(&handle);

        stream.write_all(&header)?;
        if !payload.is_empty() {
            stream.write_all(payload)?;
        }
        Ok(())
    }

    fn run_nbd_do_it(fd: RawFd) -> anyhow::Result<()> {
        let rc = unsafe { libc::ioctl(fd, NBD_DO_IT, 0) };
        let result = if rc < 0 {
            let err = io::Error::last_os_error();
            let errno = err.raw_os_error().unwrap_or_default();
            // Disconnect paths often surface as ENOTCONN/EINVAL once the socket is cleared.
            if errno == libc::ENOTCONN || errno == libc::EINVAL || errno == libc::EPIPE {
                Ok(())
            } else {
                Err(anyhow!("NBD_DO_IT failed: {err}"))
            }
        } else {
            Ok(())
        };

        close_fd(fd);
        result
    }

    fn ioctl_with_arg(fd: RawFd, request: libc::c_ulong, arg: libc::c_ulong) -> io::Result<()> {
        let rc = unsafe { libc::ioctl(fd, request, arg) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn ioctl_noarg(fd: RawFd, request: libc::c_ulong) -> io::Result<()> {
        let rc = unsafe { libc::ioctl(fd, request, 0) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn dup_fd(fd: RawFd) -> io::Result<RawFd> {
        let duped = unsafe { libc::dup(fd) };
        if duped < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(duped)
    }

    fn close_fd(fd: RawFd) {
        let _ = unsafe { libc::close(fd) };
    }
}
