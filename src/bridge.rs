use crate::engine::BlockDeviceEngine;
use crate::errors::EngineError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum BridgeCommand {
    Read {
        offset_bytes: u64,
        length_bytes: u32,
    },
    Write {
        offset_bytes: u64,
        data: Vec<u8>,
    },
    Flush,
    Disconnect,
}

#[derive(Debug)]
pub struct BridgeRequest {
    pub command: BridgeCommand,
    pub response: oneshot::Sender<BridgeResponse>,
}

#[derive(Debug, Clone)]
pub struct BridgeResponse {
    pub errno: i32,
    pub data: Vec<u8>,
}

impl BridgeResponse {
    pub fn ok(data: Vec<u8>) -> Self {
        Self { errno: 0, data }
    }

    pub fn from_error(err: EngineError) -> Self {
        Self {
            errno: err.errno(),
            data: Vec::new(),
        }
    }
}

pub type BridgeRequestTx = mpsc::Sender<BridgeRequest>;
pub type BridgeRequestRx = mpsc::Receiver<BridgeRequest>;

pub fn channel(capacity: usize) -> (BridgeRequestTx, BridgeRequestRx) {
    mpsc::channel(capacity.max(1))
}

pub async fn run_engine_loop<E: BlockDeviceEngine + 'static>(
    mut engine: E,
    block_size_bytes: u32,
    mut requests: BridgeRequestRx,
    shutdown: CancellationToken,
) {
    let mut did_disconnect = false;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            next = requests.recv() => {
                let Some(request) = next else {
                    break;
                };
                let (response, should_exit) =
                    handle_request(&mut engine, block_size_bytes, request.command).await;
                let _ = request.response.send(response);
                if should_exit {
                    did_disconnect = true;
                    break;
                }
            }
        }
    }

    if !did_disconnect {
        let _ = engine.disconnect().await;
    }
}

async fn handle_request<E: BlockDeviceEngine>(
    engine: &mut E,
    block_size_bytes: u32,
    command: BridgeCommand,
) -> (BridgeResponse, bool) {
    match command {
        BridgeCommand::Read {
            offset_bytes,
            length_bytes,
        } => {
            let translated =
                offset_len_to_lba(offset_bytes, u64::from(length_bytes), block_size_bytes);
            match translated {
                Ok((start_lba, block_count)) => {
                    match engine.read_blocks(start_lba, block_count).await {
                        Ok(data) => (BridgeResponse::ok(data), false),
                        Err(err) => {
                            eprintln!(
                                "bridge read failed: offset_bytes={} length_bytes={} start_lba={} block_count={} err={}",
                                offset_bytes, length_bytes, start_lba, block_count, err
                            );
                            (BridgeResponse::from_error(err), false)
                        }
                    }
                }
                Err(err) => {
                    eprintln!(
                        "bridge read request rejected: offset_bytes={} length_bytes={} err={}",
                        offset_bytes, length_bytes, err
                    );
                    (BridgeResponse::from_error(err), false)
                }
            }
        }
        BridgeCommand::Write { offset_bytes, data } => {
            let translated = offset_len_to_lba(
                offset_bytes,
                u64::try_from(data.len()).unwrap_or(u64::MAX),
                block_size_bytes,
            );
            match translated {
                Ok((start_lba, block_count)) => match engine.write_blocks(start_lba, &data).await {
                    Ok(()) => (BridgeResponse::ok(Vec::new()), false),
                    Err(err) => {
                        eprintln!(
                            "bridge write failed: offset_bytes={} data_len={} start_lba={} block_count={} err={}",
                            offset_bytes,
                            data.len(),
                            start_lba,
                            block_count,
                            err
                        );
                        (BridgeResponse::from_error(err), false)
                    }
                },
                Err(err) => {
                    eprintln!(
                        "bridge write request rejected: offset_bytes={} data_len={} err={}",
                        offset_bytes,
                        data.len(),
                        err
                    );
                    (BridgeResponse::from_error(err), false)
                }
            }
        }
        BridgeCommand::Flush => match engine.flush().await {
            Ok(()) => (BridgeResponse::ok(Vec::new()), false),
            Err(err) => {
                eprintln!("bridge flush failed: err={}", err);
                (BridgeResponse::from_error(err), false)
            }
        },
        BridgeCommand::Disconnect => {
            let response = match engine.disconnect().await {
                Ok(()) => BridgeResponse::ok(Vec::new()),
                Err(err) => {
                    eprintln!("bridge disconnect failed: err={}", err);
                    BridgeResponse::from_error(err)
                }
            };
            (response, true)
        }
    }
}

pub fn offset_len_to_lba(
    offset_bytes: u64,
    length_bytes: u64,
    block_size_bytes: u32,
) -> Result<(u64, u32), EngineError> {
    if block_size_bytes == 0 {
        return Err(EngineError::invalid("block size must be > 0"));
    }
    if length_bytes == 0 {
        return Err(EngineError::invalid("request length must be > 0"));
    }

    let block_size = u64::from(block_size_bytes);
    if !offset_bytes.is_multiple_of(block_size) {
        return Err(EngineError::invalid(format!(
            "offset {} is not aligned to block size {}",
            offset_bytes, block_size
        )));
    }
    if !length_bytes.is_multiple_of(block_size) {
        return Err(EngineError::invalid(format!(
            "length {} is not aligned to block size {}",
            length_bytes, block_size
        )));
    }

    let start_lba = offset_bytes / block_size;
    let block_count_u64 = length_bytes / block_size;
    let block_count = u32::try_from(block_count_u64)
        .map_err(|_| EngineError::invalid("block_count does not fit into u32"))?;

    Ok((start_lba, block_count))
}

pub async fn request(requests: &BridgeRequestTx, command: BridgeCommand) -> BridgeResponse {
    let (tx, rx) = oneshot::channel();
    let send_result = requests
        .send(BridgeRequest {
            command,
            response: tx,
        })
        .await;

    if let Err(err) = send_result {
        return BridgeResponse {
            errno: libc::EIO,
            data: format!("bridge queue closed: {err}").into_bytes(),
        };
    }

    match rx.await {
        Ok(response) => response,
        Err(err) => BridgeResponse {
            errno: libc::EIO,
            data: format!("bridge response dropped: {err}").into_bytes(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::BlockDeviceEngine;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct FakeEngine {
        disconnected: Arc<AtomicBool>,
    }

    #[async_trait]
    impl BlockDeviceEngine for FakeEngine {
        async fn read_blocks(
            &mut self,
            _start_lba: u64,
            block_count: u32,
        ) -> Result<Vec<u8>, EngineError> {
            Ok(vec![
                0_u8;
                usize::try_from(block_count).expect("fits") * 4096
            ])
        }

        async fn write_blocks(&mut self, _start_lba: u64, _data: &[u8]) -> Result<(), EngineError> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), EngineError> {
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), EngineError> {
            self.disconnected.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn offset_len_to_lba_rejects_unaligned_offset() {
        let err = offset_len_to_lba(3, 4096, 4096).expect_err("unaligned offset must fail");
        assert!(matches!(err, EngineError::InvalidRequest { .. }));
    }

    #[test]
    fn offset_len_to_lba_translates_aligned_ranges() {
        let (lba, count) = offset_len_to_lba(8192, 12288, 4096).expect("must translate");
        assert_eq!(lba, 2);
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn run_engine_loop_disconnects_on_command() {
        let disconnected = Arc::new(AtomicBool::new(false));
        let engine = FakeEngine {
            disconnected: disconnected.clone(),
        };
        let (tx, rx) = channel(2);
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(run_engine_loop(engine, 4096, rx, shutdown.clone()));

        let response = request(&tx, BridgeCommand::Disconnect).await;
        assert_eq!(response.errno, 0);

        task.await.expect("engine task should finish");
        assert!(disconnected.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn read_command_returns_data() {
        let disconnected = Arc::new(AtomicBool::new(false));
        let engine = FakeEngine {
            disconnected: disconnected.clone(),
        };
        let (tx, rx) = channel(2);
        let shutdown = CancellationToken::new();

        let task = tokio::spawn(run_engine_loop(engine, 4096, rx, shutdown.clone()));
        let response = request(
            &tx,
            BridgeCommand::Read {
                offset_bytes: 0,
                length_bytes: 4096,
            },
        )
        .await;
        assert_eq!(response.errno, 0);
        assert_eq!(response.data.len(), 4096);

        shutdown.cancel();
        task.await.expect("engine task should finish");
    }
}
