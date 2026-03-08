use crate::errors::{EngineError, TransportError};
use async_trait::async_trait;
use std::cmp::min;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub const MAX_BLOCKS_PER_RPC: u32 = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeGeometry {
    pub size_bytes: u64,
    pub block_size_bytes: u32,
    pub total_blocks: u64,
}

impl VolumeGeometry {
    pub fn new(size_bytes: u64, block_size_bytes: u32) -> Result<Self, EngineError> {
        if block_size_bytes == 0 {
            return Err(EngineError::invalid("block_size_bytes must be > 0"));
        }
        if size_bytes == 0 {
            return Err(EngineError::invalid("size_bytes must be > 0"));
        }
        let block_size_u64 = u64::from(block_size_bytes);
        if !size_bytes.is_multiple_of(block_size_u64) {
            return Err(EngineError::invalid(
                "size_bytes must be a multiple of block_size_bytes",
            ));
        }

        Ok(Self {
            size_bytes,
            block_size_bytes,
            total_blocks: size_bytes / block_size_u64,
        })
    }

    fn block_size_usize(&self) -> usize {
        usize::try_from(self.block_size_bytes).expect("block size must fit in usize")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockWrite {
    pub lba: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub dirty_high_watermark_blocks: usize,
    pub flush_retry_deadline: Duration,
    pub flush_retry_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            dirty_high_watermark_blocks: 2048,
            flush_retry_deadline: Duration::from_secs(20),
            flush_retry_interval: Duration::from_millis(200),
        }
    }
}

#[async_trait]
pub trait BackendTransport: Send {
    async fn read_blocks(
        &mut self,
        start_lba: u64,
        block_count: u32,
    ) -> Result<Vec<u8>, TransportError>;
    async fn write_batch(&mut self, writes: &[BlockWrite]) -> Result<(), TransportError>;
    async fn disconnect(&mut self) -> Result<(), TransportError>;
}

#[async_trait]
pub trait BlockDeviceEngine: Send {
    async fn read_blocks(
        &mut self,
        start_lba: u64,
        block_count: u32,
    ) -> Result<Vec<u8>, EngineError>;
    async fn write_blocks(&mut self, start_lba: u64, data: &[u8]) -> Result<(), EngineError>;
    async fn flush(&mut self) -> Result<(), EngineError>;
    async fn disconnect(&mut self) -> Result<(), EngineError>;
}

pub struct CachingBlockEngine<T: BackendTransport> {
    geometry: VolumeGeometry,
    transport: T,
    config: EngineConfig,
    dirty: BTreeMap<u64, Vec<u8>>,
}

impl<T: BackendTransport> CachingBlockEngine<T> {
    pub fn new(geometry: VolumeGeometry, transport: T, mut config: EngineConfig) -> Self {
        if config.dirty_high_watermark_blocks == 0 {
            config.dirty_high_watermark_blocks = 1;
        }

        Self {
            geometry,
            transport,
            config,
            dirty: BTreeMap::new(),
        }
    }

    pub fn dirty_count(&self) -> usize {
        self.dirty.len()
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    fn ensure_read_range(&self, start_lba: u64, block_count: u32) -> Result<(), EngineError> {
        if block_count == 0 {
            return Err(EngineError::invalid("block_count must be > 0"));
        }
        self.ensure_range(start_lba, u64::from(block_count))
    }

    fn ensure_write_payload(&self, data: &[u8]) -> Result<u64, EngineError> {
        let block_size = self.geometry.block_size_usize();
        if data.is_empty() {
            return Err(EngineError::invalid("write payload must not be empty"));
        }
        if !data.len().is_multiple_of(block_size) {
            return Err(EngineError::invalid(format!(
                "write payload len {} is not aligned to block size {}",
                data.len(),
                block_size
            )));
        }
        let block_count = data.len() / block_size;
        u64::try_from(block_count)
            .map_err(|_| EngineError::invalid("write payload block count does not fit into u64"))
    }

    fn ensure_range(&self, start_lba: u64, block_count: u64) -> Result<(), EngineError> {
        let end_lba = start_lba
            .checked_add(block_count)
            .ok_or_else(|| EngineError::invalid("request range overflows u64"))?;
        if end_lba > self.geometry.total_blocks {
            return Err(EngineError::invalid(format!(
                "request out of range: end_lba {} > total_blocks {}",
                end_lba, self.geometry.total_blocks
            )));
        }
        Ok(())
    }

    fn map_transport(err: TransportError) -> (EngineError, bool) {
        let retryable = err.is_retryable();
        let mapped = match err {
            TransportError::Retryable { message } | TransportError::Terminal { message } => {
                EngineError::io(message)
            }
        };
        (mapped, retryable)
    }

    async fn flush_once(&mut self) -> Result<(), (EngineError, bool)> {
        if self.dirty.is_empty() {
            return Ok(());
        }

        let chunk_limit = usize::try_from(MAX_BLOCKS_PER_RPC).expect("const fits");
        while !self.dirty.is_empty() {
            let mut flushed_lbas = Vec::with_capacity(chunk_limit);
            let mut writes = Vec::with_capacity(chunk_limit);
            for (lba, data) in self.dirty.iter().take(chunk_limit) {
                flushed_lbas.push(*lba);
                writes.push(BlockWrite {
                    lba: *lba,
                    data: data.clone(),
                });
            }
            if let Err(err) = self.transport.write_batch(&writes).await {
                return Err(Self::map_transport(err));
            }
            for lba in flushed_lbas {
                let removed = self.dirty.remove(&lba);
                debug_assert!(removed.is_some(), "dirty cache entry unexpectedly missing");
            }
        }
        Ok(())
    }

    async fn flush_with_deadline(&mut self, deadline: Duration) -> Result<(), EngineError> {
        if self.dirty.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        loop {
            match self.flush_once().await {
                Ok(()) => return Ok(()),
                Err((_err, retryable)) if retryable && started.elapsed() < deadline => {
                    sleep(self.config.flush_retry_interval).await;
                }
                Err((err, _)) => return Err(err),
            }
        }
    }
}

#[async_trait]
impl<T: BackendTransport> BlockDeviceEngine for CachingBlockEngine<T> {
    async fn read_blocks(
        &mut self,
        start_lba: u64,
        block_count: u32,
    ) -> Result<Vec<u8>, EngineError> {
        self.ensure_read_range(start_lba, block_count)?;

        let block_size = self.geometry.block_size_usize();
        let total_len = usize::try_from(block_count)
            .ok()
            .and_then(|count| count.checked_mul(block_size))
            .ok_or_else(|| EngineError::invalid("requested read size is too large"))?;

        let mut output = vec![0_u8; total_len];
        let mut remaining = block_count;
        let mut current_lba = start_lba;
        let mut out_offset = 0_usize;

        while remaining > 0 {
            let chunk_count = min(remaining, MAX_BLOCKS_PER_RPC);
            let chunk = self
                .transport
                .read_blocks(current_lba, chunk_count)
                .await
                .map_err(|err| Self::map_transport(err).0)?;

            let chunk_len = usize::try_from(chunk_count)
                .ok()
                .and_then(|count| count.checked_mul(block_size))
                .ok_or_else(|| EngineError::io("backend read size overflow"))?;

            if chunk.len() != chunk_len {
                return Err(EngineError::io(format!(
                    "backend returned {} bytes for {} blocks (expected {})",
                    chunk.len(),
                    chunk_count,
                    chunk_len
                )));
            }

            output[out_offset..out_offset + chunk_len].copy_from_slice(&chunk);
            out_offset += chunk_len;
            remaining -= chunk_count;
            current_lba = current_lba
                .checked_add(u64::from(chunk_count))
                .ok_or_else(|| EngineError::io("read cursor overflow"))?;
        }

        let end_lba = start_lba
            .checked_add(u64::from(block_count))
            .ok_or_else(|| EngineError::invalid("read range overflow"))?;
        for (lba, block_data) in self.dirty.range(start_lba..end_lba) {
            if block_data.len() != block_size {
                return Err(EngineError::io(format!(
                    "dirty cache block {} has unexpected size {}",
                    lba,
                    block_data.len()
                )));
            }
            let relative = usize::try_from(*lba - start_lba)
                .map_err(|_| EngineError::io("dirty cache index overflow"))?;
            let offset = relative
                .checked_mul(block_size)
                .ok_or_else(|| EngineError::io("dirty cache offset overflow"))?;
            output[offset..offset + block_size].copy_from_slice(block_data);
        }

        Ok(output)
    }

    async fn write_blocks(&mut self, start_lba: u64, data: &[u8]) -> Result<(), EngineError> {
        let block_count = self.ensure_write_payload(data)?;
        self.ensure_range(start_lba, block_count)?;

        let block_size = self.geometry.block_size_usize();
        for (index, chunk) in data.chunks_exact(block_size).enumerate() {
            let lba = start_lba
                .checked_add(u64::try_from(index).expect("usize index should fit into u64"))
                .ok_or_else(|| EngineError::invalid("write LBA overflow"))?;
            self.dirty.insert(lba, chunk.to_vec());
        }

        if self.dirty.len() >= self.config.dirty_high_watermark_blocks {
            self.flush_with_deadline(self.config.flush_retry_deadline)
                .await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), EngineError> {
        self.flush_with_deadline(self.config.flush_retry_deadline)
            .await
    }

    async fn disconnect(&mut self) -> Result<(), EngineError> {
        let flush_result = self
            .flush_with_deadline(self.config.flush_retry_deadline)
            .await;
        let disconnect_result = self.transport.disconnect().await;

        if let Err(err) = disconnect_result {
            let mapped = Self::map_transport(err).0;
            return if flush_result.is_err() {
                flush_result
            } else {
                Err(mapped)
            };
        }

        flush_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[derive(Debug)]
    struct MockTransport {
        block_size: usize,
        backend: BTreeMap<u64, Vec<u8>>,
        read_calls: Vec<(u64, u32)>,
        write_batches: Vec<Vec<u64>>,
        write_calls: usize,
        fail_on_write_calls: VecDeque<usize>,
        fail_writes: VecDeque<TransportError>,
        disconnected: bool,
    }

    impl MockTransport {
        fn new(block_size: usize) -> Self {
            Self {
                block_size,
                backend: BTreeMap::new(),
                read_calls: Vec::new(),
                write_batches: Vec::new(),
                write_calls: 0,
                fail_on_write_calls: VecDeque::new(),
                fail_writes: VecDeque::new(),
                disconnected: false,
            }
        }

        fn with_block(mut self, lba: u64, fill: u8) -> Self {
            self.backend.insert(lba, vec![fill; self.block_size]);
            self
        }
    }

    #[async_trait]
    impl BackendTransport for MockTransport {
        async fn read_blocks(
            &mut self,
            start_lba: u64,
            block_count: u32,
        ) -> Result<Vec<u8>, TransportError> {
            self.read_calls.push((start_lba, block_count));

            let mut out = Vec::with_capacity(
                usize::try_from(block_count).expect("block_count fits") * self.block_size,
            );
            for offset in 0..u64::from(block_count) {
                let lba = start_lba + offset;
                if let Some(block) = self.backend.get(&lba) {
                    out.extend_from_slice(block);
                } else {
                    out.extend(std::iter::repeat_n(0_u8, self.block_size));
                }
            }

            Ok(out)
        }

        async fn write_batch(&mut self, writes: &[BlockWrite]) -> Result<(), TransportError> {
            self.write_calls = self.write_calls.saturating_add(1);

            if self.fail_on_write_calls.is_empty() {
                if let Some(err) = self.fail_writes.pop_front() {
                    return Err(err);
                }
            } else if self
                .fail_on_write_calls
                .front()
                .is_some_and(|call| *call == self.write_calls)
            {
                self.fail_on_write_calls.pop_front();
                if let Some(err) = self.fail_writes.pop_front() {
                    return Err(err);
                }
            }

            self.write_batches
                .push(writes.iter().map(|write| write.lba).collect());
            for write in writes {
                self.backend.insert(write.lba, write.data.clone());
            }
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<(), TransportError> {
            self.disconnected = true;
            Ok(())
        }
    }

    fn geometry(block_size: u32, total_blocks: u64) -> VolumeGeometry {
        VolumeGeometry::new(u64::from(block_size) * total_blocks, block_size)
            .expect("valid geometry")
    }

    fn test_config() -> EngineConfig {
        EngineConfig {
            dirty_high_watermark_blocks: 4096,
            flush_retry_deadline: Duration::from_millis(50),
            flush_retry_interval: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn read_overlays_dirty_blocks() {
        let transport = MockTransport::new(4)
            .with_block(1, 0x11)
            .with_block(2, 0x22)
            .with_block(3, 0x33);
        let mut engine = CachingBlockEngine::new(geometry(4, 16), transport, test_config());

        engine
            .write_blocks(2, &[0xAA, 0xAA, 0xAA, 0xAA])
            .await
            .expect("write succeeds");

        let data = engine.read_blocks(1, 3).await.expect("read should succeed");

        assert_eq!(&data[0..4], &[0x11, 0x11, 0x11, 0x11]);
        assert_eq!(&data[4..8], &[0xAA, 0xAA, 0xAA, 0xAA]);
        assert_eq!(&data[8..12], &[0x33, 0x33, 0x33, 0x33]);
    }

    #[tokio::test]
    async fn flush_chunks_write_batches_to_contract_limit() {
        let mut payload = Vec::new();
        for i in 0_u32..700 {
            payload.extend_from_slice(&i.to_le_bytes());
        }

        let mut engine =
            CachingBlockEngine::new(geometry(4, 2000), MockTransport::new(4), test_config());

        engine
            .write_blocks(0, &payload)
            .await
            .expect("write succeeds");
        engine.flush().await.expect("flush succeeds");

        let transport = engine.into_transport();
        assert_eq!(transport.write_batches.len(), 2);
        assert_eq!(transport.write_batches[0].len(), 512);
        assert_eq!(transport.write_batches[1].len(), 188);
    }

    #[tokio::test]
    async fn read_chunks_requests_to_contract_limit() {
        let mut engine =
            CachingBlockEngine::new(geometry(4, 2000), MockTransport::new(4), test_config());

        let data = engine
            .read_blocks(0, 600)
            .await
            .expect("read should succeed");
        assert_eq!(data.len(), 600 * 4);

        let transport = engine.into_transport();
        assert_eq!(transport.read_calls, vec![(0, 512), (512, 88)]);
    }

    #[tokio::test]
    async fn high_watermark_flush_failure_returns_io_and_preserves_dirty() {
        let mut transport = MockTransport::new(4);
        for _ in 0..64 {
            transport
                .fail_writes
                .push_back(TransportError::retryable("temporary backend outage"));
        }

        let mut engine = CachingBlockEngine::new(
            geometry(4, 128),
            transport,
            EngineConfig {
                dirty_high_watermark_blocks: 2,
                flush_retry_deadline: Duration::from_millis(5),
                flush_retry_interval: Duration::from_millis(1),
            },
        );

        engine
            .write_blocks(0, &[1, 1, 1, 1])
            .await
            .expect("first write should queue dirty block");

        let err = engine
            .write_blocks(1, &[2, 2, 2, 2])
            .await
            .expect_err("high-water flush should fail");
        assert!(matches!(err, EngineError::Io { .. }));
        assert_eq!(engine.dirty_count(), 2);
    }

    #[tokio::test]
    async fn flush_retries_retryable_failures_until_success() {
        let mut transport = MockTransport::new(4);
        transport
            .fail_writes
            .push_back(TransportError::retryable("transient error"));

        let mut engine = CachingBlockEngine::new(
            geometry(4, 128),
            transport,
            EngineConfig {
                dirty_high_watermark_blocks: 4096,
                flush_retry_deadline: Duration::from_millis(50),
                flush_retry_interval: Duration::from_millis(1),
            },
        );

        engine
            .write_blocks(0, &[1, 2, 3, 4])
            .await
            .expect("write succeeds");
        engine
            .flush()
            .await
            .expect("flush should eventually succeed");

        assert_eq!(engine.dirty_count(), 0);
        let transport = engine.into_transport();
        assert_eq!(transport.write_batches.len(), 1);
    }

    #[tokio::test]
    async fn flush_retries_only_failed_tail_chunk() {
        let mut payload = Vec::new();
        for i in 0_u32..700 {
            payload.extend_from_slice(&i.to_le_bytes());
        }

        let mut transport = MockTransport::new(4);
        transport.fail_on_write_calls.push_back(2);
        transport
            .fail_writes
            .push_back(TransportError::retryable("second chunk transient error"));

        let mut engine = CachingBlockEngine::new(
            geometry(4, 2000),
            transport,
            EngineConfig {
                dirty_high_watermark_blocks: 4096,
                flush_retry_deadline: Duration::from_millis(50),
                flush_retry_interval: Duration::from_millis(1),
            },
        );

        engine
            .write_blocks(0, &payload)
            .await
            .expect("write succeeds");
        engine
            .flush()
            .await
            .expect("flush should eventually succeed");

        let transport = engine.into_transport();
        assert_eq!(transport.write_batches.len(), 2);
        assert_eq!(transport.write_batches[0].len(), 512);
        assert_eq!(transport.write_batches[1].len(), 188);
        assert_eq!(transport.write_batches[0][0], 0);
        assert_eq!(transport.write_batches[0][511], 511);
        assert_eq!(transport.write_batches[1][0], 512);
        assert_eq!(transport.write_batches[1][187], 699);
    }

    #[tokio::test]
    async fn write_rejects_unaligned_payload() {
        let mut engine =
            CachingBlockEngine::new(geometry(4, 128), MockTransport::new(4), test_config());

        let err = engine
            .write_blocks(0, &[1, 2, 3])
            .await
            .expect_err("unaligned write should fail");

        assert!(matches!(err, EngineError::InvalidRequest { .. }));
        assert_eq!(err.errno(), libc::EINVAL);
    }

    #[tokio::test]
    async fn disconnect_flushes_then_disconnects_transport() {
        let mut engine =
            CachingBlockEngine::new(geometry(4, 128), MockTransport::new(4), test_config());

        engine
            .write_blocks(0, &[1, 2, 3, 4])
            .await
            .expect("write succeeds");
        engine
            .disconnect()
            .await
            .expect("disconnect should succeed");

        let transport = engine.into_transport();
        assert!(transport.disconnected);
        assert_eq!(transport.backend.get(&0).cloned(), Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn geometry_validation_rejects_unaligned_size() {
        let err = VolumeGeometry::new(1000, 512).expect_err("should reject unaligned size");
        assert!(matches!(err, EngineError::InvalidRequest { .. }));
    }

    #[test]
    fn geometry_validation_rejects_zero_block_size() {
        let err = VolumeGeometry::new(1024, 0).expect_err("should reject zero block size");
        assert!(matches!(err, EngineError::InvalidRequest { .. }));
    }
}
