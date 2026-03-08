use crate::engine::{BackendTransport, BlockWrite, VolumeGeometry};
use crate::errors::TransportError;
use crate::workflowservicepb;
use crate::WorkflowServiceClient;
use async_trait::async_trait;
use rand::Rng;
use std::cmp::min;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tonic::transport::Endpoint;
use tonic::Code;

// Keep WriteBatch requests conservative to avoid overflowing Temporal workflow mutable state.
const MAX_WRITES_PER_WRITE_BATCH_RPC: usize = 64;

#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub jitter_ratio: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(150),
            max_backoff: Duration::from_secs(2),
            jitter_ratio: 0.2,
        }
    }
}

impl RetryConfig {
    fn capped_attempts(&self) -> usize {
        self.max_attempts.max(1)
    }

    fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let shift = attempt.saturating_sub(1).min(12);
        let multiplier = 1_u32 << shift;
        let exponential = self
            .initial_backoff
            .checked_mul(multiplier)
            .unwrap_or(self.max_backoff);
        let capped = min(exponential, self.max_backoff);

        if self.jitter_ratio <= 0.0 {
            return capped;
        }

        let low = (1.0 - self.jitter_ratio).max(0.0);
        let high = 1.0 + self.jitter_ratio;
        let mut rng = rand::thread_rng();
        let jitter = rng.gen_range(low..=high);

        Duration::from_secs_f64((capped.as_secs_f64() * jitter).max(0.001))
    }
}

#[derive(Clone, Debug)]
pub struct VolumeSessionConfig {
    pub frontend_endpoint: String,
    pub namespace: String,
    pub volume_id: String,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub retry: RetryConfig,
}

pub struct VolumeSession {
    config: VolumeSessionConfig,
    endpoint_url: String,
    client: Option<WorkflowServiceClient>,
    geometry: VolumeGeometry,
    write_rpc_sent: u64,
    write_payload_bytes_sent: u64,
    write_blocks_sent: u64,
}

impl VolumeSession {
    pub async fn connect_and_open(config: VolumeSessionConfig) -> Result<Self, TransportError> {
        let endpoint_url = normalize_endpoint(&config.frontend_endpoint);
        let attempts = config.retry.capped_attempts();

        for attempt in 1..=attempts {
            let connect =
                connect_client(&endpoint_url, config.connect_timeout, config.rpc_timeout).await;
            let mut client = match connect {
                Ok(client) => client,
                Err(err) => {
                    if attempt < attempts {
                        sleep(config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(TransportError::retryable(format!(
                        "connect failed after {attempts} attempts: {err}"
                    )));
                }
            };

            let open_req = workflowservicepb::OpenVolumeRequest {
                namespace: config.namespace.clone(),
                volume_id: config.volume_id.clone(),
            };

            let open_result = timeout(config.rpc_timeout, client.open_volume(open_req)).await;
            match open_result {
                Ok(Ok(response)) => {
                    let open = response.into_inner();
                    if open.volume_id != config.volume_id {
                        return Err(TransportError::terminal(format!(
                            "OpenVolume returned mismatched volume_id: got {}, want {}",
                            open.volume_id, config.volume_id
                        )));
                    }

                    let size_bytes = u64::try_from(open.size_bytes).map_err(|_| {
                        TransportError::terminal(format!(
                            "OpenVolume returned invalid size_bytes: {}",
                            open.size_bytes
                        ))
                    })?;
                    let block_size_bytes = u32::try_from(open.block_size_bytes).map_err(|_| {
                        TransportError::terminal(format!(
                            "OpenVolume returned invalid block_size_bytes: {}",
                            open.block_size_bytes
                        ))
                    })?;
                    let geometry =
                        VolumeGeometry::new(size_bytes, block_size_bytes).map_err(|err| {
                            TransportError::terminal(format!(
                                "invalid volume geometry from OpenVolume: {err}"
                            ))
                        })?;

                    return Ok(Self {
                        config,
                        endpoint_url,
                        client: Some(client),
                        geometry,
                        write_rpc_sent: 0,
                        write_payload_bytes_sent: 0,
                        write_blocks_sent: 0,
                    });
                }
                Ok(Err(status)) => {
                    let retryable = is_retryable_status(status.code());
                    if retryable && attempt < attempts {
                        sleep(config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(status_to_transport("OpenVolume", status, retryable));
                }
                Err(_) => {
                    if attempt < attempts {
                        sleep(config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(TransportError::retryable(format!(
                        "OpenVolume timed out after {} attempts",
                        attempts
                    )));
                }
            }
        }

        Err(TransportError::retryable(
            "OpenVolume exhausted retry loop unexpectedly",
        ))
    }

    pub fn geometry(&self) -> VolumeGeometry {
        self.geometry.clone()
    }

    async fn ensure_connected(&mut self) -> Result<(), TransportError> {
        if self.client.is_some() {
            return Ok(());
        }

        let client = connect_client(
            &self.endpoint_url,
            self.config.connect_timeout,
            self.config.rpc_timeout,
        )
        .await
        .map_err(|err| TransportError::retryable(format!("reconnect failed: {err}")))?;
        self.client = Some(client);
        Ok(())
    }

    async fn run_read_blocks_with_retry(
        &mut self,
        start_lba: u64,
        block_count: u32,
    ) -> Result<Vec<u8>, TransportError> {
        let attempts = self.config.retry.capped_attempts();

        for attempt in 1..=attempts {
            if let Err(err) = self.ensure_connected().await {
                if attempt < attempts {
                    sleep(self.config.retry.delay_for_attempt(attempt)).await;
                    continue;
                }
                return Err(err);
            }

            let request = workflowservicepb::ReadBlocksRequest {
                namespace: self.config.namespace.clone(),
                volume_id: self.config.volume_id.clone(),
                start_lba,
                block_count,
            };

            let result = {
                let client = self.client.as_mut().expect("client should be initialized");
                timeout(self.config.rpc_timeout, client.read_blocks(request)).await
            };

            match result {
                Ok(Ok(response)) => return Ok(response.into_inner().data),
                Ok(Err(status)) => {
                    let retryable = is_retryable_status(status.code());
                    self.client = None;
                    if retryable && attempt < attempts {
                        sleep(self.config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(status_to_transport("ReadBlocks", status, retryable));
                }
                Err(_) => {
                    self.client = None;
                    if attempt < attempts {
                        sleep(self.config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(TransportError::retryable(format!(
                        "ReadBlocks timed out after {} attempts",
                        attempts
                    )));
                }
            }
        }

        Err(TransportError::retryable(
            "ReadBlocks exhausted retry loop unexpectedly",
        ))
    }

    async fn run_write_batch_rpc_with_retry(
        &mut self,
        writes: &[BlockWrite],
    ) -> Result<(), TransportError> {
        let Some((payload_bytes, min_lba, max_lba)) = summarize_writes(writes) else {
            return Ok(());
        };
        let attempts = self.config.retry.capped_attempts();

        for attempt in 1..=attempts {
            if let Err(err) = self.ensure_connected().await {
                if attempt < attempts {
                    sleep(self.config.retry.delay_for_attempt(attempt)).await;
                    continue;
                }
                return Err(err);
            }

            let request = workflowservicepb::WriteBatchRequest {
                namespace: self.config.namespace.clone(),
                volume_id: self.config.volume_id.clone(),
                writes: writes
                    .iter()
                    .map(|write| workflowservicepb::BlockWrite {
                        lba: write.lba,
                        data: write.data.clone(),
                    })
                    .collect(),
            };
            let request_encoded_len = prost::Message::encoded_len(&request);
            if attempt == 1 {
                eprintln!(
                    "write_batch send: writes={} payload_bytes={} request_bytes={} lba_range=[{}..={}] totals_before[rpc={},payload_bytes={},blocks={}]",
                    writes.len(),
                    payload_bytes,
                    request_encoded_len,
                    min_lba,
                    max_lba,
                    self.write_rpc_sent,
                    self.write_payload_bytes_sent,
                    self.write_blocks_sent
                );
            }

            let result = {
                let client = self.client.as_mut().expect("client should be initialized");
                timeout(self.config.rpc_timeout, client.write_batch(request)).await
            };

            match result {
                Ok(Ok(_response)) => {
                    self.write_rpc_sent = self.write_rpc_sent.saturating_add(1);
                    self.write_payload_bytes_sent = self
                        .write_payload_bytes_sent
                        .saturating_add(u64::try_from(payload_bytes).unwrap_or(u64::MAX));
                    self.write_blocks_sent = self
                        .write_blocks_sent
                        .saturating_add(u64::try_from(writes.len()).unwrap_or(u64::MAX));
                    return Ok(());
                }
                Ok(Err(status)) => {
                    let retryable = is_retryable_status(status.code());
                    eprintln!(
                        "write_batch failure: attempt={}/{} retryable={} code={} message={} writes={} payload_bytes={} request_bytes={} lba_range=[{}..={}]",
                        attempt,
                        attempts,
                        retryable,
                        status.code(),
                        status.message(),
                        writes.len(),
                        payload_bytes,
                        request_encoded_len,
                        min_lba,
                        max_lba
                    );
                    self.client = None;
                    if retryable && attempt < attempts {
                        sleep(self.config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(status_to_transport("WriteBatch", status, retryable));
                }
                Err(_) => {
                    eprintln!(
                        "write_batch timeout: attempt={}/{} writes={} payload_bytes={} request_bytes={} lba_range=[{}..={}]",
                        attempt,
                        attempts,
                        writes.len(),
                        payload_bytes,
                        request_encoded_len,
                        min_lba,
                        max_lba
                    );
                    self.client = None;
                    if attempt < attempts {
                        sleep(self.config.retry.delay_for_attempt(attempt)).await;
                        continue;
                    }
                    return Err(TransportError::retryable(format!(
                        "WriteBatch timed out after {} attempts",
                        attempts
                    )));
                }
            }
        }

        Err(TransportError::retryable(
            "WriteBatch exhausted retry loop unexpectedly",
        ))
    }

    async fn run_write_batch_with_retry(
        &mut self,
        writes: &[BlockWrite],
    ) -> Result<(), TransportError> {
        if writes.is_empty() {
            return Ok(());
        }

        for chunk in writes.chunks(MAX_WRITES_PER_WRITE_BATCH_RPC) {
            self.run_write_batch_rpc_with_retry(chunk).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BackendTransport for VolumeSession {
    async fn read_blocks(
        &mut self,
        start_lba: u64,
        block_count: u32,
    ) -> Result<Vec<u8>, TransportError> {
        self.run_read_blocks_with_retry(start_lba, block_count)
            .await
    }

    async fn write_batch(&mut self, writes: &[BlockWrite]) -> Result<(), TransportError> {
        self.run_write_batch_with_retry(writes).await
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        self.client = None;
        Ok(())
    }
}

fn is_retryable_status(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Unknown
            | Code::Internal
            | Code::Aborted
            | Code::ResourceExhausted
            | Code::Cancelled
    )
}

fn status_to_transport(operation: &str, status: tonic::Status, retryable: bool) -> TransportError {
    let message = format!(
        "{} failed with {} ({})",
        operation,
        status.code(),
        status.message()
    );
    if retryable {
        TransportError::retryable(message)
    } else {
        TransportError::terminal(message)
    }
}

async fn connect_client(
    endpoint_url: &str,
    connect_timeout: Duration,
    rpc_timeout: Duration,
) -> Result<WorkflowServiceClient, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(endpoint_url.to_string())?
        .connect_timeout(connect_timeout)
        .timeout(rpc_timeout);
    let channel = endpoint.connect().await?;
    Ok(WorkflowServiceClient::new(channel))
}

fn normalize_endpoint(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    }
}

fn summarize_writes(writes: &[BlockWrite]) -> Option<(usize, u64, u64)> {
    if writes.is_empty() {
        return None;
    }
    let mut payload_bytes = 0_usize;
    let mut min_lba = u64::MAX;
    let mut max_lba = 0_u64;
    for write in writes {
        payload_bytes = payload_bytes.saturating_add(write.data.len());
        min_lba = min_lba.min(write.lba);
        max_lba = max_lba.max(write.lba);
    }
    Some((payload_bytes, min_lba, max_lba))
}
