# temporal-nbd

Rust client for Temporal CHASM blockdevice over `workflowservice`.

## Modes

- `smoke` (default): Create/Open/Write/Read contract validation against frontend `workflowservice`.
- `attach`: Attach one Temporal volume to one Linux NBD device and serve `READ`, `WRITE`, `FLUSH`, `DISC`.

## Prereqs

- Temporal server running with blockdevice module exposed via frontend/workflowservice.
- Temporal namespace (name, not namespace ID).
- Linux host for `attach` mode.
- NBD kernel module loaded (`sudo modprobe nbd max_part=0`).
- NBD device node available (for example `/dev/nbd0`).
- Permission to open/configure `/dev/nbdX` (typically root).

## Smoke Mode

Required env:

- `TEMPORAL_NAMESPACE`

Optional env:

- `TEMPORAL_FRONTEND_ENDPOINT` (default: `127.0.0.1:7233`)
- `TEMPORAL_VOLUME_ID` (default: generated)
- `TEMPORAL_VOLUME_SIZE_BYTES` (default: `1073741824`)
- `TEMPORAL_VOLUME_BLOCK_SIZE_BYTES` (default: `0`)
- `TEMPORAL_VOLUME_ID_FILE` (default: `./phase2-volume-id.txt`)
- `TEMPORAL_CONNECT_TIMEOUT_SECS` (default: `5`)
- `TEMPORAL_RPC_TIMEOUT_SECS` (default: `5`)
- `TEMPORAL_API_REPO` path to Temporal API repo (default: `../api`)

Run:

```bash
cargo run
# or
cargo run -- smoke
```

## Attach Mode

Attach required options:

- namespace (`--namespace` or `TEMPORAL_NAMESPACE`)
- volume id (`--volume-id` or `TEMPORAL_VOLUME_ID`)

Typical run:

```bash
sudo cargo run -- attach \
  --namespace default \
  --volume-id my-volume \
  --nbd-device /dev/nbd0
```

Attach flags (all also support env vars):

- `--frontend-endpoint` / `TEMPORAL_FRONTEND_ENDPOINT` (default `127.0.0.1:7233`)
- `--nbd-device` / `TEMPORAL_NBD_DEVICE` (default `/dev/nbd0`)
- `--connect-timeout-secs` / `TEMPORAL_CONNECT_TIMEOUT_SECS` (default `5`)
- `--rpc-timeout-secs` / `TEMPORAL_RPC_TIMEOUT_SECS` (default `5`)
- `--retry-max-attempts` / `TEMPORAL_RETRY_MAX_ATTEMPTS` (default `8`)
- `--retry-initial-backoff-ms` / `TEMPORAL_RETRY_INITIAL_BACKOFF_MS` (default `150`)
- `--retry-max-backoff-ms` / `TEMPORAL_RETRY_MAX_BACKOFF_MS` (default `2000`)
- `--dirty-high-water-blocks` / `TEMPORAL_DIRTY_HIGH_WATER_BLOCKS` (default `4096`)
- `--flush-retry-deadline-secs` / `TEMPORAL_FLUSH_RETRY_DEADLINE_SECS` (default `20`)
- `--flush-retry-interval-ms` / `TEMPORAL_FLUSH_RETRY_INTERVAL_MS` (default `200`)
- `--engine-queue-capacity` / `TEMPORAL_ENGINE_QUEUE_CAPACITY` (default `1024`)
- `--nbd-timeout-secs` / `TEMPORAL_NBD_TIMEOUT_SECS` (default `30`)

## Runtime Semantics (Attach)

- Dirty cache is keyed by LBA and coalesces overwrite writes (last write wins).
- Reads fetch backend blocks and overlay dirty cache (read-your-writes).
- `FLUSH` persists dirty blocks with chunked `WriteBatch` calls (`<= 512` writes per call).
- High-water pressure triggers synchronous flush; if retry budget/deadline is exhausted, writes/flush fail with I/O error.
- Dirty data is never silently dropped; failed flush keeps dirty entries for retry.
- Transient RPC failures use bounded retry with exponential backoff and reconnect.
- `SIGINT`/`SIGTERM` triggers graceful detach flow with best-effort final flush.

## Validation

```bash
cargo test
cargo test --test create_volume_smoke -- --nocapture
cargo test --test e2e_sqlite -- --ignored --nocapture
# kernel-gated: attach + mkfs + mount + unmount + remount roundtrip
cargo test --test e2e_nbd_mount -- --ignored --nocapture
```

## Remote Prebuilt Tests (No Rust on Remote)

Use the helper script to build tests locally, copy executables to a remote host/container, and run them there:

```bash
scripts/run_prebuilt_tests_remote.sh \
  --remote dev@192.168.64.13 \
  --cargo-arg --test --cargo-arg create_volume_smoke \
  --remote-env TEMPORAL_NAMESPACE=default

# kernel-gated NBD mount test against Temporal server on this host
scripts/run_prebuilt_tests_remote.sh \
  --remote dev@192.168.64.13 \
  --cargo-arg --test --cargo-arg e2e_nbd_mount \
  --run-ignored \
  --remote-env TEMPORAL_NAMESPACE=default
```

By default, the script sets remote `TEMPORAL_FRONTEND_ENDPOINT` to this host's IPv4 (`<this-host-ip>:7233`) so remote tests can target a Temporal server running here. Disable that behavior with `--no-auto-host-endpoint`.

## Troubleshooting

- Missing `/dev/nbdX`: create/load NBD support (`modprobe nbd`) and verify device node exists.
- Missing `/sys/module/nbd`: module not loaded; run `sudo modprobe nbd max_part=0`.
- Permission denied on `/dev/nbdX`: rerun with sufficient privileges (typically root).
- Device busy: ensure no existing consumer is attached; disconnect old session before attach.
