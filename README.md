# temporal-nbd

Rust client for Temporal CHASM blockdevice over `workflowservice`.

See also:
- `DESIGN.md` for architecture.
- `CONTRIBUTING.md` for branch pairing, build, and remote test workflows.

## Runtime

- `create-volume`: Provision one Temporal volume.
- `attach`: Attach one Temporal volume to one Linux NBD device and serve `READ`, `WRITE`, `FLUSH`, `DISC`.
- `temporal-ublk attach` (Phase 1): runs single-volume ublk lifecycle supervision (preflight, volume open, runtime start/stop orchestration) and waits for shutdown signals.
- `temporal-ublk serve` (Phase 1): Unix-socket control-plane manager implementing `AddDevice`/`RemoveDevice`/`ListDevices`/`Health` with idempotency and per-device lifecycle runtime supervision.
- `create_volume_smoke` test: workflowservice Create/Open/Write/Read contract validation lives in `tests/create_volume_smoke.rs` (validation, not provisioning flow).

## Prereqs

- Temporal server running with blockdevice module exposed via frontend/workflowservice.
- Temporal namespace (name, not namespace ID).
- Linux host for attach runtime.
- NBD kernel module loaded (`sudo modprobe nbd max_part=0`).
- NBD device node available (for example `/dev/nbd0`).
- Permission to open/configure `/dev/nbdX` (typically root).
- Build-time proto source: set `TEMPORAL_API_REPO` only when compiling if your API checkout is not at `../api`.

## Smoke E2E Test

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

Run:

```bash
cargo test --test create_volume_smoke -- --nocapture
```

## Create Volume Mode

Create required options:

- namespace (`--namespace` or `TEMPORAL_NAMESPACE`)
- volume id (`--volume-id` or `TEMPORAL_VOLUME_ID`)

Typical run:

```bash
cargo run -- create-volume \
  --namespace default \
  --volume-id my-volume \
  --size-bytes 1073741824
```

Create flags (all also support env vars):

- `--frontend-endpoint` / `TEMPORAL_FRONTEND_ENDPOINT` (default `127.0.0.1:7233`)
- `--size-bytes` / `TEMPORAL_VOLUME_SIZE_BYTES` (default `1073741824`)
- `--block-size-bytes` / `TEMPORAL_VOLUME_BLOCK_SIZE_BYTES` (default `0`, let server choose)
- `--connect-timeout-secs` / `TEMPORAL_CONNECT_TIMEOUT_SECS` (default `5`)
- `--rpc-timeout-secs` / `TEMPORAL_RPC_TIMEOUT_SECS` (default `5`)
- `--request-id` / `TEMPORAL_CREATE_REQUEST_ID` (default: auto-generated)
- `--if-not-exists` / `TEMPORAL_CREATE_IF_NOT_EXISTS` (default `false`)

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

`attach` opens an existing volume and fails if it does not exist.

## Using Precompiled Binaries

If you already have a built `temporal-nbd` binary, use it directly instead of `cargo run`.

Example:

```bash
export TEMPORAL_NBD_BIN=/path/to/temporal-nbd
export TEMPORAL_FRONTEND_ENDPOINT=127.0.0.1:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_VOLUME_ID=my-volume

# Create volume once
"$TEMPORAL_NBD_BIN" create-volume \
  --frontend-endpoint "$TEMPORAL_FRONTEND_ENDPOINT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --volume-id "$TEMPORAL_VOLUME_ID" \
  --size-bytes 1073741824

# Attach
sudo "$TEMPORAL_NBD_BIN" attach \
  --frontend-endpoint "$TEMPORAL_FRONTEND_ENDPOINT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --volume-id "$TEMPORAL_VOLUME_ID" \
  --nbd-device /dev/nbd0
```

If you built locally with Cargo, the binary is usually at:
- `target/debug/temporal-nbd`
- `target/release/temporal-nbd`
- `target/debug/temporal-ublk`
- `target/release/temporal-ublk`

## UBLK Phase 1

Run `serve`:

```bash
cargo run --bin temporal-ublk -- serve \
  --frontend-endpoint 127.0.0.1:7233 \
  --namespace default \
  --ublk-control-device /dev/ublk-control \
  --control-socket /tmp/temporal-ublk.sock \
  --max-devices 64
```

Run `attach` (single-volume foundation path):

```bash
cargo run --bin temporal-ublk -- attach \
  --frontend-endpoint 127.0.0.1:7233 \
  --namespace default \
  --volume-id my-volume \
  --ublk-control-device /dev/ublk-control
```

Protocol details are in `docs/ublk/phase0-interface-contracts.md`.

## Use the Device End-to-End

Example workflow for `/dev/nbd0`:

1. Load NBD support and choose a volume ID.

```bash
sudo modprobe nbd max_part=0
export TEMPORAL_FRONTEND_ENDPOINT=127.0.0.1:7233
export TEMPORAL_NAMESPACE=default
export TEMPORAL_VOLUME_ID=my-volume
```

2. Create the volume once.

```bash
cargo run -- create-volume \
  --frontend-endpoint "$TEMPORAL_FRONTEND_ENDPOINT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --volume-id "$TEMPORAL_VOLUME_ID" \
  --size-bytes 1073741824
```

3. Start attach mode in terminal A.

```bash
sudo cargo run -- attach \
  --frontend-endpoint "$TEMPORAL_FRONTEND_ENDPOINT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --volume-id "$TEMPORAL_VOLUME_ID" \
  --nbd-device /dev/nbd0
```

4. Format, mount, and write data in terminal B.

```bash
sudo mkfs.ext4 -F /dev/nbd0
sudo mkdir -p /mnt/temporal-nbd
sudo mount /dev/nbd0 /mnt/temporal-nbd
sudo chown "$(id -u):$(id -g)" /mnt/temporal-nbd
echo "hello from temporal-nbd" > /mnt/temporal-nbd/hello.txt
sync
sudo umount /mnt/temporal-nbd
```

5. Detach by stopping attach mode (`Ctrl-C` in terminal A), then re-attach and verify.

```bash
sudo cargo run -- attach \
  --frontend-endpoint "$TEMPORAL_FRONTEND_ENDPOINT" \
  --namespace "$TEMPORAL_NAMESPACE" \
  --volume-id "$TEMPORAL_VOLUME_ID" \
  --nbd-device /dev/nbd0

sudo mount /dev/nbd0 /mnt/temporal-nbd
cat /mnt/temporal-nbd/hello.txt
sudo umount /mnt/temporal-nbd
```

`attach` is single-volume-per-process and intended for one active writer per volume in this POC.

Attach flags (all also support env vars):

- `--frontend-endpoint` / `TEMPORAL_FRONTEND_ENDPOINT` (default `127.0.0.1:7233`)
- `--nbd-device` / `TEMPORAL_NBD_DEVICE` (default `/dev/nbd0`)
- `--connect-timeout-secs` / `TEMPORAL_CONNECT_TIMEOUT_SECS` (default `5`)
- `--rpc-timeout-secs` / `TEMPORAL_RPC_TIMEOUT_SECS` (default `5`)
- `--retry-max-attempts` / `TEMPORAL_RETRY_MAX_ATTEMPTS` (default `8`)
- `--retry-initial-backoff-ms` / `TEMPORAL_RETRY_INITIAL_BACKOFF_MS` (default `150`)
- `--retry-max-backoff-ms` / `TEMPORAL_RETRY_MAX_BACKOFF_MS` (default `2000`)
- `--retry-jitter-ratio` / `TEMPORAL_RETRY_JITTER_RATIO` (default `0.2`)
- `--dirty-high-water-blocks` / `TEMPORAL_DIRTY_HIGH_WATER_BLOCKS` (default `4096`)
- `--flush-retry-deadline-secs` / `TEMPORAL_FLUSH_RETRY_DEADLINE_SECS` (default `20`)
- `--flush-retry-interval-ms` / `TEMPORAL_FLUSH_RETRY_INTERVAL_MS` (default `200`)
- `--engine-queue-capacity` / `TEMPORAL_ENGINE_QUEUE_CAPACITY` (default `1024`)
- `--nbd-timeout-secs` / `TEMPORAL_NBD_TIMEOUT_SECS` (default `30`)

## Runtime Semantics (Attach)

- Dirty cache is keyed by LBA and coalesces overwrite writes (last write wins).
- Reads fetch backend blocks and overlay dirty cache (read-your-writes).
- `FLUSH` persists dirty blocks with chunked `WriteBatch` calls (`<= 512` writes per engine batch, sub-chunked to `<= 64` writes per RPC).
- High-water pressure triggers synchronous flush; if retry budget/deadline is exhausted, writes/flush fail with I/O error.
- Dirty data is never silently dropped; failed flush keeps dirty entries for retry.
- Transient RPC failures use bounded retry with exponential backoff and reconnect.
- `SIGINT`/`SIGTERM` triggers graceful detach flow with best-effort final flush.

## Validation

```bash
cargo test
cargo test --test create_volume_smoke -- --nocapture
cargo test --test e2e_sqlite -- --ignored --nocapture
# kernel-gated: attach + mkfs + mount + write + unmount + detach + re-attach + remount verify
cargo test --test e2e_nbd_mount -- --ignored --nocapture
```

## Remote Prebuilt Tests (No Rust on Remote)

Use the helper script to build tests locally, copy executables to a remote host/container, and run them there:

```bash
scripts/run_prebuilt_tests_remote.sh \
  --remote <user@host> \
  --cargo-arg --test --cargo-arg create_volume_smoke \
  --remote-env TEMPORAL_NAMESPACE=default

# kernel-gated NBD mount test against Temporal server on this host
scripts/run_prebuilt_tests_remote.sh \
  --remote <user@host> \
  --cargo-arg --test --cargo-arg e2e_nbd_mount \
  --run-ignored \
  --remote-env TEMPORAL_NAMESPACE=default
```

By default, the script sets remote `TEMPORAL_FRONTEND_ENDPOINT` to this host's IPv4 (`<this-host-ip>:7233`) so remote tests can target a Temporal server running here. Disable that behavior with `--no-auto-host-endpoint`.
Remote SSH target must be provided explicitly via `--remote` or `REMOTE_SSH_TARGET`.

## Troubleshooting

- Missing `/dev/nbdX`: create/load NBD support (`modprobe nbd`) and verify device node exists.
- Missing `/sys/module/nbd`: module not loaded; run `sudo modprobe nbd max_part=0`.
- Permission denied on `/dev/nbdX`: rerun with sufficient privileges (typically root).
- Device busy: ensure no existing consumer is attached; disconnect old session before attach.
