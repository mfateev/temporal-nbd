# Temporal Blockdevice Linux Client Design Overview

## Purpose

`temporal-nbd` and `temporal-ublk` expose a Temporal blockdevice volume as a Linux block device by translating kernel block I/O requests into Temporal `workflowservice` volume RPCs.

The target workflow is:
1. create/open a Temporal volume,
2. attach as a Linux block device,
3. format/mount/read/write through the filesystem,
4. unmount + detach,
5. re-attach and read data again.

## Status

- Implemented: NBD frontend (`/dev/nbdX`).
- Designed in this document: UBLK frontend (`/dev/ublkbN`) as a separate attach binary with single-device and multi-device modes.

## High-Level Architecture

The runtime has four layers:

1. Linux block frontend binary (`temporal-nbd` today, `temporal-ublk` planned)
- Receives kernel block operations (`READ`, `WRITE`, `FLUSH`, `DISC`/teardown).
- Presents a Linux block device node to filesystems and tools.

2. Frontend adapter (current `src/nbd.rs`; planned `src/attach/nbd.rs` + `src/attach/ublk.rs`)
- Owns kernel-facing resources and protocol translation.
- Converts frontend-specific requests into shared bridge commands.
- Returns Linux errno values back to the kernel.

3. Bridge + engine (`src/bridge.rs`, `src/engine.rs`)
- Converts byte offsets into block ranges.
- Maintains in-memory dirty cache keyed by LBA.
- Enforces alignment and bounds.
- Implements read overlay and flush semantics.

4. Temporal session/RPC layer (`src/session.rs`)
- Opens volume metadata (`OpenVolume`).
- Calls `ReadBlocks` and `WriteBatch`.
- Applies retry/backoff and reconnect logic for retryable failures.

Current wiring is in `src/attach.rs`; planned split is shared attach core plus per-frontend wiring modules.

## Existing Request Flow (NBD)

### Read Path

1. Kernel issues read request.
2. Bridge validates alignment/range and maps to LBA range.
3. Engine fetches backend blocks with `ReadBlocks` (chunked to request limits).
4. Engine overlays dirty cache entries so reads observe local unflushed writes.
5. Frontend adapter replies to kernel.

### Write Path

1. Kernel issues write request.
2. Bridge validates alignment/range and maps to blocks.
3. Engine stores blocks in dirty cache (`BTreeMap<LBA, block>`), preserving ascending LBA iteration during flush.
4. Rewrites to same LBA coalesce (last write wins before flush).

### Flush Path

1. Kernel issues `FLUSH`.
2. Engine drains dirty cache via `WriteBatch` calls.
3. Writes are chunked to API contract limits.
4. `FLUSH` ACK is sent only after all chunks succeed.
5. On failure, dirty blocks remain buffered and operation returns I/O error.

### Disconnect Path

1. Kernel or user requests disconnect/teardown.
2. Attach flow attempts best-effort final flush.
3. Frontend device is detached and process exits.

## UBLK Support Design

### Goals

- Add a second Linux frontend driver using UBLK without changing core cache/flush correctness.
- Keep the same Temporal RPC semantics (`OpenVolume`, `ReadBlocks`, `WriteBatch`).
- Preserve NBD behavior while introducing separate binaries for NBD and UBLK attach.
- Support a UBLK multi-device mode so one process can serve many Firecracker VM disks concurrently.

### Non-goals (initial UBLK milestone)

- Multi-writer semantics.
- Dynamic queue auto-tuning.
- Implementing optional block ops (discard/write-zeroes) beyond explicit rejection.
- Multi-attach of the same `volume_id` across multiple devices/processes.

### Binary Split and Shared Core

Use separate binaries instead of a runtime `--driver` switch:

- `temporal-nbd`: NBD attach only.
- `temporal-ublk`: UBLK attach only.

Both binaries share the same core attach flow for:
- volume open/session setup,
- bridge request channel and engine loop,
- retry/flush/disconnect semantics.

Proposed shape:

```rust
pub struct FrontendContext {
    pub geometry: VolumeGeometry,
    pub requests: BridgeRequestTx,
    pub shutdown: CancellationToken,
}

#[async_trait]
pub trait BlockFrontend {
    fn preflight(&self) -> anyhow::Result<()>;
    async fn serve(self, ctx: FrontendContext) -> anyhow::Result<()>;
}

pub async fn run_attach_nbd(config: NbdAttachConfig) -> anyhow::Result<()>;
pub async fn run_attach_ublk(config: UblkAttachConfig) -> anyhow::Result<()>;
```

Notes:
- Frontend abstraction remains useful for shared lifecycle code, but each binary has its own config type and CLI.
- Existing NBD logic can be moved with minimal behavior change.

### CLI and Config Surface

Split CLI by binary:

- `temporal-nbd`:
  - keeps existing `create-volume`,
  - keeps existing NBD `attach` flags (`--nbd-device`, `--nbd-timeout-secs`, etc).
- `temporal-ublk`:
  - supports `attach` for single-device UBLK with UBLK-only flags:
    - `--ublk-control-device` (default `/dev/ublk-control`),
    - `--ublk-device-id` (optional fixed id; omitted means allocate),
    - `--ublk-queues` (default `1` for MVP),
    - `--ublk-queue-depth` (default `128`),
    - `--ublk-timeout-secs` (default aligned with NBD timeout semantics).
  - adds `serve` for multi-device mode:
    - `--control-socket` (unix socket for add/remove/list control),
    - `--max-devices` (hard cap for one process),
    - `--default-ublk-queues`,
    - `--default-ublk-queue-depth`,
    - `--default-ublk-timeout-secs`,
    - `--graceful-drain-timeout-secs` (wait for flush/disconnect before forcing detach),
    - `--force-detach-timeout-secs` (upper bound for forced teardown),
    - `--metrics-listen` (optional metrics endpoint),
    - `--ready-file` (optional readiness file path).

Shared flags/env stay consistent across binaries for Temporal connectivity (`frontend endpoint`, `namespace`, `volume id`, retry/timeout knobs).

Validation rules:
- No `--driver` or `TEMPORAL_ATTACH_DRIVER`.
- Each binary validates only its own attach flags.
- Existing NBD invocations continue to work unchanged.
- `temporal-ublk serve` rejects duplicate `volume_id` attachments in one process unless explicitly overridden by a future unsafe/debug flag.

### UBLK Multi-Device Mode (Firecracker-Oriented)

`temporal-ublk serve` runs as a long-lived manager process and controls many independent UBLK devices.

Model:
- One process-level supervisor.
- One attachment state machine per device.
- One engine loop per device (same bridge/engine/session semantics as single-device mode).
- Shared Tokio runtime and connection pools where safe.

Control operations (over unix socket API):
- `AddDevice`: attach `volume_id` and create/bind one `/dev/ublkbN`.
- `RemoveDevice`: flush + disconnect one attachment.
- `ListDevices`: enumerate active devices and health/state.

Per-device state machine:
- `Allocating` -> `OpeningVolume` -> `Serving` -> `Draining` -> `Detached` (or `Failed`).

Failure isolation:
- Device-scoped failures transition only that device to `Failed`.
- Supervisor process stays alive and continues serving other devices.
- Fatal process-level failures still affect all devices; this is an acknowledged tradeoff of single-process operation.

### Control Socket Protocol

`temporal-ublk serve` control-plane API is a stable local contract for VM orchestration.

Transport and framing:
- Unix domain stream socket at `--control-socket`.
- Message framing: 4-byte big-endian length prefix + UTF-8 JSON payload.
- One request maps to one response; both carry `request_id` for correlation.

Request envelope:
- `version` (initially `v1`)
- `request_id` (caller-provided)
- `op` (`AddDevice`, `RemoveDevice`, `ListDevices`, `Health`)
- `body` (operation payload)

Response envelope:
- `version`
- `request_id`
- `ok` (`true`/`false`)
- `result` (operation-specific payload when `ok=true`)
- `error` (`code`, `message`, optional `details` when `ok=false`)

`AddDevice` result must include:
- `device_id` (assigned numeric UBLK id),
- `device_path` (for example `/dev/ublkb7`),
- `volume_id`,
- `state`.

If `AddDevice` omits `ublk_device_id`, the manager allocates one and returns it in the response. The returned `device_path` is stable for that attachment lifetime and is the value to hand to Firecracker.

### UBLK I/O Runtime Model (io_uring)

UBLK frontend uses native `io_uring`; it does not depend on `tokio-uring` in the first implementation.

Execution model:
- `attach` mode: one dedicated io_uring thread for the single device.
- `serve` mode: one dedicated io_uring thread per device for v1 (isolation-first design).
- Tokio tasks handle control-plane, session management, and bridge/engine async work.
- io_uring threads and Tokio tasks communicate via bounded channels.

Rationale:
- Keep io_uring lifecycle explicit and decoupled from Tokio scheduler behavior.
- Preserve per-device fault isolation in multi-device mode.
- Allow future optimization to shared io_uring workers after baseline correctness.

### Buffer Management

Hot-path buffer management is frontend-owned:
- Per-device/per-queue fixed-size buffer pools are pre-allocated at attach time.
- Request parsing and completion reuse pooled buffers; steady-state I/O should avoid per-request heap allocation.
- Read/write payload copies between frontend buffers and engine payloads remain explicit in v1 for correctness and API simplicity.

Future optimization (not in initial milestone): extend bridge interfaces for zero-copy style handoff where safe.

### Observability and Health

Per-device metrics (tagged by `device_id`, `volume_id`):
- read/write/flush op counts and bytes,
- latency histograms (read, write, flush),
- queue depth and in-flight requests,
- dirty blocks and flush retry counters,
- error counters by errno/category.

Process-level metrics:
- active device count,
- control-plane request counts/failures,
- forced detach count,
- supervisor uptime.

Health/readiness:
- `Health` control operation returns process health and degraded device list.
- Optional `--ready-file` is created after control socket bind + supervisor startup and removed on shutdown.
- If `--metrics-listen` is configured, expose metrics and a simple readiness signal from the same endpoint.

### UBLK Request Mapping

Map UBLK operations to existing bridge commands:

- read -> `BridgeCommand::Read`
- write -> `BridgeCommand::Write`
- flush/fua -> `BridgeCommand::Flush`
- teardown/stop -> `BridgeCommand::Disconnect`
- unsupported ops (discard/write-zeroes) -> `EOPNOTSUPP` initially

Range and alignment checks remain centralized in bridge/engine.

### Concurrency Model

UBLK can issue requests from multiple queues. The initial design keeps engine semantics unchanged:

- For each device: io_uring worker thread handles queue submission/completion.
- For each device: frontend queue workers forward requests through that device's bridge channel.
- For each device: engine loop remains the serialization point for that device's block semantics.
- Reply completion can remain per-request and independent even with concurrent queue workers.
- Across devices: attachments run concurrently and independently under the same process supervisor.

This avoids correctness regressions while still enabling UBLK integration. Shared io_uring workers and deeper parallel engine designs are follow-ups.

### Lifecycle and Shutdown

Single-device mode (`temporal-ublk attach`):
1. Binary-specific preflight checks.
2. Open Temporal volume and build engine.
3. Initialize one frontend device.
4. Serve requests until signal/device disconnect/fatal error.
5. Cancel shutdown token.
6. Best-effort `BridgeCommand::Disconnect` + flush semantics.
7. Destroy/cleanup frontend resources.

Multi-device mode (`temporal-ublk serve`):
1. Process-level preflight checks and supervisor startup.
2. Accept control-plane requests (`AddDevice`, `RemoveDevice`, `ListDevices`).
3. For each `AddDevice`, create per-device session/engine/frontend tasks.
4. On `RemoveDevice`, drain and detach only that device.
5. On process shutdown signal, stop accepting new requests, then drain/detach all active devices.
6. Graceful phase: wait up to `--graceful-drain-timeout-secs` per device for `Disconnect`/flush completion.
7. Forced phase (on timeout): issue hard UBLK detach (`DEL_DEV`-equivalent), cancel device tasks, and mark device status as `ForceDetached`.
8. Wait up to `--force-detach-timeout-secs` for forced teardown completion, then exit process.

Shutdown must remain idempotent and safe when partially initialized.

### Error Handling

- Frontend protocol or device failures map to `EIO` unless a more specific errno is known.
- Unsupported UBLK operations return `EOPNOTSUPP`.
- Engine validation errors remain `EINVAL`/`EIO` per existing mapping.
- In `serve` mode, device-level failure should not trigger global shutdown by default.
- Supervisor exposes per-device error state through `ListDevices`.
- Forced detach after graceful-timeout is reported explicitly (`ForceDetached`) and may lose unflushed dirty data; this must be surfaced in response payloads, logs, and metrics.

### Operational Constraints

- Linux-only attach mode.
- NBD runtime constraints remain unchanged.
- UBLK runtime requires kernel/user-space support for UBLK and access to control/device nodes (typically elevated privileges).
- Multi-device mode is still single process failure domain; deploy multiple manager processes if stronger blast-radius isolation is required.
- v1 uses one io_uring worker thread per device; `--max-devices` must be sized with host CPU and memory limits in mind.

## Consistency and Durability Model

- Single active writer per volume (POC constraint).
- Read-your-writes is guaranteed inside one attach session via dirty-cache overlay.
- Durability is flush-driven:
  - `sync` or filesystem flush triggers frontend flush semantics,
  - flush completion means writes reached Temporal via `WriteBatch`.
- Detach attempts final flush to reduce risk of lost buffered writes.

This model must be identical across NBD and UBLK frontends.
In multi-device mode, this model applies independently per attached volume/device.

## Implementation Plan

1. Refactor shared attach core and keep `temporal-nbd` dedicated
- Move session + bridge + engine orchestration into reusable library code.
- Preserve current NBD behavior and NBD-only CLI/config surface.
- Keep existing NBD tests passing through the refactor.

2. Add `temporal-ublk` single-device attach mode
- Implement UBLK preflight, device init, io_uring request loop, teardown.
- Translate UBLK ops into bridge commands.
- Implement frontend-owned buffer pools.

3. Add UBLK multi-device supervisor mode (`serve`)
- Add process-level manager for `AddDevice`/`RemoveDevice`/`ListDevices`/`Health`.
- Add per-device lifecycle state machine and task supervision.
- Enforce device and volume uniqueness constraints per process.
- Return stable `device_id` + `device_path` in `AddDevice` responses.

4. Add observability and shutdown policy hardening
- Add per-device/process metrics and readiness/health signals.
- Implement graceful-drain then forced-detach policy with explicit `ForceDetached` reporting.

5. Validation and hardening
- Unit tests for per-binary config validation and control protocol framing.
- Unit tests for multi-device supervisor state transitions and failure isolation.
- Kernel-gated `e2e_ublk_mount` test mirroring NBD durability test.
- Add kernel-gated `e2e_ublk_multi_device` test (at least two concurrent devices).
- Ensure existing `e2e_nbd_mount` remains green.

## Testing Strategy

- Unit tests:
  - per-binary CLI/config parsing and validation,
  - control socket protocol framing and schema validation,
  - bridge command mapping for UBLK ops,
  - frontend shutdown idempotency,
  - multi-device supervisor state transitions and failure isolation,
  - graceful-timeout -> forced-detach transitions and status reporting.
- Integration tests:
  - keep current NBD tests unchanged,
  - add UBLK kernel-gated durability test with `#[ignore]` by default.
  - add UBLK kernel-gated concurrent multi-device attach/detach test with `#[ignore]` by default.
- Manual verification:
  - single-device attach with `temporal-ublk attach`,
  - multi-device attach/detach in `temporal-ublk serve` for multiple VM volumes.

## Key Files

Current:
- `src/main.rs`: CLI entrypoint.
- `src/attach.rs`: attach orchestration and shutdown handling.
- `src/nbd.rs`: Linux NBD ioctl and request/reply loop.
- `src/bridge.rs`: protocol-to-block translation.
- `src/engine.rs`: cache, flush, chunking, and core block engine semantics.
- `src/session.rs`: Temporal RPC session with retries and transport recovery.
- `tests/e2e_nbd_mount.rs`: kernel-gated NBD durability test.

Planned additions/refactors for UBLK:
- `src/bin/temporal-nbd.rs`: NBD-only CLI binary (may start as migration of current `main.rs`).
- `src/bin/temporal-ublk.rs`: UBLK-only CLI binary.
- `src/attach/core.rs`: shared attach orchestration.
- `src/attach/nbd.rs`: NBD adapter/wiring.
- `src/attach/ublk.rs`: UBLK adapter/wiring.
- `src/attach/ublk_manager.rs`: multi-device supervisor and control-plane handling.
- `src/control/protocol.rs`: control socket request/response framing and schema.
- `src/metrics.rs`: process/device metrics and readiness/health wiring.
- `tests/e2e_ublk_mount.rs`: kernel-gated UBLK durability test.
- `tests/e2e_ublk_multi_device.rs`: kernel-gated UBLK multi-device test.
