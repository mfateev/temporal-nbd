# Phase0-v1-Scope and Phase0-NonGoals

## v1 Scope Statement

UBLK v1 adds a Linux UBLK frontend to the existing Temporal blockdevice client stack while preserving current engine/session correctness guarantees:
- same `OpenVolume`, `ReadBlocks`, `WriteBatch` backend contract,
- same read-your-writes + flush durability behavior,
- NBD support retained in `temporal-nbd`,
- UBLK support provided through a dedicated `temporal-ublk` binary with `attach` (single device) and `serve` (multi-device manager) commands.

v1 targets correctness and operability first; throughput optimization and deep scheduler tuning are explicitly deferred.

## Required v1 Deliverables

1. Binary split:
- `temporal-nbd` remains NBD-only and backward compatible.
- `temporal-ublk` provides UBLK-only commands.

2. Single-device UBLK attach:
- open one volume, create one UBLK device, serve read/write/flush/disconnect, shutdown safely.

3. Multi-device manager mode:
- control socket API with `AddDevice`, `RemoveDevice`, `ListDevices`, `Health`.
- one attachment lifecycle per device.
- per-device failure isolation.

4. Data semantics parity:
- aligned/bounded validation,
- dirty cache overlay and flush semantics unchanged,
- retry/backoff behavior unchanged,
- explicit `EOPNOTSUPP` for unsupported discard/write-zeroes.

5. Operability minimum:
- per-device and process-level metrics,
- stable logs and error codes,
- readiness signal (ready file and/or metrics endpoint readiness).

## Phase0-NonGoals

| Non-goal | Rationale | Revisit trigger |
| --- | --- | --- |
| Multi-writer semantics for same volume | Existing consistency model assumes one active writer | Temporal backend adds explicit lease/fencing contract |
| Auto-tuning queue count/depth | Adds complexity before correctness baseline | Stable production workload profiles show fixed tuning is insufficient |
| Shared io_uring workers across devices | v1 favors fault isolation with one worker thread per device | CPU overhead proves unacceptable in baseline benchmarks |
| Discard/write-zeroes implementation | Not required for initial correctness parity | Workload explicitly depends on discard/zeroes semantics |
| Built-in crash reconciliation of orphaned devices | Requires persistent state + stronger orchestration model | Operations requires self-healing manager restart semantics |
| Cross-process dedup/idempotency store | Scope increase without immediate v1 requirement | Multi-manager deployments need global dedup guarantees |
| Rollout automation and migration orchestration tooling | Belongs to later production rollout phases | Phase 2+ deployment plan starts |

## Backward Compatibility Expectations

1. `temporal-nbd` CLI flags and behavior remain unchanged for existing NBD users.
2. Existing volumes do not require data migration to be attached through UBLK.
3. Shared engine/session semantics are identical across frontends.
4. Error class mapping remains stable (`EINVAL` for invalid requests, `EIO` for backend/transport failures unless a narrower errno is known).

## Migration Assumptions

1. One active writer per volume remains an operational requirement.
2. Cutover from NBD to UBLK is performed by detach then attach, not simultaneous dual attach to same volume.
3. Firecracker or equivalent consumers will receive and mount returned `/dev/ublkbN` device paths from manager responses.
4. Platform teams can provide hosts with UBLK-capable kernel/userspace prerequisites before rollout.
