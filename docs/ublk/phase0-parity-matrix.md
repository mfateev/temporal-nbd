# Phase0-Parity-Matrix

## Baseline and Legend

Baseline behavior is taken from:
- `DESIGN.md`
- `src/main.rs`, `src/attach.rs`, `src/nbd.rs`, `src/bridge.rs`, `src/engine.rs`, `src/session.rs`
- `tests/create_volume_smoke.rs`, `tests/e2e_sqlite.rs`, `tests/e2e_nbd_mount.rs`

Disposition values:
- `Required for v1`: must be implemented before Phase 1 exit.
- `Deferred`: intentionally out of v1, tracked in follow-on phases.
- `Not supported`: intentionally unsupported in v1 and surfaced explicitly.

## Control-Plane Capabilities (`AddDevice`, `RemoveDevice`, `ListDevices`, `Health`)

| Capability | Current `temporal-nbd` baseline | UBLK v1 disposition | Phase 0 pass/fail criterion |
| --- | --- | --- | --- |
| Provision volume (`create-volume`) | Supported in `temporal-nbd` binary via `CreateVolume` RPC | Required for v1 compatibility | Existing `temporal-nbd create-volume` behavior remains unchanged and passing `create_volume_smoke` test |
| Open existing volume for attach | `attach` opens volume with `OpenVolume` before serving I/O | Required for v1 | `temporal-ublk attach` fails fast on open failure; success path exposes usable Linux block device |
| Single-device attach | One process attaches one volume to `/dev/nbdX` | Required for v1 | `temporal-ublk attach` supports one attached volume/device per process with graceful shutdown |
| Multi-device attach manager | Not supported | Required for v1 | `temporal-ublk serve` can add/remove/list at least 2 concurrent devices in one process |
| Control op: `AddDevice` | Not supported | Required for v1 | `AddDevice` creates one attachment and returns stable `device_id` + `device_path`; replay with same idempotency key returns prior result |
| Control op: `RemoveDevice` (`detach`) | NBD disconnect path performs best-effort final flush and teardown | Required for v1 | Remove operation transitions device to drained or force-detached terminal state with explicit status |
| Control op: `ListDevices` | Not supported | Required for v1 | `ListDevices` returns state for all active and terminal-recent devices with unique `device_id` and `volume_id` |
| Control op: `Health` | Not supported | Required for v1 | `Health` operation returns process health plus degraded device list |
| Duplicate same-volume attach protection in one process | Implicitly single attach per process | Required for v1 | `AddDevice` rejects duplicate `volume_id` in same manager process |
| Explicit resize operation | Not supported in client | Deferred | No resize API in v1 manager contract; documented as follow-up |
| Explicit delete operation | Not supported in client | Deferred | No delete API in v1 manager contract; documented as follow-up |
| Crash-time reconciliation API | Not supported | Deferred | v1 documents restart behavior but does not auto-reconcile orphaned devices |

## Data-Plane Semantics

| Capability | Current `temporal-nbd` baseline | UBLK v1 disposition | Phase 0 pass/fail criterion |
| --- | --- | --- | --- |
| Read path | `READ` mapped to bridge read + backend `ReadBlocks` + dirty overlay | Required for v1 | Read-after-write consistency within one attach session is preserved |
| Write path | `WRITE` populates dirty cache keyed by LBA | Required for v1 | Writes coalesce per LBA before flush and remain visible to reads |
| Flush path | `FLUSH` calls engine flush with deadline/retry policy | Required for v1 | Flush success guarantees `WriteBatch` completion for all dirty blocks |
| Disconnect semantics | `DISC` triggers engine disconnect with final flush attempt | Required for v1 | Disconnect performs best-effort flush before transport disconnect |
| Alignment validation | Unaligned ranges rejected (`EINVAL`) | Required for v1 | Unaligned offset/length always rejected with `EINVAL` |
| Bounds validation | Out-of-range I/O rejected (`EINVAL`) | Required for v1 | Request end past volume size always rejected with `EINVAL` |
| Retry/backoff for retryable transport failures | Implemented in session with bounded exponential backoff + jitter | Required for v1 | Retryable failures are retried according to configured retry policy; terminal failures are surfaced |
| Dirty high-watermark pressure flush | Implemented in engine | Required for v1 | Exceeding dirty watermark triggers synchronous flush attempt |
| Unsupported op: discard | NBD side currently rejects unsupported commands | Not supported | UBLK discard returns `EOPNOTSUPP` and does not mutate data |
| Unsupported op: write-zeroes | Not implemented | Not supported | UBLK write-zeroes returns `EOPNOTSUPP` and does not mutate data |
| FUA semantics | NBD command flags are not modeled separately | Required for v1 | UBLK FUA write is handled as `Write` followed immediately by `Flush`; completion is returned only after both succeed |

## Failure and Recovery Semantics

| Scenario | Current `temporal-nbd` baseline | UBLK v1 disposition | Phase 0 pass/fail criterion |
| --- | --- | --- | --- |
| Retryable backend read/write error | Retried up to configured attempts | Required for v1 | Fault injection confirms retry path and eventual success/failure mapping |
| Persistent backend failure on flush | Flush retries until deadline then returns I/O error | Required for v1 | Flush deadline exhaustion returns `EIO`; dirty cache is not silently dropped |
| Frontend request with invalid size/data | Rejected before backend call | Required for v1 | Validation failures never invoke backend RPCs |
| Device-level fatal failure in multi-device process | Not applicable in NBD single-device process | Required for v1 | One failed device transitions to `Failed` while other devices continue serving |
| Process shutdown with active devices | Signal path triggers graceful detach in attach mode | Required for v1 | Manager stops accepting new requests, drains existing devices, then force-detaches on timeout |
| Control protocol malformed request | Not applicable | Required for v1 | Malformed control message returns structured error without crashing process |
| Control protocol duplicate request replay | Not applicable | Required for v1 | Replay with same idempotency key returns prior outcome or safe no-op |
