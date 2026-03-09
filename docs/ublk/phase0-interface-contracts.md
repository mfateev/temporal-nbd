# Phase0-Interface-Contracts

## Contract Scope

This document locks interface boundaries for UBLK v1 between:
- local orchestrator/caller and `temporal-ublk serve` manager,
- manager and per-device attach state machines,
- frontend request loop and shared bridge/engine/session stack.

## Control-Plane API Contract

### Transport and Framing

1. Unix domain stream socket at configured `--control-socket` path.
2. Each message frame:
- 4-byte big-endian unsigned payload length.
- UTF-8 JSON payload of exactly that length.
3. One request yields exactly one response.
4. All responses include original `request_id`.

### Envelope Schema

Request:
```json
{
  "version": "v1",
  "request_id": "string",
  "idempotency_key": "string|null",
  "op": "AddDevice|RemoveDevice|ListDevices|Health",
  "body": {}
}
```

`idempotency_key` handling:
- Required and non-empty for mutating ops: `AddDevice`, `RemoveDevice`.
- Optional for read-only ops: `ListDevices`, `Health`.
- If supplied on read-only ops, it is accepted and logged for trace correlation but ignored for semantic behavior.

Response:
```json
{
  "version": "v1",
  "request_id": "string",
  "ok": true,
  "result": {},
  "error": null
}
```

Error response:
```json
{
  "version": "v1",
  "request_id": "string",
  "ok": false,
  "result": null,
  "error": {
    "code": "InvalidArgument|AlreadyExists|NotFound|Busy|Internal|Timeout|Unavailable",
    "message": "human-readable summary",
    "details": {}
  }
}
```

### Operation Contracts

| Operation | Required request fields | Success result | Idempotency behavior |
| --- | --- | --- | --- |
| `AddDevice` | `idempotency_key`, `volume_id`, optional `ublk_device_id`, optional per-device overrides | `device_id`, `device_path`, `volume_id`, `state` | Same `idempotency_key` with equivalent body returns original result |
| `RemoveDevice` | `idempotency_key`, `device_id` or `volume_id`, optional `force` | terminal state (`Detached` or `ForceDetached`) | Repeated remove for already detached device returns success no-op |
| `ListDevices` | optional filter fields, optional `idempotency_key` | full device list with states and last error | Pure read; idempotent by definition |
| `Health` | optional `idempotency_key` | process health summary + degraded device list | Pure read; idempotent by definition |

`AddDevice` response must include a stable `device_path` (for example `/dev/ublkb7`) for that attachment lifetime.

### Error Code Contract

| Code | Meaning | Retry guidance |
| --- | --- | --- |
| `InvalidArgument` | malformed or invalid request | do not retry without fixing request |
| `AlreadyExists` | duplicate `volume_id` or conflicting `device_id` | retry only with different identifiers |
| `NotFound` | requested device not present | safe to treat as converged for remove workflows |
| `Busy` | operation conflicts with current state (for example draining) | retry with backoff |
| `Timeout` | operation exceeded configured timeout | retry with same `idempotency_key` |
| `Unavailable` | manager temporarily unable to process | retry with backoff |
| `Internal` | unexpected manager/device failure | retry carefully; escalate if persistent |

## Device Lifecycle State Model

States:
- `Allocating`
- `OpeningVolume`
- `Serving`
- `Draining`
- `Detached`
- `Failed`
- `ForceDetached`

State transition rules:
1. `Allocating -> OpeningVolume` after UBLK id/node assignment starts.
2. `OpeningVolume -> Serving` after backend open + bridge + frontend loop are ready.
3. `Serving -> Draining` on remove request or process shutdown.
4. `Draining -> Detached` when disconnect and final flush complete.
5. `Draining -> ForceDetached` when graceful timeout expires and forced teardown succeeds.
6. Any state except terminal may transition to `Failed` on fatal device-scoped errors.

Terminal states:
- `Detached`, `ForceDetached`, `Failed`.

## Idempotency, Retries, and Compensation

1. Manager requires `idempotency_key` for mutating operations (`AddDevice`, `RemoveDevice`) and rejects missing/empty values with `InvalidArgument`.
2. Manager stores recent operation outcomes long enough to make client retries safe.
3. Callers must retry timed-out mutating operations with the same `idempotency_key`.
4. Duplicate `AddDevice` for same volume with different key is rejected unless prior device is removed.
5. Compensation model:
- failed `AddDevice`: release partial resources and return terminal error.
- failed `RemoveDevice`: keep device state explicit (`Draining`/`Failed`) for operator action.

## Frontend-to-Engine Contract

UBLK operation mapping to bridge commands:
- read -> `BridgeCommand::Read`
- write -> `BridgeCommand::Write`
- flush -> `BridgeCommand::Flush`
- write with FUA -> `BridgeCommand::Write` then immediate `BridgeCommand::Flush` before completion
- stop/teardown -> `BridgeCommand::Disconnect`
- discard/write-zeroes -> reject with `EOPNOTSUPP`

Validation ownership:
- alignment/range enforcement stays in bridge/engine.
- frontend must preserve request correlation so completion maps to original queue request.

## Observability Contract

Minimum structured log events:
- manager start/stop,
- control request accepted/completed/failed (`request_id`, `op`, outcome),
- device state transitions,
- forced detach events with data-loss warning flag,
- backend retry exhaustion and terminal I/O failures.

Minimum metrics:
- process: active devices, control requests total/failures, forced detaches, uptime.
- per-device: read/write/flush counts, bytes, latency, in-flight depth, dirty blocks, error counters by class.

Minimum audit events:
- `AddDevice` and `RemoveDevice` with caller identity/context where available,
- explicit recording when `ForceDetached` occurs.

## Security Boundary

1. Control socket path and permissions must restrict access to trusted local principals.
2. Manager must reject unknown protocol versions.
3. Manager must validate payload sizes before JSON parse to prevent unbounded memory growth.
4. Sensitive fields (tokens/secrets) must never be emitted in logs.
