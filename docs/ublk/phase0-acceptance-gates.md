# Phase0-Acceptance-Gates

## Purpose

Define objective, measurable gates that determine readiness for UBLK implementation and Phase 1 completion criteria.

## Gate Categories and Thresholds

## 1) Correctness Gates

| Gate | Measurement | Threshold |
| --- | --- | --- |
| C1: Attach lifecycle correctness | Single-device attach -> mkfs -> mount -> write -> sync -> unmount -> detach -> reattach -> verify | 100% pass in kernel-gated integration test |
| C2: Multi-device isolation correctness | Two concurrent attached devices; injected failure on one device | Healthy device continues serving with no data mismatch |
| C3: Alignment/bounds enforcement | Unit + integration negative tests for unaligned/out-of-range requests | All invalid requests rejected with expected errno (`EINVAL`) |
| C4: Flush durability | Dirty writes + explicit flush + reconnect read verification | No acknowledged flush loses data |
| C5: Unsupported ops behavior | Discard/write-zeroes requests | Always return `EOPNOTSUPP`; no backend mutation |

## 2) Reliability and Recovery Gates

| Gate | Measurement | Threshold |
| --- | --- | --- |
| R1: Retry behavior | Fault injection for retryable backend RPC failures | Operation retries per policy and either recovers or fails with mapped error |
| R2: Flush deadline behavior | Force persistent write/flush failure past deadline | Flush returns `EIO`; dirty state not silently dropped |
| R3: Manager shutdown policy | `serve` shutdown with active devices and timeout paths | Graceful drain attempted first, forced detach only after timeout |
| R4: Device failure isolation | Device task panic/termination in multi-device mode | Manager stays alive; unaffected devices continue serving |
| R5: Restart semantics clarity | Stop/start manager with previously attached devices | Post-restart state is explicit and documented; no silent phantom devices |

## 3) Operability Gates

| Gate | Measurement | Threshold |
| --- | --- | --- |
| O1: Control-plane contract conformance | Protocol tests for framing, schema validation, request/response correlation | 100% pass for valid/invalid protocol cases |
| O2: Metrics completeness | Scrape metrics while handling read/write/flush/add/remove/failure | All required process and per-device metrics present |
| O3: Logging quality | Structured logs from lifecycle + error scenarios | Required fields present (`request_id`, `device_id`, `volume_id`, state, error code) |
| O4: Readiness signaling | Startup/shutdown behavior with ready file and/or metrics readiness | Ready signal appears only when manager can serve control operations |

## 4) Baseline Performance Gates

Baseline comparison target: current `temporal-nbd` on same host, same volume geometry, same backend endpoint.

| Gate | Measurement | Threshold |
| --- | --- | --- |
| P1: Single-device random 4KiB read IOPS | fio-like random read workload, queue depth 1 | >= 70% of NBD baseline |
| P2: Single-device random 4KiB write IOPS | fio-like random write workload, queue depth 1, periodic flush | >= 70% of NBD baseline |
| P3: p99 latency | Read/write p99 latency under same workload | <= 1.5x NBD baseline |
| P4: Idle multi-device overhead | Manager with 8 attached idle devices | CPU < 5% of one core and RSS < 300 MiB |

These are Phase 1 baseline gates, not final production SLO targets.

## Required Test Categories for Later Phases

1. Unit tests:
- config/CLI validation,
- protocol framing and schema,
- state machine transitions,
- operation idempotency.

2. Integration tests:
- kernel-gated single-device mount durability test (`#[ignore]` default),
- kernel-gated multi-device attach/remove/isolation test (`#[ignore]` default),
- existing NBD tests remain green.

3. Fault tests:
- backend transient and terminal RPC fault injection,
- graceful timeout to forced-detach path.

4. Performance tests:
- repeatable benchmark harness with fixed host profile and workload definitions.

5. Soak tests:
- long-running multi-device attach/remove cycles with periodic I/O and fault injection.

## Gate Evaluation Process

1. Capture results in CI artifact or reproducible local run log.
2. Compare against thresholds in this document.
3. Any failed gate must include:
- root cause hypothesis,
- mitigation plan,
- owner and target date.

No Phase 1 exit sign-off if any correctness gate (C*) is failing.
