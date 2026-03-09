# Phase0-Environment-Readiness

## Objective

Validate that target hosts and runtime packaging can support UBLK v1 before Phase 1 implementation starts.

Status values:
- `Ready`: prerequisite verified and owner assigned for maintenance.
- `Blocked`: prerequisite not yet verified or not yet available.

## Readiness Checklist

Target dates below are planning targets in UTC and should be updated as owners confirm schedules.

| Area | Requirement | Verification method | Status | Owner | Target date (UTC) | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| OS support | Linux-only runtime for attach/serve | Host inventory review | Ready | Platform owner | 2026-03-09 | Non-Linux hosts are out of scope for attach runtime |
| Kernel UBLK support | Kernel built with UBLK support and module available | `grep UBLK /boot/config-$(uname -r)` and `modprobe` validation | Blocked | Kernel/platform owner | 2026-03-20 | Must be validated on all target distro/kernel combinations |
| UBLK control node | `/dev/ublk-control` exists and is accessible | `test -e /dev/ublk-control` and open syscall test | Blocked | Platform owner | 2026-03-20 | Required by `temporal-ublk attach/serve` |
| Device node creation path | `/dev/ublkbN` nodes can be created and consumed by VM runtime | Attach smoke script on target host | Blocked | Platform + virtualization owner | 2026-03-24 | Must confirm handoff to Firecracker (or equivalent) |
| io_uring availability | Kernel and userspace support required io_uring ops | liburing self-test and minimal UBLK request loop smoke | Blocked | Kernel/platform owner | 2026-03-20 | Required for frontend I/O worker model |
| Privileges/capabilities | Service user has required privileges to manage UBLK devices | Run capability audit for service unit/container profile | Blocked | Security/ops owner | 2026-03-21 | Expected to require elevated privileges on host |
| Containerization constraints | If containerized, run with host devices/capabilities required for UBLK | Deployment manifest review + startup probe | Blocked | SRE owner | 2026-03-24 | Rootless/unprivileged container model is not expected to work |
| Temporal connectivity | Same frontend endpoint + namespace reachability as NBD client | Existing create/open smoke tests | Ready | Temporal workflow owner | 2026-03-09 | Reuses current RPC surface |
| Service supervisor integration | Supervisor strategy for long-lived `serve` process defined | `systemd` unit (or equivalent) reviewed | Blocked | SRE owner | 2026-03-25 | Must include restart policy and stop timeouts |
| Control socket ownership | File-system path, permissions, and cleanup policy defined | Socket path policy doc + startup/shutdown tests | Blocked | Security/ops owner | 2026-03-21 | Prevent unauthorized local control-plane calls |
| Metrics and logs pipeline | Manager metrics/logs scraped and retained | Integration test with metrics endpoint/log collector | Blocked | Observability owner | 2026-03-26 | Needed for incident response and SLO tracking |
| Kernel-gated CI coverage | At least one CI/manual lane has UBLK-capable host for ignored integration tests | CI lane definition and dry run | Blocked | QA/release owner | 2026-03-27 | Required to keep regressions visible pre-merge |

## Deployment Targets for v1

Target host profile:
- Linux host with UBLK-capable kernel.
- Privileged runtime able to open `/dev/ublk-control` and serve `/dev/ublkbN`.
- Local process supervisor support for long-running `temporal-ublk serve`.

Initial deployment model assumptions:
- Single manager process per host failure domain.
- Control-plane consumers are local trusted components (same host or controlled namespace).
- One active writer per volume.

## Blocker Resolution Rule

Phase 1 implementation starts only after all `Blocker before Phase 1` items in `phase0-risk-register.md` are either:
- switched to `Ready`, or
- explicitly accepted by accountable owner with written mitigation and rollback plan.

Any missed target date must be escalated to the accountable lead and either re-baselined with justification or treated as a Phase 1 entry blocker.
