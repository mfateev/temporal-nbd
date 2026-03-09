# Phase0-Risk-Register

## Risk Register

Scales:
- Probability: `Low`, `Medium`, `High`
- Impact: `Low`, `Medium`, `High`, `Critical`
- Phase tag: `Blocker before Phase 1` or `Track during Phase 1+`

| ID | Risk | Probability | Impact | Mitigation | Owner | Phase tag |
| --- | --- | --- | --- | --- | --- | --- |
| RSK-01 | Target kernels lack stable UBLK support across deployment fleet | Medium | Critical | Validate kernel matrix early; define minimum supported distro/kernel and block unsupported hosts | Kernel/platform owner | Blocker before Phase 1 |
| RSK-02 | Required privileges/capabilities are unavailable in intended runtime model | High | High | Produce explicit privilege model and deployment manifests; run startup probes in target env | Security/ops owner | Blocker before Phase 1 |
| RSK-03 | Control socket exposed too broadly, enabling unauthorized attach/remove operations | Medium | High | Enforce socket path permissions, least-privilege runtime user, and audit logs | Security/ops owner | Blocker before Phase 1 |
| RSK-04 | Ambiguous Add/Remove retries create duplicate or orphaned devices | Medium | High | Require idempotency keys, maintain operation result cache, add replay tests | UBLK implementation lead | Blocker before Phase 1 |
| RSK-05 | Device-scoped failures cascade and impact unrelated attachments in serve mode | Medium | High | Per-device supervision boundaries, isolation tests, and failure-state reporting | UBLK implementation lead | Blocker before Phase 1 |
| RSK-06 | Forced detach can lose unflushed writes and create unclear operator signals | Medium | High | Explicit `ForceDetached` state, metrics, and warning logs; make timeout configurable | UBLK implementation lead + SRE owner | Blocker before Phase 1 |
| RSK-07 | Observability is insufficient for on-call debugging | Medium | Medium | Lock required metrics/log schema before code and include contract tests | Observability owner | Blocker before Phase 1 |
| RSK-08 | One io_uring thread per device causes scalability pressure at higher density | Medium | Medium | Set `--max-devices` conservatively; benchmark and revisit shared workers later | UBLK implementation lead | Track during Phase 1+ |
| RSK-09 | Restart reconciliation expectations are unclear between manager and orchestrator | Medium | High | Document restart contract and responsibilities; add restart behavior tests | Temporal workflow owner + SRE owner | Blocker before Phase 1 |
| RSK-10 | Benchmark methodology drifts, making performance comparisons unreliable | Medium | Medium | Lock host profile/workload definitions and keep benchmark scripts versioned | QA/perf owner | Track during Phase 1+ |

## Sequencing Constraints

1. Resolve RSK-01/02/03 before writing device runtime code.
2. Resolve RSK-04/05/09 before enabling multi-device manager by default.
3. Resolve RSK-07 before declaring Phase 1 operationally ready.
4. Track RSK-08 and RSK-10 during implementation with explicit benchmark checkpoints.

## Phase1-Entry-Criteria

Phase 1 implementation may begin only when all criteria below are checked `Done` or explicitly waived by accountable owner with written mitigation.

| Criterion | Status | Accountable owner |
| --- | --- | --- |
| Parity matrix approved with no unresolved `Required for v1` ambiguity | Pending | Project tech lead |
| v1 scope and non-goals approved | Pending | Project tech lead |
| Environment readiness blockers (RSK-01, RSK-02, RSK-03) resolved or waived | Pending | Platform + security owners |
| Interface contracts approved by implementation and workflow owners | Pending | UBLK lead + Temporal workflow owner |
| Acceptance gates approved and measurable | Pending | Project tech lead + QA/perf owner |
| Observability minimum contract agreed (metrics/log fields) | Pending | Observability owner |
| Device lifecycle and forced-detach policy approved | Pending | UBLK lead + SRE owner |
| Risk register owners assigned and acknowledged | Pending | Project tech lead |

## Sign-Off Record

| Role | Name | Decision | Date |
| --- | --- | --- | --- |
| Driver (UBLK implementation lead) | TBD | Pending | TBD |
| Accountable (project tech lead) | TBD | Pending | TBD |
| Consulted (Temporal workflow owner) | TBD | Pending | TBD |
| Consulted (SRE/operations owner) | TBD | Pending | TBD |
| Consulted (kernel/platform owner) | TBD | Pending | TBD |
