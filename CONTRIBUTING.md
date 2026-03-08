# Contributing to temporal-nbd

## Repository Pairing (Branch Matrix)

Use branch-compatible checkouts for this blockdevice work.

As of 2026-03-08, the expected branch set is:

| Repo | Branch | Why |
| --- | --- | --- |
| `temporal-nbd` | `phase1-smoke-client` | Rust client and tests for blockdevice attach flow |
| `temporal` | `blockdevice` | Server-side CHASM blockdevice implementation |
| `api` | `blockdevice` | `workflowservice` volume RPC proto definitions used at build time |

`temporal-nbd` compiles protobufs from a separate API checkout (`TEMPORAL_API_REPO` or `../api`), so API branch mismatch will usually fail code generation or runtime contract checks.

## Prerequisites

Local machine:
- Rust toolchain (`cargo`, `rustc`)
- `protoc`
- Go toolchain (needed when building/running Temporal server from source)
- `jq`, `ssh`, `tar` (used by remote prebuilt test helper)

Remote host for mount-capable tests:
- Linux with NBD support (`/sys/module/nbd`, `/dev/nbdX`)
- same architecture as local build host (the helper enforces this)
- root or passwordless sudo for mount/ioctl operations
- reachable from local host over SSH

## Build

From `temporal-nbd`:

```bash
cd /path/to/temporal-nbd
export TEMPORAL_API_REPO=/path/to/api
cargo build --bin temporal-nbd
```

If `TEMPORAL_API_REPO` is not set, build expects `../api`.

## Local Tests

Default validation set:

```bash
cargo test
cargo test --test create_volume_smoke -- --nocapture
cargo test --test e2e_sqlite -- --ignored --nocapture
```

Kernel-gated mount/reattach test (requires NBD-capable Linux host and Temporal endpoint):

```bash
TEMPORAL_NAMESPACE=default cargo test --test e2e_nbd_mount -- --ignored --nocapture
```

## Remote Host Testing (Mount-Capable)

Use `scripts/run_prebuilt_tests_remote.sh` to build test binaries locally and run on a remote host that supports mounts/NBD.

The helper can:
- auto-start local Temporal server if needed,
- auto-export `TEMPORAL_FRONTEND_ENDPOINT=<local-ip>:7233` to remote,
- upload binary artifacts and collect remote logs.

### Smoke Test on Remote

```bash
scripts/run_prebuilt_tests_remote.sh \
  --remote dev@REMOTE_HOST \
  --cargo-arg --test --cargo-arg create_volume_smoke \
  --remote-env TEMPORAL_NAMESPACE=default
```

### Phase C Mount/Detach/Re-attach Test on Remote

```bash
scripts/run_prebuilt_tests_remote.sh \
  --remote dev@REMOTE_HOST \
  --cargo-arg --test --cargo-arg e2e_nbd_mount \
  --run-ignored \
  --remote-env TEMPORAL_NAMESPACE=default \
  --remote-env TEMPORAL_NBD_DEVICE=/dev/nbd0
```

Optional flags:
- `--no-local-temporal` if you already run Temporal elsewhere.
- `--remote-env TEMPORAL_FRONTEND_ENDPOINT=HOST:7233` to override auto endpoint.
- `--copy-only` to only stage artifacts without executing remotely.

## Troubleshooting

- `TEMPORAL_API_REPO does not contain ... service.proto`: wrong API path/branch.
- `nbd kernel module is missing`: run `sudo modprobe nbd max_part=0` on test host.
- `device already attached`: pick a free `/dev/nbdX` and ensure previous attach exited.
- remote endpoint unreachable: set explicit `TEMPORAL_FRONTEND_ENDPOINT` with a routable host/IP.
