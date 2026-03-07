# temporal-nbd

Phase 1 Rust smoke client for Temporal CHASM blockdevice `CreateVolume`.

## What this does

- Calls `CreateVolume` against Temporal history gRPC endpoint.
- Calls `CreateVolume` a second time with the same `volume_id`.
- Asserts duplicate create fails with gRPC `AlreadyExists`.
- Stores `volume_id` to disk for Phase 2 reconnect (`OpenVolume`).

## Prereqs

- Temporal server running with blockdevice module wired in history service.
- A namespace created in Temporal.
- Namespace **ID** (not namespace name).

## Environment

Required:

- `TEMPORAL_HISTORY_ENDPOINT` (example: `127.0.0.1:7234`)
- `TEMPORAL_NAMESPACE_ID`

Optional:

- `TEMPORAL_VOLUME_ID` (default: generated)
- `TEMPORAL_VOLUME_SIZE_BYTES` (default: `1073741824`)
- `TEMPORAL_VOLUME_BLOCK_SIZE_BYTES` (default: `0`)
- `TEMPORAL_VOLUME_ID_FILE` (default: `./phase1-volume-id.txt`)
- `TEMPORAL_CONNECT_TIMEOUT_SECS` (default: `5`)
- `TEMPORAL_RPC_TIMEOUT_SECS` (default: `5`)
- `TEMPORAL_REPO` path to Temporal repo for proto compilation (default: `../temporal`)
- `TEMPORAL_API_REPO` path to Temporal API repo for workflowservice protos (default: `../api`)

## Run as CLI smoke test

```bash
cargo run
```

## Run as test

```bash
cargo test -- --nocapture
```

The integration test is skipped unless `TEMPORAL_HISTORY_ENDPOINT` and `TEMPORAL_NAMESPACE_ID` are set.

## Full E2E (SQLite, source-built Temporal server)

`tests/e2e_sqlite.rs` is a Rust-native integration test that:
1. builds `temporal-server` from local source,
2. starts it with `--env development-sqlite`,
3. registers + describes a namespace via frontend gRPC,
4. calls blockdevice `CreateVolume` via history gRPC and validates duplicate `AlreadyExists`.

The test is marked `#[ignore]` because it is heavyweight and builds/runs a full server.

Run:

```bash
cargo test --test e2e_sqlite -- --ignored --nocapture
```

Optional E2E env:
- `TEMPORAL_REPO` (default: `../temporal`)
- `TEMPORAL_API_REPO` (default: `../api`)
- `TEMPORAL_API_GO_REF` (default: `master`, used for `go.temporal.io/api@<ref>` during server build)
- `GO_BIN` (default: `/usr/local/go/bin/go` if present, else `go`)
- `TEMPORAL_FRONTEND_ENDPOINT` (default: `127.0.0.1:7233`)
- `TEMPORAL_HISTORY_ENDPOINT` (default: `127.0.0.1:7234`)
- `TEMPORAL_SERVER_ENV` (default: `development-sqlite`)
