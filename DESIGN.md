# temporal-nbd Design Overview

## Purpose

`temporal-nbd` exposes a Temporal blockdevice volume as a Linux block device (`/dev/nbdX`) by translating Linux NBD requests into Temporal `workflowservice` volume RPCs.

The target workflow is:
1. create/open a Temporal volume,
2. attach as `/dev/nbdX`,
3. format/mount/read/write through the filesystem,
4. unmount + detach,
5. re-attach and read data again.

## High-Level Architecture

The system has four runtime layers:

1. Linux kernel NBD driver
- Generates block I/O requests (`READ`, `WRITE`, `FLUSH`, `DISC`) on `/dev/nbdX`.

2. NBD adapter (`src/nbd.rs`)
- Configures `/dev/nbdX` via ioctls.
- Owns kernel-facing file descriptors.
- Parses request headers and writes NBD replies.

3. Bridge + engine (`src/bridge.rs`, `src/engine.rs`)
- Converts byte-offset requests into block operations.
- Maintains in-memory dirty block cache keyed by LBA.
- Enforces alignment and bounds.
- Implements read overlay and flush semantics.

4. Temporal session/RPC layer (`src/session.rs`)
- Opens volume metadata (`OpenVolume`).
- Calls `ReadBlocks` and `WriteBatch`.
- Applies retry/backoff and reconnect logic for retryable failures.

`src/attach.rs` wires these layers into the long-running attach loop.

## Request Flow

### Read Path

1. Kernel issues NBD read request.
2. Bridge checks alignment/range and maps to LBA range.
3. Engine fetches backend blocks with `ReadBlocks` (chunked to request limits).
4. Engine overlays dirty cache entries so reads observe local unflushed writes.
5. NBD adapter replies to kernel.

### Write Path

1. Kernel issues NBD write request.
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

1. Kernel or user requests disconnect (`DISC`/signal).
2. Attach flow attempts best-effort final flush.
3. NBD device is disconnected and process exits.

## Consistency and Durability Model

- Single active writer per volume (POC constraint).
- Read-your-writes is guaranteed inside one attach session via dirty-cache overlay.
- Durability is flush-driven:
  - `sync` or filesystem flush triggers `FLUSH`,
  - `FLUSH` completion means writes reached Temporal via `WriteBatch`.
- Detach attempts final flush to reduce risk of lost buffered writes.

## Error Handling

- Validation errors (unaligned requests, out-of-range offsets) are rejected at the bridge/engine boundary.
- Retryable RPC failures use bounded exponential backoff with jitter.
- Non-retryable failures are surfaced as I/O errors to kernel callers.
- Under dirty-cache pressure, engine may force synchronous flush; if that fails by policy deadline, writes fail with I/O error rather than dropping data.

## Operational Constraints

- Linux-only attach mode (requires NBD kernel support).
- Privileged access to `/dev/nbdX` is required (root or equivalent capabilities).
- Test environments without `/sys/module/nbd` cannot run mount/attach integration tests.

## Key Files

- `src/main.rs`: CLI entrypoint (`attach` only).
- `src/attach.rs`: attach orchestration and shutdown handling.
- `src/nbd.rs`: Linux NBD ioctl and request/reply loop.
- `src/bridge.rs`: protocol-to-block translation.
- `src/engine.rs`: cache, flush, chunking, and core block engine semantics.
- `src/session.rs`: Temporal RPC session with retries and transport recovery.
- `tests/support/workflow_smoke.rs`: test-only workflowservice smoke helpers.
- `tests/e2e_nbd_mount.rs`: kernel-gated durability test (detach/reattach roundtrip).
