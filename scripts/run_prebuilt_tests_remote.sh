#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_prebuilt_tests_remote.sh [options]

Build Rust test binaries locally, copy them over SSH, and execute them remotely
without requiring Rust on the remote host.

Options:
  --remote USER@HOST          Remote SSH target (default: dev@192.168.64.13)
  --remote-dir PATH           Remote staging dir (default: /tmp/temporal-nbd-prebuilt-tests)
  --cargo-arg ARG             Extra argument appended to `cargo test` (repeatable)
  --include-unit              Include unit/bin test binaries (lib + bins)
  --run-ignored               After normal run, run each binary with --ignored --nocapture
  --remote-env KEY=VALUE      Export env var on remote before running tests (repeatable)
  --no-auto-host-endpoint     Do not auto-set TEMPORAL_FRONTEND_ENDPOINT to this host
  --host-port PORT            Port for auto TEMPORAL_FRONTEND_ENDPOINT (default: 7233)
  --no-local-temporal         Do not start/stop Temporal server locally
  --temporal-repo PATH        Temporal server repo path (default: /home/dev/temporal)
  --temporal-bin PATH         Temporal server binary path (default: /tmp/temporal-server-remote-e2e)
  --temporal-env NAME         Temporal config env (default: development-sqlite)
  --temporal-start-timeout N  Startup timeout in seconds (default: 30)
  --local-log-dir PATH        Local directory for downloaded remote logs
                              (default: target/remote-test-logs/<run-id>)
  --copy-only                 Build and copy artifacts, skip remote execution
  -h, --help                  Show this help

Examples:
  scripts/run_prebuilt_tests_remote.sh \
    --cargo-arg --test --cargo-arg create_volume_smoke \
    --remote-env TEMPORAL_NAMESPACE=default

  scripts/run_prebuilt_tests_remote.sh \
    --run-ignored \
    --remote-env TEMPORAL_NAMESPACE=default \
    --remote-env TEMPORAL_VOLUME_ID=phaseb-remote-1
USAGE
}

REMOTE="dev@192.168.64.13"
REMOTE_DIR="/tmp/temporal-nbd-prebuilt-tests"
RUN_IGNORED=0
INCLUDE_UNIT=0
AUTO_HOST_ENDPOINT=1
HOST_PORT="7233"
COPY_ONLY=0
MANAGE_LOCAL_TEMPORAL=1
TEMPORAL_REPO="/home/dev/temporal"
TEMPORAL_BIN="/tmp/temporal-server-remote-e2e"
TEMPORAL_ENV="development-sqlite"
TEMPORAL_START_TIMEOUT_SECS=30
MANAGED_TEMPORAL_STARTED=0
MANAGED_TEMPORAL_PID=""
MANAGED_TEMPORAL_LOG=""
MANAGED_TEMPORAL_CONFIG_DIR=""
LOCAL_IPV4=""
TEMPORAL_HEALTH_ADDRESS=""
json_out=""
manifest_raw=""
stage_dir=""
real_nbd_cli_bin=""
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
REMOTE_LOG_SUBDIR="logs-${RUN_ID}"
REMOTE_LOG_DIR="${REMOTE_DIR}/${REMOTE_LOG_SUBDIR}"
LOCAL_LOG_DIR_OVERRIDE=""
LOCAL_LOG_DIR=""
REMOTE_EXEC_STATUS=0

declare -a CARGO_ARGS=()
declare -a REMOTE_ENVS=()
declare -a SELECTED_TESTS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote)
      REMOTE="$2"
      shift 2
      ;;
    --remote-dir)
      REMOTE_DIR="$2"
      shift 2
      ;;
    --cargo-arg)
      CARGO_ARGS+=("$2")
      shift 2
      ;;
    --include-unit)
      INCLUDE_UNIT=1
      shift
      ;;
    --run-ignored)
      RUN_IGNORED=1
      shift
      ;;
    --remote-env)
      REMOTE_ENVS+=("$2")
      shift 2
      ;;
    --no-auto-host-endpoint)
      AUTO_HOST_ENDPOINT=0
      shift
      ;;
    --host-port)
      HOST_PORT="$2"
      shift 2
      ;;
    --no-local-temporal)
      MANAGE_LOCAL_TEMPORAL=0
      shift
      ;;
    --temporal-repo)
      TEMPORAL_REPO="$2"
      shift 2
      ;;
    --temporal-bin)
      TEMPORAL_BIN="$2"
      shift 2
      ;;
    --temporal-env)
      TEMPORAL_ENV="$2"
      shift 2
      ;;
    --temporal-start-timeout)
      TEMPORAL_START_TIMEOUT_SECS="$2"
      shift 2
      ;;
    --local-log-dir)
      LOCAL_LOG_DIR_OVERRIDE="$2"
      shift 2
      ;;
    --copy-only)
      COPY_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for ((i=0; i<${#CARGO_ARGS[@]}; i++)); do
  if [[ "${CARGO_ARGS[$i]}" == "--test" ]] && (( i + 1 < ${#CARGO_ARGS[@]} )); then
    SELECTED_TESTS+=("${CARGO_ARGS[$((i + 1))]}")
  fi
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd jq
require_cmd ssh
require_cmd tar

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -n "$LOCAL_LOG_DIR_OVERRIDE" ]]; then
  LOCAL_LOG_DIR="$LOCAL_LOG_DIR_OVERRIDE"
else
  LOCAL_LOG_DIR="$repo_root/target/remote-test-logs/$RUN_ID"
fi

has_remote_env_key() {
  local key="$1"
  local kv
  for kv in "${REMOTE_ENVS[@]}"; do
    if [[ "${kv%%=*}" == "$key" ]]; then
      return 0
    fi
  done
  return 1
}

contains_selected_test() {
  local candidate="$1"
  local test_name
  for test_name in "${SELECTED_TESTS[@]}"; do
    if [[ "$test_name" == "$candidate" ]]; then
      return 0
    fi
  done
  return 1
}

get_remote_env_value() {
  local key="$1"
  local kv
  for kv in "${REMOTE_ENVS[@]}"; do
    if [[ "${kv%%=*}" == "$key" ]]; then
      echo "${kv#*=}"
      return 0
    fi
  done
  return 1
}

get_test_namespace() {
  local ns
  if ns="$(get_remote_env_value TEMPORAL_NAMESPACE 2>/dev/null)"; then
    echo "$ns"
    return 0
  fi
  echo "default"
}

detect_local_ipv4() {
  local candidate
  for candidate in $(hostname -I 2>/dev/null); do
    if [[ "$candidate" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

is_local_temporal_serving() {
  if timeout 1 bash -lc "</dev/tcp/127.0.0.1/${HOST_PORT}" >/dev/null 2>&1; then
    TEMPORAL_HEALTH_ADDRESS="127.0.0.1:${HOST_PORT}"
    return 0
  fi

  if [[ -n "$LOCAL_IPV4" ]] && timeout 1 bash -lc "</dev/tcp/${LOCAL_IPV4}/${HOST_PORT}" >/dev/null 2>&1; then
    TEMPORAL_HEALTH_ADDRESS="${LOCAL_IPV4}:${HOST_PORT}"
    return 0
  fi

  TEMPORAL_HEALTH_ADDRESS=""
  return 1
}

ensure_local_namespace_if_needed() {
  if (( MANAGE_LOCAL_TEMPORAL == 0 )) || (( COPY_ONLY == 1 )); then
    return 0
  fi

  if ! command -v temporal >/dev/null 2>&1; then
    echo "warning: temporal CLI not found; cannot auto-create namespace" >&2
    return 0
  fi

  local namespace
  namespace="$(get_test_namespace)"
  if [[ -z "$namespace" ]]; then
    return 0
  fi

  local address="${TEMPORAL_HEALTH_ADDRESS:-${LOCAL_IPV4}:${HOST_PORT}}"
  local deadline=$((SECONDS + TEMPORAL_START_TIMEOUT_SECS))
  if (( TEMPORAL_START_TIMEOUT_SECS < 1 )); then
    deadline=$((SECONDS + 1))
  fi

  while (( SECONDS < deadline )); do
    if temporal operator namespace describe --address "$address" --command-timeout 1s --namespace "$namespace" >/dev/null 2>&1; then
      echo "namespace ${namespace} already exists on ${address}"
      return 0
    fi

    if temporal operator namespace create --address "$address" --command-timeout 1s --namespace "$namespace" >/dev/null 2>&1; then
      echo "created namespace ${namespace} on ${address}"
      return 0
    fi

    sleep 0.2
  done

  echo "failed to ensure namespace ${namespace} on ${address} within ${TEMPORAL_START_TIMEOUT_SECS}s" >&2
  temporal operator namespace describe --address "$address" --command-timeout 1s --namespace "$namespace" || true
  return 1
}

build_temporal_server_bin_if_missing() {
  if [[ -x "$TEMPORAL_BIN" ]]; then
    return 0
  fi

  require_cmd go
  if [[ ! -d "$TEMPORAL_REPO" ]]; then
    echo "temporal repo does not exist: $TEMPORAL_REPO" >&2
    exit 1
  fi

  local go_bin="go"
  if [[ -x /usr/local/go/bin/go ]]; then
    go_bin=/usr/local/go/bin/go
  fi

  echo "building temporal server binary at $TEMPORAL_BIN"
  (
    cd "$TEMPORAL_REPO"
    "$go_bin" build -tags disable_grpc_modules -o "$TEMPORAL_BIN" ./cmd/server
  )
}

prepare_temporal_config() {
  local cfg_file
  MANAGED_TEMPORAL_CONFIG_DIR="$(mktemp -d /tmp/temporal-e2e-config-XXXXXX)"
  cp -a "$TEMPORAL_REPO/config/." "$MANAGED_TEMPORAL_CONFIG_DIR"/
  cfg_file="$MANAGED_TEMPORAL_CONFIG_DIR/${TEMPORAL_ENV}.yaml"
  if [[ ! -f "$cfg_file" ]]; then
    echo "temporal config file not found: $cfg_file" >&2
    exit 1
  fi

  sed -i '/^  frontend:/,/^  matching:/ s/bindOnLocalHost: true/bindOnLocalHost: false/' "$cfg_file"
  sed -E -i "s#(filepath:[[:space:]]+\")config/dynamicconfig/#\\1${MANAGED_TEMPORAL_CONFIG_DIR}/dynamicconfig/#g" "$cfg_file"
}

start_local_temporal_if_needed() {
  if (( MANAGE_LOCAL_TEMPORAL == 0 )) || (( COPY_ONLY == 1 )); then
    return 0
  fi

  if is_local_temporal_serving; then
    echo "detected existing local Temporal server on ${TEMPORAL_HEALTH_ADDRESS}; reusing it"
    return 0
  fi

  build_temporal_server_bin_if_missing
  prepare_temporal_config
  MANAGED_TEMPORAL_LOG="$(mktemp /tmp/temporal-e2e-server-XXXXXX.log)"
  local cfg_file="${MANAGED_TEMPORAL_CONFIG_DIR}/${TEMPORAL_ENV}.yaml"

  echo "starting local Temporal server from $cfg_file"
  (
    cd "$TEMPORAL_REPO"
    "$TEMPORAL_BIN" --config-file "$cfg_file" --allow-no-auth start >"$MANAGED_TEMPORAL_LOG" 2>&1 &
    echo $! > /tmp/temporal-e2e-managed.pid
  )
  MANAGED_TEMPORAL_PID="$(cat /tmp/temporal-e2e-managed.pid)"
  rm -f /tmp/temporal-e2e-managed.pid
  MANAGED_TEMPORAL_STARTED=1

  local checks=$((TEMPORAL_START_TIMEOUT_SECS * 5))
  if (( checks < 5 )); then
    checks=5
  fi

  local i
  for ((i=0; i<checks; i++)); do
    if ! kill -0 "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1; then
      echo "temporal server exited during startup; log tail:" >&2
      tail -n 80 "$MANAGED_TEMPORAL_LOG" >&2 || true
      exit 1
    fi
    if is_local_temporal_serving; then
      echo "local Temporal server is reachable on ${TEMPORAL_HEALTH_ADDRESS}"
      return 0
    fi
    sleep 0.2
  done

  echo "temporal server did not become healthy within ${TEMPORAL_START_TIMEOUT_SECS}s; log tail:" >&2
  tail -n 80 "$MANAGED_TEMPORAL_LOG" >&2 || true
  exit 1
}

stop_local_temporal_if_started() {
  if (( MANAGED_TEMPORAL_STARTED == 0 )); then
    return 0
  fi

  if [[ -n "$MANAGED_TEMPORAL_PID" ]] && kill -0 "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1; then
    kill -INT "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1 || true
    local i
    for i in $(seq 1 20); do
      if ! kill -0 "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1; then
        break
      fi
      sleep 0.2
    done
    if kill -0 "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1; then
      kill -KILL "$MANAGED_TEMPORAL_PID" >/dev/null 2>&1 || true
    fi
  fi
}

cleanup() {
  stop_local_temporal_if_started || true

  if [[ -n "$json_out" ]]; then
    rm -f "$json_out"
  fi
  if [[ -n "$manifest_raw" ]]; then
    rm -f "$manifest_raw"
  fi
  if [[ -n "$stage_dir" && -d "$stage_dir" ]]; then
    rm -rf "$stage_dir"
  fi
  if [[ -n "$MANAGED_TEMPORAL_CONFIG_DIR" && -d "$MANAGED_TEMPORAL_CONFIG_DIR" ]]; then
    rm -rf "$MANAGED_TEMPORAL_CONFIG_DIR"
  fi
  if [[ -n "$MANAGED_TEMPORAL_LOG" && -f "$MANAGED_TEMPORAL_LOG" ]]; then
    rm -f "$MANAGED_TEMPORAL_LOG"
  fi
}

trap cleanup EXIT

if LOCAL_IPV4="$(detect_local_ipv4)"; then
  :
else
  LOCAL_IPV4=""
fi

if (( AUTO_HOST_ENDPOINT )) && ! has_remote_env_key "TEMPORAL_FRONTEND_ENDPOINT"; then
  if [[ -n "$LOCAL_IPV4" ]]; then
    REMOTE_ENVS+=("TEMPORAL_FRONTEND_ENDPOINT=${LOCAL_IPV4}:${HOST_PORT}")
    echo "auto-set remote TEMPORAL_FRONTEND_ENDPOINT=${LOCAL_IPV4}:${HOST_PORT}"
  else
    echo "warning: unable to auto-detect local IPv4; set --remote-env TEMPORAL_FRONTEND_ENDPOINT=..." >&2
  fi
fi

local_arch="$(uname -m)"
remote_arch="$(ssh -o ConnectTimeout=8 "$REMOTE" 'uname -m')"
if [[ "$local_arch" != "$remote_arch" ]]; then
  echo "architecture mismatch: local=$local_arch remote=$remote_arch" >&2
  exit 1
fi

start_local_temporal_if_needed

ensure_local_namespace_if_needed

if endpoint="$(get_remote_env_value TEMPORAL_FRONTEND_ENDPOINT 2>/dev/null)"; then
  hostport="${endpoint#http://}"
  hostport="${hostport#https://}"
  hostport="${hostport%%/*}"
  if [[ "$hostport" == *:* ]]; then
    ep_host="${hostport%:*}"
    ep_port="${hostport##*:}"
    if ssh "$REMOTE" "timeout 2 bash -lc '</dev/tcp/${ep_host}/${ep_port}'" >/dev/null 2>&1; then
      echo "remote can reach TEMPORAL_FRONTEND_ENDPOINT=${endpoint}"
    else
      echo "remote cannot currently reach TEMPORAL_FRONTEND_ENDPOINT=${endpoint}" >&2
      exit 1
    fi
  fi
fi

echo "building test binaries locally"
json_out="$(mktemp)"
manifest_raw="$(mktemp)"
stage_dir=""

build_cmd=(cargo test --no-run --tests --message-format=json)
if (( INCLUDE_UNIT )); then
  build_cmd+=(--lib --bins)
fi
if ((${#CARGO_ARGS[@]} > 0)); then
  build_cmd+=("${CARGO_ARGS[@]}")
fi

"${build_cmd[@]}" >"$json_out"

jq -r '
  select(.reason == "compiler-artifact" and .executable != null and .profile.test == true)
  | [ .target.name, (.target.kind | join(",")), .executable ]
  | @tsv
' "$json_out" | sort -u >"$manifest_raw"

if [[ ! -s "$manifest_raw" ]]; then
  echo "no test executables discovered from cargo output" >&2
  exit 1
fi

stage_dir="$(mktemp -d)"
manifest_stage="$stage_dir/manifest.tsv"

declare -A seen_path=()
while IFS=$'\t' read -r target_name target_kind exec_path; do
  if [[ -z "$exec_path" || ! -x "$exec_path" ]]; then
    continue
  fi

  if ((${#SELECTED_TESTS[@]} > 0)); then
    if ! { [[ "$target_kind" == *"test"* ]] && contains_selected_test "$target_name"; }; then
      continue
    fi
  fi

  if [[ -n "${seen_path[$exec_path]:-}" ]]; then
    continue
  fi
  seen_path[$exec_path]=1

  exec_base="$(basename "$exec_path")"
  install -m 0755 "$exec_path" "$stage_dir/$exec_base"
  printf '%s\t%s\t%s\n' "$target_name" "$target_kind" "$exec_base" >>"$manifest_stage"
done <"$manifest_raw"

if [[ ! -s "$manifest_stage" ]]; then
  echo "no runnable executables staged" >&2
  exit 1
fi

echo "staged $(wc -l <"$manifest_stage") test executables"

echo "building temporal-nbd CLI binary"
cargo build --bin temporal-nbd >/dev/null
real_nbd_cli_bin="$repo_root/target/debug/temporal-nbd"
if [[ ! -x "$real_nbd_cli_bin" ]]; then
  echo "temporal-nbd CLI binary not found at $real_nbd_cli_bin" >&2
  exit 1
fi
install -m 0755 "$real_nbd_cli_bin" "$stage_dir/temporal-nbd-cli"

echo "preparing remote directory $REMOTE:$REMOTE_DIR"
ssh "$REMOTE" "rm -rf $(printf '%q' "$REMOTE_DIR") && mkdir -p $(printf '%q' "$REMOTE_DIR")"
tar -C "$stage_dir" -cf - . | ssh "$REMOTE" "tar -C $(printf '%q' "$REMOTE_DIR") -xf -"

echo "copied artifacts to remote"

if (( COPY_ONLY )); then
  echo "copy-only mode; skipping remote execution"
  exit 0
fi

remote_exports=""
for kv in "${REMOTE_ENVS[@]}"; do
  if [[ "$kv" != *=* ]]; then
    echo "invalid --remote-env (expected KEY=VALUE): $kv" >&2
    exit 1
  fi
  key="${kv%%=*}"
  val="${kv#*=}"
  remote_exports+="export $(printf '%q' "$key")=$(printf '%q' "$val")\n"
done

if ! has_remote_env_key "TEMPORAL_NBD_BIN"; then
  remote_exports+="export TEMPORAL_NBD_BIN=./temporal-nbd-cli\n"
fi

if ! has_remote_env_key "RUST_BACKTRACE"; then
  remote_exports+="export RUST_BACKTRACE=full\n"
fi
if ! has_remote_env_key "RUST_LOG"; then
  remote_exports+="export RUST_LOG=trace\n"
fi
if ! has_remote_env_key "RUST_TEST_THREADS"; then
  remote_exports+="export RUST_TEST_THREADS=1\n"
fi

fetch_remote_logs() {
  if (( COPY_ONLY == 1 )); then
    return 0
  fi

  mkdir -p "$LOCAL_LOG_DIR"
  if ssh -o ConnectTimeout=8 "$REMOTE" "test -d $(printf '%q' "$REMOTE_LOG_DIR")"; then
    if ssh "$REMOTE" "tar -C $(printf '%q' "$REMOTE_DIR") -cf - $(printf '%q' "$REMOTE_LOG_SUBDIR")" \
      | tar -C "$LOCAL_LOG_DIR" -xf -; then
      echo "downloaded remote troubleshooting logs to $LOCAL_LOG_DIR/$REMOTE_LOG_SUBDIR"
    else
      echo "warning: failed to download remote logs from $REMOTE_LOG_DIR" >&2
    fi
  else
    echo "warning: remote log directory not found: $REMOTE_LOG_DIR" >&2
  fi
}

echo "running test executables on remote (log dir: $REMOTE_LOG_DIR)"
set +e
ssh "$REMOTE" "bash -s" <<__REMOTE__
set -euo pipefail
cd $(printf '%q' "$REMOTE_DIR")
$(printf '%b' "$remote_exports")
TAB=\$(printf '\t')
LOG_DIR="./$(printf '%q' "$REMOTE_LOG_SUBDIR")"
mkdir -p "\$LOG_DIR"
ATTACH_LOG_MARKER="\$LOG_DIR/.attach-log-start"
: > "\$ATTACH_LOG_MARKER"

if sudo -n true >/dev/null 2>&1; then
  SUDO_BIN=(sudo -n)
else
  SUDO_BIN=()
fi
SUDO_PRESERVE_ENV=""
if ((\${#SUDO_BIN[@]} > 0)); then
  preserve_vars=()
  while IFS='=' read -r env_name _; do
    case "\$env_name" in
      TEMPORAL_*|RUST_*)
        preserve_vars+=("\$env_name")
        ;;
    esac
  done < <(env)
  if ((\${#preserve_vars[@]} > 0)); then
    old_ifs="\$IFS"
    IFS=,
    SUDO_PRESERVE_ENV="\${preserve_vars[*]}"
    IFS="\$old_ifs"
  fi
fi
TRACE_CMD_ENABLED=0
TRACE_CMD_REASON="trace-cmd unavailable"
if command -v trace-cmd >/dev/null 2>&1 && ((\${#SUDO_BIN[@]} > 0)); then
  if "\${SUDO_BIN[@]}" test -e /sys/kernel/debug/tracing/tracing_on >/dev/null 2>&1; then
    TRACE_CMD_ENABLED=1
    TRACE_CMD_REASON="enabled (existing debugfs tracing)"
  else
    "\${SUDO_BIN[@]}" mount -t debugfs debugfs /sys/kernel/debug >/dev/null 2>&1 || true
    if "\${SUDO_BIN[@]}" test -e /sys/kernel/debug/tracing/tracing_on >/dev/null 2>&1; then
      TRACE_CMD_ENABLED=1
      TRACE_CMD_REASON="enabled (mounted debugfs)"
    else
      TRACE_CMD_REASON="debugfs tracing unavailable"
    fi
  fi
elif command -v trace-cmd >/dev/null 2>&1; then
  TRACE_CMD_REASON="sudo -n unavailable for trace-cmd"
fi

collect_host_snapshot() {
  local out_prefix="\$1"
  {
    echo "timestamp_utc=\$(date -u +%FT%TZ)"
    uname -a || true
    id || true
    echo
    echo "tooling:"
    for t in strace trace-cmd blktrace bpftrace perf dmesg mkfs.ext4 dd mount umount blockdev; do
      if command -v "\$t" >/dev/null 2>&1; then
        echo "  \$t=\$(command -v "\$t")"
      elif [[ -x "/sbin/\$t" ]]; then
        echo "  \$t=/sbin/\$t"
      elif [[ -x "/usr/sbin/\$t" ]]; then
        echo "  \$t=/usr/sbin/\$t"
      else
        echo "  \$t=not-found"
      fi
    done
    echo "trace_cmd_state=\$TRACE_CMD_REASON"
    echo
    echo "block-devices:"
    if command -v lsblk >/dev/null 2>&1; then
      lsblk -a -o NAME,MAJ:MIN,SIZE,RO,TYPE,MOUNTPOINTS || true
    else
      ls -l /dev/nbd* 2>&1 || true
    fi
    echo
    echo "mounts:"
    if command -v findmnt >/dev/null 2>&1; then
      findmnt -A || true
    else
      mount || true
    fi
    echo
    echo "disk-usage:"
    df -h || true
  } > "\${out_prefix}.log" 2>&1
}

collect_nbd_snapshot() {
  local out_file="\$1"
  {
    echo "timestamp_utc=\$(date -u +%FT%TZ)"
    ls -l /dev/nbd* 2>&1 || true
    for dev in /sys/block/nbd*; do
      [[ -e "\$dev" ]] || continue
      echo "== \${dev} =="
      for rel in size ro pid stat queue/logical_block_size queue/physical_block_size queue/max_hw_sectors_kb queue/max_sectors_kb queue/read_ahead_kb; do
        if [[ -f "\$dev/\$rel" ]]; then
          printf '%s: %s\n' "\$rel" "\$(cat "\$dev/\$rel" 2>/dev/null || true)"
        fi
      done
    done
  } > "\$out_file" 2>&1
}

copy_attach_logs() {
  if [[ -f "\$ATTACH_LOG_MARKER" ]]; then
    find /tmp -maxdepth 1 -type f -name 'temporal-nbd-attach-*.log' -newer "\$ATTACH_LOG_MARKER" \
      -exec cp -f {} "\$LOG_DIR/" \; 2>/dev/null || true
    return 0
  fi
  find /tmp -maxdepth 1 -type f -name 'temporal-nbd-attach-*.log' -exec cp -f {} "\$LOG_DIR/" \; 2>/dev/null || true
  return 0
}

start_dmesg_stream() {
  local out_file="\$1"
  if ! command -v dmesg >/dev/null 2>&1; then
    return 0
  fi
  if ((\${#SUDO_BIN[@]} > 0)); then
    "\${SUDO_BIN[@]}" dmesg -wT >"\$out_file" 2>&1 &
  else
    dmesg -wT >"\$out_file" 2>&1 &
  fi
  echo "\$!"
}

stop_dmesg_stream() {
  local pid="\$1"
  if [[ -z "\$pid" ]]; then
    return 0
  fi
  if kill -0 "\$pid" >/dev/null 2>&1; then
    kill "\$pid" >/dev/null 2>&1 || true
    wait "\$pid" >/dev/null 2>&1 || true
  fi
}

dump_dmesg_snapshot() {
  local out_file="\$1"
  if ! command -v dmesg >/dev/null 2>&1; then
    return 0
  fi
  if ((\${#SUDO_BIN[@]} > 0)); then
    "\${SUDO_BIN[@]}" dmesg -T >"\$out_file" 2>&1 || true
  else
    dmesg -T >"\$out_file" 2>&1 || true
  fi
}

run_with_diagnostics() {
  local mode="\$1"
  local target_name="\$2"
  local target_kind="\$3"
  local exec_base="\$4"
  shift 4

  local safe_target
  safe_target="\$(printf '%s' "\$target_name" | tr -c 'A-Za-z0-9._-' '_')"
  local prefix="\$LOG_DIR/\${mode}-\${safe_target}"
  local dmesg_pid=""

  echo "==> [\${mode}] \${target_name} (\${target_kind})"
  echo "    diagnostics: \${prefix}.*"
  collect_nbd_snapshot "\${prefix}.nbd.before.log"
  dump_dmesg_snapshot "\${prefix}.dmesg.before.log"
  dmesg_pid="\$(start_dmesg_stream "\${prefix}.dmesg.stream.log" || true)"

  local -a cmd=("./\$exec_base" "\$@")
  if command -v strace >/dev/null 2>&1; then
    if ((\${#SUDO_BIN[@]} > 0)); then
      if [[ -n "\$SUDO_PRESERVE_ENV" ]]; then
        cmd=(
          "\${SUDO_BIN[@]}"
          "--preserve-env=\${SUDO_PRESERVE_ENV}"
          strace -ff -ttt -T -yy -xx -s 256 -o "\${prefix}.strace"
          "\${cmd[@]}"
        )
      else
        cmd=("\${SUDO_BIN[@]}" strace -ff -ttt -T -yy -xx -s 256 -o "\${prefix}.strace" "\${cmd[@]}")
      fi
    else
      cmd=(strace -ff -ttt -T -yy -xx -s 256 -o "\${prefix}.strace" "\${cmd[@]}")
    fi
  fi
  if (( TRACE_CMD_ENABLED == 1 )); then
    if [[ "\${cmd[0]:-}" == "sudo" ]]; then
      drop=1
      if [[ "\${cmd[\$drop]:-}" == "-n" ]]; then
        ((drop++))
      fi
      if [[ "\${cmd[\$drop]:-}" == --preserve-env=* ]]; then
        ((drop++))
      fi
      cmd=("\${cmd[@]:\$drop}")
    fi
    cmd=("\${SUDO_BIN[@]}" trace-cmd record -q -o "\${prefix}.trace-cmd.dat" -e block:* -e ext4:* -- "\${cmd[@]}")
  fi

  {
    echo "timestamp_utc=\$(date -u +%FT%TZ)"
    echo "mode=\$mode"
    echo "target_name=\$target_name"
    echo "target_kind=\$target_kind"
    echo "trace_cmd_state=\$TRACE_CMD_REASON"
    printf 'command='
    printf '%q ' "\${cmd[@]}"
    printf '\n'
  } > "\${prefix}.meta.log"

  set +e
  "\${cmd[@]}" >"\${prefix}.stdout.log" 2>"\${prefix}.stderr.log"
  local rc=\$?
  set -e

  if [[ -s "\${prefix}.stdout.log" ]]; then
    cat "\${prefix}.stdout.log"
  fi
  if [[ -s "\${prefix}.stderr.log" ]]; then
    cat "\${prefix}.stderr.log" >&2
  fi

  stop_dmesg_stream "\$dmesg_pid"
  dump_dmesg_snapshot "\${prefix}.dmesg.after.log"
  collect_nbd_snapshot "\${prefix}.nbd.after.log"
  copy_attach_logs

  if (( rc != 0 )); then
    echo "command failed: mode=\$mode target=\$target_name exit=\$rc" >&2
    echo "troubleshooting logs are in \$LOG_DIR" >&2
    return \$rc
  fi

  return 0
}

remote_cleanup() {
  collect_host_snapshot "\$LOG_DIR/host-end"
  copy_attach_logs
}
trap remote_cleanup EXIT

collect_host_snapshot "\$LOG_DIR/host-start"

while IFS="\$TAB" read -r target_name target_kind exec_base; do
  run_with_diagnostics normal "\$target_name" "\$target_kind" "\$exec_base" --nocapture
done < manifest.tsv

if [[ "$RUN_IGNORED" == "1" ]]; then
  while IFS="\$TAB" read -r target_name target_kind exec_base; do
    run_with_diagnostics ignored "\$target_name" "\$target_kind" "\$exec_base" --ignored --nocapture
  done < manifest.tsv
fi
__REMOTE__
REMOTE_EXEC_STATUS=$?
set -e

fetch_remote_logs || true

if (( REMOTE_EXEC_STATUS != 0 )); then
  echo "remote prebuilt test run failed (exit=${REMOTE_EXEC_STATUS})" >&2
  echo "use logs from: $LOCAL_LOG_DIR/$REMOTE_LOG_SUBDIR" >&2
  exit "$REMOTE_EXEC_STATUS"
fi

echo "remote prebuilt test run complete"
echo "troubleshooting logs available at: $LOCAL_LOG_DIR/$REMOTE_LOG_SUBDIR"
