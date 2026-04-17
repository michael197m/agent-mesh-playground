#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="${RUNTIME_DIR:-$ROOT_DIR/runtime}"
LOG_DIR="${LOG_DIR:-$RUNTIME_DIR/logs}"
MESH_DB="${MESH_DB:-$RUNTIME_DIR/mesh.db}"
DOCS_DIR="${DOCS_DIR:-$RUNTIME_DIR/docs}"
PID_FILE="${PID_FILE:-$RUNTIME_DIR/backend.pids}"
OLLAMA_BASE_URL="${OLLAMA_BASE_URL:-http://localhost:11434}"
OLLAMA_MODEL="${OLLAMA_MODEL:-llama3.1}"
API_GATEWAY_ADDR="${API_GATEWAY_ADDR:-:8080}"

PIDS=()
NAMES=()

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

ensure_default_docs() {
  if find "$DOCS_DIR" -mindepth 1 -print -quit >/dev/null 2>&1; then
    return
  fi

  cat >"$DOCS_DIR/runbook.md" <<'EOF'
# Sample Runbook

Packet loss after a config update:
- verify interface error counters
- compare the new config against the previous known-good version
- rollback the change if impact is ongoing
- inspect MTU, routing, and VPN tunnel health
EOF
}

cleanup() {
  local exit_code=$?

  if ((${#PIDS[@]} > 0)); then
    printf '\nStopping backend processes...\n'
    kill "${PIDS[@]}" >/dev/null 2>&1 || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi

  rm -f "$PID_FILE"

  exit "$exit_code"
}

start_service() {
  local name=$1
  local workdir=$2
  shift 2

  local log_file="$LOG_DIR/$name.log"
  printf 'Starting %-18s -> %s\n' "$name" "$log_file"

  (
    cd "$workdir"
    env "$@" go run .
  ) >"$log_file" 2>&1 &

  PIDS+=("$!")
  NAMES+=("$name")
}

stop_prior_backend() {
  if [[ ! -f "$PID_FILE" ]]; then
    return
  fi

  printf 'Cleaning up prior backend processes from %s\n' "$PID_FILE"

  while read -r pid name; do
    if [[ -z "${pid:-}" ]]; then
      continue
    fi
    if kill -0 "$pid" 2>/dev/null; then
      printf 'Stopping %-18s pid=%s\n' "${name:-unknown}" "$pid"
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done <"$PID_FILE"

  rm -f "$PID_FILE"
}

write_pid_file() {
  : >"$PID_FILE"
  for index in "${!PIDS[@]}"; do
    printf '%s %s\n' "${PIDS[$index]}" "${NAMES[$index]}" >>"$PID_FILE"
  done
}

print_service_failure() {
  local index=$1
  local name=${NAMES[$index]}
  local log_file="$LOG_DIR/$name.log"

  printf '\nService failed: %s\n' "$name" >&2
  printf 'Log tail: %s\n' "$log_file" >&2
  tail -n 40 "$log_file" >&2 || true
}

ensure_services_running() {
  local startup_delay=${1:-2}

  sleep "$startup_delay"
  for index in "${!PIDS[@]}"; do
    if ! kill -0 "${PIDS[$index]}" 2>/dev/null; then
      print_service_failure "$index"
      exit 1
    fi
  done
}

monitor_services() {
  while true; do
    if ! wait -n "${PIDS[@]}"; then
      :
    fi

    for index in "${!PIDS[@]}"; do
      if ! kill -0 "${PIDS[$index]}" 2>/dev/null; then
        print_service_failure "$index"
        exit 1
      fi
    done
  done
}

require_command go

mkdir -p "$RUNTIME_DIR" "$LOG_DIR" "$DOCS_DIR"
ensure_default_docs
stop_prior_backend

trap cleanup INT TERM EXIT

printf 'Backend runtime\n'
printf '  root:         %s\n' "$ROOT_DIR"
printf '  database:     %s\n' "$MESH_DB"
printf '  docs:         %s\n' "$DOCS_DIR"
printf '  ollama:       %s\n' "$OLLAMA_BASE_URL"
printf '  ollama model: %s\n\n' "$OLLAMA_MODEL"
printf '  gateway addr: %s\n\n' "$API_GATEWAY_ADDR"

start_service \
  "api-gateway" \
  "$ROOT_DIR/services/api-gateway" \
  API_GATEWAY_DB_PATH="$MESH_DB" \
  API_GATEWAY_ADDR="$API_GATEWAY_ADDR"

start_service \
  "planner-agent" \
  "$ROOT_DIR/services/planner-agent" \
  PLANNER_DB_PATH="$MESH_DB" \
  OLLAMA_BASE_URL="$OLLAMA_BASE_URL" \
  OLLAMA_MODEL="$OLLAMA_MODEL"

start_service \
  "retriever-agent" \
  "$ROOT_DIR/services/retriever-agent" \
  RETRIEVER_DB_PATH="$MESH_DB" \
  RETRIEVER_DOCS_PATH="$DOCS_DIR"

start_service \
  "classifier-agent" \
  "$ROOT_DIR/services/classifier-agent" \
  CLASSIFIER_DB_PATH="$MESH_DB" \
  OLLAMA_BASE_URL="$OLLAMA_BASE_URL" \
  OLLAMA_MODEL="$OLLAMA_MODEL"

start_service \
  "responder-agent" \
  "$ROOT_DIR/services/responder-agent" \
  RESPONDER_DB_PATH="$MESH_DB" \
  OLLAMA_BASE_URL="$OLLAMA_BASE_URL" \
  OLLAMA_MODEL="$OLLAMA_MODEL"

start_service \
  "aggregator-agent" \
  "$ROOT_DIR/services/aggregator-agent" \
  AGGREGATOR_DB_PATH="$MESH_DB"

printf '\nBackend is starting. Logs:\n'
printf '  %s\n' "$LOG_DIR"
printf '\nPress Ctrl-C to stop all services.\n'

write_pid_file
ensure_services_running 2
monitor_services
