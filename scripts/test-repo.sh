#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run_go_tests() {
  local module_dir=$1
  printf '\n==> go test %s\n' "$module_dir"
  (
    cd "$ROOT_DIR/$module_dir"
    GOCACHE=/tmp/go-build go test ./...
  )
}

printf 'Repo test run\n'
printf '  root: %s\n' "$ROOT_DIR"

run_go_tests "services/api-gateway"
run_go_tests "services/planner-agent"
run_go_tests "services/retriever-agent"
run_go_tests "services/classifier-agent"
run_go_tests "services/responder-agent"
run_go_tests "services/aggregator-agent"
run_go_tests "shared/event-schema"

printf '\n==> frontend tests\n'
(
  cd "$ROOT_DIR/frontend"
  npm test
)

printf '\n==> frontend build\n'
(
  cd "$ROOT_DIR/frontend"
  npm run build
)

printf '\nAll repo checks passed.\n'
