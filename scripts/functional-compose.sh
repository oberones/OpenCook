#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${OPENCOOK_FUNCTIONAL_COMPOSE_FILE:-$root_dir/deploy/functional/docker-compose.yml}"
project_name="${COMPOSE_PROJECT_NAME:-opencook-functional}"

compose() {
	docker compose -p "$project_name" -f "$compose_file" "$@"
}

compose_with_tests() {
	docker compose -p "$project_name" -f "$compose_file" --profile test "$@"
}

cleanup() {
  if [[ "${KEEP_STACK:-0}" != "1" ]]; then
    compose down -v --remove-orphans
  fi
}

wait_for_opencook() {
  for _ in $(seq 1 90); do
    if compose exec -T opencook wget -qO- http://127.0.0.1:4000/readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  compose logs --tail=200 opencook
  echo "OpenCook did not become healthy in the functional Compose stack" >&2
  return 1
}

run_phase() {
	local phase="$1"
	echo
	echo "==> functional phase: $phase"
	compose_with_tests run --rm -e "OPENCOOK_FUNCTIONAL_PHASE=$phase" functional-tests
}

restart_opencook() {
  echo
  echo "==> restarting OpenCook"
  compose restart opencook
  wait_for_opencook
}

trap cleanup EXIT

if [[ "${PULL:-0}" == "1" ]]; then
  compose pull postgres opensearch
fi

up_args=(-d)
if [[ "${REBUILD:-1}" == "1" ]]; then
  up_args+=(--build)
fi

compose up "${up_args[@]}" postgres opensearch opencook
if [[ "${REBUILD:-1}" == "1" ]]; then
	compose_with_tests build functional-tests
fi
wait_for_opencook

if [[ "$#" -gt 0 ]]; then
  for phase in "$@"; do
    if [[ "$phase" == "restart" ]]; then
      restart_opencook
    else
      run_phase "$phase"
    fi
  done
  exit 0
fi

run_phase create
restart_opencook
run_phase verify
run_phase query-compat
run_phase invalid
restart_opencook
run_phase verify
run_phase search-update
run_phase verify-search-updated
restart_opencook
run_phase verify-search-updated
run_phase query-compat
run_phase operational
restart_opencook
run_phase operational-verify
run_phase delete
restart_opencook
run_phase verify-deleted
