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

if [[ -n "${OPENCOOK_FUNCTIONAL_OPENSEARCH_MATRIX:-}" && "${OPENCOOK_FUNCTIONAL_MATRIX_CHILD:-0}" != "1" ]]; then
  for image in $OPENCOOK_FUNCTIONAL_OPENSEARCH_MATRIX; do
    safe_image="$(printf '%s' "$image" | tr -cs '[:alnum:]' '-' | tr '[:upper:]' '[:lower:]' | sed 's/^-//;s/-$//')"
    if [[ -z "$safe_image" ]]; then
      safe_image="provider"
    fi
    echo
    echo "==> functional OpenSearch provider image: $image"
    COMPOSE_PROJECT_NAME="${project_name}-${safe_image}" OPENSEARCH_IMAGE="$image" OPENCOOK_FUNCTIONAL_MATRIX_CHILD=1 "$0" "$@"
  done
  echo
  echo "==> functional OpenSearch provider matrix passed successfully"
  exit 0
fi

cleanup() {
  if [[ "${KEEP_STACK:-0}" != "1" ]]; then
    if [[ "${OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS:-0}" == "1" ]]; then
      compose down --remove-orphans
    else
      compose down -v --remove-orphans
    fi
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

# run_phase forwards phase and artifact-retention settings into the isolated test
# container so diagnostics created there follow the same KEEP_STACK contract.
run_phase() {
	local phase="$1"
	echo
	echo "==> functional phase: $phase"
	compose_with_tests run --rm \
		-e "OPENCOOK_FUNCTIONAL_PHASE=$phase" \
		-e "KEEP_STACK=${KEEP_STACK:-0}" \
		-e "OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=${OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS:-${KEEP_STACK:-0}}" \
		-e "OPENCOOK_FUNCTIONAL_SCALE_PROFILE=${OPENCOOK_FUNCTIONAL_SCALE_PROFILE:-small}" \
		functional-tests
}

restart_opencook() {
  echo
  echo "==> restarting OpenCook"
  compose restart opencook
  wait_for_opencook
}

print_success() {
  echo
  if [[ "$#" -gt 0 ]]; then
    echo "==> functional tests passed successfully for phases: $*"
  else
    echo "==> functional tests passed successfully"
  fi
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
  print_success "$@"
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
run_phase maintenance
run_phase migration-preflight
run_phase migration-backup
run_phase migration-backup-inspect
run_phase delete
restart_opencook
run_phase verify-deleted
print_success
