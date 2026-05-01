#!/usr/bin/env bash
set -euo pipefail

phase="${1:-${OPENCOOK_FUNCTIONAL_PHASE:-verify}}"
base_url="${OPENCOOK_FUNCTIONAL_BASE_URL:-http://opencook:4000}"

if [[ "$phase" == "wait" ]]; then
  for _ in $(seq 1 90); do
    if curl -fsS "$base_url/readyz" >/dev/null; then
      exit 0
    fi
    sleep 2
  done
  echo "OpenCook did not become ready at $base_url" >&2
  exit 1
fi

case "$phase" in
  operational|operational-verify)
    exec bash /src/scripts/run-operational-functional-tests-in-container.sh "$phase"
    ;;
  migration-*)
    exec bash /src/scripts/run-migration-functional-tests-in-container.sh "$phase"
    ;;
  create|verify|query-compat|invalid|search-update|verify-search-updated|delete|verify-deleted|all)
    ;;
  *)
    echo "unknown functional test phase: $phase" >&2
    exit 2
    ;;
esac

export OPENCOOK_FUNCTIONAL_PHASE="$phase"
exec go test ./test/functional -count=1 -run TestFunctional -v
