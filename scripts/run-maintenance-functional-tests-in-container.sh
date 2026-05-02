#!/usr/bin/env bash
set -euo pipefail

phase="${1:-${OPENCOOK_FUNCTIONAL_PHASE:-maintenance}}"
base_url="${OPENCOOK_ADMIN_SERVER_URL:-${OPENCOOK_FUNCTIONAL_BASE_URL:-http://opencook:4000}}"
org="${OPENCOOK_FUNCTIONAL_ORG:-ponyville}"
state_dir="${OPENCOOK_FUNCTIONAL_STATE_DIR:-/var/lib/opencook-functional}"
cli="${OPENCOOK_FUNCTIONAL_CLI:-/tmp/opencook-functional-cli}"
admin_private_key="${OPENCOOK_ADMIN_PRIVATE_KEY_PATH:-${OPENCOOK_FUNCTIONAL_PRIVATE_KEY_PATH:-/src/test/functional/fixtures/bootstrap_private.pem}}"
admin_requestor="${OPENCOOK_ADMIN_REQUESTOR_NAME:-${OPENCOOK_FUNCTIONAL_ACTOR_NAME:-pivotal}}"
maintenance_active=0

# build_cli compiles the in-repo binary so the functional phase exercises the
# exact admin command implementation from the checked-out source tree.
build_cli() {
  go build -trimpath -o "$cli" ./cmd/opencook
}

admin() {
  "$cli" admin "$@"
}

admin_json() {
  "$cli" admin --json "$@"
}

expect_exit() {
  local want="$1"
  shift
  set +e
  "$@"
  local got="$?"
  set -e
  if [[ "$got" != "$want" ]]; then
    echo "command exited $got, want $want: $*" >&2
    return 1
  fi
}

require_json_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq "$needle" "$file"; then
    echo "expected $file to contain: $needle" >&2
    echo "actual output:" >&2
    cat "$file" >&2
    return 1
  fi
}

# enable_functional_maintenance opens the shared write gate and records local
# ownership so the EXIT trap can avoid leaving a stale maintenance window.
enable_functional_maintenance() {
  local mode="$1"
  local reason="$2"
  admin maintenance enable --mode "$mode" --reason "$reason" --actor functional-tests --yes --json >/tmp/opencook-maintenance-enable.json
  maintenance_active=1
  require_json_contains /tmp/opencook-maintenance-enable.json '"command": "maintenance_enable"'
  require_json_contains /tmp/opencook-maintenance-enable.json '"active": true'
  require_json_contains /tmp/opencook-maintenance-enable.json '"name": "postgres"'
  require_json_contains /tmp/opencook-maintenance-enable.json '"shared": true'
}

# disable_functional_maintenance is idempotent so cleanup can call it after
# successful phases and after failures without masking the original command.
disable_functional_maintenance() {
  admin maintenance disable --actor functional-tests --yes --json >/tmp/opencook-maintenance-disable.json
  maintenance_active=0
  require_json_contains /tmp/opencook-maintenance-disable.json '"command": "maintenance_disable"'
  require_json_contains /tmp/opencook-maintenance-disable.json '"active": false'
}

# cleanup_functional_maintenance closes only the maintenance window created by
# this script, keeping manual operator windows outside the phase untouched.
cleanup_functional_maintenance() {
  if [[ "$maintenance_active" == "1" ]]; then
    admin maintenance disable --actor functional-tests --yes --json >/tmp/opencook-maintenance-disable-cleanup.json 2>/tmp/opencook-maintenance-disable-cleanup.err || true
    maintenance_active=0
  fi
}

# ensure_functional_fixture_state makes the maintenance phase independently
# runnable from a fresh Compose stack before it freezes Chef-facing writes.
ensure_functional_fixture_state() {
  echo "==> maintenance fixture setup"
  OPENCOOK_FUNCTIONAL_PHASE=create go test ./test/functional -count=1 -run TestFunctional -v
}

# require_workflows_reject_without_maintenance proves online repair/reindex
# mutations fail closed before the write gate is explicitly enabled.
require_workflows_reject_without_maintenance() {
  echo "==> maintenance workflow gates reject inactive state"
  expect_exit 4 admin reindex --org "$org" --index node --no-drop --json >/tmp/opencook-maintenance-reindex-required.json 2>/tmp/opencook-maintenance-reindex-required.err
  require_json_contains /tmp/opencook-maintenance-reindex-required.json '"code": "maintenance_required"'

  expect_exit 4 admin search repair --org "$org" --index node --yes --json >/tmp/opencook-maintenance-search-repair-required.json 2>/tmp/opencook-maintenance-search-repair-required.err
  require_json_contains /tmp/opencook-maintenance-search-repair-required.json '"code": "maintenance_required"'

  expect_exit 4 admin_json acls repair-defaults --online --yes --org "$org" >/tmp/opencook-maintenance-acl-repair-required.json 2>/tmp/opencook-maintenance-acl-repair-required.err
  require_json_contains /tmp/opencook-maintenance-acl-repair-required.err 'returned HTTP 409'
}

# require_active_maintenance_status_surfaces checks the operator-facing status,
# doctor, and metrics surfaces against the same shared PostgreSQL state.
require_active_maintenance_status_surfaces() {
  echo "==> maintenance status surfaces"
  admin maintenance status --json --with-timing >/tmp/opencook-maintenance-status.json
  require_json_contains /tmp/opencook-maintenance-status.json '"command": "maintenance_status"'
  require_json_contains /tmp/opencook-maintenance-status.json '"active": true'
  require_json_contains /tmp/opencook-maintenance-status.json '"name": "postgres"'
  require_json_contains /tmp/opencook-maintenance-status.json '"shared": true'

  admin maintenance check --json >/tmp/opencook-maintenance-check.json
  require_json_contains /tmp/opencook-maintenance-check.json '"command": "maintenance_check"'
  require_json_contains /tmp/opencook-maintenance-check.json '"active": true'

  admin_json status >/tmp/opencook-maintenance-admin-status.json
  require_json_contains /tmp/opencook-maintenance-admin-status.json '"maintenance"'
  require_json_contains /tmp/opencook-maintenance-admin-status.json '"active": true'
  require_json_contains /tmp/opencook-maintenance-admin-status.json 'mutating Chef-facing writes are currently blocked'

  admin service doctor --offline --json --with-timing >/tmp/opencook-maintenance-service-doctor.json
  require_json_contains /tmp/opencook-maintenance-service-doctor.json '"maintenance_state"'
  require_json_contains /tmp/opencook-maintenance-service-doctor.json 'writes are blocked'
  require_json_contains /tmp/opencook-maintenance-service-doctor.json '"opensearch_ping"'
  require_json_contains /tmp/opencook-maintenance-service-doctor.json '"blob_inventory"'

  curl -fsS "$base_url/metrics" >/tmp/opencook-maintenance-metrics.prom
  require_json_contains /tmp/opencook-maintenance-metrics.prom 'opencook_maintenance_enabled{backend="postgres",shared="true"} 1'
}

# require_workflows_succeed_with_maintenance proves mutation-capable admin
# workflows explicitly observe the active shared gate before doing work.
require_workflows_succeed_with_maintenance() {
  echo "==> maintenance-gated repair and reindex workflows"
  admin reindex --org "$org" --index node --no-drop --json --with-timing >/tmp/opencook-maintenance-reindex.json
  require_json_contains /tmp/opencook-maintenance-reindex.json '"command": "reindex"'
  require_json_contains /tmp/opencook-maintenance-reindex.json 'active maintenance mode confirmed'

  admin search repair --org "$org" --index node --yes --json --with-timing >/tmp/opencook-maintenance-search-repair.json
  require_json_contains /tmp/opencook-maintenance-search-repair.json '"command": "search_repair"'
  require_json_contains /tmp/opencook-maintenance-search-repair.json 'active maintenance mode confirmed'

  admin_json acls repair-defaults --online --yes --org "$org" >/tmp/opencook-maintenance-acl-repair.json
  require_json_contains /tmp/opencook-maintenance-acl-repair.json '"operation": "acl-default-repair"'
  require_json_contains /tmp/opencook-maintenance-acl-repair.json '"mode": "online"'
  require_json_contains /tmp/opencook-maintenance-acl-repair.json '"verifier_cache": "unchanged"'

  admin migration preflight --all-orgs --json >/tmp/opencook-maintenance-migration-preflight.json
  require_json_contains /tmp/opencook-maintenance-migration-preflight.json '"command": "migration_preflight"'
  require_json_contains /tmp/opencook-maintenance-migration-preflight.json '"name": "postgres"'
  require_json_contains /tmp/opencook-maintenance-migration-preflight.json '"name": "opensearch"'
  require_json_contains /tmp/opencook-maintenance-migration-preflight.json '"name": "blob"'
}

# run_go_maintenance_checks uses the signed functional client helpers to prove
# Chef-facing reads and read-like POST routes keep working while writes block.
run_go_maintenance_checks() {
  echo "==> maintenance Chef-facing route checks"
  OPENCOOK_FUNCTIONAL_PHASE=maintenance go test ./test/functional -count=1 -run TestFunctional -v
}

run_maintenance_phase() {
  build_cli
  mkdir -p "$state_dir"
  disable_functional_maintenance
  ensure_functional_fixture_state
  require_workflows_reject_without_maintenance
  enable_functional_maintenance repair "functional maintenance coverage"
  require_active_maintenance_status_surfaces
  run_go_maintenance_checks
  require_workflows_succeed_with_maintenance
  disable_functional_maintenance
  echo "==> maintenance functional tests passed successfully"
}

export OPENCOOK_ADMIN_SERVER_URL="$base_url"
export OPENCOOK_ADMIN_REQUESTOR_NAME="$admin_requestor"
export OPENCOOK_ADMIN_REQUESTOR_TYPE="${OPENCOOK_ADMIN_REQUESTOR_TYPE:-user}"
export OPENCOOK_ADMIN_PRIVATE_KEY_PATH="$admin_private_key"
export OPENCOOK_ADMIN_DEFAULT_ORG="$org"

trap cleanup_functional_maintenance EXIT

case "$phase" in
  maintenance)
    run_maintenance_phase
    ;;
  *)
    echo "unknown maintenance functional phase: $phase" >&2
    exit 2
    ;;
esac
