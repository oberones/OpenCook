#!/usr/bin/env bash
set -euo pipefail

phase="${1:-${OPENCOOK_FUNCTIONAL_PHASE:-migration-preflight}}"
base_url="${OPENCOOK_ADMIN_SERVER_URL:-${OPENCOOK_FUNCTIONAL_BASE_URL:-http://opencook:4000}}"
org="${OPENCOOK_FUNCTIONAL_ORG:-ponyville}"
state_dir="${OPENCOOK_FUNCTIONAL_STATE_DIR:-/var/lib/opencook-functional}"
cli="${OPENCOOK_FUNCTIONAL_CLI:-/tmp/opencook-functional-cli}"
admin_private_key="${OPENCOOK_ADMIN_PRIVATE_KEY_PATH:-${OPENCOOK_FUNCTIONAL_PRIVATE_KEY_PATH:-/src/test/functional/fixtures/bootstrap_private.pem}}"
admin_requestor="${OPENCOOK_ADMIN_REQUESTOR_NAME:-${OPENCOOK_FUNCTIONAL_ACTOR_NAME:-pivotal}}"
backup_dir="${OPENCOOK_FUNCTIONAL_MIGRATION_BACKUP_DIR:-$state_dir/migration-backup}"
restore_admin_dsn="${OPENCOOK_FUNCTIONAL_RESTORE_POSTGRES_ADMIN_DSN:-postgres://opencook:opencook@postgres:5432/postgres?sslmode=disable}"
restore_dsn="${OPENCOOK_FUNCTIONAL_RESTORE_POSTGRES_DSN:-postgres://opencook:opencook@postgres:5432/opencook_restore?sslmode=disable}"
restore_db="${OPENCOOK_FUNCTIONAL_RESTORE_DB:-opencook_restore}"
restore_blob_url="${OPENCOOK_FUNCTIONAL_RESTORE_BLOB_STORAGE_URL:-file://$state_dir/restore-blobs}"
restore_blob_dir="${restore_blob_url#file://}"
restore_server_url="${OPENCOOK_FUNCTIONAL_RESTORE_SERVER_URL:-http://127.0.0.1:4400}"
restore_listen_address="${OPENCOOK_FUNCTIONAL_RESTORE_LISTEN_ADDRESS:-127.0.0.1:4400}"
opensearch_url="${OPENCOOK_OPENSEARCH_URL:-http://opensearch:9200}"
restore_server_pid=""

build_cli() {
  go build -trimpath -o "$cli" ./cmd/opencook
}

admin() {
  "$cli" admin "$@"
}

admin_restore_target() {
  OPENCOOK_POSTGRES_DSN="$restore_dsn" \
  OPENCOOK_OPENSEARCH_URL="$opensearch_url" \
  OPENCOOK_BLOB_BACKEND=filesystem \
  OPENCOOK_BLOB_STORAGE_URL="$restore_blob_url" \
    "$cli" admin "$@"
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

print_file_if_exists() {
  local file="$1"
  if [[ -f "$file" ]]; then
    cat "$file" >&2
  fi
}

require_backup_bundle() {
  if [[ ! -f "$backup_dir/manifest.json" ]]; then
    echo "migration backup bundle not found at $backup_dir; run migration-backup or migration-all first" >&2
    return 1
  fi
}

restore_database_exists() {
  local exists
  if ! exists="$(psql "$restore_admin_dsn" -v ON_ERROR_STOP=1 -tAc "SELECT 1 FROM pg_database WHERE datname = '$restore_db'" 2>/dev/null)"; then
    return 1
  fi
  exists="${exists//[[:space:]]/}"
  [[ "$exists" == "1" ]]
}

restore_database_has_bootstrap_state() {
  local table_count
  if ! restore_database_exists; then
    return 1
  fi
  if ! table_count="$(psql "$restore_dsn" -v ON_ERROR_STOP=1 -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'oc_bootstrap_orgs'" 2>/dev/null)"; then
    return 1
  fi
  table_count="${table_count//[[:space:]]/}"
  if [[ "$table_count" != "1" ]]; then
    return 1
  fi

  local org_count
  if ! org_count="$(psql "$restore_dsn" -v ON_ERROR_STOP=1 -tAc "SELECT count(*) FROM oc_bootstrap_orgs" 2>/dev/null)"; then
    return 1
  fi
  org_count="${org_count//[[:space:]]/}"
  [[ "$org_count" != "0" ]]
}

ensure_restore_target_ready() {
  require_backup_bundle
  if restore_database_has_bootstrap_state; then
    return 0
  fi

  echo "restore target database $restore_db is missing or empty; restoring backup bundle before continuing"
  run_migration_restore
}

print_restore_server_log() {
  if [[ -f "$state_dir/restore-server.log" ]]; then
    tail -200 "$state_dir/restore-server.log" >&2
  fi
}

cleanup_restore_server() {
  if [[ -n "${restore_server_pid:-}" ]]; then
    kill "$restore_server_pid" 2>/dev/null || true
    wait "$restore_server_pid" 2>/dev/null || true
    restore_server_pid=""
  fi
}

wait_for_restore_server() {
  for _ in $(seq 1 60); do
    if [[ -n "${restore_server_pid:-}" ]] && ! kill -0 "$restore_server_pid" 2>/dev/null; then
      echo "restore OpenCook server exited before becoming ready at $restore_server_url" >&2
      print_restore_server_log
      return 1
    fi
    if curl -fsS "$restore_server_url/readyz" >/dev/null 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "restore OpenCook server did not become ready at $restore_server_url" >&2
  print_restore_server_log
  return 1
}

reset_restore_target() {
  mkdir -p "$state_dir"
  rm -rf "$restore_blob_dir"
  mkdir -p "$restore_blob_dir"
  psql "$restore_admin_dsn" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"$restore_db\" WITH (FORCE)"
  psql "$restore_admin_dsn" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$restore_db\""
}

run_migration_preflight() {
  build_cli

  echo "==> migration preflight on active stack"
  admin migration preflight --all-orgs --with-timing --json >/tmp/opencook-migration-preflight.json
  require_json_contains /tmp/opencook-migration-preflight.json '"command": "migration_preflight"'
  require_json_contains /tmp/opencook-migration-preflight.json '"name": "postgres"'
  require_json_contains /tmp/opencook-migration-preflight.json '"name": "blob"'
  require_json_contains /tmp/opencook-migration-preflight.json '"name": "opensearch"'
}

run_migration_backup() {
  build_cli

  echo "==> migration backup create from active stack"
  rm -rf "$backup_dir"
  mkdir -p "$state_dir"
  admin migration backup create --output "$backup_dir" --offline --yes --with-timing --json >/tmp/opencook-migration-backup-create.json
  require_json_contains /tmp/opencook-migration-backup-create.json '"command": "migration_backup_create"'
  require_json_contains /tmp/opencook-migration-backup-create.json '"write_backup_bundle"'
  test -f "$backup_dir/manifest.json"
}

run_migration_backup_inspect() {
  build_cli
  require_backup_bundle

  echo "==> migration backup inspect"
  admin migration backup inspect "$backup_dir" --json >/tmp/opencook-migration-backup-inspect.json
  require_json_contains /tmp/opencook-migration-backup-inspect.json '"command": "migration_backup_inspect"'
  require_json_contains /tmp/opencook-migration-backup-inspect.json '"backup_bundle"'
}

run_migration_restore_preflight() {
  build_cli
  require_backup_bundle
  reset_restore_target

  echo "==> migration restore preflight against fresh target"
  admin_restore_target migration restore preflight "$backup_dir" --offline --with-timing --json >/tmp/opencook-migration-restore-preflight.json
  require_json_contains /tmp/opencook-migration-restore-preflight.json '"command": "migration_restore_preflight"'
  require_json_contains /tmp/opencook-migration-restore-preflight.json '"restore_target"'
  require_json_contains /tmp/opencook-migration-restore-preflight.json '"state": "empty"'
}

run_migration_restore() {
  build_cli
  require_backup_bundle
  reset_restore_target

  echo "==> migration restore apply into fresh target"
  admin_restore_target migration restore apply "$backup_dir" --offline --yes --with-timing --json >/tmp/opencook-migration-restore-apply.json
  require_json_contains /tmp/opencook-migration-restore-apply.json '"command": "migration_restore_apply"'
  require_json_contains /tmp/opencook-migration-restore-apply.json '"restored_backup_bundle"'
  require_json_contains /tmp/opencook-migration-restore-apply.json '"restored_blob_objects"'
}

run_migration_reindex() {
  build_cli
  ensure_restore_target_ready

  echo "==> migration complete reindex from restored target"
  admin_restore_target reindex --all-orgs --complete --with-timing --json >/tmp/opencook-migration-reindex.json
  require_json_contains /tmp/opencook-migration-reindex.json '"ok": true'
  require_json_contains /tmp/opencook-migration-reindex.json '"command": "reindex"'
  require_json_contains /tmp/opencook-migration-reindex.json '"mode": "complete"'
  require_json_contains /tmp/opencook-migration-reindex.json '"upserted"'
  require_json_contains /tmp/opencook-migration-reindex.json '"deleted"'
}

run_migration_rehearsal() {
  build_cli
  ensure_restore_target_ready

  echo "==> migration cutover rehearsal against restored target"
  OPENCOOK_SERVICE_NAME=opencook-restore \
  OPENCOOK_ENV=functional-restore \
  OPENCOOK_LISTEN_ADDRESS="$restore_listen_address" \
  OPENCOOK_DEFAULT_ORGANIZATION="$org" \
  OPENCOOK_BOOTSTRAP_MODE=true \
  OPENCOOK_BOOTSTRAP_REQUESTOR_NAME="$admin_requestor" \
  OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user \
  OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID="${OPENCOOK_FUNCTIONAL_ACTOR_KEY_ID:-default}" \
  OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/src/test/functional/fixtures/bootstrap_public.pem \
  OPENCOOK_POSTGRES_DSN="$restore_dsn" \
  OPENCOOK_OPENSEARCH_URL="$opensearch_url" \
  OPENCOOK_BLOB_BACKEND=filesystem \
  OPENCOOK_BLOB_STORAGE_URL="$restore_blob_url" \
  OPENCOOK_AUTH_SKEW=15m \
    "$cli" serve >"$state_dir/restore-server.log" 2>&1 &
  restore_server_pid="$!"
  trap cleanup_restore_server EXIT
  echo "waiting for restored OpenCook server at $restore_server_url"
  wait_for_restore_server
  echo "restored OpenCook server is ready"

  echo "running live cutover rehearsal checks"
  if ! admin migration cutover rehearse \
    --manifest "$backup_dir/manifest.json" \
    --server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >/tmp/opencook-migration-cutover-rehearsal.json; then
    echo "migration cutover rehearsal command failed; output:" >&2
    print_file_if_exists /tmp/opencook-migration-cutover-rehearsal.json
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains /tmp/opencook-migration-cutover-rehearsal.json '"command": "migration_cutover_rehearse"'
  require_json_contains /tmp/opencook-migration-cutover-rehearsal.json '"cutover_rehearsal"'
  require_json_contains /tmp/opencook-migration-cutover-rehearsal.json '"family": "rehearsal_failed"'
  require_json_contains /tmp/opencook-migration-cutover-rehearsal.json '"count": 0'
  require_json_contains /tmp/opencook-migration-cutover-rehearsal.json '"shadow_read_advisory"'
  cleanup_restore_server
  trap - EXIT
}

export OPENCOOK_ADMIN_SERVER_URL="$base_url"
export OPENCOOK_ADMIN_REQUESTOR_NAME="$admin_requestor"
export OPENCOOK_ADMIN_REQUESTOR_TYPE="${OPENCOOK_ADMIN_REQUESTOR_TYPE:-user}"
export OPENCOOK_ADMIN_PRIVATE_KEY_PATH="$admin_private_key"
export OPENCOOK_ADMIN_DEFAULT_ORG="$org"

case "$phase" in
  migration-preflight)
    run_migration_preflight
    ;;
  migration-backup)
    run_migration_backup
    ;;
  migration-backup-inspect)
    run_migration_backup_inspect
    ;;
  migration-restore-preflight)
    run_migration_restore_preflight
    ;;
  migration-restore)
    run_migration_restore
    ;;
  migration-reindex)
    run_migration_reindex
    ;;
  migration-rehearsal)
    run_migration_rehearsal
    ;;
  migration-all)
    run_migration_preflight
    run_migration_backup
    run_migration_backup_inspect
    run_migration_restore_preflight
    run_migration_restore
    run_migration_reindex
    run_migration_rehearsal
    ;;
  *)
    echo "unknown migration functional phase: $phase" >&2
    exit 2
    ;;
esac
