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
source_fixture_dir="${OPENCOOK_FUNCTIONAL_SOURCE_FIXTURE_DIR:-/src/test/compat/fixtures/chef-source-import/v1}"
source_dir="${OPENCOOK_FUNCTIONAL_SOURCE_NORMALIZED_DIR:-$state_dir/source-normalized}"
source_backup_dir="${OPENCOOK_FUNCTIONAL_SOURCE_BACKUP_DIR:-$state_dir/source-import-backup}"
source_import_progress="${OPENCOOK_FUNCTIONAL_SOURCE_IMPORT_PROGRESS_PATH:-$source_dir/opencook-source-import-progress.json}"
source_sync_progress="${OPENCOOK_FUNCTIONAL_SOURCE_SYNC_PROGRESS_PATH:-$source_dir/opencook-source-sync-progress.json}"
source_import_sentinel="$state_dir/source-import-complete"
source_reindex_result="$state_dir/source-migration-reindex.json"
source_search_result="$state_dir/source-migration-search-check.json"
source_shadow_result="$state_dir/source-migration-shadow-compare.json"
source_cutover_result="$state_dir/source-migration-cutover-rehearsal.json"
source_backup_create_result="$state_dir/source-migration-backup-create.json"
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
  OPENCOOK_BOOTSTRAP_MODE=false \
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

# keep_functional_artifacts mirrors the Compose wrapper's retention contract so
# source import diagnostics disappear with normal ephemeral functional stacks.
keep_functional_artifacts() {
  [[ "${OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS:-${KEEP_STACK:-0}}" == "1" ]]
}

# clean_source_artifacts removes generated source-import outputs while leaving
# checked-in fixtures and normal backup/restore artifacts untouched.
clean_source_artifacts() {
  rm -rf "$source_dir" "$source_backup_dir"
  rm -f \
    "$source_import_sentinel" \
    "$source_reindex_result" \
    "$source_search_result" \
    "$source_shadow_result" \
    "$source_cutover_result" \
    "$source_backup_create_result"
}

# require_normalized_source lazily creates the normalized source bundle so each
# phase can be run independently from a fresh functional-test container.
require_normalized_source() {
  if [[ -f "$source_dir/opencook-source-manifest.json" ]]; then
    return 0
  fi
  run_migration_source_normalize
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

# start_restore_server launches a local restored-target OpenCook process inside
# the test container so shadow and cutover checks exercise signed HTTP routes.
start_restore_server() {
  cleanup_restore_server
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
}

reset_restore_target() {
  mkdir -p "$state_dir"
  rm -rf "$restore_blob_dir"
  mkdir -p "$restore_blob_dir"
  rm -f "$source_import_sentinel"
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

  # Search validation must run after reindex so OpenSearch is proven to reflect
  # restored PostgreSQL state rather than any source-provider search artifacts.
  echo "==> migration search consistency check from restored target"
  admin_restore_target search check --all-orgs --with-timing --json >/tmp/opencook-migration-search-check.json
  require_json_contains /tmp/opencook-migration-search-check.json '"ok": true'
  require_json_contains /tmp/opencook-migration-search-check.json '"command": "search_check"'
  require_json_contains /tmp/opencook-migration-search-check.json '"clean": 1'
  require_json_contains /tmp/opencook-migration-search-check.json '"unsupported": 0'
}

# source_imported_target_ready checks the sentinel plus PostgreSQL state so a
# stale progress file alone never masquerades as an imported restore target.
source_imported_target_ready() {
  [[ -f "$source_import_sentinel" ]] && restore_database_has_bootstrap_state
}

# require_source_imported_target makes later source phases independently
# runnable by importing the fixture into the restore target when needed.
require_source_imported_target() {
  require_normalized_source
  if source_imported_target_ready; then
    return 0
  fi
  echo "source import target is missing; importing normalized source before continuing"
  run_migration_source_import
}

# require_source_search_evidence ensures shadow and hardened cutover checks see
# OpenSearch rebuilt from imported PostgreSQL state, not fixture sidecars.
require_source_search_evidence() {
  require_source_imported_target
  if [[ -f "$source_search_result" ]]; then
    return 0
  fi
  run_migration_source_reindex
}

# require_source_sync_progress guarantees hardened cutover has a final cursor
# proving the normalized source snapshot was applied after import.
require_source_sync_progress() {
  require_source_imported_target
  if [[ -f "$source_sync_progress" ]]; then
    return 0
  fi
  run_migration_source_sync
}

# require_source_shadow_result captures read-only source-vs-target evidence once
# and reuses it across separate functional phase containers.
require_source_shadow_result() {
  require_source_search_evidence
  if [[ -f "$source_shadow_result" ]]; then
    return 0
  fi
  run_migration_shadow_compare
}

# ensure_source_cutover_manifest creates a backup-style manifest from the
# imported source target because cutover rehearsal derives its read set there.
ensure_source_cutover_manifest() {
  require_source_imported_target
  if [[ -f "$source_backup_dir/manifest.json" ]]; then
    return 0
  fi

  echo "==> migration source backup manifest for cutover rehearsal"
  rm -rf "$source_backup_dir"
  if ! admin_restore_target migration backup create --output "$source_backup_dir" --offline --yes --with-timing --json >"$source_backup_create_result"; then
    echo "migration source backup create command failed; output:" >&2
    print_file_if_exists "$source_backup_create_result"
    return 1
  fi
  require_json_contains "$source_backup_create_result" '"command": "migration_backup_create"'
  require_json_contains "$source_backup_create_result" '"write_backup_bundle"'
  test -f "$source_backup_dir/manifest.json"
}

# run_migration_source_normalize turns the checked-in source fixture into the
# hash-pinned normalized bundle consumed by import, sync, and shadow phases.
run_migration_source_normalize() {
  build_cli
  mkdir -p "$state_dir"
  if ! keep_functional_artifacts; then
    clean_source_artifacts
  fi

  echo "==> migration source normalize fixture"
  if ! admin migration source normalize "$source_fixture_dir" --output "$source_dir" --yes --with-timing --json >"$state_dir/source-migration-normalize.json"; then
    echo "migration source normalize command failed; output:" >&2
    print_file_if_exists "$state_dir/source-migration-normalize.json"
    return 1
  fi
  require_json_contains "$state_dir/source-migration-normalize.json" '"command": "migration_source_normalize"'
  require_json_contains "$state_dir/source-migration-normalize.json" '"normalized_source_output"'
  require_json_contains "$state_dir/source-migration-normalize.json" '"source_search_rebuild_required"'
  test -f "$source_dir/opencook-source-manifest.json"
}

# run_migration_source_import_preflight validates the normalized source against
# an empty PostgreSQL/blob target without mutating imported state.
run_migration_source_import_preflight() {
  build_cli
  require_normalized_source
  reset_restore_target

  echo "==> migration source import preflight against fresh target"
  if ! admin_restore_target migration source import preflight "$source_dir" --offline --with-timing --json >"$state_dir/source-migration-import-preflight.json"; then
    echo "migration source import preflight command failed; output:" >&2
    print_file_if_exists "$state_dir/source-migration-import-preflight.json"
    return 1
  fi
  require_json_contains "$state_dir/source-migration-import-preflight.json" '"command": "migration_source_import_preflight"'
  require_json_contains "$state_dir/source-migration-import-preflight.json" '"source_bundle"'
  require_json_contains "$state_dir/source-migration-import-preflight.json" '"source_import_target"'
  require_json_contains "$state_dir/source-migration-import-preflight.json" '"copied_blobs"'
}

# run_migration_source_import applies normalized source metadata and copied blob
# bytes into the restore target, recording retry progress for later gates.
run_migration_source_import() {
  build_cli
  require_normalized_source
  reset_restore_target
  rm -rf "$source_backup_dir"
  rm -f "$source_import_progress" "$source_sync_progress" "$source_shadow_result" "$source_search_result" "$source_reindex_result" "$source_cutover_result" "$source_backup_create_result"

  echo "==> migration source import apply into fresh target"
  if ! admin_restore_target migration source import apply "$source_dir" --offline --yes --progress "$source_import_progress" --with-timing --json >"$state_dir/source-migration-import-apply.json"; then
    echo "migration source import apply command failed; output:" >&2
    print_file_if_exists "$state_dir/source-migration-import-apply.json"
    return 1
  fi
  require_json_contains "$state_dir/source-migration-import-apply.json" '"command": "migration_source_import_apply"'
  require_json_contains "$state_dir/source-migration-import-apply.json" '"source_import_blobs"'
  require_json_contains "$state_dir/source-migration-import-apply.json" '"source_import_write"'
  require_json_contains "$source_import_progress" '"metadata_imported": true'
  touch "$source_import_sentinel"
}

# run_migration_source_reindex proves imported PostgreSQL state can rebuild and
# validate OpenSearch without trusting source-side derived search artifacts.
run_migration_source_reindex() {
  build_cli
  require_source_imported_target

  echo "==> migration source complete reindex from imported target"
  if ! admin_restore_target reindex --all-orgs --complete --with-timing --json >"$source_reindex_result"; then
    echo "migration source reindex command failed; output:" >&2
    print_file_if_exists "$source_reindex_result"
    return 1
  fi
  require_json_contains "$source_reindex_result" '"ok": true'
  require_json_contains "$source_reindex_result" '"command": "reindex"'
  require_json_contains "$source_reindex_result" '"mode": "complete"'
  require_json_contains "$source_reindex_result" '"upserted"'

  echo "==> migration source search consistency check"
  if ! admin_restore_target search check --all-orgs --with-timing --json >"$source_search_result"; then
    echo "migration source search check command failed; output:" >&2
    print_file_if_exists "$source_search_result"
    return 1
  fi
  require_json_contains "$source_search_result" '"ok": true'
  require_json_contains "$source_search_result" '"command": "search_check"'
  require_json_contains "$source_search_result" '"clean": 1'
  require_json_contains "$source_search_result" '"unsupported": 0'
}

# run_migration_source_sync_preflight confirms a repeated source snapshot is
# stable and reports the cursor gate that a later apply will persist.
run_migration_source_sync_preflight() {
  build_cli
  require_source_imported_target

  echo "==> migration source sync preflight against imported target"
  if ! admin_restore_target migration source sync preflight "$source_dir" --offline --progress "$source_sync_progress" --with-timing --json >"$state_dir/source-migration-sync-preflight.json"; then
    echo "migration source sync preflight command failed; output:" >&2
    print_file_if_exists "$state_dir/source-migration-sync-preflight.json"
    return 1
  fi
  require_json_contains "$state_dir/source-migration-sync-preflight.json" '"command": "migration_source_sync_preflight"'
  require_json_contains "$state_dir/source-migration-sync-preflight.json" '"source_sync_progress"'
  require_json_contains "$state_dir/source-migration-sync-preflight.json" '"users_unchanged"'
}

# run_migration_source_sync applies the no-op cursor for the imported source so
# hardened cutover can prove the final source snapshot was reconciled.
run_migration_source_sync() {
  build_cli
  require_source_imported_target

  echo "==> migration source sync apply against imported target"
  if ! admin_restore_target migration source sync apply "$source_dir" --offline --yes --progress "$source_sync_progress" --with-timing --json >"$state_dir/source-migration-sync-apply.json"; then
    echo "migration source sync apply command failed; output:" >&2
    print_file_if_exists "$state_dir/source-migration-sync-apply.json"
    return 1
  fi
  require_json_contains "$state_dir/source-migration-sync-apply.json" '"command": "migration_source_sync_apply"'
  require_json_contains "$state_dir/source-migration-sync-apply.json" '"source_sync_write"'
  require_json_contains "$source_sync_progress" '"last_status": "applied"'
  rm -rf "$source_backup_dir"
  rm -f "$source_shadow_result" "$source_cutover_result" "$source_backup_create_result"
}

# run_migration_shadow_compare starts the restored target and compares read-only
# source-derived payloads, including signed cookbook/artifact downloads.
run_migration_shadow_compare() {
  build_cli
  require_source_search_evidence

  echo "==> migration shadow-read comparison against imported target"
  start_restore_server
  if ! admin migration shadow compare \
    --source "$source_dir" \
    --target-server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$source_shadow_result"; then
    echo "migration shadow compare command failed; output:" >&2
    print_file_if_exists "$source_shadow_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$source_shadow_result" '"command": "migration_shadow_compare"'
  require_json_contains "$source_shadow_result" '"shadow_read_compare"'
  require_json_contains "$source_shadow_result" '"family": "shadow_failed"'
  require_json_contains "$source_shadow_result" '"count": 0'
  require_json_contains "$source_shadow_result" '"shadow_downloads"'
  cleanup_restore_server
  trap - EXIT
}

# run_migration_source_rehearsal feeds all prior evidence into cutover rehearsal
# so the functional suite covers blockers versus advisory warnings end-to-end.
run_migration_source_rehearsal() {
  build_cli
  require_source_sync_progress
  require_source_search_evidence
  require_source_shadow_result
  ensure_source_cutover_manifest

  echo "==> migration hardened cutover rehearsal against imported target"
  start_restore_server
  if ! admin migration cutover rehearse \
    --manifest "$source_backup_dir/manifest.json" \
    --source "$source_dir" \
    --source-import-progress "$source_import_progress" \
    --source-sync-progress "$source_sync_progress" \
    --search-check-result "$source_search_result" \
    --shadow-result "$source_shadow_result" \
    --rollback-ready \
    --server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$source_cutover_result"; then
    echo "migration hardened cutover rehearsal command failed; output:" >&2
    print_file_if_exists "$source_cutover_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$source_cutover_result" '"command": "migration_cutover_rehearse"'
  require_json_contains "$source_cutover_result" '"source_import_progress"'
  require_json_contains "$source_cutover_result" '"source_sync_freshness"'
  require_json_contains "$source_cutover_result" '"search_cleanliness"'
  require_json_contains "$source_cutover_result" '"shadow_read_evidence"'
  require_json_contains "$source_cutover_result" '"rollback_readiness"'
  require_json_contains "$source_cutover_result" '"family": "cutover_blockers"'
  require_json_contains "$source_cutover_result" '"count": 0'
  cleanup_restore_server
  trap - EXIT
}

# run_migration_source_all exercises the opt-in end-to-end source import, sync,
# search, shadow-read, and hardened cutover path without changing default smoke.
run_migration_source_all() {
  run_migration_source_normalize
  run_migration_source_import_preflight
  run_migration_source_import
  run_migration_source_reindex
  run_migration_source_sync_preflight
  run_migration_source_sync
  run_migration_shadow_compare
  run_migration_source_rehearsal
  if ! keep_functional_artifacts; then
    clean_source_artifacts
  fi
}

run_migration_rehearsal() {
  build_cli
  ensure_restore_target_ready

  echo "==> migration cutover rehearsal against restored target"
  start_restore_server

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
  migration-source-normalize)
    run_migration_source_normalize
    ;;
  migration-source-import-preflight)
    run_migration_source_import_preflight
    ;;
  migration-source-import)
    run_migration_source_import
    ;;
  migration-source-reindex)
    run_migration_source_reindex
    ;;
  migration-source-sync-preflight)
    run_migration_source_sync_preflight
    ;;
  migration-source-sync)
    run_migration_source_sync
    ;;
  migration-shadow-compare)
    run_migration_shadow_compare
    ;;
  migration-source-rehearsal)
    run_migration_source_rehearsal
    ;;
  migration-source-all)
    run_migration_source_all
    ;;
  migration-all)
    run_migration_preflight
    run_migration_backup
    run_migration_backup_inspect
    run_migration_restore_preflight
    run_migration_restore
    run_migration_reindex
    run_migration_rehearsal
    run_migration_source_all
    ;;
  *)
    echo "unknown migration functional phase: $phase" >&2
    exit 2
    ;;
esac
