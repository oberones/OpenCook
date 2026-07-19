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
live_source_root="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_STATE_DIR:-$state_dir/live-source}"
live_source_db="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_DB:-opencook_live_source}"
live_source_admin_dsn="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_POSTGRES_ADMIN_DSN:-$restore_admin_dsn}"
live_source_dsn="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_POSTGRES_DSN:-postgres://opencook:opencook@postgres:5432/$live_source_db?sslmode=disable}"
live_source_dir="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_DIR:-$live_source_root/source}"
live_source_bookshelf_dir="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_BOOKSHELF_ROOT:-$live_source_root/bookshelf}"
live_source_backup_dir="${OPENCOOK_FUNCTIONAL_LIVE_SOURCE_BACKUP_DIR:-$live_source_root/backup}"
live_source_import_progress="$live_source_root/opencook-source-import-progress.json"
live_source_sync_progress="$live_source_root/opencook-source-sync-progress.json"
live_source_import_sentinel="$live_source_root/source-import-complete"
live_source_preflight_result="$live_source_root/migration-live-source-preflight.json"
live_source_extract_result="$live_source_root/migration-live-source-extract.json"
live_source_import_preflight_result="$live_source_root/migration-live-source-import-preflight.json"
live_source_import_result="$live_source_root/migration-live-source-import-apply.json"
live_source_reindex_result="$live_source_root/migration-live-source-reindex.json"
live_source_search_result="$live_source_root/migration-live-source-search-check.json"
live_source_sync_result="$live_source_root/migration-live-source-sync-apply.json"
live_source_shadow_result="$live_source_root/migration-live-source-shadow-compare.json"
live_source_backup_create_result="$live_source_root/migration-live-source-backup-create.json"
live_source_cutover_result="$live_source_root/migration-live-source-cutover-rehearsal.json"
live_source_checksum="2bf4a922bbf40fb1ae4268646116853c"
scale_profile="${OPENCOOK_FUNCTIONAL_SCALE_PROFILE:-small}"
scale_root="${OPENCOOK_FUNCTIONAL_SCALE_STATE_DIR:-$state_dir/migration-scale/$scale_profile}"
scale_source_dir="${OPENCOOK_FUNCTIONAL_SCALE_SOURCE_DIR:-$scale_root/source}"
scale_backup_dir="${OPENCOOK_FUNCTIONAL_SCALE_BACKUP_DIR:-$scale_root/backup}"
scale_import_progress="$scale_root/opencook-source-import-progress.json"
scale_sync_progress="$scale_root/opencook-source-sync-progress.json"
scale_fixture_result="$scale_root/migration-scale-fixture-create.json"
scale_import_preflight_result="$scale_root/migration-scale-import-preflight.json"
scale_import_result="$scale_root/migration-scale-import-apply.json"
scale_backup_create_result="$scale_root/migration-scale-backup-create.json"
scale_backup_inspect_result="$scale_root/migration-scale-backup-inspect.json"
scale_restore_result="$scale_root/migration-scale-restore-apply.json"
scale_reindex_result="$scale_root/migration-scale-reindex.json"
scale_search_result="$scale_root/migration-scale-search-check.json"
scale_sync_result="$scale_root/migration-scale-sync-apply.json"
scale_shadow_result="$scale_root/migration-scale-shadow-compare.json"
scale_cutover_result="$scale_root/migration-scale-cutover-rehearsal.json"
scale_import_sentinel="$scale_root/source-import-complete"
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

# run_restore_json_under_maintenance enables maintenance in the restored target
# database for one OpenSearch mutation, then disables it before returning.
run_restore_json_under_maintenance() {
  local mode="$1"
  local reason="$2"
  local output="$3"
  shift 3

  mkdir -p "$state_dir"
  admin_restore_target maintenance enable --mode "$mode" --reason "$reason" --actor functional-tests --yes --json >"$state_dir/restore-maintenance-enable.json"
  require_json_contains "$state_dir/restore-maintenance-enable.json" '"active": true'
  require_json_contains "$state_dir/restore-maintenance-enable.json" '"shared": true'

  set +e
  "$@" >"$output"
  local code="$?"
  set -e

  local disable_code=0
  admin_restore_target maintenance disable --actor functional-tests --yes --json >"$state_dir/restore-maintenance-disable.json" || disable_code="$?"
  if [[ "$code" != "0" ]]; then
    return "$code"
  fi
  return "$disable_code"
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

# path_has_parent_segment recognizes explicit traversal before cleanup path
# checks compare prefixes, avoiding false safety from strings like root/../..
path_has_parent_segment() {
  local value="$1"
  case "$value" in
    ".." | "../"* | *"/.." | *"/../"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

# require_no_symlink_components rejects existing symlink path components before
# recursive cleanup so an override cannot point through the state dir to outside.
require_no_symlink_components() {
  local label="$1"
  local target="${2%/}"
  local current="/"
  local part
  local parts=()
  IFS='/' read -r -a parts <<<"${target#/}"
  for part in "${parts[@]}"; do
    if [[ -z "$part" || "$part" == "." ]]; then
      continue
    fi
    current="${current%/}/$part"
    if [[ -L "$current" ]]; then
      echo "refusing to remove $label through symlink component: $current" >&2
      return 1
    fi
    if [[ ! -e "$current" ]]; then
      break
    fi
  done
  return 0
}

# require_state_dir_artifact_path keeps cleanup narrowly scoped to generated
# functional artifacts, rejecting traversal and symlinks before rm -rf.
require_state_dir_artifact_path() {
  local path="$1"
  local root="${state_dir%/}"
  if [[ -z "$root" || "$root" == "/" || "$root" != /* ]]; then
    echo "refusing to remove source artifact because functional state dir is unsafe: $state_dir" >&2
    return 1
  fi
  if [[ -z "$path" || "$path" == "/" || "$path" != /* ]]; then
    echo "refusing to remove empty, root, or relative source artifact path: $path" >&2
    return 1
  fi
  if path_has_parent_segment "$root" || path_has_parent_segment "$path"; then
    echo "refusing to remove source artifact path with parent traversal: $path" >&2
    return 1
  fi
  require_no_symlink_components "functional state dir" "$root" || return 1
  require_no_symlink_components "source artifact" "$path" || return 1
  case "$path" in
    "$root"/*)
      return 0
      ;;
    *)
      echo "refusing to remove source artifact outside functional state dir: $path" >&2
      return 1
      ;;
  esac
}

# remove_source_tree_artifact validates generated directories before recursive
# deletion; callers should only pass paths derived from the functional state dir.
remove_source_tree_artifact() {
  local path="$1"
  require_state_dir_artifact_path "$path"
  rm -rf "$path"
}

# clean_source_artifacts removes generated source-import outputs while leaving
# checked-in fixtures and normal backup/restore artifacts untouched.
clean_source_artifacts() {
  remove_source_tree_artifact "$source_dir"
  remove_source_tree_artifact "$source_backup_dir"
  rm -f \
    "$source_import_sentinel" \
    "$source_reindex_result" \
    "$source_search_result" \
    "$source_shadow_result" \
    "$source_cutover_result" \
    "$source_backup_create_result"
}

# require_live_source_artifact_paths keeps live-source fixture outputs inside
# the Compose-managed functional state volume, matching scale fixture safety.
require_live_source_artifact_paths() {
  require_state_dir_artifact_path "$live_source_root"
  require_state_dir_artifact_path "$live_source_dir"
  require_state_dir_artifact_path "$live_source_bookshelf_dir"
  require_state_dir_artifact_path "$live_source_backup_dir"
}

# clean_live_source_artifacts removes generated live-source fixtures and
# evidence unless the caller explicitly keeps the functional stack artifacts.
clean_live_source_artifacts() {
  require_live_source_artifact_paths
  remove_source_tree_artifact "$live_source_root"
}

# require_scale_artifact_paths keeps production-scale fixture output inside the
# Compose-managed functional volume even when callers override path variables.
require_scale_artifact_paths() {
  require_state_dir_artifact_path "$scale_root"
  require_state_dir_artifact_path "$scale_source_dir"
  require_state_dir_artifact_path "$scale_backup_dir"
}

# clean_scale_artifacts removes generated production-scale bundles and evidence
# unless the wrapper was asked to keep the functional stack or artifacts.
clean_scale_artifacts() {
  require_scale_artifact_paths
  remove_source_tree_artifact "$scale_root"
}

print_scale_phase_success() {
  local phase_name="$1"
  echo "==> $phase_name passed successfully for scale profile: $scale_profile"
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

prepare_live_source_fixture() {
  require_live_source_artifact_paths
  mkdir -p "$live_source_root" "$live_source_bookshelf_dir"
  printf 'live source cookbook blob bytes' >"$live_source_bookshelf_dir/$live_source_checksum"

  local public_key
  public_key="$(cat /src/test/functional/fixtures/bootstrap_public.pem)"

  psql "$live_source_admin_dsn" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"$live_source_db\" WITH (FORCE)"
  psql "$live_source_admin_dsn" -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"$live_source_db\""

  # This intentionally tiny schema is a deterministic stand-in for live Chef
  # Server PostgreSQL. It includes only the columns the read-only extractor uses.
  psql "$live_source_dsn" -v ON_ERROR_STOP=1 \
    -v org="$org" \
    -v public_key="$public_key" \
    -v checksum="$live_source_checksum" <<'SQL'
CREATE TABLE users (id text PRIMARY KEY, username text NOT NULL, email text, serialized_object jsonb NOT NULL DEFAULT '{}'::jsonb, admin boolean NOT NULL DEFAULT false, authz_id text NOT NULL);
CREATE TABLE keys (id text NOT NULL, key_name text NOT NULL, public_key text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE orgs (id text PRIMARY KEY, name text NOT NULL, full_name text NOT NULL, authz_id text NOT NULL);
CREATE TABLE org_user_associations (org_id text, user_id text);
CREATE TABLE clients (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL, validator boolean NOT NULL DEFAULT false, admin boolean NOT NULL DEFAULT false, public_key text, authz_id text NOT NULL);
CREATE TABLE groups (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL, authz_id text NOT NULL);
CREATE TABLE containers (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL, authz_id text NOT NULL);
CREATE TABLE auth_container (id text PRIMARY KEY, authz_id text NOT NULL);
CREATE TABLE auth_actor (id text PRIMARY KEY, authz_id text NOT NULL);
CREATE TABLE auth_group (id text PRIMARY KEY, authz_id text NOT NULL);
CREATE TABLE auth_object (id text PRIMARY KEY, authz_id text NOT NULL);
CREATE TABLE object_acl_group (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE object_acl_actor (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE actor_acl_group (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE actor_acl_actor (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE group_acl_group (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE group_acl_actor (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE container_acl_group (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE container_acl_actor (target text NOT NULL, authorizee text NOT NULL, permission text NOT NULL);
CREATE TABLE group_group_relations (parent text NOT NULL, child text NOT NULL);
CREATE TABLE group_actor_relations (parent text NOT NULL, child text NOT NULL);
CREATE TABLE nodes (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL, environment text NOT NULL, policy_name text, policy_group text, serialized_object bytea NOT NULL);
CREATE TABLE environments (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL, serialized_object bytea NOT NULL);
CREATE TABLE roles (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL, serialized_object bytea NOT NULL);
CREATE TABLE data_bags (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL);
CREATE TABLE data_bag_items (id text PRIMARY KEY, org_id text NOT NULL, data_bag_name text NOT NULL, item_name text NOT NULL, serialized_object bytea NOT NULL);
CREATE TABLE policies (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL);
CREATE TABLE policy_revisions (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL, revision_id text NOT NULL, serialized_object bytea NOT NULL);
CREATE TABLE policy_groups (id text PRIMARY KEY, org_id text NOT NULL, authz_id text NOT NULL, name text NOT NULL, serialized_object bytea NOT NULL);
CREATE TABLE policy_revisions_policy_groups_association (org_id text NOT NULL, policy_group_name text NOT NULL, policy_revision_name text NOT NULL, policy_revision_revision_id text NOT NULL);
CREATE TABLE checksums (org_id text NOT NULL, checksum text NOT NULL);
CREATE TABLE sandboxed_checksums (org_id text NOT NULL, sandbox_id text NOT NULL, checksum text NOT NULL);
CREATE TABLE cookbooks (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL);
CREATE TABLE cookbook_versions (id text PRIMARY KEY, org_id text NOT NULL, serialized_object bytea NOT NULL, metadata bytea NOT NULL, name text NOT NULL, major integer NOT NULL, minor integer NOT NULL, patch integer NOT NULL);
CREATE TABLE cookbook_version_checksums (cookbook_version_id text NOT NULL, org_id text NOT NULL, checksum text NOT NULL);
CREATE TABLE cookbook_artifacts (id text PRIMARY KEY, org_id text NOT NULL, name text NOT NULL);
CREATE TABLE cookbook_artifact_versions (id text PRIMARY KEY, cookbook_artifact_id text NOT NULL, serialized_object bytea NOT NULL, metadata bytea NOT NULL, identifier text NOT NULL);
CREATE TABLE cookbook_artifact_version_checksums (cookbook_artifact_version_id text NOT NULL, org_id text NOT NULL, checksum text NOT NULL);

INSERT INTO users VALUES ('user-pivotal', 'pivotal', 'pivotal@example.test', jsonb_build_object('display_name', 'Pivotal User'), true, 'actor-pivotal');
INSERT INTO auth_actor VALUES ('actor-row-pivotal', 'actor-pivotal');
INSERT INTO keys VALUES ('user-pivotal', 'default', :'public_key', 'infinity'::timestamptz);

INSERT INTO orgs VALUES ('org-' || :'org', :'org', 'Ponyville', 'object-org-' || :'org');
INSERT INTO org_user_associations VALUES ('org-' || :'org', 'user-pivotal');
INSERT INTO auth_object VALUES ('object-row-org', 'object-org-' || :'org');

INSERT INTO clients VALUES ('client-validator', 'org-' || :'org', :'org' || '-validator', true, false, :'public_key', 'actor-validator');
INSERT INTO auth_actor VALUES ('actor-row-validator', 'actor-validator');
INSERT INTO keys VALUES ('client-validator', 'default', :'public_key', 'infinity'::timestamptz);

INSERT INTO groups
SELECT 'group-' || name, 'org-' || :'org', name, 'group-' || name
FROM unnest(ARRAY['admins','billing-admins','users','clients']) AS name;
INSERT INTO auth_group
SELECT 'auth-group-' || name, 'group-' || name
FROM unnest(ARRAY['admins','billing-admins','users','clients']) AS name;

INSERT INTO containers
SELECT 'container-' || name, 'org-' || :'org', name, 'container-' || name
FROM unnest(ARRAY['clients','containers','cookbooks','data','environments','groups','nodes','roles','sandboxes','policies','policy_groups','cookbook_artifacts']) AS name;
INSERT INTO auth_container
SELECT 'auth-container-' || name, 'container-' || name
FROM unnest(ARRAY['clients','containers','cookbooks','data','environments','groups','nodes','roles','sandboxes','policies','policy_groups','cookbook_artifacts']) AS name;

INSERT INTO group_actor_relations
SELECT admins.id, pivotal.id
FROM auth_group admins, auth_actor pivotal
WHERE admins.authz_id = 'group-admins' AND pivotal.authz_id = 'actor-pivotal';
INSERT INTO group_actor_relations
SELECT clients.id, validator.id
FROM auth_group clients, auth_actor validator
WHERE clients.authz_id = 'group-clients' AND validator.authz_id = 'actor-validator';

INSERT INTO nodes VALUES ('node-web01', 'org-' || :'org', 'object-node-web01', 'web01', '_default', 'base', 'prod',
	convert_to(jsonb_build_object('name', 'web01', 'chef_environment', '_default', 'run_list', jsonb_build_array('role[web]'), 'normal', jsonb_build_object('app', 'opencook'), 'default', '{}'::jsonb, 'override', '{}'::jsonb, 'automatic', '{}'::jsonb, 'policy_name', 'base', 'policy_group', 'prod')::text, 'UTF8'));
INSERT INTO environments VALUES ('env-default', 'org-' || :'org', 'object-env-default', '_default',
	convert_to(jsonb_build_object('name', '_default', 'description', 'The default Chef environment', 'cookbook_versions', '{}'::jsonb)::text, 'UTF8'));
INSERT INTO roles VALUES ('role-web', 'org-' || :'org', 'object-role-web', 'web',
	convert_to(jsonb_build_object('name', 'web', 'run_list', jsonb_build_array('recipe[base]'), 'env_run_lists', jsonb_build_object('_default', jsonb_build_array('recipe[base]')))::text, 'UTF8'));
INSERT INTO data_bags VALUES ('bag-secrets', 'org-' || :'org', 'object-bag-secrets', 'secrets');
INSERT INTO data_bag_items VALUES ('item-secrets-db', 'org-' || :'org', 'secrets', 'db',
	convert_to(jsonb_build_object('id', 'db', 'encrypted_data', 'fixture', 'iv', 'still-opaque')::text, 'UTF8'));
INSERT INTO policies VALUES ('policy-base', 'org-' || :'org', 'object-policy-base', 'base');
INSERT INTO policy_revisions VALUES ('policy-base-rev', 'org-' || :'org', 'base', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
	convert_to(jsonb_build_object('name', 'base', 'revision_id', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'run_list', jsonb_build_array('recipe[base]'), 'named_run_lists', '{}'::jsonb, 'cookbook_locks', '{}'::jsonb, 'solution_dependencies', '{}'::jsonb)::text, 'UTF8'));
INSERT INTO policy_groups VALUES ('policy-group-prod', 'org-' || :'org', 'object-policy-group-prod', 'prod',
	convert_to(jsonb_build_object('name', 'prod')::text, 'UTF8'));
INSERT INTO policy_revisions_policy_groups_association VALUES ('org-' || :'org', 'prod', 'base', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
INSERT INTO checksums VALUES ('org-' || :'org', :'checksum');
INSERT INTO sandboxed_checksums VALUES ('org-' || :'org', 'sandbox-fixture', :'checksum');

INSERT INTO cookbooks VALUES ('cookbook-base', 'org-' || :'org', 'base');
INSERT INTO cookbook_versions VALUES ('cookbook-base-1', 'org-' || :'org',
	convert_to(jsonb_build_object('name', 'base-1.0.0', 'cookbook_name', 'base', 'version', '1.0.0', 'all_files', jsonb_build_array(jsonb_build_object('name', 'default.rb', 'path', 'recipes/default.rb', 'checksum', :'checksum', 'specificity', 'default')), 'recipes', jsonb_build_array(jsonb_build_object('name', 'default.rb', 'path', 'recipes/default.rb', 'checksum', :'checksum', 'specificity', 'default')))::text, 'UTF8'),
	convert_to(jsonb_build_object('name', 'base', 'version', '1.0.0', 'dependencies', '{}'::jsonb, 'platforms', '{}'::jsonb)::text, 'UTF8'),
	'base', 1, 0, 0);
INSERT INTO cookbook_version_checksums VALUES ('cookbook-base-1', 'org-' || :'org', :'checksum');
INSERT INTO cookbook_artifacts VALUES ('artifact-base', 'org-' || :'org', 'base');
INSERT INTO cookbook_artifact_versions VALUES ('artifact-base-1', 'artifact-base',
	convert_to(jsonb_build_object('name', 'base', 'identifier', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'version', '1.0.0', 'all_files', jsonb_build_array(jsonb_build_object('name', 'default.rb', 'path', 'recipes/default.rb', 'checksum', :'checksum', 'specificity', 'default')), 'recipes', jsonb_build_array(jsonb_build_object('name', 'default.rb', 'path', 'recipes/default.rb', 'checksum', :'checksum', 'specificity', 'default')))::text, 'UTF8'),
	convert_to(jsonb_build_object('name', 'base', 'version', '1.0.0', 'dependencies', '{}'::jsonb, 'platforms', '{}'::jsonb)::text, 'UTF8'),
	'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
INSERT INTO cookbook_artifact_version_checksums VALUES ('artifact-base-1', 'org-' || :'org', :'checksum');

INSERT INTO auth_object
SELECT id, authz_id
FROM (VALUES
	('object-row-node-web01', 'object-node-web01'),
	('object-row-env-default', 'object-env-default'),
	('object-row-role-web', 'object-role-web'),
	('object-row-bag-secrets', 'object-bag-secrets'),
	('object-row-policy-base', 'object-policy-base'),
	('object-row-policy-group-prod', 'object-policy-group-prod')
) AS objects(id, authz_id);

CREATE TEMP TABLE live_acl_permissions(permission text);
INSERT INTO live_acl_permissions VALUES ('create'), ('read'), ('update'), ('delete'), ('grant');
INSERT INTO actor_acl_actor SELECT id, 'actor-row-pivotal', permission FROM auth_actor CROSS JOIN live_acl_permissions WHERE authz_id IN ('actor-pivotal', 'actor-validator');
INSERT INTO actor_acl_group SELECT id, 'auth-group-admins', permission FROM auth_actor CROSS JOIN live_acl_permissions WHERE authz_id IN ('actor-pivotal', 'actor-validator');
INSERT INTO group_acl_actor SELECT id, 'actor-row-pivotal', permission FROM auth_group CROSS JOIN live_acl_permissions;
INSERT INTO group_acl_group SELECT id, 'auth-group-admins', permission FROM auth_group CROSS JOIN live_acl_permissions;
INSERT INTO container_acl_actor SELECT id, 'actor-row-pivotal', permission FROM auth_container CROSS JOIN live_acl_permissions;
INSERT INTO container_acl_group SELECT id, 'auth-group-admins', permission FROM auth_container CROSS JOIN live_acl_permissions;
INSERT INTO object_acl_actor SELECT id, 'actor-row-pivotal', permission FROM auth_object CROSS JOIN live_acl_permissions;
INSERT INTO object_acl_group SELECT id, 'auth-group-admins', permission FROM auth_object CROSS JOIN live_acl_permissions;
SQL
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
  OPENCOOK_BOOTSTRAP_MODE=false \
  OPENCOOK_BOOTSTRAP_REQUESTOR_NAME= \
  OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user \
  OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID="${OPENCOOK_FUNCTIONAL_ACTOR_KEY_ID:-default}" \
  OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH= \
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
  run_restore_json_under_maintenance reindex "functional restored target reindex" /tmp/opencook-migration-reindex.json \
    admin_restore_target reindex --all-orgs --complete --with-timing --json
  require_json_contains /tmp/opencook-migration-reindex.json '"ok": true'
  require_json_contains /tmp/opencook-migration-reindex.json '"command": "reindex"'
  require_json_contains /tmp/opencook-migration-reindex.json '"mode": "complete"'
  require_json_contains /tmp/opencook-migration-reindex.json 'active maintenance mode confirmed'
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
  if ! run_restore_json_under_maintenance reindex "functional source imported target reindex" "$source_reindex_result" \
    admin_restore_target reindex --all-orgs --complete --with-timing --json; then
    echo "migration source reindex command failed; output:" >&2
    print_file_if_exists "$source_reindex_result"
    return 1
  fi
  require_json_contains "$source_reindex_result" '"ok": true'
  require_json_contains "$source_reindex_result" '"command": "reindex"'
  require_json_contains "$source_reindex_result" '"mode": "complete"'
  require_json_contains "$source_reindex_result" 'active maintenance mode confirmed'
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

# require_live_source_bundle lazily prepares and extracts the deterministic
# live-source PostgreSQL fixture so each phase can run independently.
require_live_source_bundle() {
  if [[ -f "$live_source_dir/opencook-source-manifest.json" ]]; then
    return 0
  fi
  run_migration_live_source_extract
}

live_source_imported_target_ready() {
  [[ -f "$live_source_import_sentinel" ]] && restore_database_has_bootstrap_state
}

# require_live_source_imported_target imports the live-extracted bundle into the
# restore target when a later phase needs PostgreSQL-backed OpenCook state.
require_live_source_imported_target() {
  require_live_source_bundle
  if live_source_imported_target_ready; then
    return 0
  fi
  echo "live source import target is missing; importing extracted source before continuing"
  run_migration_live_source_import
}

require_live_source_search_evidence() {
  require_live_source_imported_target
  if [[ -f "$live_source_search_result" ]]; then
    return 0
  fi
  run_migration_live_source_reindex
}

require_live_source_sync_progress() {
  require_live_source_imported_target
  if [[ -f "$live_source_sync_progress" ]]; then
    return 0
  fi

  echo "==> migration live source sync apply"
  if ! admin_restore_target migration source sync apply "$live_source_dir" --offline --yes --progress "$live_source_sync_progress" --with-timing --json >"$live_source_sync_result"; then
    echo "migration live source sync command failed; output:" >&2
    print_file_if_exists "$live_source_sync_result"
    return 1
  fi
  require_json_contains "$live_source_sync_result" '"command": "migration_source_sync_apply"'
  require_json_contains "$live_source_sync_result" '"source_sync_write"'
  require_json_contains "$live_source_sync_progress" '"last_status": "applied"'
}

require_live_source_shadow_result() {
  require_live_source_search_evidence
  if [[ -f "$live_source_shadow_result" ]]; then
    return 0
  fi
  run_migration_live_source_shadow
}

ensure_live_source_cutover_manifest() {
  require_live_source_imported_target
  if [[ -f "$live_source_backup_dir/manifest.json" ]]; then
    return 0
  fi

  echo "==> migration live source backup manifest for cutover rehearsal"
  rm -rf "$live_source_backup_dir"
  if ! admin_restore_target migration backup create --output "$live_source_backup_dir" --offline --yes --with-timing --json >"$live_source_backup_create_result"; then
    echo "migration live source backup create command failed; output:" >&2
    print_file_if_exists "$live_source_backup_create_result"
    return 1
  fi
  require_json_contains "$live_source_backup_create_result" '"command": "migration_backup_create"'
  require_json_contains "$live_source_backup_create_result" '"write_backup_bundle"'
  test -f "$live_source_backup_dir/manifest.json"
}

run_migration_live_source_preflight() {
  build_cli
  require_live_source_artifact_paths
  if ! keep_functional_artifacts; then
    clean_live_source_artifacts
  fi
  prepare_live_source_fixture

  echo "==> migration live source preflight"
  if ! admin migration source live preflight \
    --source-postgres-dsn "$live_source_dsn" \
    --source-bookshelf-root "$live_source_bookshelf_dir" \
    --org "$org" \
    --with-timing \
    --json >"$live_source_preflight_result"; then
    echo "migration live source preflight command failed; output:" >&2
    print_file_if_exists "$live_source_preflight_result"
    return 1
  fi
  require_json_contains "$live_source_preflight_result" '"command": "migration_source_live_preflight"'
  require_json_contains "$live_source_preflight_result" '"source_postgres"'
  require_json_contains "$live_source_preflight_result" '"source_schema"'
  require_json_contains "$live_source_preflight_result" '"read_only": "true"'
}

run_migration_live_source_extract() {
  build_cli
  require_live_source_artifact_paths
  if ! keep_functional_artifacts; then
    clean_live_source_artifacts
  fi
  prepare_live_source_fixture

  echo "==> migration live source extract"
  if ! admin migration source live extract \
    --source-postgres-dsn "$live_source_dsn" \
    --source-bookshelf-root "$live_source_bookshelf_dir" \
    --copy-blobs \
    --org "$org" \
    --output "$live_source_dir" \
    --yes \
    --with-timing \
    --json >"$live_source_extract_result"; then
    echo "migration live source extract command failed; output:" >&2
    print_file_if_exists "$live_source_extract_result"
    return 1
  fi
  require_json_contains "$live_source_extract_result" '"command": "migration_source_live_extract"'
  require_json_contains "$live_source_extract_result" '"source_bootstrap"'
  require_json_contains "$live_source_extract_result" '"source_blob"'
  require_json_contains "$live_source_extract_result" '"normalized_source_output"'
  require_json_contains "$live_source_extract_result" '"source_type": "live_chef_infra_server"'
  test -f "$live_source_dir/opencook-source-manifest.json"
}

run_migration_live_source_import() {
  build_cli
  require_live_source_bundle
  reset_restore_target
  rm -rf "$live_source_backup_dir"
  rm -f "$live_source_import_progress" "$live_source_sync_progress" "$live_source_search_result" "$live_source_reindex_result" "$live_source_shadow_result" "$live_source_cutover_result" "$live_source_backup_create_result"

  echo "==> migration live source import preflight"
  if ! admin_restore_target migration source import preflight "$live_source_dir" --offline --with-timing --json >"$live_source_import_preflight_result"; then
    echo "migration live source import preflight command failed; output:" >&2
    print_file_if_exists "$live_source_import_preflight_result"
    return 1
  fi
  require_json_contains "$live_source_import_preflight_result" '"command": "migration_source_import_preflight"'
  require_json_contains "$live_source_import_preflight_result" '"source_type": "live_chef_infra_server"'
  require_json_contains "$live_source_import_preflight_result" '"source_import_target"'

  echo "==> migration live source import apply"
  if ! admin_restore_target migration source import apply "$live_source_dir" --offline --yes --progress "$live_source_import_progress" --with-timing --json >"$live_source_import_result"; then
    echo "migration live source import command failed; output:" >&2
    print_file_if_exists "$live_source_import_result"
    return 1
  fi
  require_json_contains "$live_source_import_result" '"command": "migration_source_import_apply"'
  require_json_contains "$live_source_import_result" '"source_import_blobs"'
  require_json_contains "$live_source_import_result" '"source_import_write"'
  require_json_contains "$live_source_import_progress" '"metadata_imported": true'
  touch "$live_source_import_sentinel"
}

run_migration_live_source_reindex() {
  build_cli
  require_live_source_imported_target

  echo "==> migration live source complete reindex"
  if ! run_restore_json_under_maintenance reindex "functional live source imported target reindex" "$live_source_reindex_result" \
    admin_restore_target reindex --all-orgs --complete --with-timing --json; then
    echo "migration live source reindex command failed; output:" >&2
    print_file_if_exists "$live_source_reindex_result"
    return 1
  fi
  require_json_contains "$live_source_reindex_result" '"ok": true'
  require_json_contains "$live_source_reindex_result" '"command": "reindex"'
  require_json_contains "$live_source_reindex_result" '"mode": "complete"'

  echo "==> migration live source search consistency check"
  if ! admin_restore_target search check --all-orgs --with-timing --json >"$live_source_search_result"; then
    echo "migration live source search check command failed; output:" >&2
    print_file_if_exists "$live_source_search_result"
    return 1
  fi
  require_json_contains "$live_source_search_result" '"ok": true'
  require_json_contains "$live_source_search_result" '"command": "search_check"'
  require_json_contains "$live_source_search_result" '"clean": 1'
}

run_migration_live_source_shadow() {
  build_cli
  require_live_source_search_evidence

  echo "==> migration live source shadow-read comparison"
  start_restore_server
  if ! admin migration shadow compare \
    --source "$live_source_dir" \
    --target-server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$live_source_shadow_result"; then
    echo "migration live source shadow compare command failed; output:" >&2
    print_file_if_exists "$live_source_shadow_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$live_source_shadow_result" '"command": "migration_shadow_compare"'
  require_json_contains "$live_source_shadow_result" '"shadow_read_compare"'
  require_json_contains "$live_source_shadow_result" '"family": "shadow_failed"'
  require_json_contains "$live_source_shadow_result" '"count": 0'
  cleanup_restore_server
  trap - EXIT
}

run_migration_live_source_rehearsal() {
  build_cli
  require_live_source_sync_progress
  require_live_source_search_evidence
  require_live_source_shadow_result
  ensure_live_source_cutover_manifest

  echo "==> migration live source cutover rehearsal"
  start_restore_server
  if ! admin migration cutover rehearse \
    --manifest "$live_source_backup_dir/manifest.json" \
    --source "$live_source_dir" \
    --source-import-progress "$live_source_import_progress" \
    --source-sync-progress "$live_source_sync_progress" \
    --search-check-result "$live_source_search_result" \
    --shadow-result "$live_source_shadow_result" \
    --source-frozen \
    --rollback-ready \
    --server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$live_source_cutover_result"; then
    echo "migration live source cutover rehearsal command failed; output:" >&2
    print_file_if_exists "$live_source_cutover_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$live_source_cutover_result" '"command": "migration_cutover_rehearse"'
  require_json_contains "$live_source_cutover_result" '"source_origin": "live_extraction"'
  require_json_contains "$live_source_cutover_result" '"source_freeze_evidence"'
  require_json_contains "$live_source_cutover_result" '"rollback_readiness"'
  require_json_contains "$live_source_cutover_result" '"family": "cutover_blockers"'
  require_json_contains "$live_source_cutover_result" '"count": 0'
  cleanup_restore_server
  trap - EXIT
}

run_migration_live_source_all() {
  run_migration_live_source_preflight
  run_migration_live_source_extract
  run_migration_live_source_import
  run_migration_live_source_reindex
  run_migration_live_source_shadow
  run_migration_live_source_rehearsal
  echo "==> migration live-source functional flow passed successfully"
  if ! keep_functional_artifacts; then
    clean_live_source_artifacts
  fi
}

# require_scale_source lazily creates the generated scale source bundle so each
# scale phase can run independently in a fresh functional-test container.
require_scale_source() {
  if [[ -f "$scale_source_dir/opencook-source-manifest.json" ]]; then
    return 0
  fi
  run_migration_scale_fixtures
}

# run_migration_scale_fixtures writes a deterministic production-shaped source
# bundle into the Compose-managed state volume instead of relying on bind mounts.
run_migration_scale_fixtures() {
  build_cli
  require_scale_artifact_paths
  if ! keep_functional_artifacts; then
    clean_scale_artifacts
  fi
  mkdir -p "$scale_root"

  echo "==> migration production-scale fixture generation ($scale_profile)"
  if ! admin migration scale-fixture create --profile "$scale_profile" --output "$scale_source_dir" --include-sidecars --yes --with-timing --json >"$scale_fixture_result"; then
    echo "migration scale fixture command failed; output:" >&2
    print_file_if_exists "$scale_fixture_result"
    return 1
  fi
  require_json_contains "$scale_fixture_result" '"command": "migration_scale_fixture_create"'
  require_json_contains "$scale_fixture_result" '"scale_fixture"'
  require_json_contains "$scale_fixture_result" '"normalized_source_output"'
  require_json_contains "$scale_fixture_result" "\"scale_profile\": \"$scale_profile\""
  test -f "$scale_source_dir/opencook-source-manifest.json"
}

# scale_imported_target_ready checks both a progress sentinel and PostgreSQL so
# a stale file does not let later scale phases skip their import prerequisite.
scale_imported_target_ready() {
  [[ -f "$scale_import_sentinel" ]] && restore_database_has_bootstrap_state
}

# require_scale_imported_target imports the generated source bundle into the
# restore database when a later scale phase needs PostgreSQL-backed state.
require_scale_imported_target() {
  require_scale_source
  if scale_imported_target_ready; then
    return 0
  fi

  echo "production-scale source import target is missing; importing generated fixture"
  reset_restore_target
  rm -f "$scale_import_progress" "$scale_sync_progress" "$scale_search_result" "$scale_shadow_result" "$scale_cutover_result" "$scale_restore_result" "$scale_import_sentinel"

  echo "==> migration production-scale source import preflight"
  if ! admin_restore_target migration source import preflight "$scale_source_dir" --offline --with-timing --json >"$scale_import_preflight_result"; then
    echo "migration scale source import preflight command failed; output:" >&2
    print_file_if_exists "$scale_import_preflight_result"
    return 1
  fi
  require_json_contains "$scale_import_preflight_result" '"command": "migration_source_import_preflight"'
  require_json_contains "$scale_import_preflight_result" '"source_import_target"'

  echo "==> migration production-scale source import apply"
  if ! admin_restore_target migration source import apply "$scale_source_dir" --offline --yes --progress "$scale_import_progress" --with-timing --json >"$scale_import_result"; then
    echo "migration scale source import command failed; output:" >&2
    print_file_if_exists "$scale_import_result"
    return 1
  fi
  require_json_contains "$scale_import_result" '"command": "migration_source_import_apply"'
  require_json_contains "$scale_import_result" '"source_import_write"'
  require_json_contains "$scale_import_progress" '"metadata_imported": true'
  touch "$scale_import_sentinel"
}

require_scale_backup_bundle() {
  if [[ -f "$scale_backup_dir/manifest.json" ]]; then
    return 0
  fi
  run_migration_scale_backup
}

# run_migration_scale_backup creates and inspects a logical backup from the
# imported production-scale target so restore phases exercise real CLI IO.
run_migration_scale_backup() {
  build_cli
  require_scale_artifact_paths
  require_scale_imported_target

  echo "==> migration production-scale backup create"
  rm -rf "$scale_backup_dir"
  mkdir -p "$scale_root"
  if ! admin_restore_target migration backup create --output "$scale_backup_dir" --offline --yes --with-timing --json >"$scale_backup_create_result"; then
    echo "migration scale backup create command failed; output:" >&2
    print_file_if_exists "$scale_backup_create_result"
    return 1
  fi
  require_json_contains "$scale_backup_create_result" '"command": "migration_backup_create"'
  require_json_contains "$scale_backup_create_result" '"write_backup_bundle"'
  test -f "$scale_backup_dir/manifest.json"

  echo "==> migration production-scale backup inspect"
  if ! admin migration backup inspect "$scale_backup_dir" --json >"$scale_backup_inspect_result"; then
    echo "migration scale backup inspect command failed; output:" >&2
    print_file_if_exists "$scale_backup_inspect_result"
    return 1
  fi
  require_json_contains "$scale_backup_inspect_result" '"command": "migration_backup_inspect"'
  require_json_contains "$scale_backup_inspect_result" '"required_payloads"'
  require_json_contains "$scale_backup_inspect_result" '"copied_blobs"'
}

scale_restored_target_ready() {
  [[ -f "$scale_restore_result" ]] && restore_database_has_bootstrap_state
}

# run_migration_scale_restore restores the generated scale backup into the
# harness-managed restore database and filesystem blob target.
run_migration_scale_restore() {
  build_cli
  require_scale_artifact_paths
  require_scale_backup_bundle
  reset_restore_target
  rm -f "$scale_reindex_result" "$scale_search_result" "$scale_shadow_result" "$scale_cutover_result"

  echo "==> migration production-scale restore apply"
  if ! admin_restore_target migration restore apply "$scale_backup_dir" --offline --yes --with-timing --json >"$scale_restore_result"; then
    echo "migration scale restore command failed; output:" >&2
    print_file_if_exists "$scale_restore_result"
    return 1
  fi
  require_json_contains "$scale_restore_result" '"command": "migration_restore_apply"'
  require_json_contains "$scale_restore_result" '"restored_backup_bundle"'
  require_json_contains "$scale_restore_result" '"restored_blob_objects"'
}

require_scale_restored_target() {
  require_scale_backup_bundle
  if scale_restored_target_ready; then
    return 0
  fi
  run_migration_scale_restore
}

# run_migration_scale_reindex proves OpenSearch can be rebuilt and checked from
# restored production-scale PostgreSQL state under the maintenance gate.
run_migration_scale_reindex() {
  build_cli
  require_scale_restored_target

  echo "==> migration production-scale complete reindex"
  if ! run_restore_json_under_maintenance reindex "functional production-scale restored target reindex" "$scale_reindex_result" \
    admin_restore_target reindex --all-orgs --complete --with-timing --json; then
    echo "migration scale reindex command failed; output:" >&2
    print_file_if_exists "$scale_reindex_result"
    return 1
  fi
  require_json_contains "$scale_reindex_result" '"ok": true'
  require_json_contains "$scale_reindex_result" '"command": "reindex"'
  require_json_contains "$scale_reindex_result" '"mode": "complete"'

  echo "==> migration production-scale search consistency check"
  if ! admin_restore_target search check --all-orgs --with-timing --json >"$scale_search_result"; then
    echo "migration scale search check command failed; output:" >&2
    print_file_if_exists "$scale_search_result"
    return 1
  fi
  require_json_contains "$scale_search_result" '"ok": true'
  require_json_contains "$scale_search_result" '"command": "search_check"'
  require_json_contains "$scale_search_result" '"clean": 1'
}

require_scale_search_evidence() {
  require_scale_restored_target
  if [[ -f "$scale_search_result" ]]; then
    return 0
  fi
  run_migration_scale_reindex
}

# require_scale_sync_progress records a final no-op sync cursor against the
# restored target so cutover rehearsal can prove freshness for this fixture.
require_scale_sync_progress() {
  require_scale_search_evidence
  if [[ -f "$scale_sync_progress" ]]; then
    return 0
  fi

  echo "==> migration production-scale source sync apply"
  if ! admin_restore_target migration source sync apply "$scale_source_dir" --offline --yes --progress "$scale_sync_progress" --with-timing --json >"$scale_sync_result"; then
    echo "migration scale source sync command failed; output:" >&2
    print_file_if_exists "$scale_sync_result"
    return 1
  fi
  require_json_contains "$scale_sync_result" '"command": "migration_source_sync_apply"'
  require_json_contains "$scale_sync_result" '"source_sync_write"'
  require_json_contains "$scale_sync_progress" '"last_status": "applied"'
  rm -rf "$scale_backup_dir"
  rm -f "$scale_backup_create_result" "$scale_backup_inspect_result" "$scale_reindex_result" "$scale_search_result" "$scale_shadow_result" "$scale_cutover_result"
}

# ensure_scale_cutover_manifest creates the backup manifest after final source
# sync so representative cutover reads match the restored target under test.
ensure_scale_cutover_manifest() {
  require_scale_sync_progress
  require_scale_search_evidence
  if [[ -f "$scale_backup_dir/manifest.json" ]]; then
    return 0
  fi

  echo "==> migration production-scale backup manifest for cutover rehearsal"
  rm -rf "$scale_backup_dir"
  mkdir -p "$scale_root"
  if ! admin_restore_target migration backup create --output "$scale_backup_dir" --offline --yes --with-timing --json >"$scale_backup_create_result"; then
    echo "migration scale backup create command failed; output:" >&2
    print_file_if_exists "$scale_backup_create_result"
    return 1
  fi
  require_json_contains "$scale_backup_create_result" '"command": "migration_backup_create"'
  require_json_contains "$scale_backup_create_result" '"write_backup_bundle"'
  test -f "$scale_backup_dir/manifest.json"

  echo "==> migration production-scale backup inspect for cutover rehearsal"
  if ! admin migration backup inspect "$scale_backup_dir" --json >"$scale_backup_inspect_result"; then
    echo "migration scale backup inspect command failed; output:" >&2
    print_file_if_exists "$scale_backup_inspect_result"
    return 1
  fi
  require_json_contains "$scale_backup_inspect_result" '"command": "migration_backup_inspect"'
  require_json_contains "$scale_backup_inspect_result" '"required_payloads"'
}

# run_migration_scale_shadow compares production-scale source reads to the
# restored OpenCook target using the opt-in scale coverage mode.
run_migration_scale_shadow() {
  build_cli
  ensure_scale_cutover_manifest

  echo "==> migration production-scale shadow-read comparison"
  start_restore_server
  if ! admin migration shadow compare \
    --source "$scale_source_dir" \
    --manifest "$scale_backup_dir/manifest.json" \
    --target-server-url "$restore_server_url" \
    --coverage scale \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$scale_shadow_result"; then
    echo "migration scale shadow compare command failed; output:" >&2
    print_file_if_exists "$scale_shadow_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$scale_shadow_result" '"command": "migration_shadow_compare"'
  require_json_contains "$scale_shadow_result" '"coverage": "scale"'
  require_json_contains "$scale_shadow_result" '"family": "shadow_failed"'
  require_json_contains "$scale_shadow_result" '"count": 0'
  cleanup_restore_server
  trap - EXIT
}

require_scale_shadow_result() {
  require_scale_sync_progress
  if [[ -f "$scale_shadow_result" ]]; then
    return 0
  fi
  run_migration_scale_shadow
}

# run_migration_scale_rehearsal feeds scale source, sync, search, and shadow
# evidence into the read-only cutover rehearsal against the restored target.
run_migration_scale_rehearsal() {
  build_cli
  require_scale_shadow_result
  ensure_scale_cutover_manifest

  echo "==> migration production-scale cutover rehearsal"
  start_restore_server
  if ! admin migration cutover rehearse \
    --manifest "$scale_backup_dir/manifest.json" \
    --source "$scale_source_dir" \
    --source-import-progress "$scale_import_progress" \
    --source-sync-progress "$scale_sync_progress" \
    --search-check-result "$scale_search_result" \
    --shadow-result "$scale_shadow_result" \
    --source-frozen \
    --rollback-ready \
    --server-url "$restore_server_url" \
    --requestor-name "$admin_requestor" \
    --requestor-type user \
    --private-key "$admin_private_key" \
    --server-api-version "${OPENCOOK_ADMIN_SERVER_API_VERSION:-1}" \
    --with-timing \
    --json >"$scale_cutover_result"; then
    echo "migration scale cutover rehearsal command failed; output:" >&2
    print_file_if_exists "$scale_cutover_result"
    echo "restore server log:" >&2
    print_restore_server_log
    return 1
  fi
  require_json_contains "$scale_cutover_result" '"command": "migration_cutover_rehearse"'
  require_json_contains "$scale_cutover_result" '"source_freeze_evidence"'
  require_json_contains "$scale_cutover_result" '"source_sync_freshness"'
  require_json_contains "$scale_cutover_result" '"search_cleanliness"'
  require_json_contains "$scale_cutover_result" '"shadow_read_evidence"'
  require_json_contains "$scale_cutover_result" '"family": "cutover_blockers"'
  require_json_contains "$scale_cutover_result" '"count": 0'
  cleanup_restore_server
  trap - EXIT
}

finish_scale_phase() {
  local phase_name="$1"
  print_scale_phase_success "$phase_name"
  if ! keep_functional_artifacts; then
    clean_scale_artifacts
  fi
}

# run_migration_scale_all keeps production-shaped validation opt-in while still
# offering one command that exercises fixture, backup, restore, search, shadow,
# and cutover readiness evidence end to end.
run_migration_scale_all() {
  run_migration_scale_fixtures
  run_migration_scale_backup
  run_migration_scale_restore
  run_migration_scale_reindex
  run_migration_scale_shadow
  run_migration_scale_rehearsal
  echo "==> migration production-scale functional flow passed successfully for scale profile: $scale_profile"
  if ! keep_functional_artifacts; then
    clean_scale_artifacts
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
  migration-live-source-preflight)
    run_migration_live_source_preflight
    ;;
  migration-live-source-extract)
    run_migration_live_source_extract
    ;;
  migration-live-source-import)
    run_migration_live_source_import
    ;;
  migration-live-source-reindex)
    run_migration_live_source_reindex
    ;;
  migration-live-source-shadow)
    run_migration_live_source_shadow
    ;;
  migration-live-source-rehearsal)
    run_migration_live_source_rehearsal
    ;;
  migration-live-source-all)
    run_migration_live_source_all
    ;;
  migration-scale-fixtures)
    run_migration_scale_fixtures
    finish_scale_phase "$phase"
    ;;
  migration-scale-backup)
    run_migration_scale_backup
    finish_scale_phase "$phase"
    ;;
  migration-scale-restore)
    run_migration_scale_restore
    finish_scale_phase "$phase"
    ;;
  migration-scale-reindex)
    run_migration_scale_reindex
    finish_scale_phase "$phase"
    ;;
  migration-scale-shadow)
    run_migration_scale_shadow
    finish_scale_phase "$phase"
    ;;
  migration-scale-rehearsal)
    run_migration_scale_rehearsal
    finish_scale_phase "$phase"
    ;;
  migration-scale-all)
    run_migration_scale_all
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
