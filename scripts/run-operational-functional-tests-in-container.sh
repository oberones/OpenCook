#!/usr/bin/env bash
set -euo pipefail

phase="${1:-${OPENCOOK_FUNCTIONAL_PHASE:-operational}}"
base_url="${OPENCOOK_ADMIN_SERVER_URL:-${OPENCOOK_FUNCTIONAL_BASE_URL:-http://opencook:4000}}"
org="${OPENCOOK_FUNCTIONAL_ORG:-ponyville}"
state_dir="${OPENCOOK_FUNCTIONAL_STATE_DIR:-/var/lib/opencook-functional}"
cli="${OPENCOOK_FUNCTIONAL_CLI:-/tmp/opencook-functional-cli}"
admin_private_key="${OPENCOOK_ADMIN_PRIVATE_KEY_PATH:-${OPENCOOK_FUNCTIONAL_PRIVATE_KEY_PATH:-/src/test/functional/fixtures/bootstrap_private.pem}}"
admin_requestor="${OPENCOOK_ADMIN_REQUESTOR_NAME:-${OPENCOOK_FUNCTIONAL_ACTOR_NAME:-pivotal}}"
operational_user="${OPENCOOK_FUNCTIONAL_ADMIN_USER:-functional-admin-user}"
operational_org="${OPENCOOK_FUNCTIONAL_ADMIN_ORG:-functional-admin-org}"
operational_key_name="${OPENCOOK_FUNCTIONAL_ADMIN_KEY_NAME:-functional-operational-key}"
operational_key_path="$state_dir/$operational_key_name.pem"
opensearch_url="${OPENCOOK_OPENSEARCH_URL:-http://opensearch:9200}"
stale_doc_id="$org/node/functional-stale-node"
stale_doc_path="$org%2Fnode%2Ffunctional-stale-node"
encrypted_bag="${OPENCOOK_FUNCTIONAL_ENCRYPTED_BAG:-encrypted_secrets}"
encrypted_stale_doc_id="$org/$encrypted_bag/functional-stale-encrypted"
encrypted_stale_doc_path="$org%2F$encrypted_bag%2Ffunctional-stale-encrypted"
unsupported_stale_doc_id="$org/cookbooks/functional-search-cookbook"
unsupported_stale_doc_path="$org%2Fcookbooks%2Ffunctional-search-cookbook"

build_cli() {
  go build -trimpath -o "$cli" ./cmd/opencook
}

admin() {
  "$cli" admin "$@"
}

admin_json() {
  "$cli" admin --json "$@"
}

admin_with_key() {
  "$cli" admin --requestor-name "$admin_requestor" --requestor-type user --private-key "$1" --json "${@:2}"
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

require_opensearch_capability_status() {
  local file="$1"
  require_json_contains "$file" '"opensearch"'
  require_json_contains "$file" 'search-after pagination'
  require_json_contains "$file" 'delete-by-query'
  require_json_contains "$file" 'total hits'
}

encrypted_search_index_available() {
  local stdout="/tmp/opencook-admin-encrypted-search-preflight.json"
  local stderr="/tmp/opencook-admin-encrypted-search-preflight.err"

  set +e
  admin search check --org "$org" --index "$encrypted_bag" --json >"$stdout" 2>"$stderr"
  local code="$?"
  set -e

  case "$code" in
    0|3)
      return 0
      ;;
    2)
      if grep -Fq 'search index not found' "$stdout" "$stderr"; then
        return 1
      fi
      ;;
  esac

  echo "encrypted data bag search preflight failed with exit $code" >&2
  echo "stdout:" >&2
  cat "$stdout" >&2
  echo "stderr:" >&2
  cat "$stderr" >&2
  exit "$code"
}

ensure_user() {
  mkdir -p "$state_dir"
  if admin_json users show "$operational_user" >/tmp/opencook-admin-user.json 2>/tmp/opencook-admin-user.err; then
    return 0
  fi
  rm -f "$state_dir/$operational_user.pem"
  admin_json users create "$operational_user" \
    --first-name Functional \
    --last-name Admin \
    --email functional-admin@example.test \
    --private-key-out "$state_dir/$operational_user.pem" >/tmp/opencook-admin-user-create.json
}

ensure_org_named() {
  local name="$1"
  local full_name="$2"
  if admin_json orgs show "$name" >/tmp/opencook-admin-org.json 2>/tmp/opencook-admin-org.err; then
    return 0
  fi
  admin_json orgs create "$name" --full-name "$full_name" >/tmp/opencook-admin-org-create.json
}

ensure_org() {
  ensure_org_named "$operational_org" "Functional Admin Org"
}

rotate_operational_key() {
  mkdir -p "$state_dir"
  rm -f "$operational_key_path"
  if admin_json users keys show "$admin_requestor" "$operational_key_name" >/tmp/opencook-admin-key.json 2>/tmp/opencook-admin-key.err; then
    admin_json users keys update "$admin_requestor" "$operational_key_name" \
      --create-key \
      --private-key-out "$operational_key_path" >/tmp/opencook-admin-key-update.json
  else
    admin_json users keys add "$admin_requestor" \
      --key-name "$operational_key_name" \
      --private-key-out "$operational_key_path" >/tmp/opencook-admin-key-add.json
  fi
  chmod 0600 "$operational_key_path"
}

write_stale_opensearch_document() {
  curl -fsS -XPUT "$opensearch_url/chef/_doc/$stale_doc_path?refresh=true" \
    -H 'Content-Type: application/json' \
    --data-binary @- >/tmp/opencook-stale-opensearch.json <<JSON
{
  "document_id": "$stale_doc_id",
  "organization": "$org",
  "index": "node",
  "name": "functional-stale-node",
  "resource_type": "node",
  "resource_name": "functional-stale-node",
  "compat_terms": [
    "__org=$org",
    "__index=node",
    "name=functional-stale-node",
    "__any=functional-stale-node"
  ]
}
JSON
}

write_stale_encrypted_opensearch_document() {
  curl -fsS -XPUT "$opensearch_url/chef/_doc/$encrypted_stale_doc_path?refresh=true" \
    -H 'Content-Type: application/json' \
    --data-binary @- >/tmp/opencook-stale-encrypted-opensearch.json <<JSON
{
  "document_id": "$encrypted_stale_doc_id",
  "organization": "$org",
  "index": "$encrypted_bag",
  "name": "functional-stale-encrypted",
  "resource_type": "data_bag",
  "resource_name": "$encrypted_bag",
  "compat_terms": [
    "__org=$org",
    "__index=$encrypted_bag",
    "id=functional-stale-encrypted",
    "environment=functional-stale-encrypted",
    "__any=functional-stale-encrypted"
  ]
}
JSON
}

write_stale_unsupported_opensearch_document() {
  curl -fsS -XPUT "$opensearch_url/chef/_doc/$unsupported_stale_doc_path?refresh=true" \
    -H 'Content-Type: application/json' \
    --data-binary @- >/tmp/opencook-stale-unsupported-opensearch.json <<JSON
{
  "document_id": "$unsupported_stale_doc_id",
  "organization": "$org",
  "index": "cookbooks",
  "name": "functional-search-cookbook",
  "resource_type": "cookbooks",
  "resource_name": "functional-search-cookbook",
  "compat_terms": [
    "__org=$org",
    "__index=cookbooks",
    "name=functional-search-cookbook",
    "__any=functional-search-cookbook"
  ]
}
JSON
}

require_unsupported_search_admin_surfaces() {
  local unsupported_index
  for unsupported_index in cookbooks cookbook_artifacts policy policy_groups sandbox checksums; do
    expect_exit 2 admin reindex --org "$org" --index "$unsupported_index" --no-drop --json >/tmp/opencook-admin-reindex-unsupported-"$unsupported_index".json
    require_json_contains /tmp/opencook-admin-reindex-unsupported-"$unsupported_index".json 'search index not found'
    expect_exit 2 admin search check --org "$org" --index "$unsupported_index" --json >/tmp/opencook-admin-search-check-unsupported-"$unsupported_index".json
    require_json_contains /tmp/opencook-admin-search-check-unsupported-"$unsupported_index".json 'search index not found'
    expect_exit 2 admin search repair --org "$org" --index "$unsupported_index" --yes --json >/tmp/opencook-admin-search-repair-unsupported-"$unsupported_index".json
    require_json_contains /tmp/opencook-admin-search-repair-unsupported-"$unsupported_index".json 'search index not found'
  done
}

run_operational_phase() {
  build_cli

  echo "==> operational admin status"
  admin_json status >/tmp/opencook-admin-status.json
  require_opensearch_capability_status /tmp/opencook-admin-status.json
  require_json_contains /tmp/opencook-admin-status.json '"postgres"'

  echo "==> operational live-safe user/org commands"
  ensure_user
  ensure_org_named "$org" "Ponyville"
  ensure_org

  echo "==> operational key creation and signed follow-up request"
  rotate_operational_key
  admin_with_key "$operational_key_path" users show "$admin_requestor" >/tmp/opencook-admin-new-key-followup.json
  require_json_contains /tmp/opencook-admin-new-key-followup.json "\"username\": \"$admin_requestor\""

  echo "==> operational group/container/ACL inspection"
  admin_json groups show "$org" admins >/tmp/opencook-admin-group.json
  require_json_contains /tmp/opencook-admin-group.json '"groupname": "admins"'
  admin_json containers show "$org" data >/tmp/opencook-admin-container.json
  require_json_contains /tmp/opencook-admin-container.json '"containername": "data"'
  admin_json acls get org "$org" >/tmp/opencook-admin-acl.json
  require_json_contains /tmp/opencook-admin-acl.json '"read"'

  local encrypted_search_checks=0
  if encrypted_search_index_available; then
    encrypted_search_checks=1
  else
    echo "==> operational encrypted data bag fixture absent; skipping scoped encrypted search checks"
  fi

  echo "==> operational complete org reindex"
  admin reindex --org "$org" --complete --with-timing --json >/tmp/opencook-admin-reindex.json
  require_json_contains /tmp/opencook-admin-reindex.json '"mode": "complete"'
  if [[ "$encrypted_search_checks" == "1" ]]; then
    admin search check --org "$org" --index "$encrypted_bag" --json >/tmp/opencook-admin-encrypted-search-clean.json
    require_json_contains /tmp/opencook-admin-encrypted-search-clean.json '"clean": 1'
    admin reindex --org "$org" --index "$encrypted_bag" --no-drop --json >/tmp/opencook-admin-encrypted-reindex.json
    require_json_contains /tmp/opencook-admin-encrypted-reindex.json '"upserted": 1'
  fi

  echo "==> operational unsupported search admin surfaces"
  require_unsupported_search_admin_surfaces
  write_stale_unsupported_opensearch_document
  expect_exit 3 admin search check --org "$org" --json >/tmp/opencook-admin-search-check-unsupported-drift.json
  require_json_contains /tmp/opencook-admin-search-check-unsupported-drift.json "$unsupported_stale_doc_id"
  require_json_contains /tmp/opencook-admin-search-check-unsupported-drift.json "$org/cookbooks"
  expect_exit 3 admin search repair --org "$org" --dry-run --json >/tmp/opencook-admin-search-repair-unsupported-dry-run.json
  require_json_contains /tmp/opencook-admin-search-repair-unsupported-dry-run.json '"skipped": 1'
  admin search repair --org "$org" --yes --json >/tmp/opencook-admin-search-repair-unsupported.json
  require_json_contains /tmp/opencook-admin-search-repair-unsupported.json '"deleted": 1'
  admin search check --org "$org" --json >/tmp/opencook-admin-search-check-after-unsupported-cleanup.json
  require_json_contains /tmp/opencook-admin-search-check-after-unsupported-cleanup.json '"clean": 1'

  echo "==> operational stale OpenSearch detection"
  write_stale_opensearch_document
  expect_exit 3 admin search check --org "$org" --index node --with-timing --json >/tmp/opencook-admin-search-check-drift.json
  require_json_contains /tmp/opencook-admin-search-check-drift.json "$stale_doc_id"
  expect_exit 3 admin search repair --org "$org" --index node --dry-run --json >/tmp/opencook-admin-search-repair-dry-run.json
  require_json_contains /tmp/opencook-admin-search-repair-dry-run.json '"skipped": 1'

  echo "==> operational search repair"
  admin search repair --org "$org" --index node --yes --with-timing --json >/tmp/opencook-admin-search-repair.json
  require_json_contains /tmp/opencook-admin-search-repair.json '"deleted": 1'
  admin search check --org "$org" --index node --json >/tmp/opencook-admin-search-check-clean.json
  require_json_contains /tmp/opencook-admin-search-check-clean.json '"clean": 1'

  if [[ "$encrypted_search_checks" == "1" ]]; then
    echo "==> operational encrypted data bag search repair"
    write_stale_encrypted_opensearch_document
    expect_exit 3 admin search check --org "$org" --index "$encrypted_bag" --with-timing --json >/tmp/opencook-admin-encrypted-search-check-drift.json
    require_json_contains /tmp/opencook-admin-encrypted-search-check-drift.json "$encrypted_stale_doc_id"
    expect_exit 3 admin search repair --org "$org" --index "$encrypted_bag" --dry-run --json >/tmp/opencook-admin-encrypted-search-repair-dry-run.json
    require_json_contains /tmp/opencook-admin-encrypted-search-repair-dry-run.json '"skipped": 1'
    admin search repair --org "$org" --index "$encrypted_bag" --yes --with-timing --json >/tmp/opencook-admin-encrypted-search-repair.json
    require_json_contains /tmp/opencook-admin-encrypted-search-repair.json '"deleted": 1'
    admin search check --org "$org" --index "$encrypted_bag" --json >/tmp/opencook-admin-encrypted-search-check-clean-after-repair.json
    require_json_contains /tmp/opencook-admin-encrypted-search-check-clean-after-repair.json '"clean": 1'
  fi
}

run_operational_verify_phase() {
  build_cli

  echo "==> operational post-restart verification"
  admin_json status >/tmp/opencook-admin-post-restart-status.json
  require_opensearch_capability_status /tmp/opencook-admin-post-restart-status.json
  admin_with_key "$operational_key_path" users show "$admin_requestor" >/tmp/opencook-admin-post-restart-key.json
  require_json_contains /tmp/opencook-admin-post-restart-key.json "\"username\": \"$admin_requestor\""
  require_unsupported_search_admin_surfaces
  admin search check --org "$org" --index node --json >/tmp/opencook-admin-post-restart-search.json
  require_json_contains /tmp/opencook-admin-post-restart-search.json '"clean": 1'
  if encrypted_search_index_available; then
    admin search check --org "$org" --index "$encrypted_bag" --json >/tmp/opencook-admin-post-restart-encrypted-search.json
    require_json_contains /tmp/opencook-admin-post-restart-encrypted-search.json '"clean": 1'
  else
    echo "==> operational encrypted data bag fixture absent after restart; skipping scoped encrypted search check"
  fi
}

export OPENCOOK_ADMIN_SERVER_URL="$base_url"
export OPENCOOK_ADMIN_REQUESTOR_NAME="$admin_requestor"
export OPENCOOK_ADMIN_REQUESTOR_TYPE="${OPENCOOK_ADMIN_REQUESTOR_TYPE:-user}"
export OPENCOOK_ADMIN_PRIVATE_KEY_PATH="$admin_private_key"
export OPENCOOK_ADMIN_DEFAULT_ORG="$org"

case "$phase" in
  operational)
    run_operational_phase
    ;;
  operational-verify)
    run_operational_verify_phase
    ;;
  *)
    echo "unknown operational functional phase: $phase" >&2
    exit 2
    ;;
esac
