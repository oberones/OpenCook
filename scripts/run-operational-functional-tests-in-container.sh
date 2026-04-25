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

run_operational_phase() {
  build_cli

  echo "==> operational admin status"
  admin_json status >/tmp/opencook-admin-status.json
  require_json_contains /tmp/opencook-admin-status.json '"opensearch"'
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

  echo "==> operational complete org reindex"
  admin reindex --org "$org" --complete --with-timing --json >/tmp/opencook-admin-reindex.json
  require_json_contains /tmp/opencook-admin-reindex.json '"mode": "complete"'

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
}

run_operational_verify_phase() {
  build_cli

  echo "==> operational post-restart verification"
  admin_json status >/tmp/opencook-admin-post-restart-status.json
  admin_with_key "$operational_key_path" users show "$admin_requestor" >/tmp/opencook-admin-post-restart-key.json
  require_json_contains /tmp/opencook-admin-post-restart-key.json "\"username\": \"$admin_requestor\""
  admin search check --org "$org" --index node --json >/tmp/opencook-admin-post-restart-search.json
  require_json_contains /tmp/opencook-admin-post-restart-search.json '"clean": 1'
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
