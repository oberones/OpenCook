#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
functional_script="${OPENCOOK_FUNCTIONAL_COMPOSE:-$root_dir/scripts/functional-compose.sh}"
preset="${1:-smoke}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir="${OPENCOOK_DEPLOYMENT_EVIDENCE_DIR:-$root_dir/.local/deployment-evidence/$timestamp}"
manifest_path="$evidence_dir/manifest.json"

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"
commands=()
reports=()
warnings=()
failure=""

usage() {
  cat >&2 <<'USAGE'
Usage:
  scripts/deployment-evidence.sh [smoke|migration|scale|all]

Collect repeatable OpenCook deployment evidence by running the existing
functional Docker harness and writing redacted logs plus a manifest.
USAGE
}

# json_escape keeps the manifest dependency-free while still safely encoding
# paths and command strings that may contain quotes or whitespace.
json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

json_array() {
  local first=1
  printf '['
  for value in "$@"; do
    if [[ "$first" == "0" ]]; then
      printf ','
    fi
    first=0
    printf '"%s"' "$(json_escape "$value")"
  done
  printf ']'
}

# redact_stream removes the credential-bearing patterns OpenCook evidence logs
# are most likely to include while preserving enough context to debug failures.
redact_stream() {
  sed -E \
    -e 's#postgres://([^:/@]+):([^@]+)@#postgres://[redacted]@#g' \
    -e 's#(OPENCOOK_[A-Z0-9_]*(PRIVATE_KEY|PASSWORD|SECRET|TOKEN|DSN)[A-Z0-9_]*=)[^[:space:]]+#\1[redacted]#g' \
    -e 's#(X-Ops-Authorization[^[:space:]]*)#[redacted-authorization]#g' \
    -e 's#-----BEGIN [A-Z ]*PRIVATE KEY-----#[redacted-private-key]#g' \
    -e 's#-----END [A-Z ]*PRIVATE KEY-----#[redacted-private-key]#g'
}

write_manifest() {
  local ok="$1"
  local completed_at duration_ms next_steps
  completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration_ms="$(( ($(date +%s) - started_epoch) * 1000 ))"
  if [[ "$ok" == "true" ]]; then
    next_steps=("review the evidence manifest and logs" "run migration or scale presets if smoke evidence is clean")
  else
    next_steps=("inspect the failing preset log" "patch only evidence-backed Chef compatibility or harness issues" "rerun the same preset after fixing the blocker")
  fi

  {
    printf '{\n'
    printf '  "ok": %s,\n' "$ok"
    printf '  "preset": "%s",\n' "$(json_escape "$preset")"
    printf '  "started_at": "%s",\n' "$(json_escape "$started_at")"
    printf '  "completed_at": "%s",\n' "$(json_escape "$completed_at")"
    printf '  "duration_ms": %s,\n' "$duration_ms"
    printf '  "commands": '
    json_array "${commands[@]}"
    printf ',\n'
    printf '  "reports": '
    json_array "${reports[@]}"
    printf ',\n'
    printf '  "warnings": '
    json_array "${warnings[@]}"
    printf ',\n'
    printf '  "next_steps": '
    json_array "${next_steps[@]}"
    printf '\n}\n'
  } >"$manifest_path"
}

# run_functional_phases records one redacted log for a logical evidence phase
# while letting scripts/functional-compose.sh keep ownership of stack behavior.
run_functional_phases() {
  local label="$1"
  shift
  local log_path="$evidence_dir/$label.log"
  local command="scripts/functional-compose.sh $*"
  commands+=("$command")
  reports+=("$log_path")

  echo "==> deployment evidence phase: $label"
  echo "    command: $command"
  echo "    log: $log_path"

  set +e
  "$functional_script" "$@" 2>&1 | redact_stream >"$log_path"
  local code="${PIPESTATUS[0]}"

  if [[ "$code" != "0" ]]; then
    failure="$label failed with exit code $code"
    echo "deployment evidence phase failed: $failure" >&2
    echo "last log lines:" >&2
    tail -n 80 "$log_path" >&2 || true
    return "$code"
  fi
  set -e
}

run_smoke() {
  run_functional_phases smoke \
    create restart verify query-compat object-compat operational restart \
    operational-verify admin-repair restart admin-repair maintenance
}

run_migration() {
  run_functional_phases migration migration-source-all migration-live-source-all
}

run_scale() {
  export OPENCOOK_FUNCTIONAL_SCALE_PROFILE="${OPENCOOK_FUNCTIONAL_SCALE_PROFILE:-small}"
  commands+=("OPENCOOK_FUNCTIONAL_SCALE_PROFILE=$OPENCOOK_FUNCTIONAL_SCALE_PROFILE scripts/functional-compose.sh migration-scale-all")
  reports+=("$evidence_dir/scale.log")
  echo "==> deployment evidence phase: scale"
  echo "    profile: $OPENCOOK_FUNCTIONAL_SCALE_PROFILE"
  echo "    log: $evidence_dir/scale.log"

  set +e
  "$functional_script" migration-scale-all 2>&1 | redact_stream >"$evidence_dir/scale.log"
  local code="${PIPESTATUS[0]}"

  if [[ "$code" != "0" ]]; then
    failure="scale failed with exit code $code"
    echo "deployment evidence phase failed: $failure" >&2
    echo "last log lines:" >&2
    tail -n 80 "$evidence_dir/scale.log" >&2 || true
    return "$code"
  fi
  set -e
}

if [[ "$#" -gt 1 ]]; then
  usage
  exit 2
fi

case "$preset" in
  smoke|migration|scale|all)
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 2
    ;;
esac

mkdir -p "$evidence_dir"
warnings+=("Compose-managed artifacts are preserved only when KEEP_STACK=1 or OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1 is set")

set +e
case "$preset" in
  smoke)
    run_smoke
    ;;
  migration)
    run_migration
    ;;
  scale)
    run_scale
    ;;
  all)
    run_smoke && run_migration && run_scale
    ;;
esac
code="$?"
set -e

if [[ "$code" == "0" ]]; then
  write_manifest true
  echo
  echo "==> deployment evidence passed successfully"
  echo "manifest: $manifest_path"
  exit 0
fi

warnings+=("$failure")
write_manifest false
echo "manifest: $manifest_path" >&2
exit "$code"
