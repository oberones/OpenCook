# Deployment Evidence And Compatibility Triage Plan

## Summary

This bucket turns the roadmap's evidence-driven phase into a repeatable
operator and agent workflow. The goal is to run realistic deployment drills,
collect redacted evidence artifacts, and patch only narrow Chef-contract gaps
proven by those drills.

This is not a speculative Chef API hardening bucket. Completed API-version,
auth, search, blob, maintenance, migration, and online repair contracts should
remain closed unless deployment evidence proves a concrete compatibility bug.

## Key Changes

- Add `scripts/deployment-evidence.sh [smoke|migration|scale|all]`.
- Emit redacted logs and a manifest under
  `.local/deployment-evidence/<timestamp>` by default.
- Preserve existing Compose and remote Docker behavior by delegating stack
  lifecycle to `scripts/functional-compose.sh`.
- Add an operator runbook entry for deployment evidence collection.
- Update README, functional testing docs, roadmap, milestones, compatibility
  matrix, and agent guidance.

## Task Breakdown

Status: complete for the deterministic local deployment evidence scope.

Implementation notes:

- The runner, runbook discovery, README guidance, functional testing docs, and
  roadmap/milestone references are in place.
- Smoke, migration, and small scale evidence pass on the final implementation.
- The scale drill exposed two evidence-backed fixes:
  - production-scale source fixtures now export ACL rows for generated nested
    group placeholders.
  - PostgreSQL bootstrap-core saves now preserve still-existing org rows so
    bootstrap requestor seeding and other bootstrap mutations cannot cascade
    delete core-object rows for active organizations.
- Restored-target functional servers now start without bootstrap seeding so
  migration shadow/cutover checks remain read-only against restored state.

### Task 1: Create The Plan And Runner

- Add this plan document.
- Add `scripts/deployment-evidence.sh`.
- Support `smoke`, `migration`, `scale`, and `all` presets.
- Produce `manifest.json` with `ok`, `preset`, timestamps, duration, commands,
  reports, warnings, and next steps.
- Redact obvious credential-bearing values in captured logs.

### Task 2: Wire Operator Discovery

- Add a `deployment-evidence` entry to `opencook admin runbook list/show`.
- Document the new runner in the README, functional testing guide, and
  operational runbooks.
- Keep the root `compose.yml` deployment reference distinct from the functional
  validation harness.

### Task 3: Run Smoke Evidence And Patch Only Proven Issues

- Run `scripts/deployment-evidence.sh smoke`.
- Patch deterministic harness bugs or narrow Chef compatibility regressions
  found by the evidence.
- Do not broaden Chef-facing surfaces without evidence.

### Task 4: Run Migration Evidence And Patch Only Proven Issues

- Run `scripts/deployment-evidence.sh migration`.
- Validate normalized source, live-source extraction, source import/sync,
  shadow-read comparison, and cutover rehearsal evidence.
- Patch reporting or safety issues only where evidence proves ambiguity or
  failure.

### Task 5: Run Scale Evidence And Close The Bucket

- Run `OPENCOOK_FUNCTIONAL_SCALE_PROFILE=small scripts/deployment-evidence.sh scale`.
- Ensure the manifest points operators to medium/large follow-up drills.
- Update docs to mark deployment evidence triage complete for the current
  deterministic harness scope.
- If no compatibility gap appears, point the next bucket at broader migration
  safety or operational polish.

## Test Plan

- `bash -n scripts/deployment-evidence.sh scripts/functional-compose.sh scripts/run-functional-tests-in-container.sh scripts/run-operational-functional-tests-in-container.sh scripts/run-migration-functional-tests-in-container.sh`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`
- `scripts/deployment-evidence.sh smoke`
- `scripts/deployment-evidence.sh migration`
- `OPENCOOK_FUNCTIONAL_SCALE_PROFILE=small scripts/deployment-evidence.sh scale`

## Assumptions And Defaults

- No real external Chef Infra Server is required for routine local evidence.
  Compose live-source fixtures remain the deterministic proxy.
- Real deployment or live-source findings can interrupt this bucket, but fixes
  must stay narrow and evidence-backed.
- Compose-managed functional artifacts are preserved only when `KEEP_STACK=1`
  or `OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` is set.
- No licensing, license enforcement, or license telemetry behavior is in scope.
