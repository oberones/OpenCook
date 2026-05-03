# Production-Scale Migration Validation And Cutover Readiness Plan

Status: complete - Tasks 1-13 complete

## Summary

This bucket hardens OpenCook migration confidence after the maintenance-mode,
source import/sync, shadow-read, backup/restore, and operational tooling slices.

The goal is not to add new Chef-facing behavior. The goal is to prove that the
existing migration and cutover path stays correct, observable, retry-safe, and
operator-friendly under larger production-shaped datasets that include the
implemented identity, authorization, core object, cookbook/blob, policy,
sandbox/checksum, data bag, and OpenSearch-derived search surfaces.

Use this file as the reference checklist for this bucket.

## Current State

OpenCook already has:

- PostgreSQL-backed persistence for implemented identity, authorization, core
  object, cookbook, sandbox/checksum, policy, and ACL state.
- Provider-backed blobs in memory, filesystem, and S3-compatible modes.
- Active OpenSearch-backed search for implemented Chef-searchable families.
- Logical OpenCook backup create/inspect, offline restore preflight/apply,
  restored-target reindex, and cutover rehearsal.
- Normalized Chef Server source artifact inventory, normalize, offline import,
  source sync, shadow-read comparison, and source cutover rehearsal.
- Maintenance-mode write blocking for OpenCook targets, PostgreSQL-shared
  maintenance state, maintenance-gated reindex/search repair, and a narrow
  online default ACL repair path.
- Functional Docker coverage against PostgreSQL, OpenSearch, and
  filesystem-backed blobs.

Remaining gap:

- Existing functional fixtures are compatibility-rich but not yet
  production-shaped enough to stress migration count validation, pagination,
  retry behavior, blob integrity reporting, and large-result OpenSearch repair.
- Shadow-read and restored-target validation need stronger normalized diff
  reporting, inventory summaries, timing output, and rerun evidence.
- Cutover runbooks need a clearer evidence chain for source Chef write freezes,
  OpenCook maintenance windows, rollback readiness, and post-cutover smoke
  checks.
- Direct live upstream extraction remains deferred. This bucket should make the
  normalized-artifact path safer before broadening source connectivity.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, successful payloads, status codes,
  signed-auth behavior, API-version behavior, or established compatibility
  responses.
- Do not proxy or mutate upstream Chef Infra Server writes. Source Chef access
  remains read-only and operators must still freeze source writes externally for
  final sync and cutover rehearsal.
- Keep PostgreSQL as the authoritative restored/imported OpenCook state.
- Keep OpenSearch as derived state. Validation should rebuild, check, and repair
  OpenSearch from PostgreSQL-backed state instead of treating provider documents
  as authoritative.
- Keep blob providers as checksum-addressed content stores. Validation should
  prove reachability and content hashes without leaking signed URLs, provider
  response bodies, credentials, or local secret paths.
- Preserve existing `opencook admin migration` command shapes unless an
  additive subcommand or flag materially improves validation clarity.
- Keep destructive commands explicit with `--yes`; keep dry-run/preflight
  behavior safe by default.
- Keep maintenance-mode semantics stable. OpenCook maintenance protects the
  OpenCook target only and does not freeze writes still going to source Chef.
- Do not add licensing, license enforcement, license telemetry, or
  license-management behavior.

## Validation Contract

Production-scale validation should produce evidence operators can trust:

- Deterministic input fixtures with stable seeds, counts, object names, and
  checksum payloads.
- Per-family source, backup, restore, sync, search, and rehearsal inventory
  counts.
- Stable finding codes for validation failures and warnings.
- Timing summaries that help operators spot slow phases without creating
  pass/fail compatibility contracts around wall-clock duration.
- Normalized read diffs that avoid false positives from documented
  compatibility differences and redact private keys, signatures, DSNs, provider
  URLs, and blob query strings.
- Retry-safe reruns for backup inspect, restore preflight, source import/sync
  preflight, source import/sync apply, reindex, search repair, shadow compare,
  and cutover rehearsal.
- Clear rollback evidence: source Chef remains available, OpenCook target state
  can be restored from backup, OpenSearch can be rebuilt from PostgreSQL, and
  blob content can be verified by checksum.

Task 1 freezes the additive validation contract in
`cmd/opencook/admin_migration.go` as
`opencook.migration.production_scale_validation.v1`. Later tasks should reuse
that contract instead of inventing new phase names, scale profile names,
required inventory families, or finding-code spellings.

Frozen evidence phases:

- `backup`
- `restore`
- `source_import_sync`
- `shadow_compare`
- `opensearch_check_repair`
- `blob_verification`
- `cutover_rehearsal`

Frozen scale profiles:

- `small`: default CI-friendly profile.
- `medium`: opt-in local rehearsal profile for pagination, repeated sync,
  shared blobs, and multi-family validation.
- `large`: opt-in slower stress profile for release confidence and operator
  cutover drills.

Required global families:

- `users`
- `user_acls`
- `user_keys`
- `organizations`
- `server_admin_memberships`

Required organization-scoped families:

- `clients`
- `client_keys`
- `groups`
- `group_memberships`
- `containers`
- `acls`
- `nodes`
- `environments`
- `roles`
- `data_bags`
- `data_bag_items`
- `policy_revisions`
- `policy_groups`
- `policy_assignments`
- `sandboxes`
- `checksum_references`
- `cookbook_versions`
- `cookbook_artifacts`

Required blob families:

- `referenced_blobs`
- `reachable_blobs`
- `missing_blobs`
- `provider_unavailable_checks`
- `content_verified_blobs`
- `checksum_mismatch_blobs`
- `candidate_orphan_blobs`
- `copied_blobs`

Required OpenSearch families:

- `opensearch_expected_documents`
- `opensearch_observed_documents`
- `opensearch_missing_documents`
- `opensearch_stale_documents`
- `opensearch_unsupported_scopes`

Required rehearsal families:

- `rehearsal_checks`
- `rehearsal_passed`
- `rehearsal_failed`
- `rehearsal_skipped`
- `rehearsal_downloads`

Frozen production-scale finding codes:

- `migration_count_mismatch`
- `migration_restored_object_missing`
- `migration_unexpected_extra_object`
- `migration_blob_mismatch`
- `migration_missing_blob`
- `migration_stale_search_document`
- `migration_missing_search_document`
- `migration_unsupported_source_family`
- `migration_retry_progress_drift`
- `migration_retry_safe`
- `migration_retry_unsafe`
- `migration_manual_cleanup_required`
- `cutover_maintenance_evidence_invalid`
- `cutover_maintenance_inactive`
- `cutover_maintenance_process_local`

## Proposed Functional Surface

The exact phase names can be adjusted during implementation, but the bucket
should converge on an additive functional surface like:

```sh
scripts/functional-compose.sh migration-scale-fixtures
scripts/functional-compose.sh migration-scale-backup
scripts/functional-compose.sh migration-scale-restore
scripts/functional-compose.sh migration-scale-reindex
scripts/functional-compose.sh migration-scale-shadow
scripts/functional-compose.sh migration-scale-rehearsal
scripts/functional-compose.sh migration-scale-all
```

The default functional flow should stay reasonably fast. The production-shaped
fixtures may be opt-in or controlled by an environment variable such as:

```sh
OPENCOOK_FUNCTIONAL_SCALE_PROFILE=medium scripts/functional-compose.sh migration-scale-all
```

Scale profile names should be deterministic and documented. Suggested profiles:

- `small`: current default-style fixture volume for CI speed.
- `medium`: enough objects to exercise pagination, search-after, repeated sync,
  shared blobs, and multi-family validation without making local development
  painful.
- `large`: optional, slower stress profile for operator rehearsal and release
  confidence.

## Task Breakdown

### Task 1: Freeze The Production-Scale Validation Contract

Task status: complete.

- Inventory the migration and cutover evidence OpenCook already emits.
- Define required per-family counts for backup, restore, source import/sync,
  shadow compare, OpenSearch check/repair, blob verification, and rehearsal.
- Define stable finding codes for count mismatch, missing restored object,
  unexpected extra object, blob mismatch, missing blob, stale search document,
  missing search document, unsupported source family, and retry-progress drift.
- Record the scale profiles, default profile, and opt-in heavier profiles in
  this plan.
- Add focused tests for validation report shape without changing Chef-facing
  routes or payloads.

### Task 2: Add Deterministic Production-Shaped Migration Fixtures

Task status: complete.

- Add a fixture generator or fixture builder that creates deterministic
  migration data with stable names, seeds, object counts, and checksum content.
- Cover users, user keys, user ACLs, organizations, clients, client keys,
  groups, group memberships, containers, ACLs, nodes, environments, roles, data
  bags/items, encrypted-looking data bag items, policy revisions/groups,
  policy assignments, sandboxes, checksum references, cookbook versions,
  cookbook artifacts, and shared checksum blobs.
- Include multi-org data and default-org alias coverage.
- Include shared checksum reuse across cookbook versions, cookbook artifacts,
  and sandbox-held blobs.
- Add fixture tests that prove deterministic output, unique object names, valid
  references, and expected per-family counts.

Completion notes:

- Added the internal `adminMigrationProductionScaleFixture` builder for the
  frozen `small`, `medium`, and `large` profiles.
- The fixture uses the stable seed
  `opencook-production-scale-migration-fixture-v1`.
- The default organization is `ponyville`, and every profile includes multiple
  organizations so default-org alias and explicit-org validation can share the
  same dataset.
- The fixture covers bootstrap core rows, core object rows, cookbook versions,
  cookbook artifacts, sandbox checksum references, encrypted-looking data bag
  payloads, policyfile rows, ACLs, and copied checksum blob bytes.
- One checksum is intentionally shared across sandbox rows, cookbook version
  files, and cookbook artifact files.
- Focused tests now prove deterministic JSON output, profile growth, expected
  inventory counts, source-sync row uniqueness, valid blob checksums, and shared
  checksum coverage.

### Task 3: Harden Logical Backup And Inspect At Scale

Task status: complete.

- Extend backup inspect or its internal report model with per-family counts,
  payload hash coverage, referenced/reachable blob counts, and timing summaries.
- Prove backup creation includes every required PostgreSQL and blob payload for
  the scale fixtures.
- Prove backup inspect detects missing payloads, tampered payloads, truncated
  blobs, unexpected manifest omissions, and checksum mismatches.
- Keep output redacted and stable for JSON consumers.
- Add tests for retry-safe backup inspect reruns against the same bundle.

Completion notes:

- Backup inspect now emits provider-free integrity details for payload count,
  hashed payload count, payload bytes, required restore payload count,
  referenced blobs, copied blobs, and verified copied blobs.
- Backup inspect now cross-checks `blobs/manifest.json` copied entries against
  the top-level manifest payload list, copied payload SHA-256/size metadata, and
  the Chef checksum addressed by the copied blob body.
- The scale fixture now drives backup-create coverage through the normal CLI
  path with filesystem-backed blobs.
- Focused tests prove required PostgreSQL payloads, required blob payloads,
  copied blob payload hashes, redaction, timing output, retry-safe repeated
  inspect, missing listed payloads, omitted copied-blob manifest payloads,
  truncated copied blobs, copied-blob checksum mismatches, and required restore
  payload omissions.

### Task 4: Harden Restore Preflight And Apply Validation

Task status: complete.

- Add restored-target validation that compares backup manifest inventory to
  restored PostgreSQL rows, provider-backed blob reachability, and expected
  checksum content.
- Prove restore preflight catches target conflicts, missing required payloads,
  malformed payloads, missing blobs, and blob checksum mismatches before apply.
- Prove restore apply can be rerun safely after a failed preflight and reports
  no partial restored state after failed apply paths covered by the current
  restore contract.
- Include restart/rehydration after restore before validation, reindex, and
  rehearsal.

Completion notes:

- Restore bundle preflight now decodes the fixed PostgreSQL state payloads
  before loading target configuration, so malformed logical payload JSON fails
  before any target state can be touched.
- Restore preflight now validates uncopied checksum references against the
  configured target blob provider, catching missing provider blobs and local
  checksum mismatches before apply.
- Restore apply now reloads the target store after writing metadata and compares
  logical PostgreSQL inventory plus checksum-reference sets against the backup
  manifest before reporting success.
- Restore apply now validates restored checksum blobs through the provider
  validation path and maps missing/mismatched blob findings onto the frozen
  production-scale validation vocabulary.
- Focused tests cover malformed restore payloads, rerunnable failed preflights,
  missing required payloads, missing uncopied blobs, checksum-mismatched
  uncopied blobs, successful rehydration validation, cookbook-restore rollback,
  rehydrated inventory mismatch reporting, and production-shaped scale fixture
  restore validation.

### Task 5: Strengthen Source Import And Source Sync Rerun Safety

Task status: complete.

- Run normalized source import/sync against the production-shaped source
  fixtures.
- Prove import and sync preflight report per-family counts, unsupported families,
  orphan references, duplicate records, missing blobs, and advisory derived
  OpenSearch artifacts.
- Prove source import apply and source sync apply are idempotent or
  retry-safe with progress metadata.
- Prove invalid source payloads and failed blob copy/verification paths do not
  leave partially visible persisted rows beyond documented resumable progress.
- Keep direct live upstream PostgreSQL/OpenSearch/Redis extraction out of scope.

Completion notes:

- Added a normalized source-bundle writer for the deterministic production-scale
  fixture so source import/sync tests now run through real manifest hashes,
  payload counts, sidecar artifact metadata, copied checksum blob files, and
  source semantic normalization.
- Source import preflight now has scale coverage for per-family counts,
  copied/referenced blob counts, unsupported `oc_id` source artifacts, advisory
  OpenSearch-derived artifacts, and no-mutation behavior.
- Source import apply now supports a guarded replay path: if progress metadata
  proves metadata import completed, apply may pass the usual non-empty-target
  preflight only long enough to verify that the target still exactly matches the
  normalized source snapshot. Matching reruns recopy or reuse blobs as needed
  and do not rewrite PostgreSQL metadata; drift still fails instead of being
  silently accepted.
- Source sync apply now has scale coverage for cursor progress, stale target
  reconciliation, stable rerun preflight, and no-op apply reruns without
  metadata rewrites.
- Negative scale-source tests now cover duplicate records, orphan group
  membership references, missing copied blob payloads, provider verification
  failures, and no partially visible target metadata after blocked apply paths.

### Task 6: Add Scale-Aware Shadow-Read Comparison

- Expand shadow-read comparison so production-shaped source artifacts and
  restored targets produce normalized per-family diff reports.
- Cover collection reads, named reads, key reads, ACL reads, cookbook/artifact
  reads, signed cookbook/artifact downloads, search reads, partial search, and
  depsolver reads where the current implemented surfaces support them.
- Normalize documented compatibility differences and redact sensitive fields.
- Add pagination coverage for large collections and search result windows.
- Prove shadow compare never proxies writes and remains safe during source Chef
  freeze windows.

Completion notes:

- Added opt-in `opencook admin migration shadow compare --coverage scale` while
  keeping representative coverage as the default for existing automation.
- Scale coverage now derives deterministic read-like checks from normalized
  source artifacts for global/user/org collections, named identity and core
  object reads, user/client key lists and key detail reads, supported ACL
  routes, policy and policy-group reads, cookbook/cookbook-artifact
  collections, `_latest`, `_recipes`, `/universe`, signed cookbook/artifact
  downloads, OpenSearch query windows, partial search, and empty-run-list
  depsolver reads.
- Shadow output now includes per-family success/failure/skip/download counters
  so scale failures identify the drifted organization and family without
  emitting source/target payload bodies, private keys, signed URL query strings,
  or provider internals.
- Added safety gates so shadow compare permits only GET plus Chef read-like
  POST routes for partial search and depsolver. The scale fixture test asserts
  no write methods are proxied during source freeze windows.

### Task 7: Harden Cutover Rehearsal Evidence Gates

Task status: complete.

- Extend cutover rehearsal to require or report evidence for source freeze,
  final sync freshness, clean restored-target validation, clean OpenSearch
  check/repair, blob integrity, signed-auth smoke checks, shadow-read results,
  rollback readiness, and OpenCook maintenance state.
- Keep warnings explicit where OpenCook cannot enforce an external source Chef
  write freeze.
- Add tests for missing evidence, stale evidence, failed evidence, and
  successful evidence aggregation.
- Preserve existing cutover rehearsal command compatibility and JSON shape where
  possible; use additive fields for richer evidence.

Completion notes:

- Added additive `opencook admin migration cutover rehearse --source-frozen`
  and `--maintenance-result PATH` inputs so operators can attach source-freeze
  acknowledgement and `opencook admin maintenance status/check --json`
  evidence without breaking older rehearsal invocations.
- Cutover rehearsal now reports explicit dependencies for source-freeze
  acknowledgement, backup blob integrity, maintenance state, restored-target
  live validation, signed auth, signed blob reachability, source import
  progress, final source-sync cursor freshness, OpenSearch cleanliness,
  shadow-read evidence, rollback readiness, and a derived `cutover_evidence`
  readiness summary.
- Missing source-freeze, progress, search, shadow, maintenance, or rollback
  evidence remains advisory so existing scripts keep working, while stale
  source-sync evidence, failed search/shadow evidence, inactive maintenance
  evidence, and live target read failures remain blockers.
- Added focused tests for successful evidence aggregation, missing-evidence
  warnings, stale/failed evidence blockers, inactive maintenance evidence, and
  live restored-target read failures.

### Task 8: Prove Interruption, Retry, And Resume Behavior

Task status: complete.

- Add tests or functional phases that interrupt or simulate failure during
  backup inspect, restore preflight/apply, source import/sync apply, reindex,
  search repair, shadow compare, and cutover rehearsal.
- Prove retry behavior is safe and reports the same or intentionally updated
  progress state on rerun.
- Prove stale progress files, mismatched manifests, and wrong target DSNs fail
  clearly without mutating target state.
- Add operator-facing findings that distinguish retryable, unsafe-to-retry, and
  needs-manual-cleanup outcomes.

Completion notes:

- Migration outputs now append one additive retry guidance finding on failed
  commands: `migration_retry_safe`, `migration_retry_unsafe`, or
  `migration_manual_cleanup_required`.
- Retry guidance is based on the command side-effect profile and stable
  dependency/finding codes, not provider error bodies, so it remains useful
  without leaking DSNs, signed URLs, credentials, or provider internals.
- Focused tests now pin retry-safe backup inspect, source import blob/write
  failures, source sync write failures, shadow compare mismatches, cutover
  rehearsal read failures, wrong-target-DSN preflight failures, and unsafe
  source import progress/target mismatch handling.
- Restore apply failures now surface manual-cleanup guidance so operators rerun
  restore preflight and inspect offline target state before trusting a retry.
- Existing reindex and search repair tests continue to prove active maintenance
  gates, provider failure redaction, dry-run safety, and caller-owned
  maintenance state preservation for interrupted derived-state rebuilds.

### Task 9: Prove Blob Integrity And Provider Recovery Drills

Task status: complete.

- Add production-shaped blob validation for filesystem-backed functional tests
  and provider-interface tests for S3-compatible behavior without requiring a
  network S3 service.
- Cover referenced blobs, reachable blobs, copied blobs, missing blobs,
  checksum mismatches, candidate orphan blobs, sandbox-held blobs, and shared
  cookbook/artifact blobs.
- Prove signed cookbook and cookbook-artifact downloads still work after
  restore, reindex, and cutover rehearsal.
- Prove blob validation redacts provider URLs, signed query strings, response
  bodies, and credential-like values.

Completion notes:

- Production-scale filesystem blob validation now exercises the deterministic
  scale fixture through the same checksum-reference validator used by migration
  preflight and restore validation.
- Focused tests pin referenced, reachable, copied, content-verified, missing,
  checksum-mismatched, provider-unavailable, and candidate-orphan blob evidence.
- The shared checksum intentionally held by sandbox metadata, cookbook version
  files, and cookbook artifact files is now asserted as one aggregated provider
  check so shared references do not inflate reachability or cleanup signals.
- Scale cutover rehearsal now proves restored cookbook and cookbook-artifact
  payloads still expose usable signed blob URLs after the backup/source-progress,
  clean-search, clean-shadow, maintenance, source-freeze, and rollback evidence
  gates pass.
- S3-compatible provider-interface tests now pin transient HEAD recovery and
  error redaction without requiring a network S3 service.
- Blob validation and S3-compatible errors are now covered against provider
  URLs, signed query strings, response bodies, and credential-like values.

### Task 10: Prove OpenSearch Reindex, Check, And Repair At Scale

Task status: complete.

- Exercise OpenSearch startup rebuild, complete reindex, scoped reindex,
  consistency check, and repair against production-shaped fixtures.
- Cover search-after pagination, delete-by-query and fallback-delete provider
  modes, stale unsupported documents, stale supported documents, missing
  supported documents, and ACL-filtered search results after restore.
- Prove cookbook, cookbook-artifact, policy, policy-group, sandbox, and
  checksum families remain non-searchable except for documented node policy
  fields.
- Add functional coverage for reindex/search repair under active OpenCook
  maintenance mode.

Completion notes:

- The production-scale fixture now drives OpenSearch startup-style rebuild,
  complete org reindex, scoped node reindex, consistency check, dry-run repair,
  confirmed repair, and post-repair clean checks through the admin command
  path.
- The scale test runs against both direct delete-by-query and fallback-delete
  provider modes. Fallback mode seeds enough stale provider rows to force
  search-after pagination before deletion.
- Drift coverage now includes missing supported documents, stale supported
  documents, stale unsupported provider scopes, and non-searchable cookbook,
  cookbook-artifact, policy, policy-group, sandbox, and checksum documents.
- Repair coverage proves missing supported documents are upserted, stale
  supported and unsupported provider rows are deleted, unsupported indexes are
  not advertised in object counts, and unsupported families are not reintroduced
  by reindex or repair.
- The test uses the same active-maintenance admin command harness as operational
  reindex/search repair tests, keeping the maintenance-gated mutation contract
  pinned without changing Chef-facing search behavior.

### Task 11: Extend Functional Compose And Remote-Docker Coverage

Task status: complete.

- Add opt-in production-scale functional phases and document cleanup behavior.
- Ensure generated fixtures and artifacts live inside Compose-managed volumes
  and are removed unless `KEEP_STACK=1` or
  `OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` is set.
- Add clear success footers for every new targeted phase and for the aggregate
  scale flow.
- Keep remote Docker support intact by baking required scripts and fixtures into
  the functional image instead of relying on bind mounts.
- Keep the default flow fast unless the scale profile is explicitly enabled.

Implementation notes:

- Added `opencook admin migration scale-fixture create` as an additive
  local-only migration helper that writes deterministic production-scale
  normalized source bundles for the functional harness.
- Added opt-in Compose phases for `migration-scale-fixtures`,
  `migration-scale-backup`, `migration-scale-restore`,
  `migration-scale-reindex`, `migration-scale-shadow`,
  `migration-scale-rehearsal`, and `migration-scale-all`.
- Scale artifacts are rooted in the Compose-managed functional state volume and
  are cleaned by default; `KEEP_STACK=1` or
  `OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` preserves them for inspection.
- The functional image still bakes scripts and fixtures into `/src`, preserving
  remote Docker support without host bind mounts.

### Task 12: Add Operator Reports And Runbook Updates

Task status: complete.

- Update migration reports so operators can read inventory counts, validation
  findings, timings, retry guidance, source-freeze warnings, maintenance-window
  status, and rollback evidence without reading raw logs.
- Update `README.md`, `docs/functional-testing.md`, and
  `docs/chef-server-ctl-operational-runbooks.md` with the new scale validation
  workflows.
- Include explicit source Chef write-freeze guidance and emergency rollback
  steps.
- Include examples for small/medium/large scale profiles and remote Docker
  execution.

Implementation notes:

- Migration command output now includes an additive `operator_report` section
  with a compact summary, inventory totals, finding counts, dependency evidence,
  retry/source-freeze/maintenance/rollback guidance, and safe next steps.
- README, functional testing docs, and operational runbooks now document the
  scale fixture command, `migration-scale-all`, small/medium/large profiles,
  remote Docker examples, artifact retention, source Chef write-freeze
  expectations, and emergency rollback behavior.
- The embedded `opencook admin runbook show migration-cutover` catalog now
  points operators at the same production-scale drill and report summary.

### Task 13: Sync Docs And Close The Bucket

Task status: complete.

- Update `docs/chef-infra-server-rewrite-roadmap.md`.
- Update `docs/milestones.md`.
- Update `docs/compatibility-matrix-template.md`.
- Update `AGENTS.md`.
- Mark this bucket complete once implementation and functional coverage land.
- Point the next bucket at direct live upstream extraction or the highest-risk
  Chef compatibility gap discovered by production-scale validation.

Implementation notes:

- The roadmap, milestones, compatibility matrix, and agent guidance now mark
  production-scale migration validation and cutover readiness as complete for
  the current normalized OpenCook source/import path.
- The next recommended bucket is direct live Chef Infra Server source
  extraction beyond normalized artifacts, while still allowing
  deployment-test-discovered Chef compatibility gaps to interrupt if they prove
  higher risk.

## Test Plan

Focused tests:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional
```

Full verification:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```text
scripts/functional-compose.sh migration-scale-all
OPENCOOK_FUNCTIONAL_SCALE_PROFILE=medium scripts/functional-compose.sh migration-scale-all
scripts/functional-compose.sh all
```

Required scenarios:

- Production-shaped fixtures are deterministic and validate their own expected
  counts.
- Backup create/inspect covers every required payload and referenced blob.
- Restore preflight/apply detects missing or tampered payloads and blobs.
- Restart/rehydration happens before restored-target validation, reindex, and
  cutover rehearsal.
- Source import and sync are retry-safe and progress-aware.
- Shadow compare reports normalized diffs without leaking secrets or proxying
  writes.
- Cutover rehearsal reports source freeze, final sync freshness, maintenance
  state, search cleanliness, blob integrity, signed-auth smoke checks, and
  rollback readiness.
- OpenSearch reindex/check/repair works after restore and at larger result
  sizes.
- Signed cookbook and cookbook-artifact downloads work after restore.
- Missing blobs, checksum mismatches, stale search documents, and unsupported
  source families produce stable finding codes.
- Functional scale phases clean up generated artifacts by default and preserve
  them only when explicitly requested.

## Assumptions And Defaults

- Use deterministic generated fixtures instead of large checked-in fixture
  payloads where possible.
- Keep the default CI-friendly profile small and make heavier profiles opt-in.
- Use filesystem-backed blob storage for functional scale coverage. It is
  deterministic, local, and exercises a real non-memory provider path without
  network dependency.
- Use provider doubles or focused package tests for S3-compatible failure paths
  rather than adding a network S3 dependency to the functional stack.
- Keep source Chef direct live extraction out of scope. This bucket validates
  normalized artifacts and restored OpenCook targets first.
- Treat timing values as operator evidence, not compatibility pass/fail
  thresholds, unless a task explicitly defines a bounded timeout to prevent
  hangs.

## Non-Goals

- Changing Chef-facing API behavior.
- Adding direct live upstream PostgreSQL, OpenSearch, Redis, or oc-id
  extraction.
- Importing unsupported Chef object families.
- Treating OpenSearch provider documents as authoritative source data.
- Adding licensing, license enforcement, license telemetry, or
  license-management endpoints.
- Making heavy production-scale validation part of every default local test run
  unless it remains fast enough for normal development.
