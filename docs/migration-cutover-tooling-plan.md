# Migration And Cutover Tooling Plan

Status: planned

## Summary

This bucket adds the first migration, backup, restore, validation, and cutover-rehearsal tooling for moving real Chef Infra Server installations toward OpenCook. It builds on the completed PostgreSQL persistence, provider-backed blob, validator bootstrap, active OpenSearch, operational admin/reindex/repair, encrypted data bag, API-version, unsupported-search-index, and OpenSearch provider capability buckets.

The goal is operational confidence, not new Chef-facing API behavior. PostgreSQL remains the source of truth. Blob providers hold cookbook and sandbox checksum content. OpenSearch remains a derived index that can be rebuilt and repaired from PostgreSQL-backed state.

Use this file as the reference checklist for the migration/cutover tooling bucket.

## Current State

OpenCook already has:

- Durable PostgreSQL-backed state for users, organizations, clients, actor keys, groups, containers, ACLs, cookbooks, cookbook artifacts, nodes, environments, roles, data bags/items, policies, policy groups, sandboxes, checksum references, and object ACLs.
- Provider-backed blob storage with memory, filesystem, and S3-compatible modes for sandbox and cookbook content.
- Active OpenSearch-backed search for the implemented Chef-searchable indexes, with PostgreSQL-source-of-truth reindex/check/repair tooling.
- `opencook admin` signed HTTP workflows for live-safe inspection and management.
- Offline-gated direct PostgreSQL repair commands for a small set of unsafe administrative mutations.
- Functional Docker coverage for PostgreSQL, OpenSearch, filesystem blobs, restart/rehydration, admin operations, search consistency, encrypted-looking data bag items, and provider capability status.

Known operational gaps:

- No supported backup manifest or restore workflow for OpenCook state.
- No migration preflight report that summarizes PostgreSQL, blob, OpenSearch, auth/key, and configuration readiness in one place.
- No cutover rehearsal workflow that validates a restored or imported target before users point Chef/Cinc clients at it.
- No documented source-contract for importing from an existing Chef Infra Server installation.
- No migration runbook that clearly separates dry-run, backup, restore, validation, reindex, shadow-read, cutover, and rollback phases.

## Interfaces And Behavior

- Do not change Chef-facing routes, payloads, status codes, signed-auth semantics, or API-version behavior.
- Do not add licensing, license enforcement, license telemetry, or license-management endpoints.
- Keep PostgreSQL as the authoritative state for all persisted Chef objects.
- Treat OpenSearch as derived state. Backup/restore should validate or rebuild OpenSearch rather than preserve provider documents as source data.
- Treat blob content as authoritative external content that must be checksummed, reachable, and restorable independently of PostgreSQL rows.
- Keep destructive commands offline-gated unless they are only using existing live-safe Chef-facing APIs.
- Require explicit `--yes` for mutation and restore operations. Prefer `--dry-run` or preflight-only behavior by default.
- Keep admin command JSON output stable once introduced. Additive fields are acceptable during this planned bucket before consumers depend on them.
- Redact PostgreSQL DSNs, blob credentials, OpenSearch URLs with credentials, provider bodies, private keys, and secret-like config values in all summaries and errors.
- Do not mutate an upstream Chef Infra Server source. Source migration should read from backups, exports, or documented read-only APIs/artifacts.
- Do not import or invent behavior for unimplemented Chef surfaces. Report unsupported or deferred object families explicitly.

## State Families

In scope for validation, backup, restore, and rehearsal:

- bootstrap core state: users, organizations, clients, user/client keys, groups, containers, memberships, ACLs, server-admin membership
- core objects: nodes, environments, roles, data bags/items, policies, policy groups/assignments, sandboxes, checksum references, object ACLs
- cookbook state: cookbook versions, cookbook artifacts, manifests, metadata, file checksums
- blob content: sandbox and cookbook checksum objects in the configured blob backend
- search state: supported OpenSearch indexes as derived state for clients, environments, nodes, roles, and data bags
- operational config needed for restore/rehearsal: default organization, bootstrap requestor identity, PostgreSQL DSN, OpenSearch URL, blob backend, redacted provider settings

Out of scope for this bucket:

- direct mutation of a live Chef Infra Server source
- cross-process cache invalidation for online direct PostgreSQL mutations
- Redis, oc-id, telemetry, licensing, and service-supervisor parity
- conversion for Chef surfaces OpenCook has not implemented yet
- data bag secret management or encrypted data bag decryption
- live SQL-query-on-read redesigns for OpenCook runtime services

## Proposed Command Surface

The exact command names can be adjusted during implementation, but the first slice should keep the shape small and predictable:

```sh
opencook admin migration preflight [--org ORG|--all-orgs] [--json] [--with-timing]
opencook admin migration backup create --output PATH --offline --yes [--json] [--with-timing]
opencook admin migration backup inspect PATH [--json]
opencook admin migration restore preflight PATH --offline [--json] [--with-timing]
opencook admin migration restore apply PATH --offline --yes [--json] [--with-timing]
opencook admin migration cutover rehearse --manifest PATH [--server-url URL] [--json] [--with-timing]
```

Command principles:

- `preflight` reads the current OpenCook target and reports readiness without mutation.
- `backup create` emits a portable manifest plus state payloads and blob inventory from an offline OpenCook target.
- `backup inspect` reads a bundle without connecting to providers.
- `restore preflight` validates target safety, bundle integrity, provider availability, and expected mutations.
- `restore apply` mutates only an offline OpenCook target after preflight passes.
- `cutover rehearse` validates the restored target through live HTTP/admin/search checks before real client cutover.

## Task Breakdown

### Task 1: Create The Plan And Freeze Migration Boundaries

Status: planned.

- Add `docs/migration-cutover-tooling-plan.md`.
- Inventory upstream and local signals:
  - `chef-server-ctl` backup/restore expectations where available in the local Chef Server checkout
  - `oc-chef-pedant` route-contract expectations that must not change during migration work
  - existing OpenCook admin, reindex, search repair, blob, and PostgreSQL store seams
- Record in-scope state families and explicitly deferred surfaces.
- Freeze the rule that migration tooling must not reopen completed Chef-facing API contracts.
- Decide the initial command namespace and safety vocabulary.

### Task 2: Add Migration Command Scaffolding And Output Models

Status: planned.

- Add the `opencook admin migration` command group with help text and subcommand parsing.
- Add shared JSON output structures for:
  - dependency status
  - state inventory counts
  - validation findings
  - planned mutations
  - warnings
  - timing fields
- Preserve existing admin exit-code conventions:
  - success
  - usage error
  - not found
  - partial/drift detected
  - dependency unavailable
- Add focused CLI tests for command parsing, JSON shape, redaction, `--json`, `--with-timing`, `--dry-run`, `--offline`, and `--yes` behavior.

### Task 3: Implement Target Preflight Dependency Checks

Status: planned.

- Implement `opencook admin migration preflight` as a read-only target check.
- Validate and report:
  - PostgreSQL configured and reachable
  - PostgreSQL persistence active for the in-scope state families
  - blob backend configured and reachable enough for `HEAD`/existence checks
  - OpenSearch configured, discoverable, capability-compatible, and rebuildable where expected
  - default organization and bootstrap requestor settings
  - redacted runtime config summary
- Ensure provider and database errors are classified and redacted.
- Keep the command useful even when OpenSearch is absent by reporting search as memory/unconfigured rather than failing unrelated checks.

### Task 4: Add PostgreSQL State Inventory And Consistency Validation

Status: planned.

- Build a read-only inventory from PostgreSQL-backed OpenCook state.
- Report counts by organization and object family for:
  - users, organizations, clients, user/client keys
  - groups, containers, ACLs, memberships
  - nodes, environments, roles, data bags/items
  - policies, policy groups, policy assignments
  - sandboxes, checksum references
  - cookbook versions and cookbook artifacts
- Validate core invariants already required by OpenCook:
  - each organization has required default groups, containers, and ACLs
  - validator clients and keys are represented consistently
  - actor keys required for signed requests are loadable
  - object ACL rows exist for persisted objects that require them
  - checksum references point at known metadata rows
- Emit warnings for deferred or unsupported source families rather than silently ignoring them.

### Task 5: Add Blob Inventory And Checksum Validation

Status: planned.

- Build a blob validation pass from PostgreSQL checksum references.
- Check that referenced cookbook and sandbox checksum blobs exist in the configured provider.
- Validate available size/checksum metadata when the provider exposes enough information.
- Report:
  - referenced blobs
  - reachable blobs
  - missing blobs
  - provider-unavailable checks
  - candidate orphan blobs when the provider can list safely
- Do not delete blobs in this bucket unless a later task adds a separate, dry-run-first cleanup command.
- Cover filesystem-backed blobs in tests and use provider doubles for unavailable/error cases.

### Task 6: Integrate OpenSearch Derived-State Validation

Status: planned.

- Reuse the existing reindex/check/repair services for migration preflight.
- Report whether OpenSearch is:
  - unconfigured
  - configured but unavailable
  - active and clean
  - active with stale/missing derived documents
  - active with unsupported provider documents
- Preserve the unsupported public search-index contract for cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum state.
- Add command output that recommends exact follow-up repair commands without running them automatically.
- Keep OpenSearch validation non-authoritative: PostgreSQL state wins.

### Task 7: Define And Write The Backup Bundle Format

Status: planned.

- Define a versioned backup manifest that includes:
  - OpenCook backup format version
  - OpenCook build/version metadata
  - creation timestamp
  - redacted source config
  - state-family inventory counts
  - object payload file checksums
  - blob manifest checksums
  - compatibility warnings
- Choose a portable bundle layout, for example:
  - `manifest.json`
  - `postgres/bootstrap_core.json`
  - `postgres/core_objects.json`
  - `postgres/cookbooks.json`
  - `blobs/manifest.json`
  - `runbook-notes.md`
- Prefer logical OpenCook state exports over raw PostgreSQL dumps for the first implementation so tests can round-trip state without a `pg_dump` binary dependency.
- Document why OpenSearch provider documents are excluded and must be rebuilt.

### Task 8: Implement Offline Backup Creation And Inspection

Status: planned.

- Implement `opencook admin migration backup create`.
- Require PostgreSQL configuration, offline mode, and `--yes`.
- Export PostgreSQL-backed state through repository/store load methods rather than live API requests.
- Export blob references and optionally copy blob bytes when the backend supports local deterministic reads.
- Include dry-run-like preview data in the output before writing the bundle.
- Implement `backup inspect` to validate manifest integrity without connecting to PostgreSQL, blob, or OpenSearch providers.
- Add tests for bundle integrity, redaction, missing blob handling, provider-unavailable handling, and no private-key leakage.

### Task 9: Implement Restore Preflight And Empty-Target Safety

Status: planned.

- Implement `opencook admin migration restore preflight`.
- Require offline mode for direct PostgreSQL target inspection.
- Validate:
  - bundle format version compatibility
  - manifest checksums
  - target PostgreSQL reachability
  - target blob backend reachability
  - target is empty or explicitly allowed for overwrite
  - object counts and org names expected after restore
  - OpenSearch is either absent or safe to rebuild
- Default to refusing restore into non-empty targets.
- If an overwrite mode is later added, require separate explicit flags and tests for every destructive path.

### Task 10: Implement Restore Apply And Post-Restore Rebuild Hooks

Status: planned.

- Implement `opencook admin migration restore apply`.
- Require a successful preflight-equivalent check, `--offline`, and `--yes`.
- Restore PostgreSQL-backed state transactionally where possible.
- Restore or verify referenced blobs before making restored metadata visible.
- Never restore OpenSearch as authoritative data.
- After restore, recommend or optionally run existing `opencook admin reindex --complete` against the restored target.
- Add rollback-oriented failure tests proving partial restore does not leave mixed visible state when a write or blob copy fails.

### Task 11: Add Source Migration Inventory For Existing Chef Infra Server

Status: planned.

- Inventory practical read-only source paths from upstream Chef Infra Server:
  - `chef-server-ctl` backup artifacts
  - documented export APIs or generated JSON where available
  - pedant-visible object payloads
  - cookbook blob/bookshelf references
  - OpenSearch/Elasticsearch rebuild expectations
- Define the first supported import source contract for OpenCook.
- Report unsupported source artifacts and unimplemented object families explicitly.
- Do not directly mutate or depend on live upstream Chef Server databases in this bucket.
- Add parser/manifest tests using small fixtures before attempting broad import coverage.

### Task 12: Add Cutover Rehearsal And Shadow-Read Validation

Status: planned.

- Implement a rehearsal command or functional phase that validates a restored target through live OpenCook HTTP/admin flows.
- Cover:
  - `/status` and readiness
  - signed request authentication with restored user/client keys
  - org, client, group, container, and ACL reads
  - node, environment, role, data bag, policy, sandbox, cookbook, and cookbook-artifact representative reads
  - signed cookbook/artifact/blob download URLs where restored blob content exists
  - search queries after reindex
  - unsupported search-index behavior remains unchanged
- Document a shadow-read strategy for comparing source Chef responses to restored OpenCook responses without proxying writes.
- Keep any response-diff tooling advisory and contract-aware; do not require byte-for-byte matching where prior compatibility docs intentionally allow differences.

### Task 13: Extend Functional Docker Coverage

Status: planned.

- Add functional phases for:
  - migration preflight on the active stack
  - backup creation
  - backup inspection
  - restore preflight against a fresh target
  - restore apply
  - complete reindex after restore
  - cutover rehearsal checks
- Keep the default flow runtime reasonable; allow heavier restore/rehearsal phases to be opt-in if needed.
- Preserve remote Docker support.
- Ensure the functional harness proves PostgreSQL, filesystem-backed blobs, and OpenSearch survive backup/restore/reindex as expected.

### Task 14: Sync Docs And Close The Bucket

Status: planned.

- Update:
  - `README.md`
  - `AGENTS.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - this plan file
- Mark migration/cutover tooling complete for the first supported operational scope.
- Point the next recommended bucket at whichever remains highest risk after migration rehearsal:
  - deeper Chef object compatibility discovered by deployment testing
  - broader `chef-server-ctl` parity
  - metrics/health/service-management hardening
  - remaining core object edge cases
- Keep completed API-version, search-route, unsupported-index, encrypted-data-bag, provider capability, blob, and PostgreSQL-source-of-truth contracts visible as preserved boundaries.

## Test Plan

Focused verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/admin
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/blob
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```sh
scripts/functional-compose.sh
```

Future targeted functional phases:

```sh
KEEP_STACK=1 scripts/functional-compose.sh migration-preflight migration-backup
KEEP_STACK=1 scripts/functional-compose.sh migration-restore-preflight migration-restore migration-reindex migration-rehearsal
```

Required scenarios:

- read-only preflight succeeds on a healthy PostgreSQL plus blob plus OpenSearch stack
- preflight reports degraded or unavailable dependencies with redacted errors
- PostgreSQL inventory counts match persisted bootstrap/core/cookbook state
- blob validation catches missing referenced checksums without mutating metadata
- OpenSearch validation reports clean, stale, missing, unsupported, and unavailable states
- backup creation writes a versioned manifest and state payloads without leaking secrets
- backup inspection catches checksum and format errors without provider connections
- restore preflight refuses non-empty targets by default
- restore apply is offline-gated and rollback-safe on write/blob failures
- restored keys authenticate signed requests after restart
- restored cookbook/artifact blob downloads work when blob content is present
- complete reindex after restore produces clean search consistency
- cutover rehearsal preserves completed Chef-facing compatibility contracts

## Assumptions

- The first implementation can focus on OpenCook-to-OpenCook backup/restore plus source-migration inventory before attempting a broad live Chef Server importer.
- Logical state bundles are preferred over raw PostgreSQL dumps for the first slice because they are testable, portable, and align with the existing store load/save seams.
- OpenSearch data is derived and should be rebuilt, not backed up as authoritative state.
- Blob provider behavior should be validated through existing provider interfaces and provider doubles; no network S3 integration is required by default.
- Existing encrypted-looking data bag payloads remain opaque JSON. Migration tooling must not require data bag secrets.
- Direct PostgreSQL restore remains offline-only until the project has online cache invalidation or a live API-based restore path.
- Migration tooling must document unsupported source artifacts rather than silently dropping them.

## Completion Criteria

- A versioned migration/cutover plan exists and stays updated through the bucket.
- `opencook admin migration preflight` provides a redacted, read-only operational readiness report.
- Backup create/inspect and restore preflight/apply flows are implemented with offline/destructive safety gates.
- PostgreSQL, blob, and OpenSearch validation are covered by focused tests.
- Functional Docker coverage exercises the happy path for backup, restore, reindex, and rehearsal.
- Docs explain the supported first migration path, rollback expectations, and source-contract limits.
- Completed Chef-facing contracts remain unchanged.
