# Migration And Cutover Tooling Plan

Status: complete

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

This bucket now provides:

- redacted migration preflight reporting for PostgreSQL, blob, OpenSearch, auth/key, configuration, and persisted object-family readiness
- versioned logical backup create/inspect for OpenCook PostgreSQL-backed state plus reachable provider-backed blob bytes
- offline restore preflight/apply with empty-target safety, destructive confirmation, rollback-oriented failure handling, and post-restore reindex guidance
- read-only source artifact inventory for existing Chef Infra Server backup/export artifacts, with unsupported/deferred families reported explicitly
- cutover rehearsal against a restored live OpenCook target using signed read-only checks, signed cookbook/artifact download validation, and shadow-read advisory output
- Docker functional phases that prove backup, restore, restored-target reindex, and cutover rehearsal on a PostgreSQL plus OpenSearch plus filesystem-blob stack

Remaining operational gaps after this bucket:

- no live mutation or online import/sync against an upstream Chef Infra Server source
- no cross-process cache invalidation for online direct PostgreSQL mutation
- no service-supervisor parity, metrics endpoint, or broad `chef-server-ctl` command coverage beyond the current `opencook admin` surface
- no conversion for Chef surfaces OpenCook has not implemented yet

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

Task 1 freezes the first command namespace as `opencook admin migration`. Later tasks can add flags or additive JSON fields as the implementation hardens, but they should not rename the command group or move these flows out of the admin namespace without a new compatibility review.

```sh
opencook admin migration preflight [--org ORG|--all-orgs] [--json] [--with-timing]
opencook admin migration backup create --output PATH --offline [--dry-run|--yes] [--json] [--with-timing]
opencook admin migration backup inspect PATH [--json]
opencook admin migration restore preflight PATH --offline [--json] [--with-timing]
opencook admin migration restore apply PATH --offline [--dry-run|--yes] [--json] [--with-timing]
opencook admin migration source inventory PATH [--json] [--with-timing]
opencook admin migration cutover rehearse --manifest PATH [--server-url URL] [--requestor-name NAME] [--private-key PATH] [--json] [--with-timing]
```

Command principles:

- `preflight` reads the current OpenCook target and reports readiness without mutation.
- `backup create` emits a portable manifest plus state payloads and blob inventory from an offline OpenCook target.
- `backup inspect` reads a bundle without connecting to providers.
- `restore preflight` validates target safety, bundle integrity, provider availability, and expected mutations.
- `restore apply` mutates only an offline OpenCook target after preflight passes.
- `source inventory` inventories read-only Chef Server backup/export artifacts without importing them.
- `cutover rehearse` validates the restored target through live HTTP/admin/search checks before real client cutover.

## Task 1 Boundary Snapshot

Status: complete.

Task 1 freezes this bucket around operational tooling only. The remaining tasks may add CLI commands, manifests, validation passes, backup/restore mechanics, and rehearsal checks, but they must not reopen completed Chef-facing route behavior or compatibility payloads.

Upstream and compatibility signals:

- `/Users/oberon/Projects/coding/ruby/chef-server/src/chef-server-ctl/plugins/backup.rb` exposes backup and restore as operational `chef-server-ctl` commands. Backup requires an explicit `--yes` acknowledgment for tar-based offline behavior, and restore has a separate destructive cleanse acknowledgment; OpenCook should keep equally explicit mutation gates.
- `/Users/oberon/Projects/coding/ruby/chef-server/src/chef-server-ctl/chef-server-ctl.gemspec` and `/Users/oberon/Projects/coding/ruby/chef-server/src/chef-server-ctl/Gemfile` include `chef_backup` and `knife-ec-backup`, so the first Chef Server source work should inspect supported archive/export artifacts before attempting any direct source integration.
- `/Users/oberon/Projects/coding/ruby/chef-server/dev-docs/BOOKSHELF.md` shows cookbook and sandbox blob bytes can live outside the main database path through Bookshelf or S3-style storage. Migration inventory must therefore treat blob checksums and blob content as a separate source family from PostgreSQL rows.
- `/Users/oberon/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md` describes search documents as expanded objects written to a provider. OpenCook should preserve the current PostgreSQL-source-of-truth model and rebuild or validate OpenSearch rather than backing provider documents up as authoritative state.
- `/Users/oberon/Projects/coding/ruby/chef-server/src/oc_erchef/priv/reindex-opc-organization` treats `drop`, `reindex`, and `complete` as administrative operations that validate organizations before touching the index. OpenCook migration commands should reuse the existing `admin reindex` and `admin search` patterns instead of inventing a second indexing model.
- `/Users/oberon/Projects/coding/ruby/chef-server/src/nginx/habitat/config/routes.lua` and `/Users/oberon/Projects/coding/ruby/chef-server/oc-chef-pedant/` remain route and wire-contract references. Migration tooling must not change route resolution, default-org aliases, `_acl` precedence, signed-auth behavior, API-version semantics, status codes, or response bodies for Chef-facing endpoints.

Local OpenCook implementation seams:

- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/command.go` defines the admin exit-code vocabulary: success, usage error, not found, partial/drift detected, and dependency unavailable.
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_offline.go` is the current direct PostgreSQL safety pattern. Offline mutations require `--offline --yes`, offline reads require `--offline`, and all offline results include the restart note that direct PostgreSQL changes are not visible to running OpenCook servers until restart.
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_reindex.go` and `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_search.go` provide the live operational model for `--dry-run`, `--yes`, `--json`, `--with-timing`, drift-as-partial exit behavior, and provider-unavailable classification.
- `/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg` exposes the current PostgreSQL load/save seams for bootstrap core, core objects, and cookbooks. The first logical backup format should use those seams rather than raw table dumps.
- `/Users/oberon/Projects/coding/go/OpenCook/internal/blob/store.go` freezes the provider families to memory, filesystem, and S3-compatible blob stores. Migration validation should use the existing blob interfaces and provider doubles instead of adding a new blob abstraction.
- `/Users/oberon/Projects/coding/go/OpenCook/internal/config/config.go` already centralizes runtime configuration and redaction. Migration summaries and errors must use that redaction boundary for DSNs, URLs with credentials, blob credentials, private keys, and secret-like paths.
- `/Users/oberon/Projects/coding/go/OpenCook/internal/search/reindex.go` and `/Users/oberon/Projects/coding/go/OpenCook/internal/search/consistency.go` already build derived OpenSearch documents from PostgreSQL-backed bootstrap state. Migration preflight and rehearsal should call or mirror these services rather than treating OpenSearch as source data.

Frozen migration boundaries:

- The first supported end-to-end path is OpenCook-to-OpenCook logical backup, restore, reindex, and cutover rehearsal.
- Existing Chef Infra Server import work starts as read-only source inventory against backup/export artifacts and documented upstream signals. Direct mutation of a live Chef Server source is out of scope.
- OpenSearch is always derived state. Backup bundles should record search capability and consistency summaries, not provider documents as source of truth.
- Blob content is authoritative external content. Backup and restore must validate checksum reachability and preserve the blob/source separation from PostgreSQL metadata.
- Direct PostgreSQL restore and repair remain offline-only until OpenCook has a separate online cache invalidation or API-based restore design.
- Deferred or unimplemented Chef surfaces must be reported explicitly as unsupported or deferred, never silently dropped or approximated.
- Completed Chef-facing contracts, including API-version-specific payloads, unsupported search-index behavior, encrypted-looking data bag opacity, and default-org route semantics, are preserved boundaries for this bucket.

Initial safety vocabulary:

- `preflight` means read-only validation of configuration, dependencies, inventory, and compatibility warnings.
- `dry-run` means planned mutation reporting without provider or database writes.
- `inspect` means local bundle parsing and checksum verification without provider connections.
- `offline` means direct PostgreSQL access is allowed only while OpenCook server processes are stopped.
- `yes` means explicit operator confirmation for mutation or restore operations; it is never implied by `--json`.
- `apply` means a mutation command that must be offline-gated and preflight-equivalent before writing.
- `rehearse` means live target validation through HTTP/admin/search checks after restore, before clients are cut over.
- `partial` means drift, warnings, or missing derived state were detected and the command should use the existing partial/drift exit-code convention.

## Task Breakdown

### Task 1: Create The Plan And Freeze Migration Boundaries

Status: complete.

- Added `docs/migration-cutover-tooling-plan.md`.
- Inventoried upstream and local signals:
  - `chef-server-ctl` backup/restore expectations where available in the local Chef Server checkout
  - `oc-chef-pedant` route-contract expectations that must not change during migration work
  - existing OpenCook admin, reindex, search repair, blob, and PostgreSQL store seams
- Recorded in-scope state families and explicitly deferred surfaces.
- Froze the rule that migration tooling must not reopen completed Chef-facing API contracts.
- Decided the initial command namespace and safety vocabulary in the Task 1 boundary snapshot above.

### Task 2: Add Migration Command Scaffolding And Output Models

Status: complete.

- Added the `opencook admin migration` command group with help text and subcommand parsing.
- Added shared JSON output structures for:
  - dependency status
  - state inventory counts
  - validation findings
  - planned mutations
  - warnings
  - timing fields
- Preserved existing admin exit-code conventions:
  - success
  - usage error
  - not found
  - partial/drift detected
  - dependency unavailable
- Added focused CLI tests for command parsing, JSON shape, redaction, `--json`, `--with-timing`, `--dry-run`, `--offline`, and `--yes` behavior.

### Task 3: Implement Target Preflight Dependency Checks

Status: complete.

- Implemented `opencook admin migration preflight` as a read-only target check.
- Validates and reports:
  - PostgreSQL configured and reachable
  - PostgreSQL persistence active for the in-scope state families
  - blob backend configured and reachable enough for `HEAD`/existence checks
  - OpenSearch configured, discoverable, capability-compatible, and rebuildable where expected
  - default organization and bootstrap requestor settings
  - redacted runtime config summary
- Ensures provider and database errors are classified and redacted.
- Keeps the command useful when OpenSearch is absent by reporting search as memory/unconfigured rather than failing unrelated checks.

### Task 4: Add PostgreSQL State Inventory And Consistency Validation

Status: complete.

- Implemented read-only PostgreSQL-backed inventory in `opencook admin migration preflight`.
- Reports counts by organization and object family for:
  - users, organizations, clients, user/client keys
  - groups, containers, ACLs, memberships
  - nodes, environments, roles, data bags/items
  - policies, policy groups, policy assignments
  - sandboxes, checksum references
  - cookbook versions and cookbook artifacts
- Validates core invariants already required by OpenCook:
  - each organization has required default groups, containers, and ACLs
  - validator clients and keys are represented consistently
  - actor keys required for signed requests are loadable
  - object ACL rows exist for persisted objects that require them
  - sandbox checksum references are syntactically valid metadata references
- Emits warnings for deferred or unsupported source families rather than silently ignoring them.
- Keeps provider/blob content reads deferred to Task 5, so this pass remains PostgreSQL-only after the existing dependency probes.

### Task 5: Add Blob Inventory And Checksum Validation

Status: complete.

- Implemented a blob validation pass from PostgreSQL-backed checksum references.
- Checks that referenced cookbook and sandbox checksum blobs exist in the configured provider.
- Validates local content checksums for filesystem and memory-compatible backends where full reads are deterministic and safe.
- Reports:
  - referenced blobs
  - reachable blobs
  - missing blobs
  - provider-unavailable checks
  - locally content-verified blobs
  - checksum mismatches
  - candidate orphan blobs when the provider can list safely
- Does not delete blobs; candidate orphan output is reporting-only until a later dry-run-first cleanup command exists.
- Covers filesystem-backed blobs in tests and uses provider doubles for unavailable/error cases.

### Task 6: Integrate OpenSearch Derived-State Validation

Status: complete.

- Reuses the existing reindex/check/repair consistency service from migration preflight.
- Reports whether OpenSearch is:
  - unconfigured
  - configured but unavailable
  - active and clean
  - active with stale/missing derived documents
  - active with unsupported provider documents
- Preserves the unsupported public search-index contract for cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum state.
- Adds command output that recommends exact follow-up `opencook admin search repair ...` commands without running them automatically.
- Keeps OpenSearch validation non-authoritative: PostgreSQL state wins and drift is reported as warnings unless the provider is unavailable.

### Task 7: Define And Write The Backup Bundle Format

Status: complete.

- Defined and tested a versioned backup manifest that includes:
  - OpenCook backup format version
  - OpenCook build/version metadata
  - creation timestamp
  - redacted source config
  - state-family inventory counts
  - object payload file checksums
  - blob manifest checksums
  - compatibility warnings
- Implemented the portable bundle layout:
  - `manifest.json`
  - `postgres/bootstrap_core.json`
  - `postgres/core_objects.json`
  - `postgres/cookbooks.json`
  - `blobs/manifest.json`
  - `runbook-notes.md`
- Uses logical OpenCook state exports over raw PostgreSQL dumps so tests can round-trip state without a `pg_dump` binary dependency.
- Documents why OpenSearch provider documents are excluded and must be rebuilt.
- Provides the format consumed by Task 8's offline backup creation and provider-free bundle inspection commands.

### Task 8: Implement Offline Backup Creation And Inspection

Status: complete.

- Implemented `opencook admin migration backup create`.
- Requires PostgreSQL configuration, offline mode, and either `--dry-run` for preview or `--yes` for bundle writing.
- Exports PostgreSQL-backed bootstrap core, core object, and cookbook state through offline store/repository load methods rather than live API requests.
- Exports blob references and copies blob bytes for deterministic local-readable backends such as filesystem and memory-compatible stores.
- Emits dependency, inventory, finding, and planned-mutation preview data before any write, and refuses to write when PostgreSQL or blob validation fails.
- Implemented `backup inspect` to validate manifest integrity without connecting to PostgreSQL, blob, or OpenSearch providers.
- Added tests for bundle integrity, manifest tamper detection, redaction, missing blob handling, provider-unavailable handling, and no private-key leakage.

### Task 9: Implement Restore Preflight And Empty-Target Safety

Status: complete.

- Implemented `opencook admin migration restore preflight`.
- Requires offline mode for direct PostgreSQL target inspection.
- Validates:
  - bundle format version compatibility
  - manifest checksums
  - backup blob-manifest readability
  - target PostgreSQL reachability
  - target blob backend reachability
  - target PostgreSQL-backed state is empty
  - object counts and organization names expected after restore
  - OpenSearch is either absent or discoverable enough to rebuild
- Defaults to refusing restore into non-empty targets.
- Keeps overwrite mode deferred; adding it later still requires separate explicit flags and tests for every destructive path.

### Task 10: Implement Restore Apply And Post-Restore Rebuild Hooks

Status: complete.

- Implemented `opencook admin migration restore apply`.
- Requires a successful preflight-equivalent check plus either `--dry-run` or confirmed `--offline --yes`.
- Restores or verifies referenced blob content before making restored PostgreSQL metadata visible.
- Restores PostgreSQL-backed bootstrap core, core object, and cookbook metadata through offline store/repository seams.
- Rolls bootstrap/core rows back where possible if a later metadata-family write fails.
- Never restores OpenSearch as authoritative data.
- Emits the post-restore recommendation to run `opencook admin reindex --all-orgs --complete`.
- Added rollback-oriented tests proving dry-runs do not mutate, blob-copy failures do not save metadata, and core-object write failures roll bootstrap state back.

### Task 11: Add Source Migration Inventory For Existing Chef Infra Server

Status: complete.

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

Completion notes:

- Added `opencook admin migration source inventory PATH`.
- Defined the first generated-source manifest contract as `opencook.migration.source_inventory.v1`.
- Supports read-only inventory of source manifests, extracted artifact directories, and tar/tar.gz backup archives.
- Reports cookbook Bookshelf/blob references, generated JSON object-family counts, derived OpenSearch/Elasticsearch rebuild expectations, deferred database artifacts, and unsupported ancillary source families.
- Keeps source import explicitly deferred and does not connect to or mutate live Chef Server databases.

### Task 12: Add Cutover Rehearsal And Shadow-Read Validation

Status: complete.

- Implemented a rehearsal command that validates a restored target through live OpenCook HTTP/admin flows.
- Covers:
  - `/status` and readiness
  - signed request authentication with restored user/client keys
  - org, client, group, container, and ACL reads
  - node, environment, role, data bag, policy, sandbox, cookbook, and cookbook-artifact representative reads
  - signed cookbook/artifact/blob download URLs where restored blob content exists
  - search queries after reindex
  - unsupported search-index behavior remains unchanged
- Documented a shadow-read strategy for comparing source Chef responses to restored OpenCook responses without proxying writes.
- Keeps response-diff tooling advisory and contract-aware; it does not require byte-for-byte matching where prior compatibility docs intentionally allow differences.

Completion notes:

- `opencook admin migration cutover rehearse --manifest PATH` now accepts either a backup directory or its `manifest.json`.
- Rehearsal validates the backup manifest, loads representative restored state from the bundle, constructs a signed admin client, and executes read-only live checks against the restored OpenCook target.
- Live checks include `_status`, `readyz`, `server_api_version`, restored users/keys/ACLs, organization/bootstrap objects, core objects, cookbooks/artifacts, search routes where restored searchable objects exist, and signed blob downloads when copied checksum bytes are available in the bundle.
- Rehearsal output adds `rehearsal_checks`, `rehearsal_passed`, `rehearsal_failed`, `rehearsal_skipped`, and `rehearsal_downloads` inventory counters for cutover gates.
- The shadow-read strategy is advisory and read-only:
  - collect a small representative source response set from Chef Infra Server using `GET`/`HEAD` only
  - collect the matching restored OpenCook responses with the same actor and API version where possible
  - normalize documented compatibility differences such as response ordering, generated timestamps, signed URL query strings, private key omission, and allowed API-version shape differences
  - treat missing objects, auth failures, malformed payloads, missing blob downloads, and search/document-count mismatches as blockers
  - never proxy writes, validator registration, sandbox commit, cookbook upload, or client mutation through shadow-read tooling during cutover rehearsal

### Task 13: Extend Functional Docker Coverage

Status: complete.

- Added functional phases for:
  - migration preflight on the active stack
  - backup creation
  - backup inspection
  - restore preflight against a fresh target
  - restore apply
  - complete reindex after restore
  - cutover rehearsal checks
- Keeps the default flow runtime reasonable by running preflight plus backup create/inspect in the default flow while leaving the heavier restore/reindex/rehearsal drill opt-in.
- Preserves remote Docker support by continuing to use Compose-managed volumes instead of bind mounts.
- Ensures the functional harness proves PostgreSQL, filesystem-backed blobs, and OpenSearch survive backup/restore/reindex as expected.

Completion notes:

- Added `scripts/run-migration-functional-tests-in-container.sh`.
- Added migration phases: `migration-preflight`, `migration-backup`, `migration-backup-inspect`, `migration-restore-preflight`, `migration-restore`, `migration-reindex`, `migration-rehearsal`, and `migration-all`.
- Mounted source filesystem blobs into the functional test container read-only so backup creation can copy real provider-backed blob bytes.
- Added a harness-managed fresh restore database plus separate restore blob directory for restore preflight/apply.
- Added a temporary restored OpenCook server for cutover rehearsal against restored PostgreSQL/blob state.
- Installed `postgresql-client` in the functional test image so the harness can reset the restore database from inside the shared Docker network.
- Made post-restore reindex and rehearsal phases self-heal when the harness restore database is missing or empty by reapplying the existing backup bundle first.

### Task 14: Sync Docs And Close The Bucket

Status: complete.

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

Completion notes:

- Updated `README.md`, `AGENTS.md`, `docs/chef-infra-server-rewrite-roadmap.md`, `docs/milestones.md`, `docs/compatibility-matrix-template.md`, `docs/functional-testing.md`, and this plan file.
- Marked the first migration/cutover tooling bucket complete for OpenCook-to-OpenCook logical backup/restore, source artifact inventory, restored-target reindex, and cutover rehearsal.
- Pointed the next recommended bucket at broader `chef-server-ctl`-style operational parity plus health, metrics, and service-management hardening, with deployment-test-discovered Chef compatibility gaps still allowed to interrupt if they prove higher risk.
- Preserved completed API-version, search-route, unsupported-index, encrypted-data-bag, provider capability, blob, migration/cutover, and PostgreSQL-source-of-truth contracts as boundaries for the next slice.

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

Targeted migration functional phases:

```sh
KEEP_STACK=1 scripts/functional-compose.sh migration-preflight migration-backup
KEEP_STACK=1 scripts/functional-compose.sh migration-restore-preflight migration-restore migration-reindex migration-rehearsal
KEEP_STACK=1 scripts/functional-compose.sh migration-all
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
