# Direct Live Chef Source Extraction Plan

Status: complete

## Summary

This bucket added read-only extraction from live Chef Infra Server source state
into OpenCook's existing normalized source bundle contract. It comes after the
production-scale migration validation bucket: the scale drill proved that
normalized source bundles can be imported, synced, backed up, restored,
reindexed, shadow-read, and rehearsed safely. This bucket replaces the
manual/exported-source input step with direct live-source extractors while
preserving the rest of that validated pipeline.

The goal is operational migration confidence, not new Chef-facing API behavior.
OpenCook must remain compatibility-first: Chef and Cinc clients, `knife`,
signed request behavior, route payloads, status codes, API-version semantics,
ACL behavior, provider-backed blobs, OpenSearch-derived search, and the
licensing-free stance must keep the contracts already pinned by earlier
buckets.

Use this file as the reference checklist for this bucket.

## Current State

OpenCook already has:

- PostgreSQL-backed persistence for implemented identity, authorization, core
  object, cookbook, sandbox/checksum, policy, and ACL state.
- Provider-backed blobs in memory, filesystem, and S3-compatible modes.
- Active OpenSearch-backed search for implemented Chef-searchable families.
- OpenCook logical backup create/inspect, offline restore preflight/apply,
  restored-target reindex, and cutover rehearsal.
- Normalized Chef Server source artifact inventory, normalize, offline import,
  source sync, shadow-read comparison, and source cutover rehearsal.
- Direct read-only live Chef source preflight/extract commands that publish the
  same normalized bundle contract for implemented PostgreSQL-backed Chef
  families and checksum blob evidence.
- Production-scale deterministic fixtures, scale-aware functional phases,
  `operator_report` summaries, rollback guidance, and remote Docker workflows.
- Maintenance-mode write blocking for OpenCook targets, PostgreSQL-shared
  maintenance state, maintenance-gated reindex/search repair, and a narrow
  online default ACL repair path.

Closed outcome:

- Operators can now use prepared normalized artifacts or direct live-source
  extraction as inputs to the same import/sync/reindex/shadow/cutover pipeline.
- Live extraction connects read-only to Chef source PostgreSQL, can copy or
  reference checksum blobs from a local Bookshelf-style source, and can record
  optional read-only Chef HTTP evidence without proxying writes.
- Source extraction reports capability gaps, unsupported ancillary families,
  source-write-freeze evidence, cursor freshness metadata, redacted dependency
  diagnostics, and retry-safe bundle publication status.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, payloads, status codes, signed-auth
  behavior, API-version behavior, ACL semantics, or established compatibility
  responses.
- Do not add licensing, license enforcement, license telemetry, or
  license-management endpoints.
- Do not mutate the upstream Chef Infra Server source. Live-source integrations
  are read-only and must not proxy writes.
- Keep source Chef write-freeze control external to OpenCook. OpenCook can
  document and validate operator-provided evidence, but it must not pretend it
  can freeze upstream Chef writes without an explicit future integration.
- Keep the existing normalized source bundle format
  `opencook.migration.chef_source.v1` as the handoff into import/sync. Live
  extraction should write that bundle instead of introducing a second import
  format.
- Keep PostgreSQL as the authoritative restored/imported OpenCook state.
- Treat upstream OpenSearch/Elasticsearch documents as derived advisory data,
  not source of truth. Rebuild and check OpenCook OpenSearch from PostgreSQL
  after import/sync.
- Treat blob providers as checksum-addressed content stores. Extraction should
  verify reachability and copy or reference checksum bytes without leaking
  signed URLs, provider response bodies, credentials, or local secret paths.
- Preserve existing `opencook admin migration` command shapes. Add live-source
  commands under the existing `source` namespace rather than inventing a second
  migration CLI.
- Keep destructive commands explicit with `--yes`; keep preflight/dry-run
  behavior safe by default. Live extraction writes only local normalized bundle
  output, not source Chef or target OpenCook state.
- Redact source PostgreSQL DSNs, blob credentials, source Chef URLs with
  credentials, private key paths, signed URL query strings, request signatures,
  provider response bodies, and secret-like local paths in all output.
- Do not import object families OpenCook has not implemented. Report them as
  unsupported or deferred with stable finding codes.

## Proposed Command Surface

This bucket should add an additive live-source command surface while preserving
the current artifact-oriented commands:

```sh
opencook admin migration source live preflight \
  --source-postgres-dsn DSN \
  [--source-bookshelf-root PATH|--source-blob-url URL] \
  [--source-server-url URL --source-requestor-name NAME --source-private-key PATH] \
  [--org ORG|--all-orgs] [--json] [--with-timing]

opencook admin migration source live extract \
  --source-postgres-dsn DSN \
  --output PATH \
  [--source-bookshelf-root PATH|--source-blob-url URL] \
  [--source-server-url URL --source-requestor-name NAME --source-private-key PATH] \
  [--org ORG|--all-orgs] [--copy-blobs|--reference-blobs] \
  [--dry-run] [--yes] [--json] [--with-timing]
```

Command principles:

- `source live preflight` connects read-only to the source and reports
  capabilities, visible organizations, implemented source families, blob
  reachability mode, unsupported/deferred families, and safety warnings.
- `source live extract` writes a normalized source bundle atomically to
  `--output`, including manifest hashes, inventory counts, extraction metadata,
  and optional copied checksum blobs.
- `--copy-blobs` should copy checksum bytes into the normalized bundle when the
  configured source blob adapter supports deterministic reads.
- `--reference-blobs` should record checksum references and provider capability
  evidence when copying is unavailable or intentionally deferred, leaving target
  import/sync to validate target-side blob reachability.
- `--source-server-url` plus source request credentials are optional and
  read-only. They should support live read probes or shadow-read evidence where
  useful, but direct source extraction must not depend on HTTP writes.
- Existing commands stay the main handoff:
  - `opencook admin migration source import preflight/apply`
  - `opencook admin migration source sync preflight/apply`
  - `opencook admin migration shadow compare`
  - `opencook admin migration cutover rehearse`

Frozen downstream command inventory:

- `opencook admin migration source inventory PATH`
- `opencook admin migration source normalize PATH --output PATH`
- `opencook admin migration source import preflight PATH --offline`
- `opencook admin migration source import apply PATH --offline`
- `opencook admin migration source sync preflight PATH --offline`
- `opencook admin migration source sync apply PATH --offline`
- `opencook admin migration backup create --output PATH --offline`
- `opencook admin migration backup inspect PATH`
- `opencook admin migration restore preflight PATH --offline`
- `opencook admin migration restore apply PATH --offline`
- `opencook admin reindex --all-orgs --complete`
- `opencook admin search check --all-orgs`
- `opencook admin search repair --all-orgs`
- `opencook admin migration shadow compare --source PATH`
- `opencook admin migration cutover rehearse --manifest PATH`

Live extraction must feed these commands by producing a normal
`opencook.migration.chef_source.v1` bundle. Later tasks may add evidence fields
or metadata checks, but should not create a parallel import/sync/cutover path.

## Live Source Contract

Live extraction should produce a normalized bundle with the current manifest
format plus additive source metadata:

- `format_version`: `opencook.migration.chef_source.v1`
- `source_type`: `live_chef_infra_server`
- `source_metadata`:
  - redacted source PostgreSQL host/database identity
  - extraction started/completed timestamps
  - selected organizations or `all_organizations`
  - extractor version
  - source capability flags
  - source write-freeze evidence status when provided
- `payloads`: existing normalized payload entries with counts and SHA-256
  hashes.
- `artifacts`: existing blob/checksum/unsupported artifact entries.
- `notes`: operator-visible non-secret extraction notes.

Supported source families should initially match the implemented normalized
source/import surface:

- Global families: `users`, `user_acls`, `user_keys`,
  `server_admin_memberships`, `organizations`.
- Organization-scoped bootstrap families: `clients`, `client_keys`, `groups`,
  `group_memberships`, `containers`, `acls`.
- Organization-scoped core object families: `nodes`, `environments`, `roles`,
  `data_bags`, `data_bag_items`, `policy_revisions`, `policy_groups`,
  `policy_assignments`, `sandboxes`, `checksum_references`, object `acls`.
- Cookbook families: `cookbook_versions`, `cookbook_artifacts`,
  `referenced_blobs`.

Deferred live source families should be reported, not dropped silently:

- oc-id/OAuth integration state.
- Redis/session/supervisor data.
- Licensing, license telemetry, and license-management data.
- Any upstream family for a Chef-facing surface OpenCook has not implemented.
- Upstream OpenSearch/Elasticsearch provider documents as authoritative source
  data.

## Task Breakdown

### Task 1: Create This Plan And Freeze The Live-Source Boundary

Task status: complete.

- Add `docs/live-chef-source-extraction-plan.md`.
- Record the read-only source boundary, preserved Chef-facing contracts, and
  normalized-bundle handoff.
- Inventory current source/import/sync/cutover commands that live extraction
  must reuse.
- Mark direct source mutation, upstream OpenSearch import-as-truth, Redis,
  oc-id, licensing, and unimplemented Chef surfaces as out of scope.

Implementation notes:

- The bucket is frozen around additive read-only extraction from a live Chef
  Infra Server source into the existing normalized source bundle contract.
- The live-source command namespace is `opencook admin migration source live`;
  it should add `preflight` and `extract` without moving existing source,
  backup, restore, reindex, shadow-read, or cutover commands.
- The source side is read-only. This bucket must not write to source
  PostgreSQL, proxy Chef API writes, mutate Bookshelf/S3 checksum content, or
  claim to freeze upstream Chef writes.
- The OpenCook target side still uses the existing offline import/sync and
  restore safety model. Live extraction writes only local bundle artifacts.
- The normalized bundle handoff remains
  `opencook.migration.chef_source.v1`; any source metadata must be additive.
- Upstream OpenSearch/Elasticsearch documents, Redis, oc-id, licensing,
  telemetry, service-supervisor state, and unimplemented Chef-facing surfaces
  remain deferred and must be reported with stable findings instead of silently
  imported.

### Task 2: Inventory Upstream Live Source Contracts

Task status: complete.

- Inspect the local Chef Server checkout for source-of-truth schemas and
  backup/export behavior:
  - `src/oc_erchef`
  - `src/oc_bifrost`
  - `src/bookshelf`
  - `dev-docs/BOOKSHELF.md`
  - `dev-docs/SEARCH_AND_INDEXING.md`
  - `chef-server-ctl` backup/restore code
- Map upstream tables/resources to the existing normalized source families.
- Identify source version/capability probes that can be tested without
  mutation.
- Document schema ambiguities and any family that must remain deferred.
- Add tests around the mapping inventory so future extractor work does not drift
  from the frozen family names.

Implementation notes:

- Added `adminMigrationLiveSourceFamilyMappings()` as the frozen code inventory
  for live extraction. It covers every current global, organization-scoped, and
  blob validation family, plus a derived-only OpenSearch mapping so future
  source code does not accidentally treat search documents as source of truth.
- Added `adminMigrationLiveSourceDeferredFamilies()` for upstream state that
  must be reported explicitly instead of imported: oc-id, Redis/runtime state,
  telemetry, licensing, service-supervisor state, org invites, upstream
  migration bookkeeping, reporting metadata, and authoritative OpenSearch
  document import.
- Added `adminMigrationLiveSourceCapabilityProbes()` for mutation-free preflight
  checks: read-only source PostgreSQL, erchef schema, Bifrost schema, visible
  organizations, optional Bookshelf SQL/S3 modes, optional Chef HTTP signed-read
  version probing, and derived-only OpenSearch evidence.
- Added tests that pin mapping coverage to
  `adminMigrationValidationGlobalFamilies()`,
  `adminMigrationValidationOrganizationFamilies()`, and
  `adminMigrationValidationBlobFamilies()` so the eventual extractor cannot
  drift from the normalized source/import contract without a failing test.

Upstream inventory:

| OpenCook normalized family | Upstream source contract |
| --- | --- |
| `users`, `user_keys` | `oc_erchef` user/key tables from `users.sql`, `opc_users.sql`, and `multiple_keys.sql`; public key metadata only. |
| `organizations`, `server_admin_memberships` | `oc_erchef` organization, customer, and org-user association rows, paired with Bifrost membership state where authorization needs it. |
| `user_acls`, `clients`, `groups`, `group_memberships`, `containers`, `acls` | `oc_bifrost` actors, groups, containers, objects, ACL join tables, and relation tables from `base.sql` plus debug ACL views where available. |
| `nodes`, `environments`, `roles`, `data_bags`, `data_bag_items` | `oc_erchef` baseline object tables; JSON payloads stay opaque and pass through existing OpenCook normalization. |
| `policy_revisions`, `policy_groups`, `policy_assignments` | `oc_erchef` policy, policy revision, policy group, and revision/group association tables. |
| `sandboxes`, `checksum_references` | `oc_erchef` checksum and sandbox/cookbook/artifact checksum reference tables. |
| `cookbook_versions`, `cookbook_artifacts` | `oc_erchef` cookbook, cookbook version, cookbook artifact, artifact version, and checksum reference tables/views. |
| `referenced_blobs`, `reachable_blobs`, `copied_blobs`, blob error counters | Bookshelf SQL tables (`buckets`, `files`, `file_data`, `file_chunks`) or external S3-compatible Bookshelf configuration from `dev-docs/BOOKSHELF.md`. |
| `opensearch_documents` | Derived/advisory only per `dev-docs/SEARCH_AND_INDEXING.md`; rebuild OpenCook OpenSearch from PostgreSQL after import/sync. |

Source capability probes for later tasks:

- Required PostgreSQL probes should connect with a read-only transaction, detect
  erchef and Bifrost schema availability, and enumerate visible organizations.
- Optional blob probes should detect internal Bookshelf SQL tables or external
  S3-compatible Bookshelf configuration, then decide whether copy or
  reference-only blob mode is possible.
- Optional Chef HTTP probes should only perform signed reads such as version or
  route-shape checks; they must not proxy or test writes against the source.
- Optional OpenSearch probes should only record advisory availability. Search
  index contents remain derived from PostgreSQL and are not migration input.

Schema ambiguities and deferred areas:

- `opc_customers` can inform organization/customer compatibility metadata, but
  `orgs` remains the primary organization source for this bucket.
- `org_user_invites`, `org_migration_state`, `reporting_schema_info`, and
  upstream telemetry are source-visible tables but not implemented OpenCook
  Chef-facing families.
- oc-id/OAuth state, Redis/session/cache state, Habitat supervisor state, and
  licensing/licensing telemetry stay out of scope by design.
- Upstream OpenSearch/Elasticsearch documents may be useful for warnings or
  comparison evidence, but importing them as authoritative state would violate
  the existing OpenCook PostgreSQL-source migration contract.

### Task 3: Add Live Source Configuration, Redaction, And Preflight Shapes

Task status: complete.

- Add internal config structs for source PostgreSQL DSN, source blob mode,
  optional source Chef HTTP URL, requestor identity, and private key path.
- Keep this config scoped to admin migration commands; do not add runtime server
  settings that imply OpenCook talks to source Chef during normal request
  serving.
- Reuse existing redaction helpers for source DSNs, credentials, private key
  paths, provider URLs, signed URLs, and secret-like local paths.
- Add JSON output fields for live source target, dependencies, capabilities,
  warnings, findings, and planned local bundle writes.
- Add command parser tests for usage errors, mutually exclusive flags, redacted
  output, and no-mutation preflight behavior.

Implementation notes:

- Added command-local live-source config plumbing for:
  - `--source-postgres-dsn`
  - `--source-bookshelf-root`
  - `--source-blob-url`
  - `--source-server-url`
  - `--source-requestor-name`
  - `--source-requestor-type`
  - `--source-private-key`
  - `--copy-blobs`
  - `--reference-blobs`
- Added `opencook admin migration source live preflight` as the no-mutation
  config/redaction/capability shape. It does not load OpenCook runtime config,
  open source PostgreSQL, contact source blob providers, contact source Chef
  HTTP, or write target state.
- Reserved `opencook admin migration source live extract` with the documented
  flag shape, but it currently returns a clear pending-extractor dependency
  error and does not write files. Task 4 owns replacing that placeholder with
  deterministic extractor doubles.
- Added redacted `live_source` and `capabilities` JSON fields. Raw source DSNs,
  provider URLs, signed/source Chef URLs, private key paths, and secret-looking
  local paths are not emitted.
- Added parser tests for missing required DSN, mutually exclusive org/blob/copy
  flags, partial HTTP credential groups, redacted output, and no-write behavior
  for the reserved extract command.

### Task 4: Add Extractor Interfaces And Deterministic Doubles

Task status: complete.

- Introduce a small internal live-source extractor interface owned by
  `cmd/opencook`, not a new runtime storage abstraction.
- Keep extractors read-only and context-aware.
- Add deterministic fake extractors for unit tests that emit current normalized
  source bundles, provider unavailable errors, unsupported family warnings, and
  partial extraction failures.
- Prove preflight and extract commands can run entirely against doubles without
  PostgreSQL, blob, or HTTP network dependencies.
- Add stable finding codes for:
  - source PostgreSQL unavailable
  - source schema unsupported
  - source family unsupported
  - source blob unavailable
  - source blob missing
  - source blob checksum mismatch
  - source HTTP read unavailable
  - extraction interrupted before bundle publication

Implementation notes:

- Added `adminMigrationLiveSourceExtractor` with context-aware `Preflight` and
  `Extract` methods. The seam is owned by `cmd/opencook` and returns migration
  envelopes, findings, inventory, planned mutations, and an optional normalized
  source bundle.
- Added a default pending extractor so real command invocations remain
  deterministic until Tasks 5-8 add concrete source PostgreSQL, blob, and HTTP
  readers.
- Updated `source live preflight` and `source live extract` to run through the
  extractor seam. Preflight remains read-only. Extract now publishes a returned
  normalized bundle through the existing atomic normalized-bundle writer only
  when the injected extractor succeeds.
- Added deterministic test fakes that prove preflight and extract can run
  without PostgreSQL, blob, Chef HTTP, OpenSearch, or OpenCook target access.
- Added test coverage for successful fake bundle publication, unsupported
  source-family warnings, provider/source failure findings, and interrupted
  extraction no-write behavior.
- Froze stable live-source finding codes:
  `source_postgres_unavailable`, `source_schema_unsupported`,
  `source_family_unsupported`, `source_blob_unavailable`,
  `source_blob_missing`, `source_blob_checksum_mismatch`,
  `source_http_read_unavailable`, and `source_extraction_interrupted`.

### Task 5: Implement Read-Only PostgreSQL Source Probes

Task status: complete.

- Add source PostgreSQL connection and read-only transaction handling for
  preflight.
- Validate source connectivity, server/schema identity, visible organizations,
  required table availability, and read-only posture.
- Detect unsupported source layouts with actionable findings instead of panics
  or raw SQL errors.
- Ensure preflight does not write to source PostgreSQL or target OpenCook
  PostgreSQL.
- Add tests for successful probes, missing tables, permission failures,
  connection failures, and redacted diagnostics.

Implementation notes:

- Replaced the default live-source preflight placeholder with a real
  PostgreSQL probe that parses the source DSN, opens a `pgx` connection, starts
  a read-only transaction, verifies `transaction_read_only`, reads
  `current_database()`/`current_user`, checks the required erchef/Bifrost table
  surface with `to_regclass`, and lists visible organizations from `orgs`.
- Kept extraction itself pending and retry-safe. Task 5 still performs no
  source writes, target OpenCook writes, blob reads, Chef HTTP calls, or bundle
  publication during preflight.
- Added classified dependency/finding output for connection failures, invalid
  DSNs, missing schema tables, read-only posture failures, organization
  visibility failures, and source table permission failures. Raw SQL errors,
  DSNs, provider URLs, private key paths, and credentials remain redacted from
  CLI output.
- Added an injectable PostgreSQL probe seam for command tests so unit coverage
  can prove successful probes and failure classes without requiring a live Chef
  Server PostgreSQL source.
- Split the non-PostgreSQL live-source dependency reporting so blob, Chef HTTP,
  and normalized-bundle contract checks remain visible while the PostgreSQL
  dependency is now backed by the real read-only probe.

### Task 6: Extract Bootstrap Identity And Authorization Families

Task status: complete.

- Extract users, organizations, server-admin membership, clients, actor keys,
  groups, group memberships, containers, and ACL documents into normalized
  payloads.
- Preserve current OpenCook normalization and validation by routing output
  through the existing normalized import reader.
- Preserve key metadata and public keys without private key material.
- Preserve ACL document shape without designing a new ACL model.
- Add fixture/double tests for single-org, multi-org, missing optional fields,
  duplicate/conflicting rows, unsupported rows, and org filtering.

Implementation notes:

- Added the first concrete `source live extract` implementation for bootstrap
  identity and authorization families. Extraction opens source PostgreSQL in a
  read-only transaction, reads users, organizations, admin memberships, clients,
  keys, groups, group relations, containers, and direct Bifrost ACL rows, then
  emits the existing `opencook.migration.chef_source.v1` normalized payload
  layout.
- Kept source rows routed through the existing source normalization validators
  before bundle materialization. This preserves current duplicate, orphan,
  malformed key, malformed ACL, and organization-scope rejection behavior
  instead of creating a parallel live-source validation path.
- Preserved key safety boundaries: extraction writes public key and expiration
  metadata only, never private key material.
- Added direct ACL reconstruction for user, organization, client, group, and
  container resources using Bifrost actor/group/container/object ACL tables.
  The extractor keeps actor/group arrays compatible with the existing OpenCook
  ACL import shape.
- Added deterministic command tests that publish a fake PostgreSQL bootstrap
  extraction bundle, read it back through the normalized source import reader,
  and prove users, validator clients, client keys, admin group membership, and
  ACL payloads survive the handoff.
- Cookbook, blob, and optional Chef HTTP live extraction remain intentionally
  deferred to Tasks 8-10; Task 7 extends the live bundle beyond bootstrap
  families to include PostgreSQL-backed core object metadata.

### Task 7: Extract Core Object Families

Task status: complete.

- Extract nodes, environments, roles, data bags/items, policy revisions,
  policy groups, policy assignments, sandboxes, checksum references, and object
  ACLs into normalized payloads.
- Preserve raw JSON compatibility fields where OpenCook already round-trips
  them.
- Preserve encrypted-looking data bag item opacity; do not decrypt or interpret
  encrypted payloads.
- Treat node policy refs as compatibility fields, not foreign keys.
- Add tests for restart/import parity by feeding live-extracted bundles through
  existing source import/sync preflight paths.

Implementation notes:

- Added live PostgreSQL readers for nodes, environments, roles, data bags,
  data bag items, policy revisions, policy groups, policy assignments,
  completed sandbox checksum metadata, checksum reference rows, and Bifrost
  object ACLs for node/environment/role/data-bag/policy/policy-group targets.
- Kept source rows inside the existing normalized source payload contract. The
  live bundle now runs identity plus core-object normalization before payload
  files are written, so duplicate, orphan, malformed, and ACL-target errors are
  reported by the same import/sync validators used for prepared artifacts.
- Added gzip-aware `serialized_object` decoding so live extraction can read the
  Chef PostgreSQL bytea payloads without database-side decompression helpers.
- Preserved raw Chef JSON fields where OpenCook round-trips them, including
  encrypted-looking data bag item payloads and node `policy_name` /
  `policy_group` compatibility fields.
- Extended command tests so a live-extracted core bundle is read back through
  `adminMigrationReadSourceImportBundle`, then exercised through existing
  `source import preflight` and `source sync preflight` paths without special
  live-source handling.

### Task 8: Extract Cookbook Metadata And Blob References

Task status: complete.

- Extract cookbook version and cookbook artifact metadata into the existing
  normalized cookbook source layout.
- Collect all referenced checksum blobs across cookbooks, artifacts, and
  sandboxes.
- Support copied blob bytes when the source blob adapter can read deterministic
  checksum content.
- Support reference-only mode with explicit warnings and later import/sync
  validation requirements.
- Preserve shared-checksum retention and checksum identity; never rewrite blob
  checksums.
- Add tests for shared checksums across cookbook versions, artifacts, and
  sandboxes; missing blobs; unavailable provider; checksum mismatch; and
  provider-error redaction.

Implementation notes:

- Added read-only live PostgreSQL extraction for cookbook versions and cookbook
  artifact versions, including metadata bytea decoding, serialized payload
  preservation, SQL-selected route identity fields, and checksum lists.
- Reused the existing normalized cookbook source layout so live-extracted
  cookbooks flow through the same source import/sync, checksum reference, and
  cookbook restore code as prepared source bundles.
- Merged cookbook and artifact checksum references into the normalized
  checksum graph while preserving shared checksum identity across cookbook
  versions, cookbook artifacts, and sandboxes.
- Added deterministic local Bookshelf/filesystem blob copying for
  `--copy-blobs`, including copied sidecar artifacts under
  `blobs/checksums`, MD5 checksum validation, and generic provider-safe error
  messages for missing, unavailable, or mismatched source blobs.
- Added reference-only behavior for `--reference-blobs` and automatic
  no-copy mode. The bundle records checksum references and emits warnings that
  import/sync must validate provider reachability before cutover.
- Extended live-source command tests for copied blob sidecars, reference-only
  extraction, shared checksum import parity, missing blobs, checksum mismatch,
  deterministic-copy unavailability for provider URLs, and provider credential
  redaction.

### Task 9: Write Atomic Normalized Live Bundles

Task status: complete.

- Implement `source live extract --output PATH` as an atomic local bundle write:
  write into a temporary sibling directory, verify hashes, then publish.
- Reuse manifest and payload hash verification from current source normalize and
  backup inspect code.
- Emit extraction inventory counts and `operator_report` guidance.
- Keep reruns safe: an interrupted output directory should be detectable and
  either replaced only with `--yes` or reported with a clear recovery command.
- Add tests for dry output paths, existing output paths, interrupted temp
  directories, manifest hash mismatch, and trailing/invalid JSON in generated
  payloads.

Implementation notes:

- Strengthened normalized source bundle publication so it writes to a sibling
  temporary directory, re-reads the staged manifest through the existing source
  import integrity path, and only renames into place after manifest hashes,
  payload JSON/counts, copied blob checksums, and sidecar counts validate.
- Added live extract `--dry-run` handling that still runs extraction and emits
  inventory/operator evidence, but skips local publication and preserves any
  existing output directory.
- Added stale temporary directory detection for interrupted atomic publishes.
  Without `--yes`, live extract now blocks with a clear recovery finding; with
  `--yes`, stale temp directories are removed before the verified publish.
- Updated live extract output evidence to report verified atomic publication,
  carry inventory counts into the shared `operator_report`, and provide
  next-step guidance toward source import, sync, search, shadow, and cutover
  rehearsal.
- Added tests for dry-run publication skipping, existing output safety,
  interrupted temp recovery, manifest hash mismatch, invalid payload JSON, and
  trailing payload JSON before publication.

### Task 10: Reuse Import, Sync, Shadow, Reindex, And Cutover Evidence

- Ensure live-extracted bundles work unchanged with:
  - `source import preflight/apply`
  - `source sync preflight/apply`
  - `shadow compare`
  - `backup create/inspect`
  - `restore preflight/apply`
  - `admin reindex`
  - `admin search check/repair`
  - `cutover rehearse`
- Add source metadata checks to cutover rehearsal so operators can see whether
  evidence came from a live extraction or a prepared artifact bundle.
- Preserve source-write-freeze and rollback-ready gates from the existing
  production-scale workflow.
- Add tests that run live-extracted fake bundles through the full logical
  pipeline without special-case import behavior.

Implementation notes:

- Cutover rehearsal now reports `source_type`, `source_origin`,
  `source_live_extraction`, `format_version`, payload count, and source cursor
  on the existing `cutover_source_bundle` dependency so operators can tell
  whether the freshness evidence came from direct live extraction or a prepared
  artifact bundle.
- The live-source pipeline test writes a `live_chef_infra_server` bundle through
  the same normalized bundle contract, then runs source import preflight/apply,
  source sync preflight/apply, shadow compare, backup create/inspect, restore
  preflight/apply, admin reindex, admin search check/repair, and cutover
  rehearsal without any live-source-specific import branch.
- Cutover evidence still requires the existing source-write-freeze and
  rollback-ready acknowledgements for final readiness.

### Task 11: Add Live Source Functional Coverage

- Extend the functional Docker harness with an opt-in live-source fixture mode.
- Prefer deterministic local source doubles or a small source PostgreSQL schema
  fixture over requiring a full upstream Chef Server container in default CI.
- Add phases such as:
  - `migration-live-source-preflight`
  - `migration-live-source-extract`
  - `migration-live-source-import`
  - `migration-live-source-reindex`
  - `migration-live-source-shadow`
  - `migration-live-source-rehearsal`
  - `migration-live-source-all`
- Reuse existing PostgreSQL, OpenSearch, and filesystem blob containers for the
  OpenCook target.
- Keep remote Docker support and artifact cleanup/retention behavior consistent
  with the production-scale phases.

Implementation notes:

- The functional harness now has an opt-in live-source fixture namespace under
  the Compose-managed state volume. It creates a tiny Chef-shaped source
  PostgreSQL database and filesystem Bookshelf checksum root inside the existing
  PostgreSQL/OpenSearch/filesystem-blob stack.
- New phases are `migration-live-source-preflight`,
  `migration-live-source-extract`, `migration-live-source-import`,
  `migration-live-source-reindex`, `migration-live-source-shadow`,
  `migration-live-source-rehearsal`, and `migration-live-source-all`.
- The aggregate phase extracts a `live_chef_infra_server` normalized source
  bundle, imports it into the restore target, rebuilds/checks OpenSearch, runs
  shadow-read comparison, records source-sync freshness, and finishes with
  cutover rehearsal evidence that proves `source_origin: live_extraction`.
- Cutover rehearsal keeps sandbox rows in import inventory but does not invent a
  direct sandbox `GET` check, matching the current Chef-compatible sandbox
  create/commit HTTP surface.
- Generated live-source databases and artifacts stay isolated from the active
  OpenCook database. File artifacts are cleaned with the same
  `KEEP_STACK`/`OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS` contract as the source and
  production-scale migration phases.

### Task 12: Harden Operational Guidance And Docs

Task status: complete.

- Update README migration guidance with direct live-source extraction examples.
- Update `docs/functional-testing.md` with the new opt-in phases and remote
  Docker examples.
- Update `docs/chef-server-ctl-operational-runbooks.md` with source-write
  freeze, extraction, final sync, shadow-read, cutover rehearsal, and rollback
  steps.
- Document credential handling and redaction expectations.
- Document when operators should choose copied blobs versus reference-only blob
  mode.
- Keep wording clear that source Chef writes must remain externally frozen for
  final sync/cutover.

Implementation notes:

- README migration guidance now includes direct live-source preflight/extract
  examples, copied-vs-reference blob mode guidance, credential-handling
  expectations, and final source-write freeze reminders.
- `docs/chef-server-ctl-operational-runbooks.md` now splits prepared-artifact
  and direct-live-source migration patterns, including final sync, restored
  target backup evidence, reindex/search check, shadow-read comparison, cutover
  rehearsal, rollback readiness, and external source freeze language.
- The in-process `opencook admin runbook show migration-cutover` catalog now
  includes live-source command references and the same credential/blob/freeze
  guidance so machine-readable runbooks match the Markdown catalog.
- Functional docs now call out the deterministic Compose-backed live-source
  fixture and the fact that the opt-in phases exercise the same read-only live
  extraction command surface operators will use.

### Task 13: Sync Roadmap, Matrix, And Close The Bucket

Task status: complete.

- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `AGENTS.md`
  - `docs/live-chef-source-extraction-plan.md`
- Mark this bucket complete once live-source extraction, pipeline reuse,
  functional coverage, and operator docs land.
- Point the next bucket at the highest-risk Chef compatibility gap discovered
  during live extraction, or at remaining core Chef object compatibility if no
  live-extraction blocker is found.

Implementation notes:

- Roadmap, milestone, compatibility matrix, and agent guidance now mark direct
  live Chef source extraction complete for the implemented PostgreSQL-backed
  Chef families and checksum blob evidence.
- The bucket closes with pipeline reuse intact: live-extracted bundles flow
  through the existing source import/sync, backup/restore, reindex/search
  check, shadow-read, and cutover rehearsal commands instead of a parallel
  migration path.
- No live-extraction blocker displaced the roadmap. The next recommended bucket
  is remaining core Chef object compatibility hardening, with deployment-test
  or live-source-discovered Chef compatibility gaps allowed to interrupt if
  they prove higher risk.

## Test Plan

Focused tests:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/blob
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
```

Full verification:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```text
scripts/functional-compose.sh migration-live-source-all
scripts/functional-compose.sh migration-source-all migration-scale-all
```

Required scenarios:

- Live source preflight succeeds without source or target mutation.
- Source PostgreSQL failures are redacted and surfaced as dependency failures.
- Unsupported source schemas/families are reported with stable finding codes.
- Multi-org extraction can target one org or all orgs.
- Identity/authz, core object, cookbook/artifact, sandbox/checksum, policy, and
  data bag families are emitted into the existing normalized bundle contract.
- Copied blob mode verifies checksum content and preserves shared checksums.
- Reference-only blob mode reports clear follow-up validation requirements.
- Live-extracted bundles import into an empty target and sync into a previously
  imported target without inventing a second import path.
- OpenSearch is rebuilt and checked from imported PostgreSQL state.
- Shadow-read comparison and cutover rehearsal consume live-extracted bundle
  evidence without proxying writes.
- Extraction outputs redact DSNs, credentials, private key paths, signed URLs,
  provider responses, and secret-like local paths.
- Interrupted extraction can be rerun safely without publishing partial bundle
  metadata.

## Assumptions

- Live-source extraction should start from upstream PostgreSQL plus Bookshelf or
  S3-compatible checksum blob access because those are the source-of-truth paths
  for implemented Chef data.
- A full upstream Chef Server container is not required for default functional
  coverage in this bucket; deterministic source fixtures and extractor doubles
  are acceptable for CI.
- Optional read-only source Chef HTTP access is useful for probes and
  shadow-read evidence, but it should not become a write proxy.
- OpenSearch provider documents remain derived and should not be imported as
  authoritative state.
- Redis, oc-id, licensing, telemetry, and service-supervisor data stay out of
  scope unless a future compatibility bucket explicitly needs them.
- The first live-source implementation should favor clear findings and
  retry-safe output over broad but ambiguous best-effort extraction.

## Non-Goals

- Changing Chef-facing HTTP behavior.
- Adding source Chef write controls or write proxies.
- Adding new runtime OpenCook dependencies on an upstream Chef Server.
- Importing upstream OpenSearch/Elasticsearch documents as source of truth.
- Decrypting encrypted data bag items.
- Importing unimplemented Chef surfaces.
- Adding licensing or license telemetry compatibility behavior.
- Replacing the existing normalized source import/sync path.
