# Chef Infra Server Source Import, Sync, And Cutover Hardening Plan

Status: planned

## Summary

This bucket turns the completed migration/cutover foundation into a practical
path for moving existing Chef Infra Server installations toward OpenCook. The
previous migration bucket proved OpenCook-to-OpenCook logical backup, restore,
reindex, source artifact inventory, and restored-target rehearsal. This bucket
extends that work into source import/sync planning, safe offline import,
repeatable source reconciliation, and production-scale shadow-read/cutover
hardening.

The goal is operational migration confidence without changing the Chef-facing
HTTP contract. OpenCook remains compatibility-first: Chef and Cinc clients,
`knife`, signed request behavior, route payloads, status codes, API-version
semantics, ACL behavior, provider-backed blobs, and OpenSearch-derived search
must keep the contracts already pinned by earlier buckets.

Use this file as the reference checklist for this bucket.

## Current State

OpenCook already has:

- PostgreSQL-backed persistence for the implemented identity, authorization,
  core object, cookbook, sandbox/checksum, policy, and ACL state.
- Provider-backed blobs in memory, filesystem, and S3-compatible modes.
- Active OpenSearch-backed search for implemented Chef-searchable families, with
  reindex/check/repair from PostgreSQL-backed state.
- `opencook admin migration source inventory PATH`, which inventories read-only
  source manifests, extracted artifact directories, and tar/tar.gz archives.
- OpenCook logical backup/restore with bundle integrity checks, offline restore
  safety, blob restoration/verification, restored-target reindex, and cutover
  rehearsal.
- Operational parity tools for config checks, service status/doctor, metrics,
  logs, diagnostics, runbooks, and functional Docker verification.

Remaining gap:

- Source inventory is read-only and advisory; it does not normalize source
  artifacts into importable OpenCook state.
- There is no supported import apply path from existing Chef Infra Server
  artifacts into an OpenCook target.
- There is no repeatable sync/reconciliation model for successive read-only
  source snapshots.
- Shadow-read comparison is advisory text, not a structured command with
  documented normalizers and cutover gates.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, payloads, status codes, signed-auth
  behavior, or API-version behavior.
- Do not add licensing, license enforcement, license telemetry, or
  license-management endpoints.
- Do not mutate an upstream Chef Infra Server source. Source integrations are
  read-only: backup/export artifacts, source manifests, and GET/HEAD-only source
  HTTP reads where supported.
- Keep PostgreSQL as the restored/imported OpenCook source of truth.
- Treat OpenSearch as derived state. Import/sync should rebuild, check, or repair
  OpenSearch rather than importing provider documents as authoritative data.
- Treat blob providers as authoritative content stores. Import/sync must verify
  checksum reachability and copy or confirm checksum-addressed blob bytes before
  metadata becomes visible.
- Keep direct OpenCook PostgreSQL mutations offline-gated with `--offline`.
- Require explicit `--yes` for mutations. Prefer `preflight`, `plan`, or
  `--dry-run` by default.
- Keep failed imports no-mutation or rollback-safe. If a transaction cannot cover
  an entire operation, write resumable progress metadata before and after each
  independently safe phase.
- Preserve existing OpenCook backup/restore and rehearsal command shapes. Add
  source import/sync and shadow-read commands under `opencook admin migration`
  rather than inventing a new operational namespace.
- Redact source URLs, PostgreSQL DSNs, blob credentials, private key paths,
  signed URL query strings, request signatures, provider response bodies, and
  secret-like local paths in all output.
- Do not import object families OpenCook has not implemented. Report them as
  unsupported or deferred with stable finding codes.

## Source Contracts

This bucket should support source inputs in layers, starting with deterministic
fixtures and exported artifacts before broad live-source integrations:

- Explicit source manifests, using a versioned format such as
  `opencook.migration.chef_source.v1`.
- Extracted backup/export directories containing generated JSON for Chef-visible
  object families.
- Tar or tar.gz source archives that can be inspected and normalized without
  extraction to unsafe paths.
- Bookshelf/S3-style checksum blob references, with optional copied blob payloads
  or provider locations that can be verified by checksum.
- Read-only source Chef HTTP checks for shadow-read comparison and targeted
  sync validation where upstream APIs expose the needed family.

Deferred unless a later task proves a safe contract:

- Direct import from live upstream PostgreSQL databases.
- Direct import from OpenSearch/Elasticsearch provider documents as source data.
- Redis, oc-id, telemetry, licensing, secrets, and service-supervisor data.
- Any source family for a Chef-facing surface OpenCook has not implemented yet.

## Proposed Command Surface

Existing commands remain valid:

```sh
opencook admin migration source inventory PATH [--json] [--with-timing]
opencook admin migration restore preflight PATH --offline [--json] [--with-timing]
opencook admin migration restore apply PATH --offline [--dry-run|--yes] [--json] [--with-timing]
opencook admin migration cutover rehearse --manifest PATH [--server-url URL] [--json] [--with-timing]
```

Planned additive commands:

```sh
opencook admin migration source normalize PATH --output PATH [--json] [--with-timing]
opencook admin migration source import preflight PATH --offline [--json] [--with-timing]
opencook admin migration source import apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
opencook admin migration source sync preflight PATH --offline [--progress PATH] [--json] [--with-timing]
opencook admin migration source sync apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
opencook admin migration shadow compare --source PATH --target-server-url URL [--manifest PATH] [--json] [--with-timing]
```

Command principles:

- `source normalize` converts a supported source artifact into the normalized
  source manifest and payload layout without mutating OpenCook.
- `source import preflight` validates normalized source content, target safety,
  blob reachability, unsupported families, and planned mutations.
- `source import apply` writes to an offline OpenCook target with rollback-safe
  metadata and blob verification.
- `source sync preflight/apply` re-runs import from a later read-only source
  snapshot and reports creates, updates, deletes, conflicts, and skipped
  unsupported families.
- `shadow compare` performs read-only source-vs-restored target comparisons with
  documented compatibility normalizers and no write proxying.

## Task Breakdown

### Task 1: Freeze Source Contract And Fixture Taxonomy

Status: planned.

- Inventory upstream Chef Infra Server export/backup shapes available in the
  local checkout and any generated fixture formats already represented in tests.
- Freeze the first normalized source manifest shape and payload directory layout.
- Define source family names, stable finding codes, and unsupported/deferred
  family behavior.
- Add tiny source fixture directories and archives for users, orgs, clients,
  groups, containers, ACLs, core objects, cookbooks, checksum blobs, and
  unsupported ancillary families.
- Keep this task read-only and parser-focused.

### Task 2: Add Source Normalize Command And Output Models

Status: planned.

- Add `opencook admin migration source normalize PATH --output PATH`.
- Reuse the existing migration JSON envelope: dependencies, inventory, findings,
  planned mutations, warnings, timing, and redaction.
- Emit a normalized source manifest plus payload files with deterministic
  ordering and SHA-256 integrity metadata.
- Refuse to overwrite output without `--yes` only if the command writes into an
  existing path; keep normalizer output non-mutating with respect to OpenCook and
  the source.
- Add parser and CLI tests for directories, archives, explicit manifests,
  unsupported artifacts, malformed JSON, path traversal attempts, and redaction.

### Task 3: Normalize Identity, Authorization, And Key Families

Status: planned.

- Normalize users, user ACLs, user keys, organizations, organization ACLs,
  server-admin membership, clients, client keys, groups, group memberships,
  containers, and container ACLs.
- Preserve Chef-compatible names, key IDs, expiration metadata, validator flags,
  org membership, default groups, and default containers.
- Validate duplicate actors, missing org references, orphan memberships, invalid
  public keys, invalid ACL documents, and unsupported key formats.
- Do not generate replacement private keys during import.
- Add round-trip tests from source fixtures to normalized import payloads.

### Task 4: Normalize Core Object And Policy Families

Status: planned.

- Normalize nodes, environments, roles, data bags/items, encrypted-looking data
  bag JSON, policy revisions, policy groups, policy assignments, object ACLs,
  sandboxes, and checksum references.
- Preserve canonical payloads and API-version-sensitive fields already pinned by
  previous buckets.
- Validate org ownership, object names, policy revision IDs, sandbox completion
  state, checksum references, and ACL links.
- Report unimplemented or unknown object families explicitly instead of dropping
  them silently.
- Add round-trip tests for plain and encrypted-looking data bag payloads,
  policyfile metadata, sandbox references, and malformed object fixtures.

### Task 5: Normalize Cookbooks, Cookbook Artifacts, And Blob References

Status: planned.

- Normalize cookbook versions, cookbook artifacts, metadata, manifests, file
  collections, legacy segment shapes, and checksum references.
- Preserve v0/v2 file-shape compatibility metadata and cookbook-artifact
  identifiers already pinned by route tests.
- Detect missing blob payloads, checksum mismatches, duplicate cookbook
  identifiers, malformed versions, and source route/payload name mismatches.
- Support copied blob payloads when present and provider references when they can
  be verified by checksum.
- Add tests for shared checksum reuse, missing blobs, checksum mismatch,
  cookbook/artifact overlap, and unsupported cookbook source layouts.

### Task 6: Add Source Import Preflight And Conflict Planning

Status: planned.

- Add `opencook admin migration source import preflight PATH --offline`.
- Validate normalized source integrity, target PostgreSQL reachability, target
  emptiness or explicit conflict policy, blob backend readiness, and OpenSearch
  reindex requirements.
- Produce planned mutations grouped by family: create, update, skip, conflict,
  unsupported, blob copy, and reindex recommendation.
- Default to refusing non-empty targets until conflict behavior is explicitly
  implemented and documented.
- Prove invalid source input and failed provider checks do not mutate target
  PostgreSQL, blob, or OpenSearch state.

### Task 7: Implement Offline Source Import Apply

Status: planned.

- Add `opencook admin migration source import apply PATH --offline --yes`.
- Import normalized state into an offline OpenCook PostgreSQL target through
  existing store seams where possible.
- Copy or verify blob content before publishing metadata that references it.
- Roll back PostgreSQL state and avoid stale verifier/search/blob side effects on
  failure.
- Write progress metadata when a phase cannot be wrapped in a single transaction,
  so retries can detect completed blob copies and not duplicate work.
- Add restart/rehydration tests proving imported keys authenticate, imported orgs
  bootstrap correctly, imported objects read correctly, imported cookbook blob
  downloads work, and OpenSearch can be rebuilt cleanly.

### Task 8: Add Repeatable Source Sync And Reconciliation

Status: planned.

- Add `source sync preflight/apply` for successive normalized source snapshots.
- Define idempotency and conflict behavior for unchanged rows, changed rows,
  source deletions, target-only rows, renamed keys, reused checksums, and
  unsupported families.
- Store and validate sync progress/cursor metadata without trusting wall-clock
  timestamps alone.
- Default destructive delete behavior to dry-run or explicit confirmation.
- Prove repeated syncs are stable, failed syncs are retryable, and stale target
  state is not removed unless the source contract proves deletion intent.

### Task 9: Rebuild And Validate Derived Search After Import/Sync

Status: planned.

- Integrate import/sync output with `opencook admin reindex --all-orgs --complete`
  and `opencook admin search check`.
- Ensure OpenSearch documents are rebuilt from imported PostgreSQL state, not
  imported from source provider artifacts.
- Validate clients, environments, nodes, roles, data bags, encrypted-looking data
  bags, and unsupported search indexes after import.
- Include provider-unavailable and partial-drift behavior using existing admin
  exit-code conventions.
- Add package and functional coverage for imported searchable families and
  unsupported cookbook/policy/sandbox/checksum search families.

### Task 10: Implement Shadow-Read Comparison Normalizers

Status: planned.

- Add `opencook admin migration shadow compare`.
- Compare read-only source responses with restored OpenCook responses using
  documented normalizers rather than byte-for-byte equality.
- Normalize ordering, generated timestamps, signed URL query strings, private key
  omission, known API-version shape differences, and allowed status wording
  differences.
- Treat missing objects, auth failures, ACL mismatches, unsupported source
  families, missing blob downloads, search count mismatches, and unexpected
  Chef-facing payload differences as blockers.
- Ensure the command never proxies writes, validator registration, cookbook
  upload, sandbox commit, key mutation, or client mutation.

### Task 11: Harden Cutover Rehearsal And Rollback Runbooks

Status: planned.

- Extend `migration cutover rehearse` to consume import/sync progress and
  shadow-read comparison outputs.
- Add cutover gates for source import success, sync freshness, reindex
  cleanliness, blob reachability, signed auth, representative reads, and
  rollback readiness.
- Document client cutover sequencing for DNS/load balancers, Chef/Cinc client
  configuration, source read/write freeze windows, and rollback.
- Keep source Chef Infra Server available until post-cutover smoke checks pass.
- Add runbook output that clearly distinguishes blockers from advisory warnings.

### Task 12: Extend Functional Docker Coverage

Status: planned.

- Add functional fixtures for normalized Chef source artifacts and copied blob
  payloads.
- Add functional phases for source normalize, import preflight, import apply,
  reindex after import, sync preflight/apply, shadow compare, and hardened
  cutover rehearsal.
- Keep remote Docker support by using image-baked scripts and Compose-managed
  volumes.
- Clean generated source/import/shadow artifacts unless `KEEP_STACK=1` or the
  functional artifact-retention flag is set.
- Preserve existing default-flow runtime by making the heaviest source
  import/sync/shadow drill opt-in unless it proves fast enough for default
  coverage.

### Task 13: Sync Docs And Close The Bucket

Status: planned.

- Update:
  - `README.md`
  - `AGENTS.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `docs/chef-server-ctl-operational-runbooks.md`
  - this plan file
- Mark source import/sync plus shadow-read/cutover hardening complete for the
  implemented source families.
- Point the next bucket at the highest-risk remaining gap:
  - deployment-test-discovered Chef compatibility issues
  - remaining core object edge cases
  - live maintenance-mode request blocking
  - online direct PostgreSQL repair mutation with cache invalidation
  - production scale/load validation

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
scripts/functional-compose.sh migration-source-normalize
scripts/functional-compose.sh migration-source-import
scripts/functional-compose.sh migration-source-sync
scripts/functional-compose.sh migration-shadow-compare
scripts/functional-compose.sh migration-source-all
```

Required scenarios:

- source inventory still works for existing source fixtures and unsupported
  artifacts
- source normalize emits deterministic manifests and payload hashes
- import preflight refuses invalid source input, unsafe targets, unavailable blob
  providers, missing blobs, and unsupported families without mutation
- import apply writes identity, authz, core objects, policies, cookbooks,
  sandboxes/checksums, ACLs, and blob bytes into an offline OpenCook target
- restart/rehydration after import restores verifier keys and all implemented
  route-visible state
- reindex after import produces clean OpenSearch consistency for supported
  search families
- repeated sync is idempotent and failed sync is retryable
- shadow compare normalizes documented differences while flagging real
  Chef-facing incompatibilities
- cutover rehearsal gates on import, sync, reindex, blob, signed-auth,
  representative-read, shadow-read, and rollback readiness

## Assumptions

- The first source importer should target normalized artifacts and exported
  source content, not direct live upstream database mutation.
- Read-only source HTTP support is valuable for shadow-read and spot-check sync,
  but not every family may be discoverable through upstream public APIs.
- Blob bytes must be verified by checksum before referenced metadata is made
  visible in OpenCook.
- OpenSearch provider documents are never source data; they are rebuilt from
  imported PostgreSQL state.
- Licensing, telemetry, oc-id, Redis, and service-supervisor data remain outside
  the OpenCook compatibility target.
- If source import uncovers a higher-risk Chef-facing compatibility gap, that
  gap should interrupt this bucket before broader import coverage expands.
