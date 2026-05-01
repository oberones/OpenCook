# Chef Infra Server Source Import, Sync, And Cutover Hardening Plan

Status: complete

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
- `opencook admin migration source normalize`, `source import`, and
  `source sync`, which turn normalized source artifacts into offline OpenCook
  PostgreSQL metadata plus provider-backed checksum blobs with rollback-safe
  progress metadata.
- `opencook admin migration shadow compare` and cutover rehearsal evidence gates
  for source import progress, sync freshness, search cleanliness, shadow-read
  results, signed-auth reads, blob downloads, and rollback readiness.
- OpenCook logical backup/restore with bundle integrity checks, offline restore
  safety, blob restoration/verification, restored-target reindex, and cutover
  rehearsal.
- Operational parity tools for config checks, service status/doctor, metrics,
  logs, diagnostics, runbooks, and functional Docker verification.

Remaining gap:

- Direct live upstream extraction beyond normalized/exported source artifacts is
  still deferred.
- Production-scale shadow-read and cutover validation still need larger
  deployment rehearsal guidance.
- Live maintenance-mode request blocking is not implemented yet, so operators
  should still freeze writes externally during backup, source sync, reindex,
  repair, and cutover windows.
- Online direct PostgreSQL repair mutation still needs an explicit
  cache-invalidation and process-safety contract before offline gates can be
  relaxed.

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

Frozen normalized source manifest v1:

```json
{
  "format_version": "opencook.migration.chef_source.v1",
  "source_type": "normalized_chef_source_fixture",
  "payloads": [
    {
      "organization": "ponyville",
      "family": "nodes",
      "path": "payloads/organizations/ponyville/nodes.json",
      "count": 1,
      "sha256": "optional-later-normalizer-digest"
    }
  ],
  "artifacts": [
    {
      "family": "bookshelf",
      "path": "blobs/checksums",
      "count": 1,
      "supported": true
    }
  ],
  "notes": []
}
```

Frozen payload layout:

- `opencook-source-manifest.json` at the source root.
- `payloads/bootstrap/users.json`
- `payloads/bootstrap/user_acls.json`
- `payloads/bootstrap/user_keys.json`
- `payloads/bootstrap/server_admin_memberships.json`
- `payloads/organizations/{org}/organization.json`
- `payloads/organizations/{org}/{family}.json` for org-scoped families.
- `blobs/checksums/{checksum}` for copied checksum-addressed bytes.
- `derived/opensearch/...` for advisory derived search artifacts that must be
  rebuilt instead of imported as source of truth.
- `unsupported/{family}/...` for explicitly retained unsupported/deferred source
  material.

Frozen source family names:

- Global payload families: `users`, `user_acls`, `user_keys`,
  `server_admin_memberships`, `organizations`.
- Org-scoped payload families: `clients`, `client_keys`, `groups`,
  `group_memberships`, `containers`, `acls`, `nodes`, `environments`, `roles`,
  `data_bags`, `data_bag_items`, `policy_revisions`, `policy_groups`,
  `policy_assignments`, `sandboxes`, `checksum_references`,
  `cookbook_versions`, `cookbook_artifacts`.
- Side-channel inventory families: `cookbook_blob_references`,
  `opensearch_source_artifacts`.
- Unsupported or deferred source-family examples: `oc_id`, `redis`,
  `telemetry`, `licensing`, raw database dumps, and supervisor/service data.

Frozen source inventory finding codes:

- `source_import_not_implemented` for read-only inventory output until import
  apply exists.
- `source_search_rebuild_required` when OpenSearch/Elasticsearch/Solr artifacts
  are present.
- `source_database_artifact_deferred` when raw SQL or database dump artifacts
  are present.
- `source_artifact_unsupported` when an unsupported ancillary family is present.
- `source_manifest_missing`, `source_manifest_invalid_json`, and
  `source_manifest_unsupported_format` for manifest parse failures.
- `source_artifact_unavailable` and `source_archive_unreadable` for unreadable
  paths or archives.
- `source_payload_invalid_json` for supported-family JSON that cannot be
  canonicalized as an object or array of objects.
- `source_path_unsafe` for archive or manifest paths that are absolute or escape
  the source root.
- `source_manifest_payloads_missing` when a manifest can be inventoried but does
  not contain normalizable payload file references.
- `source_normalize_output_exists`,
  `source_normalize_output_overlaps_source`,
  `source_normalize_output_unsafe`, and `source_normalize_write_failed` for
  output safety or write failures.
- `source_duplicate_user`, `source_duplicate_organization`,
  `source_duplicate_client`, `source_duplicate_group`,
  `source_duplicate_container`, `source_duplicate_user_key`,
  `source_duplicate_client_key`, `source_duplicate_group_membership`,
  `source_duplicate_server_admin`, and `source_duplicate_acl` for duplicate
  identity, authorization, or key records.
- `source_user_invalid`, `source_organization_invalid`,
  `source_client_invalid`, `source_group_invalid`, `source_container_invalid`,
  `source_group_membership_invalid`, `source_acl_invalid`,
  `source_key_invalid`, and `source_key_public_key_missing` for malformed
  identity/auth/key records.
- `source_missing_organization`, `source_orphan_user_key`,
  `source_orphan_client_key`, `source_orphan_group_member`,
  `source_orphan_group_membership`, `source_orphan_server_admin`, and
  `source_acl_target_missing` for references that point at missing normalized
  source records.

## Proposed Command Surface

Existing commands remain valid:

```sh
opencook admin migration source inventory PATH [--json] [--with-timing]
opencook admin migration source normalize PATH --output PATH [--yes] [--json] [--with-timing]
opencook admin migration source import preflight PATH --offline [--json] [--with-timing]
opencook admin migration source import apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
opencook admin migration source sync preflight PATH --offline [--progress PATH] [--json] [--with-timing]
opencook admin migration source sync apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
opencook admin migration restore preflight PATH --offline [--json] [--with-timing]
opencook admin migration restore apply PATH --offline [--dry-run|--yes] [--json] [--with-timing]
opencook admin migration shadow compare --source PATH --target-server-url URL [--manifest PATH] [--json] [--with-timing]
opencook admin migration cutover rehearse --manifest PATH [--source PATH] [--source-import-progress PATH] [--source-sync-progress PATH] [--search-check-result PATH] [--shadow-result PATH] [--rollback-ready] [--server-url URL] [--json] [--with-timing]
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
- `cutover rehearse` remains read-only and now optionally consumes source
  import progress, source sync cursor progress, search-check output,
  shadow-compare output, and rollback acknowledgement so cutover runbooks can
  distinguish blockers from advisory warnings.

## Task Breakdown

### Task 1: Freeze Source Contract And Fixture Taxonomy

Status: complete.

- Inventory upstream Chef Infra Server export/backup shapes available in the
  local checkout and any generated fixture formats already represented in tests.
- Freeze the first normalized source manifest shape and payload directory layout.
- Define source family names, stable finding codes, and unsupported/deferred
  family behavior.
- Add tiny source fixture directories and archives for users, orgs, clients,
  groups, containers, ACLs, core objects, cookbooks, checksum blobs, and
  unsupported ancillary families.
- Keep this task read-only and parser-focused.

Completed notes:

- Added the first normalized fixture under
  `test/compat/fixtures/chef-source-import/v1`.
- Froze the read-only `opencook.migration.chef_source.v1` manifest shape with
  explicit payload family, path, organization, and count fields plus side-channel
  artifact records for copied blobs, derived OpenSearch artifacts, and deferred
  unsupported source families.
- Kept existing `opencook.migration.source_inventory.v1` manifests readable
  while allowing source inventory to report normalized payload-family counts.
- Added generated archive taxonomy coverage for bootstrap, org-scoped identity,
  authorization, core object, cookbook, blob, derived search, and unsupported
  ancillary source paths.

### Task 2: Add Source Normalize Command And Output Models

Status: complete.

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

Completed notes:

- Added `opencook admin migration source normalize PATH --output PATH`.
- The command writes `opencook-source-manifest.json` plus canonical JSON payload
  arrays under the frozen payload layout. Each payload manifest record includes
  deterministic SHA-256 metadata.
- Existing normalized manifests can be re-normalized, and extracted directories
  or tar/tar.gz archives can be normalized into raw family payloads for later
  semantic tasks.
- Copied checksum blobs, derived OpenSearch artifacts, and unsupported ancillary
  artifacts are preserved as side-channel manifest records.
- Output is written through a temporary sibling directory, existing output
  requires `--yes`, output inside the source tree is rejected, and malformed JSON
  or unsafe archive paths leave no partial output behind.

### Task 3: Normalize Identity, Authorization, And Key Families

Status: complete.

- Normalize users, user ACLs, user keys, organizations, organization ACLs,
  server-admin membership, clients, client keys, groups, group memberships,
  containers, and container ACLs.
- Preserve Chef-compatible names, key IDs, expiration metadata, validator flags,
  org membership, default groups, and default containers.
- Validate duplicate actors, missing org references, orphan memberships, invalid
  public keys, invalid ACL documents, and unsupported key formats.
- Do not generate replacement private keys during import.
- Add round-trip tests from source fixtures to normalized import payloads.

Completed notes:

- Source normalize now semantically canonicalizes the identity and authorization
  root families: users, user ACLs, user keys, organizations, clients, client
  keys, groups, group memberships, containers, org/client/group/container ACLs,
  and server-admin memberships.
- Public keys must be RSA public key PEMs. Private key material is never emitted
  into normalized payloads, and missing or unsupported key material fails before
  output is written.
- Group, membership, key, ACL, and org-scoped payload references are checked
  against the normalized source graph so orphaned rows cannot silently flow into
  import preflight.
- The source fixture now includes valid key PEMs plus the default group and
  container rows needed by Chef-compatible org bootstrap behavior.
- Added round-trip and negative CLI coverage for duplicate actors, invalid keys,
  orphan memberships, missing organizations, invalid ACL documents, and
  canonical ACL/key/default group/container output.

### Task 4: Normalize Core Object And Policy Families

Status: complete.

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

Completed notes:

- Source normalize now performs a second semantic pass for core object and
  policy families after identity/org normalization, preserving Chef-observable
  node, environment, role, data bag item, and policyfile payload fields while
  pinning canonical names, Chef metadata defaults, policy revision IDs, sandbox
  completion state, and checksum references.
- Ordered run lists remain ordered. Data bag item bodies are wrapped in the
  normalized `bag`/`id`/`payload` shape without rewriting encrypted-looking or
  plain payload JSON.
- Object ACLs for nodes, environments, roles, data bags, policies, policy
  groups, and sandboxes are validated after their targets are normalized, while
  identity ACL validation remains in the earlier pass.
- Unknown org-scoped JSON object families and unknown manifest payload families
  now emit `source_family_unsupported` warnings instead of disappearing from
  operator output.
- Added fixture and negative CLI coverage for encrypted and plain data bag item
  round trips, policyfile metadata, sandbox/checksum references, malformed
  policy revisions, orphaned policy assignments, incomplete sandboxes, missing
  checksum references, orphan object ACLs, and unsupported object families.

### Task 5: Normalize Cookbooks, Cookbook Artifacts, And Blob References

Status: complete.

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

Completed notes:

- Source normalize now semantically canonicalizes cookbook versions and cookbook
  artifacts after core object normalization, including Chef route-compatible
  cookbook names, `x.y.z` cookbook-version routes, 40-hex artifact identifiers,
  metadata version anchors, `frozen?`, `json_class`, `chef_type`, `all_files`,
  and legacy file segment arrays.
- Raw `metadata.json` style source payloads and wrapper-style payloads are both
  accepted. Route-derived cookbook name/version data is used only for validation
  and is not emitted into normalized payload files.
- Cookbook and artifact checksum references are merged into
  `checksum_references` with `family: cookbook`, so shared checksum reuse across
  cookbook versions and cookbook artifacts is represented once in the normalized
  source graph.
- Copied checksum-addressed blob bytes are verified against their checksum path.
  Missing copied blob bytes now emit `source_blob_payload_missing` warnings so
  provider-backed verification can happen during import preflight without
  silently losing the reference.
- Unsupported raw cookbook source trees under org-scoped cookbook directories
  now emit `source_cookbook_layout_unsupported` warnings instead of being
  silently ignored.
- Added fixture and negative CLI coverage for Chef-shaped cookbook and artifact
  round trips, shared cookbook/artifact checksum overlap, missing copied blob
  payload warnings, checksum mismatches, duplicate cookbook versions,
  route/payload mismatches, malformed versions, invalid artifact identifiers,
  and unsupported raw cookbook source layouts.

### Task 6: Add Source Import Preflight And Conflict Planning

Status: complete.

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

Completed notes:

- Added `opencook admin migration source import preflight PATH --offline`.
- Import preflight now requires a normalized Chef source manifest, verifies
  payload SHA-256 metadata and object counts before provider configuration is
  loaded, re-runs semantic source validation, validates copied checksum blob
  bytes, and surfaces unsupported/deferred source families in the same migration
  output envelope as backup/restore preflight.
- The command loads the offline PostgreSQL target only after source integrity
  passes, requires an empty target, and emits a blocking
  `source_import_target_not_empty` conflict until explicit source-vs-target
  update behavior exists.
- Blob preflight distinguishes copied source blobs from provider-only checksum
  references: copied bytes are planned for future copy, while uncopied
  references must already be reachable through the configured blob backend.
- Planned mutations now describe per-family PostgreSQL creates, unsupported or
  deferred skips, blocked non-empty target conflicts, blob copy/verify work, and
  the required OpenSearch rebuild after import.
- Added no-mutation coverage for tampered normalized source payloads,
  non-empty targets, and missing provider-only blobs.

### Task 7: Implement Offline Source Import Apply

Status: complete.

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

Completed notes:

- Added `opencook admin migration source import apply PATH --offline
  [--dry-run|--yes] [--progress PATH]`.
- Reused normalized source bundle preflight, target emptiness checks, existing
  offline store seams, and cookbook restore/import rollback behavior.
- Added blob apply gating that copies source-packaged checksum bytes or verifies
  provider-only references before PostgreSQL metadata is published.
- Added retry progress metadata for non-transactional blob copy/verify phases,
  with a default `opencook-source-import-progress.json` file beside the
  normalized source manifest.
- Added command tests for successful import, dry-run no-mutation behavior, blob
  failure no-metadata behavior, PostgreSQL rollback on write failure, and
  imported key verifier rehydration plus OpenSearch reindexability from imported
  state.

### Task 8: Add Repeatable Source Sync And Reconciliation

Status: complete.

- Add `source sync preflight/apply` for successive normalized source snapshots.
- Define idempotency and conflict behavior for unchanged rows, changed rows,
  source deletions, target-only rows, renamed keys, reused checksums, and
  unsupported families.
- Store and validate sync progress/cursor metadata without trusting wall-clock
  timestamps alone.
- Default destructive delete behavior to dry-run or explicit confirmation.
- Prove repeated syncs are stable, failed syncs are retryable, and stale target
  state is not removed unless the source contract proves deletion intent.

Completed notes:

- Added `opencook admin migration source sync preflight PATH --offline
  [--progress PATH]` and `opencook admin migration source sync apply PATH
  --offline [--dry-run|--yes] [--progress PATH]`.
- Sync now compares canonical row digests for manifest-covered source families,
  preserving target-only orgs and absent families while planning create, update,
  delete, and unchanged counts.
- Destructive deletes remain explicit: preflight reports them, dry-run does not
  mutate, and apply still requires `--yes`.
- Added cursor progress metadata in
  `opencook.migration.source_sync_progress.v1`, keyed by a SHA-256 digest of the
  normalized manifest, payloads, and copied blob sidecars rather than wall-clock
  timestamps.
- Added retryable apply coverage for repeated no-op snapshots, successful
  reconciliation, dry-run no-mutation behavior, and PostgreSQL write rollback.

### Task 9: Rebuild And Validate Derived Search After Import/Sync

Status: complete.

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

Implemented notes:

- Source import and sync now surface exact follow-up commands for rebuilding
  OpenSearch from PostgreSQL-backed state and validating it with `search check`;
  no-op syncs still recommend the cheap consistency check.
- Package coverage runs source import, complete reindex, clean search check,
  unsupported/stale-scope drift checks, and provider-unavailable checks against
  imported clients, environments, nodes, roles, plain data bags, and
  encrypted-looking data bag items.
- Migration functional coverage now validates `admin search check --all-orgs`
  after restored-target reindex and asserts unsupported search scopes are absent.

### Task 10: Implement Shadow-Read Comparison Normalizers

Status: complete.

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

Implemented notes:

- Added `opencook admin migration shadow compare --source PATH
  --target-server-url URL`, with optional restored-target manifest validation
  and the same signed target admin configuration overrides as cutover rehearsal.
- Shadow compare derives representative reads from normalized source metadata,
  issues only target `GET` requests plus unsigned signed-blob downloads, and
  skips unsafe write-only surfaces such as sandbox commit instead of proxying
  writes.
- Compatibility normalization now covers unordered membership/file arrays,
  volatile generated timestamps, private key omission, key-list API-version
  public-key differences, generated read URIs, and signed blob URL query/host
  differences while preserving order-sensitive fields such as run lists.
- Blockers are reported for target read/auth/missing-object failures, payload
  mismatches, search count drift, missing or invalid signed blob downloads, and
  unsupported normalized source families.

### Task 11: Harden Cutover Rehearsal And Rollback Runbooks

Status: complete.

- Extend `migration cutover rehearse` to consume import/sync progress and
  shadow-read comparison outputs.
- Add cutover gates for source import success, sync freshness, reindex
  cleanliness, blob reachability, signed auth, representative reads, and
  rollback readiness.
- Document client cutover sequencing for DNS/load balancers, Chef/Cinc client
  configuration, source read/write freeze windows, and rollback.
- Keep source Chef Infra Server available until post-cutover smoke checks pass.
- Add runbook output that clearly distinguishes blockers from advisory warnings.

Implemented:

- `migration cutover rehearse` accepts optional `--source`,
  `--source-import-progress`, `--source-sync-progress`, `--search-check-result`,
  `--shadow-result`, and `--rollback-ready` evidence flags while preserving the
  old manifest-only read-only rehearsal flow.
- Source import completion, source sync cursor freshness, search cleanliness,
  shadow-read comparison success, signed-auth reads, cookbook blob downloads,
  and rollback acknowledgement are surfaced as explicit dependencies.
- Missing optional evidence is advisory; failing provided evidence becomes a
  blocker with stable finding codes and top-level non-zero admin exit behavior.
- The migration runbook now documents final source write freeze, reindex/search,
  shadow comparison, DNS/load-balancer or Chef/Cinc `chef_server_url` cutover,
  and rollback availability sequencing.

### Task 12: Extend Functional Docker Coverage

Status: complete.

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

Implemented:

- Added opt-in Docker functional phases for source normalize, source import
  preflight/apply, source reindex/search-check, source sync preflight/apply,
  shadow compare, hardened source cutover rehearsal, and an aggregate
  `migration-source-all` drill.
- The source drill uses the baked Chef source fixture, the harness-managed
  restore PostgreSQL database, filesystem blob storage, and the shared
  OpenSearch container without bind mounts, so it still works with remote
  Docker daemons.
- Hardened functional rehearsal now passes source import progress, source sync
  progress, search-check output, shadow-compare output, and explicit rollback
  readiness into `migration cutover rehearse`.
- Generated source/import/search/shadow/cutover artifacts live under the
  Compose-managed functional state volume and are removed unless `KEEP_STACK=1`
  or `OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` is set.
- The default functional flow remains unchanged; the heavier source
  import/sync/shadow drill is opt-in through the new phases or `migration-all`.

### Task 13: Sync Docs And Close The Bucket

Status: complete.

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

Implemented:

- Synced the roadmap, milestones, compatibility matrix, README, AGENTS guidance,
  functional testing guide, operational runbook, and this plan around the
  completed normalized source import/sync plus shadow-read/cutover contract.
- Marked the bucket complete for implemented normalized source families and
  documented the current boundary: direct live upstream extraction and
  production-scale validation remain follow-on work.
- Pointed the next recommended operational bucket at live maintenance-mode
  request blocking plus cache-safe online repair/cutover controls, with
  deployment-test-discovered Chef compatibility gaps still allowed to interrupt
  if they prove higher risk.

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
