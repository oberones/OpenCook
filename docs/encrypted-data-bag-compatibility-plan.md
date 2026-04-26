# Encrypted Data Bag Compatibility Plan

Status: complete

## Summary

This bucket pins encrypted data bag compatibility on top of the existing data bag CRUD, PostgreSQL core object persistence, active OpenSearch-backed search, and operational reindex/repair work.

The compatibility goal is not to implement a new encryption system in OpenCook. Chef and Cinc clients encrypt and decrypt data bag item fields locally using their configured data bag secret. Chef Infra Server stores those encrypted field envelopes as ordinary JSON, returns them unchanged, indexes the stored JSON representation, and enforces authorization at the data bag level.

OpenCook should therefore treat encrypted data bag items as opaque data bag item payloads while proving that the full lifecycle works through:

- default-org and explicit-org routes
- in-memory and PostgreSQL-backed persistence
- memory-backed and OpenSearch-backed search
- partial search
- ACL filtering
- invalid-write no-mutation behavior
- restart, reindex, and repair paths
- Docker functional coverage

## Current State

OpenCook already has:

- `/data`, `/data/{bag}`, and `/data/{bag}/{item}` on default-org and explicit-org routes.
- Data bag list/get/create/delete.
- Data bag item get/create/update/delete.
- Chef-style data bag item response wrapping with `chef_type` and `data_bag`.
- Parent-data-bag authorization for item create/read/update/delete.
- PostgreSQL-backed persistence for data bags and data bag items.
- Active OpenSearch indexing for data bag items when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Dynamic data bag search indexes and partial search.
- Operational reindex/check/repair for data bag indexes.

This bucket now adds:

- explicit encrypted data bag item fixtures and tests
- route-level proof that encrypted field envelopes round-trip unchanged
- restart/rehydration coverage for encrypted-looking item payloads
- search and partial-search coverage for encrypted field envelopes
- operational reindex/check/repair coverage for encrypted-looking item search documents
- functional Docker coverage for encrypted-looking item payloads
- documentation that clearly states OpenCook does not decrypt or manage encrypted data bag secrets

## Upstream Compatibility Signals

Primary local upstream references:

- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/data_bag/complete_endpoint_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/data_bag_util.rb`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_data.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_objects/test/chef_data_bag_item_tests.erl`
- `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`

The important signals for this bucket are:

- Pedant models data bag items as arbitrary JSON objects with an `id`.
- The Chef-facing route surface for encrypted data bags is the normal data bag item route surface.
- Erchef adds or normalizes `chef_type: data_bag_item` and `data_bag: <bag>` on returned item payloads.
- Data bag items are authorized through the parent data bag, not independent item ACLs.
- Search treats each data bag as a dynamic index of data bag items.
- Encrypted data bag item fields are client-side encrypted JSON envelopes; the server should not need the secret to store, return, index, reindex, or repair them.

## Interfaces And Behavior

- Do not add, remove, or rename Chef-facing data bag routes.
- Do not add secret upload, secret storage, decryption, encryption, key rotation, or key management behavior.
- Do not require Chef/Cinc client changes.
- Preserve existing signed-auth behavior.
- Preserve existing data bag item authorization through the parent data bag.
- Preserve existing response wrapping for data bag item create/update/delete.
- Preserve existing default-org and explicit-org aliases.
- Preserve existing in-memory fallback behavior when PostgreSQL is not configured.
- Preserve PostgreSQL as the source of truth when configured.
- Preserve OpenSearch as a derived search projection when configured.
- Treat encrypted field envelopes as ordinary JSON values. Do not validate or reject envelope internals such as `encrypted_data`, `iv`, `version`, `cipher`, or `auth_tag`.
- Do not attempt to search decrypted plaintext. Search only sees stored encrypted JSON envelope values and metadata.
- Treat Chef Vault payloads as normal data bag item JSON unless a future compatibility slice proves server-side behavior is required.

## Frozen Payload Contract

The first implementation should use fixture payloads that represent the major encrypted data bag shapes Chef clients emit, without requiring OpenCook to know or validate their cryptographic meaning:

- A simple encrypted field:
  - `id`
  - `password.encrypted_data`
  - `password.iv`
  - `password.version`
  - `password.cipher`
- A GCM-style encrypted field:
  - `api_key.encrypted_data`
  - `api_key.iv`
  - `api_key.auth_tag`
  - `api_key.version`
  - `api_key.cipher`
- A mixed item containing encrypted fields plus ordinary clear metadata:
  - `id`
  - `kind`
  - `environment`
  - one or more encrypted field envelopes
- An item with arrays or nested encrypted fields, to prove OpenCook clones and persists nested JSON without flattening it into a different response shape.

The server-side contract is semantic JSON preservation, not byte-for-byte preservation of request key order.

## Task Breakdown

### Task 1: Freeze The Encrypted Data Bag Contract

Status:

- Completed. The plan now records the upstream data bag compatibility signals, server-opacity contract, explicit non-goals, and the initial encrypted payload shapes.
- Added shared fixture helpers in `internal/testfixtures` for ordinary, simple encrypted, GCM-style encrypted, updated, and deeply nested encrypted-looking data bag item payloads.
- Added fixture contract tests that keep the ordinary baseline shape separate from encrypted-looking envelope shapes and prove fixture clones are independent for later route/search mutation tests.

- Add canonical encrypted data bag item fixtures for route, store, search, and functional tests.
- Record the current upstream signals from pedant and `oc_erchef`.
- Define the explicit non-goals:
  - no server-side encryption or decryption
  - no data bag secret storage
  - no encrypted payload schema validation
  - no Chef Vault-specific semantics
- Pin the current OpenCook baseline for ordinary data bag item shape before adding encrypted fixtures.

### Task 2: Pin Encrypted Item CRUD Round-trip Behavior

Status:

- Completed. Added route-level encrypted-looking item lifecycle coverage for default-org and explicit-org data bag routes.
- Pinned opaque preservation of simple, GCM-style, and nested encrypted field envelopes across create, read, update, item delete, and bag-delete cascade flows.
- Pinned Chef-style `chef_type` and `data_bag` wrapping on create/update/delete responses while keeping direct item reads as raw stored JSON.
- Pinned compatible `id` handling for encrypted-looking items: create still requires `id`, update accepts missing body `id` from the URL, mismatched update `id` fails, and failed writes leave the stored item unchanged.

- Add route-level coverage for encrypted-looking item create/read/update/delete on:
  - `/data/{bag}`
  - `/data/{bag}/{item}`
  - `/organizations/{org}/data/{bag}`
  - `/organizations/{org}/data/{bag}/{item}`
- Prove create and update preserve nested encrypted field maps.
- Prove `chef_type` and `data_bag` response fields are added exactly as existing data bag item routes do today.
- Prove route/body `id` handling stays compatible:
  - create requires `id`
  - update accepts missing body `id` and uses the URL item name
  - update rejects mismatched body `id`
- Prove bag delete cascades encrypted items exactly like ordinary items.

### Task 3: Preserve Payload Opacity In Bootstrap And Persistence

Status:

- Completed. Added bootstrap service tests proving encrypted-looking payloads are cloned on create, read, and update, and that mutating caller-owned or returned nested maps cannot mutate stored state.
- Added PostgreSQL core-object encode/decode coverage for simple, GCM-style, and deeply nested encrypted-looking data bag item payloads.
- Added active PostgreSQL restart/rehydration coverage proving encrypted-looking field envelopes survive create/restart/read, update/restart/read, and delete/restart/not-found flows.
- Pinned preservation of nested maps, arrays, booleans, numbers, `null`, and unknown encrypted-envelope keys across the in-memory bootstrap and PostgreSQL persistence paths.

- Add focused `internal/bootstrap` tests proving encrypted-looking payloads are cloned, stored, copied, and updated as opaque JSON.
- Add `internal/store/pg` encode/decode round-trip coverage for encrypted-looking item payloads.
- Prove PostgreSQL restart/rehydration preserves encrypted field envelopes for:
  - create then restart then read
  - update then restart then read
  - delete then restart then not found
- Ensure no persistence path drops nested maps, arrays, booleans, numbers, `null`, or unknown envelope keys.

### Task 4: Pin Validation, Auth, And No-mutation Behavior

Status:

- Completed. Added encrypted-looking route coverage for malformed JSON, trailing JSON, empty bodies, missing `id`, invalid `id`, mismatched `id`, missing data bags, missing orgs, and method-not-allowed responses while preserving the existing Chef-shaped error bodies.
- Added opaque encrypted-envelope variants for missing `iv`, unknown `version`, unknown `cipher`, extra envelope fields, and non-string encrypted values, proving create/update does not reject payloads for cryptographic-shape reasons.
- Added parent-data-bag ACL coverage showing normal org users can mutate encrypted-looking items through the parent bag ACL while outside or invalid requestors cannot, with denied writes leaving item and search-visible state unchanged.
- Added active PostgreSQL and active OpenSearch-backed no-mutation coverage proving invalid encrypted writes do not mutate live service state, persisted PostgreSQL state after restart, memory search projections, or provider-backed OpenSearch documents.

- Add encrypted fixture variants to invalid-write coverage.
- Prove malformed JSON, trailing JSON, empty payloads, missing `id`, invalid `id`, mismatched `id`, missing bag, missing org, and method-not-allowed cases preserve existing data bag error shapes.
- Prove encrypted envelope variants with missing `iv`, unknown `version`, unknown `cipher`, extra fields, or non-string encrypted values are not rejected merely because they do not look cryptographically valid.
- Prove unauthorized or invalid encrypted item writes do not mutate:
  - live service state
  - persisted PostgreSQL state
  - search-visible state
  - OpenSearch documents
- Preserve parent-data-bag ACL behavior; do not add per-item ACL documents.

### Task 5: Pin Search And Partial Search Semantics

Status:

- Completed. Added memory-backed route coverage proving encrypted-looking data bag items are searchable as opaque stored JSON, full search rows include Chef-style `raw_data`, partial search can select encrypted envelope fields plus clear metadata, and no decoded plaintext or secret material is required.
- Added active PostgreSQL plus OpenSearch-backed route coverage for the same encrypted fixture, including provider-backed query matching on envelope fields and metadata.
- Preserved current `raw_data_` prefix miss behavior and added encrypted-data-bag ACL-filtering coverage after both memory and OpenSearch provider matching.

- Add memory-backed search coverage for encrypted-looking data bag items.
- Add active PostgreSQL plus OpenSearch-backed route coverage for the same fixtures.
- Prove full search returns Chef-style data bag item rows containing the encrypted JSON under `raw_data`.
- Prove partial search can select encrypted envelope fields such as `password.encrypted_data`, `password.iv`, `api_key.auth_tag`, and clear metadata fields.
- Prove search queries can match stored encrypted envelope values and metadata where the current query subset supports them.
- Prove search queries do not expose decrypted plaintext and do not require a secret.
- Preserve current `raw_data_` prefix behavior unless upstream inventory proves a change is required.
- Preserve ACL filtering after search-provider matching.

### Task 6: Pin Reindex, Repair, And Provider Behavior

Status:

- Completed. Added search-service coverage proving encrypted-looking data bag documents are reindexed from PostgreSQL-derived state and search check/repair detects missing and stale provider IDs, upserts the opaque stored JSON, deletes stale IDs, and leaves `raw_data_` field behavior unchanged.
- Added `opencook admin reindex --org ORG --index BAG` coverage for encrypted data bag indexes, including provider document field assertions and redacted provider-unavailable failure handling.
- Added `opencook admin search check/repair --org ORG --index BAG` coverage for encrypted data bag indexes, including missing/stale drift reporting, repair upsert/delete behavior, clean follow-up checks, and redacted provider-unavailable failure handling.
- Added Chef-facing active PostgreSQL plus OpenSearch unavailable route coverage for encrypted data bag search so `503 search_unavailable` remains stable and provider details stay hidden.

- Add reindex/check/repair coverage for encrypted-looking data bag item documents.
- Prove `opencook admin reindex --org ORG --index BAG` rebuilds encrypted item search documents from PostgreSQL-backed state.
- Prove `opencook admin search check --org ORG --index BAG` detects missing or stale encrypted item documents.
- Prove `opencook admin search repair --org ORG --index BAG --yes` restores or removes encrypted item drift without needing a secret.
- Keep provider-unavailable behavior on Chef-facing search routes and admin search commands stable and redacted.

### Task 7: Extend Functional Docker Coverage

Status:

- Completed. Extended the functional Go phases with encrypted-looking data bag create, read, full search, partial search, invalid-write no-mutation, update, stale-term removal, delete, and post-restart absence checks.
- Extended the operational functional shell phase so complete org reindex runs with encrypted data bag documents present, scoped encrypted-index reindex is exercised, encrypted-index search consistency is checked after restart, and stale encrypted-index OpenSearch drift is detected, dry-run repaired, actually repaired, and verified clean.
- Kept the fixture deterministic and local; no Chef data bag secret, decryption, or network dependency beyond the existing PostgreSQL/OpenSearch Compose stack is required.

- Extend the functional Compose flow with encrypted-looking data bag fixtures.
- Cover create, read, search, partial search, restart verification, update, stale-term removal, delete, and post-restart absence.
- Keep the functional fixture local and deterministic; do not require real Chef client secrets or network calls.
- Ensure the operational reindex/repair phases still pass with encrypted-looking data bag documents present.

### Task 8: Sync Docs And Close The Bucket

Status:

- Completed. Synced `README.md`, the roadmap, milestones, compatibility matrix, functional testing guide, `AGENTS.md`, root next-step wording, and this plan to mark encrypted data bag compatibility complete.
- The next recommended bucket is broader Lucene/query-string search compatibility, with cookbook/policy/sandbox search coverage, deeper API-version-specific object semantics, and migration/cutover tooling remaining visible follow-on candidates.

- Update:
  - `README.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `AGENTS.md`
  - this plan file
- Mark encrypted data bag compatibility complete.
- Point the next bucket at the strongest remaining compatibility gap after implementation, likely one of:
  - broader Lucene/query-string semantics
  - cookbook/policy/sandbox search coverage
  - deeper API-version-specific object semantics
  - migration/cutover tooling

## Bucket Status

Encrypted data bag compatibility is complete for the current server-opacity contract. OpenCook now treats encrypted-looking data bag items as opaque JSON across default-org and explicit-org CRUD, in-memory and PostgreSQL persistence, memory and OpenSearch search, partial search, ACL filtering, invalid-write no-mutation behavior, operational reindex/check/repair, and Docker functional coverage.

The next recommended bucket is broader Lucene/query-string search compatibility. Cookbook/policy/sandbox search coverage, deeper API-version-specific object semantics, and migration/cutover tooling remain follow-on candidates.

## Test Plan

Focused tests:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
scripts/functional-compose.sh
```

Required scenarios:

- encrypted-looking item create/read/update/delete on default-org and explicit-org routes
- encrypted-looking nested JSON round-trips through in-memory and PostgreSQL-backed state
- encrypted-looking items survive restart and disappear after delete across restart
- invalid encrypted item writes do not partially mutate service, persistence, or search state
- memory-backed and OpenSearch-backed search see the same encrypted item contract
- partial search can select encrypted envelope fields without decrypting
- reindex/check/repair works for encrypted item documents
- Docker functional coverage proves encrypted item lifecycle across PostgreSQL and OpenSearch

## Assumptions And Defaults

- Encrypted data bag compatibility is primarily a server opacity contract.
- Chef and Cinc clients remain responsible for encryption and decryption.
- OpenCook should not know, store, or validate data bag secrets.
- OpenCook should not reject encrypted envelopes just because the server cannot prove they are cryptographically valid.
- OpenCook should preserve JSON semantics, not request byte order.
- Chef Vault remains out of scope unless later upstream inventory proves a required server-side behavior.
- Broader search language behavior remains out of scope except where existing data bag search behavior already supports a query.
