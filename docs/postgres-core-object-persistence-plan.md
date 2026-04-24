# PostgreSQL Core Object API Persistence Plan

## Summary

This slice replaces the remaining in-memory core object API state with PostgreSQL-backed persistence after the cookbook and bootstrap-core cutovers.

The target objects are:

- nodes
- environments
- roles
- data bags and data bag items
- policy revisions, policy groups, and policy assignments
- sandbox metadata

The goal is to make the already-implemented Chef-facing object APIs durable across app restarts while preserving existing route behavior, authorization behavior, response shapes, and compatibility-focused validation. This slice should not redesign search indexing, cookbook storage, validator-authenticated client registration, or the public object APIs.

## Interfaces And Behavior

- No Chef-facing route, payload, error-shape, method, trailing-slash, or signed URL changes.
- Keep `bootstrap.Service` as the place where compatibility validation, normalization, authorization side effects, and route-facing semantics are enforced.
- Add a persistence seam for the remaining core object API state, following the current activated repository/cache model rather than switching reads to live SQL queries.
- Keep the existing in-memory behavior when PostgreSQL is not configured.
- Rehydrate persisted object state during app startup before serving requests.
- Preserve object ACL behavior across restarts for nodes, environments, roles, data bags, policies, and sandboxes.
- Keep the `/status` payload keys stable. Human-readable backend wording may become more precise once this slice is active.
- Do not add repair/admin endpoints, cross-process cache invalidation, OpenSearch indexing replacement, or validator-authenticated client registration in this slice.

## Current Contract Inventory

The in-scope state currently lives behind `bootstrap.Service` methods and route handlers:

- Nodes:
  - org-scoped records with name, chef type, JSON class, normal attributes, default attributes, override attributes, automatic attributes, run list, policy name, and policy group.
  - create/update/delete behavior creates and removes object ACL entries and feeds search-facing node reads.
- Environments:
  - org-scoped records with name, description, cookbook version constraints, default attributes, override attributes, and immutable `_default` behavior.
  - environment writes feed environment reads, environment-linked role reads, environment cookbook views, environment node views, environment recipe views, and depsolver constraints.
- Roles:
  - org-scoped records with name, description, run list, environment run lists, default attributes, and override attributes.
  - role writes feed role reads, environment-linked role reads, search-facing role reads, and depsolver role expansion.
- Data bags:
  - org-scoped bag records and item records with raw item payloads.
  - data bag writes feed data bag routes and live per-bag search routes.
- Policies:
  - org-scoped policy revision records, policy groups, and group-to-policy assignments.
  - policy writes preserve canonical payload round-tripping for named run lists, cookbook lock metadata, and solution dependencies.
- Sandboxes:
  - org-scoped sandbox metadata with checksum upload items and commit state.
  - sandbox writes coordinate with blob checksum existence and later cookbook/blob checksum retention behavior.
- Object ACLs:
  - object create/delete flows currently add or remove ACL documents for created nodes, environments, roles, data bags, policy objects, and sandboxes.
  - persisted object state must not rehydrate without the matching ACL documents, otherwise reads and mutations after restart will drift from same-process behavior.

Out of scope for this bucket:

- cookbook metadata persistence, which is already active
- bootstrap identity and authorization core persistence, which is already active
- validator-authenticated client registration compatibility
- OpenSearch-backed indexing and query parity
- `chef-server-ctl`-style administrative replacement commands
- new storage abstractions for cookbook blobs
- live SQL query-on-read behavior

## Task Breakdown

### Task 1: Freeze The Core Object Persistence Contract

- Inventory the exact state shape, ACL side effects, and read dependencies for nodes, environments, roles, data bags, policies, and sandboxes.
- Add or extend test helpers for an activated `pg.Store` that includes bootstrap core persistence, cookbook persistence, and the new object persistence path.
- Define restart/rehydration expectations for each object family.
- Capture the compatibility contract in this plan before implementation changes:
  - read behavior
  - create/update/delete behavior
  - default-org and org-scoped alias behavior
  - object ACL creation and deletion
  - search-facing in-memory index hydration
  - depsolver reads after restart
  - sandbox checksum and commit behavior after restart

### Task 2: Extract A Core Object Store Interface With An In-Memory Adapter

- Add a bootstrap object persistence interface beside the existing bootstrap-core and cookbook store seams.
- Keep validation, normalization, authorization checks, and route-facing semantics in `bootstrap.Service`.
- Move direct map ownership for nodes, environments, roles, data bags, policies, and sandbox metadata behind an in-memory adapter.
- Preserve the current default behavior when PostgreSQL is absent.
- Add delegation tests proving create/update/delete paths still normalize in `bootstrap.Service` and persist through the object store.
- Add failure tests proving failed object-store writes do not partially update service state, object ACL state, search-facing state, sandbox state, or cookbook/depsolver-visible state.

### Task 3: Add PostgreSQL Schema And Repository Scaffold

- Add migrations for:
  - nodes
  - environments
  - roles
  - data bags
  - data bag items
  - policy revisions
  - policy groups
  - policy group assignments
  - sandboxes
  - sandbox checksum items
- Use org-scoped uniqueness constraints that match current Chef-facing identity rules.
- Store compatibility-sensitive payloads as structured JSON where that avoids premature relational modeling.
- Store policy revision payload details in a way that preserves current canonical round-tripping.
- Store sandbox checksum item state separately from blob bytes. Blob bytes remain in the configured blob provider.
- Add encode/decode round-trip tests for every persisted model.
- Add migration exposure tests matching the existing PostgreSQL persistence patterns.

### Task 4: Persist Nodes, Environments, And Roles

- Persist node create/read/list/update/delete state and matching node ACL lifecycle.
- Persist environment create/read/list/update/delete state and matching environment ACL lifecycle.
- Preserve `_default` environment bootstrap and immutability semantics.
- Persist role create/read/list/update/delete state and matching role ACL lifecycle.
- Preserve role run-list normalization, environment run-list normalization, and linked missing-environment behavior.
- Add restart/rehydration route tests for:
  - `/nodes`
  - `/nodes/{name}`
  - `/environments`
  - `/environments/{name}`
  - `/environments/{name}/nodes`
  - `/environments/{name}/roles/{role}`
  - `/roles`
  - `/roles/{name}`
  - `/roles/{name}/environments`
  - `/roles/{name}/environments/{environment}`
- Add depsolver restart coverage proving persisted environments and roles are visible before the first post-start request.

### Task 5: Persist Data Bags And Data Bag Items

- Persist data bag create/read/list/delete state and matching bag ACL lifecycle.
- Persist data bag item create/read/update/delete state while preserving raw item payload shape.
- Preserve Chef-style wrapper and error response shaping.
- Add restart/rehydration route tests for:
  - `/data`
  - `/data/{bag}`
  - `/data/{bag}/{item}`
  - `/organizations/{org}/data`
  - `/organizations/{org}/data/{bag}`
  - `/organizations/{org}/data/{bag}/{item}`
- Add search-facing restart coverage for live per-data-bag indexes using rehydrated data bag state.
- Pin encrypted data bag payloads as raw JSON round-trips if the current implementation treats them as opaque item data. Do not add deeper encryption semantics in this slice.

### Task 6: Persist Policies, Policy Groups, And Assignments

- Persist policy revision create/read/list/delete state.
- Persist policy group create/read/list/delete state.
- Persist policy group assignment create/update/delete state.
- Preserve canonical payload round-tripping for:
  - `named_run_lists`
  - cookbook lock metadata
  - `solution_dependencies`
  - revision identifiers
  - policy group assignment payloads
- Add restart/rehydration route tests for:
  - `/policies`
  - `/policies/{name}`
  - `/policies/{name}/revisions`
  - `/policies/{name}/revisions/{revision}`
  - `/policy_groups`
  - `/policy_groups/{group}`
  - `/policy_groups/{group}/policies/{name}`
  - the matching `/organizations/{org}/...` policy routes
- Confirm rehydrated node policy reference fields still behave as search-facing compatibility fields, not foreign keys.

### Task 7: Persist Sandbox Metadata And Checksum State

- Persist sandbox create/read/delete metadata and matching sandbox ACL lifecycle.
- Persist sandbox checksum item state and commit status.
- Preserve signed upload URL shape and checksum upload behavior.
- Keep checksum blob bytes in the selected blob provider.
- Add restart/rehydration route tests for:
  - `/sandboxes`
  - `/sandboxes/{id}`
  - `/organizations/{org}/sandboxes`
  - `/organizations/{org}/sandboxes/{id}`
- Add provider-backed blob coverage proving committed sandbox metadata survives restart while blob availability is still determined by the blob backend.
- Add checksum retention coverage proving sandbox-held checksums still prevent cookbook/blob cleanup after persisted state is rehydrated.

### Task 8: Activate PostgreSQL Object Persistence In The App

- Extend app startup so a configured PostgreSQL store activates bootstrap core persistence, cookbook persistence, and core object API persistence in one startup path.
- Rehydrate object state before serving requests.
- Rebuild any in-memory read indexes needed by existing search, depsolver, environment, role, and sandbox routes from persisted state.
- Preserve idempotent startup against already-seeded default org, default environment, default groups, default containers, bootstrap users, and existing object rows.
- Keep `/status` payload keys stable while updating human-readable wording to describe active PostgreSQL object persistence truthfully.
- Add activation failure tests for migration and load errors.
- Add repeated app-construction tests against the same database.
- Add default in-memory mode tests proving no PostgreSQL configuration still behaves as it does today.

### Task 9: Pin Failure And Consistency Behavior

- Add write failure tests for every object family proving failed persistence does not partially mutate:
  - object service state
  - object ACL state
  - search-facing state
  - depsolver-visible state
  - sandbox checksum state
  - auth verifier state, where key-backed actors are involved indirectly
- Add invalid write no-mutation tests on the active PostgreSQL path matching current in-memory behavior.
- Add delete failure tests proving object rows, ACL rows, and derived read indexes remain consistent.
- Add restart-after-failure coverage where feasible, proving only committed rows rehydrate.
- Preserve current error shapes and avoid leaking database internals through route responses.

### Task 10: Sync Docs And Close The Bucket

- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `AGENTS.md`
  - `docs/postgres-core-object-persistence-plan.md`
- Mark PostgreSQL core object API persistence complete once the implementation and tests land.
- Point the next bucket at either:
  - Milestone 7 validator bootstrap compatibility, if client registration is the most urgent compatibility gap
  - OpenSearch-backed indexing, if durable object state makes search parity the next strongest dependency
  - administrative replacement commands, if operational management becomes the nearer blocker

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

Required scenarios:

- default in-memory object behavior remains unchanged
- active PostgreSQL startup rehydrates nodes, environments, roles, data bags, policy state, and sandboxes
- object ACLs persist and authorize correctly after restart
- default-org and org-scoped aliases behave identically to the current compatibility contract
- invalid writes and failed persistence writes do not partially mutate service state
- depsolver sees rehydrated environments and roles on the first post-start request
- search-facing routes see rehydrated nodes, environments, roles, and data bag items on the first post-start request
- policy revision and policy group payloads round-trip canonically after restart
- sandbox commit state and checksum references survive restart
- sandbox-held checksums still prevent blob cleanup after persisted state is rehydrated
- route responses preserve existing compatibility error shapes without leaking PostgreSQL internals

## Assumptions And Defaults

- Complex compatibility payloads can be stored as structured JSON in this slice to avoid premature relational modeling.
- PostgreSQL reads should continue using the activated repository/cache model; do not convert route reads to live SQL queries here.
- Existing in-memory search behavior remains the route-facing implementation for this bucket, hydrated from persisted object state at startup.
- OpenSearch-backed indexing remains a later bucket.
- Validator-authenticated client registration remains a separate Milestone 7 compatibility bucket.
- Blob bytes remain in the configured blob provider; only sandbox metadata and checksum references move into PostgreSQL.
- The implementation should prefer transaction boundaries that update object rows, ACL rows, and derived bootstrap snapshots atomically where possible.
