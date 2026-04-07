# OpenCook Chef Infra Server Rewrite Roadmap

## Goal

Build a new Chef Infra Server implementation in Go that is operationally modern but wire-compatible with existing Chef clients, `knife`, `chef-server-ctl`, and ecosystem tooling.

Compatibility target:

- Same external API shape and semantics
- Same request authentication behavior
- Same authorization behavior
- Same object model and data flow
- Same HA deployment capabilities with external PostgreSQL and OpenSearch
- Better support for current PostgreSQL and OpenSearch releases than upstream Chef Infra Server

Intentional divergence from upstream Chef Infra Server:

- no licensing subsystem
- no license enforcement
- no license telemetry or license-management endpoints

This roadmap is based on a review of the upstream Chef Infra Server repository at `~/Projects/coding/ruby/chef-server`, especially:

- `README.md`
- `src/nginx/habitat/config/routes.lua`
- `src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_base.erl`
- `src/oc_erchef/apps/chef_index/src/chef_opensearch.erl`
- `dev-docs/SEARCH_AND_INDEXING.md`
- `dev-docs/API_VERSIONING.md`
- `dev-docs/BOOKSHELF.md`
- `dev-docs/bookshelf-sigv4-flow.txt`
- `oc-chef-pedant/`

## Current Progress Snapshot

As of 2026-04-07, OpenCook has moved past pure scaffolding and into the first compatibility slices:

- Chef request signing verification is implemented in Go and enforced on the first authenticated routes
- initial user, organization, client, group, container, and ACL bootstrap flows are working in an in-memory compatibility layer
- org bootstrap creates validator clients with default key material
- actor key lifecycle now supports list, create, update, delete, and expiration-aware authentication behavior
- the first core object slice is live with in-memory node list/get/head/create/update/delete behavior
- the adjacent environment slice is now live with `_default`, list/get/head/create/update/delete, and rename-capable `PUT`
- environment-scoped node listing is implemented via `/environments/{name}/nodes`
- environment-scoped cookbook and recipe views are now implemented via `/environments/{name}/cookbooks`, `/environments/{name}/cookbooks/{cookbook}`, and `/environments/{name}/recipes`
- the first environment depsolver slice is now live via `/environments/{name}/cookbook_versions` and `/organizations/{org}/environments/{name}/cookbook_versions`
- default-org and explicit-org node routes now resolve against the same org-scoped compatibility state
- default-org and explicit-org environment routes now resolve against the same org-scoped compatibility state
- the first role slice is now live with in-memory list/get/head/create/update/delete behavior
- role environment endpoints are implemented via `/roles/{name}/environments` and `/roles/{name}/environments/{environment}`
- default-org and explicit-org role routes now resolve against the same org-scoped compatibility state
- default-org client routes are now live for `/clients`, `/clients/{name}`, and `/clients/{name}/keys`
- the first data bag slice is now live with `/data`, `/data/{bag}`, and `/data/{bag}/{item}` on both default-org and explicit-org routes
- data bag item create, update, and delete flows now reproduce Chef-style response wrapping and not-found/conflict messages
- the first search-facing slice is now live with `/search` and `/search/{client,environment,node,role}` over the in-memory compatibility state
- `/search` now also advertises live per-data-bag indexes, and `/search/{bag}` now supports Chef-style data bag search results
- partial search is now implemented for clients, environments, nodes, roles, and data bags
- search responses are now filtered through the current read ACL model
- default-org search results for clients now point at live `/clients/...` URLs
- the in-memory search layer now covers simple `AND`/`NOT` matching and escaped-slash prefix queries for the current compatibility slice
- the policyfile slice is now live on both default-org and explicit-org routes with `/policies`, `/policies/{name}`, `/policies/{name}/revisions`, `/policies/{name}/revisions/{revision}`, `/policy_groups`, and `/policy_groups/{group}/policies/{name}`
- policy revision create/get/delete, policy-group listing, policy-group get/delete, and policy-group assignment flows are now working in the in-memory compatibility layer
- policy payload normalization now preserves richer canonical structures like `named_run_lists`, nested cookbook-lock metadata, and `solution_dependencies`, with deeper validation around cookbook lock versions and shapes
- node `policy_name` and `policy_group` semantics remain compatibility-safe searchable fields rather than newly enforced foreign keys
- the first sandbox/blob slice is now live with signed sandbox create/commit flows, absolute checksum upload URLs, and in-memory checksum blob storage
- the first provider-backed blob seam is now live with backend selection, a filesystem adapter for local dev/test persistence, and S3-compatible request-path blob operations for configured endpoints and static credentials
- the S3-compatible blob path now has configurable request timeout and retry settings, and transient/provider-auth failures now degrade through the existing blob-unavailable path instead of surfacing as generic internal blob failures
- sandbox commit now enforces upload completeness before marking a sandbox complete, matching the expected Chef-style lifecycle shape
- the first cookbook slice is now live with `PUT/GET/DELETE /cookbook_artifacts/{name}/{identifier}` plus collection and named-artifact reads on both default-org and explicit-org routes
- cookbook version create/update/delete behavior is now live on `/cookbooks/{name}/{version}` and `/organizations/{org}/cookbooks/{name}/{version}`
- cookbook read views are now implemented for `/cookbooks`, `/cookbooks/_latest`, `/cookbooks/_recipes`, and named cookbook/version reads on both default-org and explicit-org routes
- cookbook version payloads now preserve Chef-style `json_class`, `cookbook_name`, and v0/v2 file-shape conversion semantics in the current compatibility layer
- `/universe` is now live on both default-org and explicit-org routes, and cookbook file responses now return signed direct blob URLs backed by the in-memory compatibility blob store
- cookbook version updates now honor Chef-style frozen/force behavior, including `409` conflicts on frozen versions and forced updates that keep the cookbook frozen
- cookbook PUT responses now preserve pedant-style omission of optional top-level fields like `version`, `json_class`, and `chef_type`, and explicit `?force=false` now has matching compatibility coverage
- cookbook create/update HTTP coverage now includes omitted-default exactness, top-level `json_class`/`chef_type`/`version` validation, invalid request-key rejection, metadata-name write-vs-read canonicalization, permissive `metadata.providing`, exact no-mutation behavior for invalid metadata payloads, and malformed route-path handling for invalid cookbook names and version strings
- cookbook metadata validation now covers more upstream pedant cases for typed metadata fields, dependency/platform constraint maps, and checksum failure messaging on updates
- cookbook version reads now return the narrower Chef-shaped metadata subset with upstream defaults inflated at read time while PUT responses remain exact echoes of the submitted payload
- cookbook version conversion is now exercised across v0 and v2 upload/download paths, including the Chef-style segment-aware `all_files[].name` contract for root files and other segmented content
- cookbook named filters now more closely match upstream pedant behavior, with `/cookbooks/_recipes` deriving names from the latest cookbook manifests and qualifying default recipes as just the cookbook name
- environment-filtered cookbook and recipe views now honor environment cookbook constraints and named-cookbook default `num_versions` behavior on both default-org and explicit-org paths
- cookbook create-path validation now matches Chef’s `Field 'name' invalid` behavior for route/payload name-version mismatches, while update-path validation stays field-specific
- cookbook collection and named-version reads now have pedant-style coverage for `num_versions` validation/zero behavior and `_latest` not-found responses
- cookbook mutation coverage now includes pedant-style v0/v2 file-collection presence and omission exactness on successful update responses
- cookbook version updates/deletes and cookbook artifact deletes now reclaim unreferenced checksum blobs while preserving shared checksum content still referenced elsewhere in the in-memory compatibility state
- cookbook HTTP coverage now includes multi-version shared-checksum retention, successful in-org normal-user cookbook read/delete/create/update behavior, usable signed recipe download URLs, create/update no-mutation guarantees for failed outside-user and invalid-user cookbook mutations, file-set replacement behavior that deletes all or some cookbook files on update, invalid-checksum update rejection without mutating the existing cookbook file set, malformed negative and overflow route-version handling, and the expected invalid-user/outside-user auth behavior on cookbook routes
- cookbook artifact HTTP coverage now also includes wrong-identifier delete no-mutation behavior, successful in-org normal-user artifact read/delete behavior, usable signed artifact recipe download URLs, and the expected invalid-user/outside-user auth behavior on cookbook artifact routes
- cookbook artifact read coverage now also includes empty and multi-identifier collections, named-artifact collection views, and explicit API v2 `all_files` response shaping
- cookbook artifact create/update coverage now also includes large-component and prerelease versions, invalid route name/identifier rejection, exact route/payload name and identifier mismatch errors, repeated-`PUT` `409` conflict behavior, and no-mutation behavior for failed outside-user and invalid-user updates
- cookbook artifact create coverage now also includes metadata default overrides and multi-identifier create behavior for the same cookbook name
- cookbook artifact validation HTTP coverage now also includes missing metadata versions, invalid legacy segment shapes, and invalid metadata dependency/platform payloads
- cookbook artifact create auth coverage now also includes normal-user create success plus invalid/outside no-mutation behavior
- the first depsolver slice now validates cookbook run lists, honors environment cookbook constraints and version pins, expands recursive cookbook dependencies, enforces cookbook-container read auth alongside environment read, returns the upstream-style minimal cookbook payloads that omit `metadata.attributes` and `metadata.long_description`, and returns the current Chef-shaped `400`/`404`/`412` responses for invalid, missing, filtered, datestamp, and dependency-failure cookbook cases
- compatibility tracking docs and route inventory are in place and being updated alongside code

Current focus:

- preserve API-version-sensitive actor key behavior without carrying forward Chef licensing concerns
- deepen search query translation beyond the current simple compatibility subset and widen object/index coverage further
- deepen cookbook/blob compatibility beyond the current cookbook write/read/artifact slice, especially the remaining cookbook pedant cases outside the current environment-filtered/named-filter/latest/version/depsolver contract and the remaining deeper provider hardening around S3-compatible object storage behavior
- replace the in-memory bootstrap layer with PostgreSQL-backed persistence after the contracts stabilize

## What Exists Upstream

Chef Infra Server is not one service today. It is a group of cooperating components:

- `oc_erchef`: core REST API
- `oc_bifrost`: authorization service
- `bookshelf`: cookbook/checksum blob storage service or S3-compatible mode
- `oc-id`: OAuth2 service for integrations
- `nginx/openresty`: routing, request shaping, and front-door behavior
- `chef-server-ctl`: operational CLI
- `oc-chef-pedant`: API compatibility test suite

Core platform expectations in the current server:

- PostgreSQL is the system of record
- OpenSearch/Elasticsearch is used for search indexing
- Redis is used in some supporting roles
- Request signing and key handling are part of the compatibility contract
- Authorization is ACL/group/container based and deeply integrated
- Search behavior depends on a specific document expansion format
- Bookshelf has its own S3-style behavior, including SigV4 edge cases

## Key Compatibility Constraints

These are the highest-risk areas for a rewrite because clients depend on behavior, not just endpoint names.

### 1. Request authentication must be behavior-compatible

The new server must preserve Chef header-based auth semantics, including:

- `X-Ops-*` headers
- canonical request construction
- key lookup behavior
- API version handling via `X-Ops-Server-API-Version`
- tolerance for legacy quirks that existing clients rely on

This also applies to Bookshelf-style signed upload/download flows and the known compatibility hacks around host header/port handling.

### 2. Routing must be endpoint-compatible

The upstream routing table in `routes.lua` shows a large compatibility surface, including:

- `/organizations/:org/...`
- `/users`
- `/authenticate_user`
- `/system_recovery`
- `/keys`
- `/_acl`
- `/nodes`
- `/clients`
- `/roles`
- `/data`
- `/sandboxes`
- `/environments`
- `/search`
- `/policies`
- `/policy_groups`
- `/cookbook_artifacts`
- `/universe`
- internal and dark-launch related routes

OpenCook should treat the route map as a contract artifact and generate tests from it.

One explicit exception is upstream licensing behavior. OpenCook should not reproduce license enforcement or license-management flows.

### 3. Authorization semantics must match Bifrost behavior

Chef authorization is not a generic RBAC layer. It includes:

- actors, groups, containers, ACLs
- recursive group membership
- object/container permission checks
- org-scoped and global group behavior
- default ACL generation during org/bootstrap flows

If this behavior diverges, clients may authenticate successfully but still fail in subtle ways.

### 4. Search behavior must match user expectations

Search compatibility is more than “return similar results.” The server must preserve:

- object-to-index expansion format
- per-type and per-data-bag indexing layout
- query translation behavior
- partial search behavior
- ACL filtering of search results
- synchronous write/index semantics expected by clients

Upstream already contains OpenSearch-version-specific logic. That is a sign this area needs an explicit compatibility layer instead of ad hoc version checks.

### 5. Key and API version behavior must remain stable

The upstream API versioning notes show user/client key management differences between API versions. The new server must reproduce:

- v0 and v1 behavior where clients still depend on it
- `/keys` endpoints and default key semantics
- public key lifecycle and backward-compatible responses

### 6. Pedant is the contract, not just the docs

`oc-chef-pedant` should be treated as a first-class acceptance suite for the rewrite. If OpenCook cannot pass pedant, it should not be considered compatible.

## Proposed Rewrite Strategy

### Principle 1: Compatibility first, simplification second

Do not begin by redesigning the API or object model. Begin with a compatibility shell that reproduces observed behavior, then simplify internals behind stable interfaces.

Compatibility is constrained by OpenCook's product stance: licensing behavior is intentionally not carried forward.

### Principle 2: One binary, multiple internal modules

Instead of re-creating Erlang-era service boundaries literally, implement a modular Go server with clear internal subsystems:

- API gateway/router
- authn/signature verification
- authz engine
- object store service
- search indexing/query service
- key management service
- org/bootstrap workflows
- admin/ops API

Keep process boundaries optional. Design modules so they can later run in-process or as separate services if scale requires it.

### Principle 3: External stateful dependencies only

Target architecture should support:

- external PostgreSQL
- external OpenSearch
- S3-compatible object storage, with optional local dev storage

Avoid inventing a bespoke HA backend cluster. Prefer stateless OpenCook API nodes in front of managed or externally-operated stateful systems.

### Principle 4: Golden compatibility fixtures

Create repeatable fixtures from real Chef Server behavior:

- canonical request/auth examples
- endpoint response bodies and error payloads
- search indexing and query results
- org/bootstrap ACL state
- cookbook upload/sandbox flows
- key lifecycle flows

These should become regression tests for OpenCook.

## Target Architecture

### API layer

- Implement exact or near-exact path and method compatibility
- Preserve JSON response shapes, status codes, and common error text where tooling depends on it
- Support both user-facing and internal/admin endpoints that current tooling uses

### Authentication layer

- Implement Chef request signing verification in Go
- Support legacy algorithm/format variants used by old Chef clients where still required
- Add exhaustive fixture-based tests from upstream and real client traffic captures

### Authorization layer

- Re-implement Bifrost semantics as a library backed by PostgreSQL
- Keep the authz model explicit in schema and code
- Make permission checks deterministic and observable

### Persistence layer

- PostgreSQL as source of truth for organizations, actors, objects, ACLs, cookbooks metadata, policyfiles, sandboxes, checksums, and related state
- Use a migration framework that supports repeatable, reviewable schema evolution
- Avoid depending on PostgreSQL behaviors that were acceptable in 9.x/10.x but fragile on current releases

### Search layer

- Build an explicit search adapter for OpenSearch
- Implement index template/version management
- Preserve Chef’s document expansion format for compatibility
- Introduce a provider capability layer rather than hardcoding version-specific branches

### Blob/object storage layer

- Support S3-compatible storage as the primary production mode
- Provide local dev/test filesystem mode
- Emulate Bookshelf upload/download contracts, including signed URL behavior expected by clients
- Current status: provider selection now exists, the filesystem adapter is live for local dev/test persistence, and the S3-compatible path now supports real request-time blob operations for configured endpoints and static credentials, with configurable timeout/retry behavior and blob-unavailable degradation for transient/provider-auth failures

### Operations layer

- Health/readiness endpoints for API, PostgreSQL, OpenSearch, and object storage
- Metrics compatible with Prometheus/OpenTelemetry
- Structured logs with request IDs
- Admin tooling for reindex, consistency checks, and data repair

## PostgreSQL Modernization Workstream

OpenCook should support current PostgreSQL releases intentionally, not incidentally.

### Objectives

- Validate on currently supported PostgreSQL majors
- Remove assumptions tied to old server defaults
- Review all SQL for deprecated syntax or planner-sensitive behavior
- Design indexes and query patterns for larger installations

### Work

1. Inventory upstream schema and query behavior from:
   - `oc_erchef/schema`
   - `oc_bifrost/schema`
   - `bookshelf/schema`
2. Normalize duplicated concepts into a coherent OpenCook schema while preserving API semantics.
3. Define supported PostgreSQL version matrix.
4. Build automated compatibility tests against each supported PostgreSQL version.
5. Add load tests for:
   - node check-ins
   - cookbook uploads
   - search-heavy reads
   - ACL-heavy reads/writes
6. Build online migration and reindex playbooks for production upgrades.

### Specific risks to address

- old extension assumptions
- trigger-heavy behavior that is hard to reason about
- row-by-row object fetch patterns after search
- contention around ACL and object update paths
- large organizations with many nodes, clients, and cookbook versions

## OpenSearch Modernization Workstream

OpenCook should treat OpenSearch as a versioned external dependency with capability negotiation.

### Objectives

- Support current OpenSearch releases cleanly
- Avoid brittle code paths tied to historical Elasticsearch/OpenSearch behavior
- Preserve Chef search behavior even if implementation internals differ

### Work

1. Document all query/index APIs currently relied on by Chef behavior.
2. Build a search provider abstraction with explicit capability flags:
   - bulk indexing
   - delete-by-query
   - index templates
   - total hits behavior
   - refresh semantics
3. Preserve the existing document expansion rules and query translation.
4. Version index mappings/templates so upgrades are explicit.
5. Add a reindex tool that can rebuild indices from PostgreSQL safely.
6. Add test coverage for partial search, ACL filtering, pagination, and object deletion behavior.

### Specific risks to address

- index API changes across OpenSearch versions
- delete-by-query behavior differences
- query-string parser edge cases
- refresh/consistency expectations after writes
- large data bag and node search result sets

## HA and Scale Design

The upstream product historically supported standalone, tiered, and HA topologies. OpenCook should preserve the operational capability, but with a simpler control plane.

### Recommended deployment model

- N stateless OpenCook API nodes behind a load balancer
- external PostgreSQL in HA configuration
- external OpenSearch cluster
- external S3-compatible object storage
- optional Redis only if clearly justified by measured bottlenecks

### Required capabilities

- no local singleton dependency for request handling
- idempotent background work
- safe rolling deploys
- reindex without full downtime
- object storage and search outage degradation strategy
- backup/restore procedures for PostgreSQL plus object storage

### Scale validation targets

- sustained chef-client convergence traffic
- concurrent cookbook uploads
- organization bootstrap and ACL mutation traffic
- large search fan-out
- reindex under production-like load

## Delivery Phases

## Phase 0: Discovery and contract capture

Deliverables:

- endpoint inventory
- auth behavior inventory
- ACL/authorization behavior inventory
- search behavior inventory
- compatibility matrix by API area
- first pass architecture decision record

Exit criteria:

- every externally relevant endpoint is cataloged
- pedant suite mapped to OpenCook workstreams
- top 20 compatibility risks documented

## Phase 1: Compatibility harness

Deliverables:

- test runner that can execute pedant or pedant-derived contract tests against OpenCook
- golden HTTP fixture library
- request signing fixture suite
- response diff tooling against upstream Chef Server

Exit criteria:

- OpenCook can be evaluated continuously against upstream behavior

## Phase 2: Core platform skeleton

Deliverables:

- Go service skeleton
- config system
- PostgreSQL connectivity and migrations
- OpenSearch adapter skeleton
- object storage adapter skeleton
- health/metrics/logging baseline

Exit criteria:

- service boots in local dev with external dependencies

## Phase 3: Authn/Authz and org bootstrap

Deliverables:

- Chef header auth verifier
- key management endpoints
- users/clients/orgs/groups/ACL core flows
- org bootstrap default ACL behavior

Exit criteria:

- bootstrap flows and core authz tests pass

## Phase 4: Core Chef object APIs

Deliverables:

- nodes
- roles
- environments
- data bags
- clients
- cookbooks and cookbook artifacts
- sandboxes
- universe

Exit criteria:

- core CRUD pedant coverage passes for these resources

## Phase 5: Search compatibility

Deliverables:

- document expansion
- indexing pipeline
- search endpoints
- partial search
- ACL-filtered results
- reindex tooling

Exit criteria:

- search-related pedant coverage passes
- behavior matches upstream on curated fixture datasets

## Phase 6: Operational parity

Deliverables:

- admin commands/APIs
- backup/restore guidance
- observability package
- rolling upgrade docs
- performance and scale reports

Exit criteria:

- reference production deployment documented
- failover and restore drills exercised

## Phase 7: Migration and cutover

Deliverables:

- import/sync tooling from existing Chef Infra Server
- dual-write or shadow-read strategy if needed
- cutover runbook
- rollback runbook

Exit criteria:

- at least one representative environment can migrate with low risk

## Recommended Initial Milestones

### Milestone A: Contract inventory

- Extract endpoint list from `routes.lua`
- Map pedant coverage by endpoint
- Identify uncovered behavior that needs bespoke tests

### Milestone B: Auth compatibility prototype

- Implement Chef request signature verification in Go
- Validate against upstream fixtures and live sample requests
- Reproduce API version edge behavior

### Milestone C: Minimal read/write vertical slice

- PostgreSQL-backed users/clients/orgs
- ACL checks
- one object type, preferably nodes
- basic search indexing for nodes

### Milestone D: Full cookbook path

- sandbox create/commit compatibility
- checksum tracking and signed blob URLs
- cookbook/cookbook artifact read paths plus initial writable cookbook and artifact lifecycles
- universe endpoint
- remaining cookbook mutation edge cases and production object storage integration

## Suggested Repository Workstreams for OpenCook

- `docs/`
  - compatibility notes
  - ADRs
  - migration docs
- `cmd/opencook/`
  - main server binary
- `internal/api/`
  - routing and handlers
- `internal/authn/`
  - request signing and key resolution
- `internal/authz/`
  - ACLs, groups, containers, permissions
- `internal/store/pg/`
  - PostgreSQL repositories and migrations
- `internal/search/`
  - OpenSearch adapter and indexing
- `internal/blob/`
  - S3/local storage adapters
- `internal/compat/`
  - response shims, legacy semantics, fixture support
- `test/compat/`
  - pedant integration and golden tests

## Risks and Non-Goals

### Major risks

- undocumented client quirks outside pedant coverage
- subtle auth signature edge cases
- ACL behavior mismatches
- search semantics drifting from Chef expectations
- migration complexity from legacy schemas and indices
- existing tooling that calls upstream licensing endpoints may need a documented migration note because OpenCook will not implement them

### Non-goals for v1

- redesigning the Chef API
- inventing a new client protocol
- requiring Chef client changes
- forcing users onto a new authorization model
- implementing upstream Chef licensing or license telemetry behavior

## Recommended Next Step

Start with a compatibility-first foundation, not implementation breadth:

1. Build the endpoint and behavior inventory from `routes.lua` and `oc-chef-pedant`.
2. Implement and validate request signing in isolation.
3. Stand up a PostgreSQL-backed authz and key model.
4. Deliver one fully-compatible vertical slice before broad endpoint expansion.

That sequence reduces the biggest rewrite risk: appearing complete at the HTTP layer while still being incompatible with real Chef traffic.
