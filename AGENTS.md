# AGENTS.md

## Purpose

OpenCook is a compatibility-first Go rewrite of Chef Infra Server.

The goal is not to design a new configuration-management server. The goal is to remain wire-compatible with existing Chef and Cinc clients, `knife`, `chef-server-ctl`, and surrounding automation while modernizing the internals and operational model.

## Product Stance

OpenCook is intended to be fully free and open source.

Intentional divergence from upstream Chef Infra Server:

- do not implement licensing
- do not implement license enforcement
- do not implement license telemetry
- do not add license-management endpoints back into the compatibility target

If a compatibility question conflicts with the licensing-free stance, prefer the licensing-free stance and document the exception clearly.

## Source Of Truth

Start with these files before making changes:

- `README.md`
- `docs/chef-infra-server-rewrite-roadmap.md`
- `docs/milestones.md`
- `docs/compatibility-matrix-template.md`
- `docs/adr/0001-compatibility-first-architecture.md`
- `docs/adr/0002-external-stateful-dependencies.md`
- `docs/adr/0003-no-licensing-subsystem.md`

For upstream behavior, the local Chef Server checkout is the main reference:

- `~/Projects/coding/ruby/chef-server`

Important upstream sources we have been using:

- `src/nginx/habitat/config/routes.lua`
- `dev-docs/API_VERSIONING.md`
- `dev-docs/SEARCH_AND_INDEXING.md`
- `dev-docs/BOOKSHELF.md`
- `oc-chef-pedant/`

Treat `oc-chef-pedant` as a contract source, not just the prose docs.

## Current Phase

OpenCook is in the compatibility-foundation phase.

Implemented so far:

- Chef request-signing verification
- initial authenticated routing
- in-memory bootstrap state for users, organizations, clients, groups, containers, and ACLs
- the first environment slice:
  - `_default` environment bootstrap
  - list
  - get
  - head
  - create
  - update
  - delete
  - rename-capable `PUT`
  - `/environments/{name}/cookbooks`
  - `/environments/{name}/nodes`
  - `/environments/{name}/recipes`
- the first core object slice for nodes:
  - list
  - get
  - head
  - create
  - update
  - delete
  - default-org and explicit-org routing
- the first role slice:
  - list
  - get
  - head
  - create
  - update
  - delete
  - `/roles/{name}/environments`
  - `/roles/{name}/environments/{environment}`
  - default-org and explicit-org routing
- the first default-org client compatibility slice:
  - `/clients`
  - `/clients/{name}`
  - `/clients/{name}/keys`
  - default-org and explicit-org client routing for reads
  - default-org and explicit-org client create/delete flows
- the current policyfile slice:
  - `/policies`
  - `/policies/{name}`
  - `/policies/{name}/revisions`
  - `/policies/{name}/revisions/{revision}`
  - `/policy_groups`
  - `/policy_groups/{group}`
  - `/policy_groups/{group}/policies/{name}`
  - `/organizations/{org}/policies`
  - `/organizations/{org}/policies/{name}`
  - `/organizations/{org}/policies/{name}/revisions`
  - `/organizations/{org}/policies/{name}/revisions/{revision}`
  - `/organizations/{org}/policy_groups`
  - `/organizations/{org}/policy_groups/{group}`
  - `/organizations/{org}/policy_groups/{group}/policies/{name}`
  - policy revision create/get/delete
  - policy-group list/get/delete
  - policy-group assignment create/update/delete
  - richer canonical payload round-tripping for `named_run_lists`, nested cookbook-lock metadata, and `solution_dependencies`
  - node policy refs remain searchable compatibility fields, not foreign keys
- the first sandbox/blob slice:
  - `/sandboxes`
  - `/sandboxes/{id}`
  - `/organizations/{org}/sandboxes`
  - `/organizations/{org}/sandboxes/{id}`
  - signed sandbox create and commit flows
  - absolute checksum upload URLs under `/_blob/checksums/{checksum}`
  - in-memory checksum blob storage with hash validation and upload size limits
- the first cookbook compatibility slice:
  - `/cookbook_artifacts`
  - `/cookbook_artifacts/{name}`
  - `/cookbook_artifacts/{name}/{identifier}`
  - `/cookbooks`
  - `/cookbooks/_latest`
  - `/cookbooks/_recipes`
  - `/cookbooks/{name}`
  - `/cookbooks/{name}/{version}`
  - `/universe`
  - `/organizations/{org}/cookbook_artifacts`
  - `/organizations/{org}/cookbook_artifacts/{name}`
  - `/organizations/{org}/cookbook_artifacts/{name}/{identifier}`
  - `/organizations/{org}/cookbooks`
  - `/organizations/{org}/cookbooks/_latest`
  - `/organizations/{org}/cookbooks/_recipes`
  - `/organizations/{org}/cookbooks/{name}`
  - `/organizations/{org}/cookbooks/{name}/{version}`
  - `/organizations/{org}/universe`
  - cookbook artifact list/get/create/delete
  - cookbook version create/update/delete
  - cookbook collection, latest, recipe, and named-version read views
  - environment-filtered cookbook collection, named-cookbook, and recipe views
  - manifest-derived `_recipes` behavior with Chef-style default recipe qualification
  - create-path cookbook validation parity for route/payload mismatch errors, with field-specific update-path validation preserved
  - cookbook collection `num_versions` and `_latest` read-edge coverage
  - pedant-shaped cookbook PUT exactness for omitted optional top-level fields like `version`, `json_class`, and `chef_type`
  - explicit `?force=false` conflict coverage plus v0/v2 file-collection presence and omission exactness on successful updates
  - broader create/update HTTP coverage for omitted-default exactness, top-level `json_class`/`chef_type`/`version` validation, invalid request-key rejection, metadata-name write-vs-read canonicalization, permissive `metadata.providing`, exact no-mutation behavior for invalid metadata payloads, and malformed route-path handling for invalid cookbook names and version strings
  - multi-version shared-checksum retention, successful in-org normal-user cookbook read/delete/create/update coverage, usable signed recipe download coverage, create/update no-mutation coverage for failed outside-user and invalid-user mutations, file-set replacement coverage for deleting all or some cookbook files on update, invalid-checksum update rejection without mutating the existing cookbook file set, malformed negative/overflow route-version coverage, and cookbook-route auth 401/403 coverage
  - API-version-sensitive cookbook version shaping with `json_class`, `cookbook_name`, legacy segments, and v2 `all_files`
  - signed direct blob URLs for cookbook file downloads
  - cleanup of unreferenced checksum blobs after cookbook version and artifact mutations, while preserving shared checksum references
- the first search-facing slice:
  - `/search`
  - `/search/{client,environment,node,role}`
  - live per-data-bag indexes under `/search/{bag}`
  - `/organizations/{org}/search`
  - `/organizations/{org}/search/{client,environment,node,role}`
  - `/organizations/{org}/search/{bag}`
  - GET search results
  - POST partial search results
  - ACL-filtered search responses
  - merged node attributes for search-facing partial search
  - Chef-style wrapped data bag search rows
  - raw-item data bag partial search rows
  - simple `AND`/`NOT` matching and escaped-slash prefix handling
- the first data bag slice:
  - `/data`
  - `/data/{bag}`
  - `/data/{bag}/{item}`
  - `/organizations/{org}/data`
  - `/organizations/{org}/data/{bag}`
  - `/organizations/{org}/data/{bag}/{item}`
  - bag list/get/create/delete
  - item get/create/update/delete
  - Chef-style item wrapper/error response shaping
- actor key lifecycle for users and clients:
  - list
  - get
  - create
  - update
  - delete
  - expiration-aware authentication

Current architectural reality:

- the API surface is partly real and partly scaffolded
- bootstrap and key lifecycle state are in-memory compatibility implementations
- PostgreSQL and OpenSearch are still placeholders or early scaffolding
- the blob layer now has an in-memory compatibility implementation for sandbox checksum uploads/downloads and cookbook file URLs, but production S3-compatible behavior is still pending

Do not mistake the current in-memory implementation for the final persistence architecture.

## Architecture Map

High-level package roles:

- `cmd/opencook`
  - server entrypoint
- `internal/app`
  - wiring and dependency assembly
- `internal/api`
  - HTTP routing, request/response shaping, authn/authz enforcement
- `internal/authn`
  - Chef request verification and key lookup
- `internal/authz`
  - Bifrost-style authorization decisions
- `internal/bootstrap`
  - in-memory users/orgs/clients/groups/containers/ACLs/key lifecycle state
- `internal/compat`
  - compatibility surface inventory and future shims
- `internal/config`
  - env-driven configuration
- `internal/store/pg`
  - future PostgreSQL-backed persistence
- `internal/search`
  - future OpenSearch-backed compatibility layer
- `internal/blob`
  - current in-memory blob compatibility storage and future Bookshelf/S3-compatible behavior

## Compatibility Rules

When in doubt, optimize for behavior compatibility over internal elegance.

Rules we have been following:

- preserve endpoint shape before redesigning internals
- preserve Chef header auth semantics before optimizing auth code
- preserve Bifrost-like ACL behavior before simplifying authorization models
- preserve API-version-sensitive actor key behavior before refactoring key flows
- prefer explicit compatibility shims over “cleaner” breaking behavior

Important current conventions:

- no licensing compatibility work
- no speculative redesigns of the Chef object model
- no migration to PostgreSQL for a surface until the behavior contract is clearer
- no hidden time sources in auth decisions

Time-sensitive auth rule:

- skew and key-expiration decisions must use the verifier’s injected clock
- do not use `time.Now()` directly for request-auth decisions when an injected clock already exists

## Engineering Conventions

### 1. Work in small vertical slices

Prefer one compatibility surface at a time.

Good examples:

- request signing
- user/org/client bootstrap
- actor `/keys`
- a single core object type such as nodes

Avoid jumping across unrelated surfaces in one change unless the work is tightly coupled.

### 2. Keep docs in sync with slices

When a slice lands, update the relevant docs in the same change:

- `docs/milestones.md`
- `docs/compatibility-matrix-template.md`
- `docs/chef-infra-server-rewrite-roadmap.md`
- `README.md` when user-facing capabilities changed materially

The docs are part of the delivery, not optional cleanup.

### 3. Keep the compatibility inventory honest

If you add or implement a route:

- update `internal/compat/registry.go`
- update `internal/api/router.go`
- update `internal/api/router.go:isImplementedPattern`

If these drift, route counts and contract reporting become misleading or break the mux.

### 4. Prefer deterministic testable seams

Use injected clocks and explicit dependencies where behavior depends on time or external systems.

Examples already in code:

- auth clock skew and key-expiration checks use verifier time injection
- bootstrap state is injected into authz and API layers

### 5. Keep storage and decision logic separate

Current key-handling convention:

- `internal/authn/MemoryKeyStore` is storage
- auth validity decisions such as expiration filtering belong in the verifier

Do not bury business rules in low-level storage helpers if they need a consistent request-time context.

### 6. Preserve current staged architecture

Right now the repo intentionally uses in-memory compatibility implementations to stabilize behavior before moving to PostgreSQL and OpenSearch.

When adding behavior:

- prefer extending the current in-memory layer first
- only move a surface to persistent storage when the contract is clearer

## Testing Conventions

Before finishing a change:

- run `gofmt -w` on changed Go files
- run `GOCACHE=/tmp/opencook-go-cache go test ./...`

Current tests are the main regression guard. Add tests whenever you change behavior, especially for:

- auth header parsing
- auth time behavior
- ACL decisions
- route handling
- actor key lifecycle
- compatibility edge cases found in review

Prefer HTTP-level tests in `internal/api/router_test.go` for route semantics and user-visible behavior.

Prefer focused package tests when the behavior is lower-level:

- `internal/authn/*_test.go`
- `internal/bootstrap/*_test.go`
- `internal/compat/*_test.go`

## Implementation Checklist For Future Agents

When you start a new compatibility slice:

1. Read the current roadmap and compatibility docs.
2. Check the local upstream Chef sources and pedant coverage for that surface.
3. Implement the smallest useful vertical slice.
4. Add or extend tests before closing the task.
5. Update the roadmap, milestone, and compatibility-matrix docs.
6. Run `gofmt -w` and `GOCACHE=/tmp/opencook-go-cache go test ./...`.

## Current Gaps Worth Knowing

These areas are still intentionally incomplete:

- deeper API-version-specific semantics beyond the current actor-key surface
- PostgreSQL-backed persistence
- OpenSearch-backed indexing and provider capability behavior
- remaining core Chef object CRUD beyond nodes, environments, roles, and data bags
- deeper node and environment compatibility such as cookbook constraint edge cases and linked object behavior
- deeper role compatibility such as run-list normalization and linked environment behavior
- broader search semantics beyond the current in-memory compatibility layer, especially richer Lucene-style query translation and wider object coverage
- deeper Bookshelf/cookbook flows beyond the current cookbook write/read/artifact slice, especially the remaining cookbook pedant cases outside the current environment-filtered/named-filter/latest/version read-write contract and production blob providers
- operational parity and migration tooling

The next likely major slice is the remaining cookbook pedant cases beyond environment-filtered and named-filter/latest-version views, or moving stabilized slices toward PostgreSQL/OpenSearch-backed providers.
