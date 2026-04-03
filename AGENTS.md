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
  - `/environments/{name}/nodes`
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
- actor key lifecycle for users and org-scoped clients:
  - list
  - get
  - create
  - update
  - delete
  - expiration-aware authentication

Current architectural reality:

- the API surface is partly real and partly scaffolded
- bootstrap and key lifecycle state are in-memory compatibility implementations
- PostgreSQL, OpenSearch, and blob layers still have placeholders or early scaffolding

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
  - future Bookshelf/S3-compatible blob behavior

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
- OpenSearch-backed indexing and query behavior
- core Chef object CRUD such as data bags and the remaining object surface beyond nodes, environments, and roles
- deeper node and environment compatibility such as search indexing, cookbook constraint edge cases, and linked object behavior
- deeper role compatibility such as run-list normalization and linked environment behavior
- Bookshelf/sandbox/cookbook flows
- operational parity and migration tooling

The next likely major slice is deeper node/environment/role compatibility or the next adjacent object API such as data bags, not more speculative infrastructure work.
