# PostgreSQL-Backed Cookbook Persistence Plan

Status: planned on 2026-04-22

## Purpose

This document breaks the next cookbook/storage bucket into one implementation slice with concrete, commit-sized tasks.

The goal is to move the now-stabilized cookbook and cookbook-artifact contract onto PostgreSQL-backed persistence without changing the current HTTP, auth, depsolver, or blob behavior that we have already pinned.

## Slice Boundary

In scope:

- PostgreSQL-backed persistence for cookbook versions and cookbook artifacts
- the read and write paths that currently depend on in-memory cookbook state
- environment cookbook views and depsolver reads that currently share the same cookbook source
- released-checksum reporting needed for visible blob cleanup and retention behavior
- tests and docs needed to prove the storage swap without changing the public contract

Out of scope:

- migrating other object types off the bootstrap in-memory layer
- replacing authz, search, node, role, or environment persistence
- new blob backends or additional provider hardening
- broad migration/import tooling beyond the minimum schema/bootstrap work needed for this slice
- speculative HTTP behavior changes not supported by the current compatibility contract

## Existing Seam

This slice is intentionally aligned to the current code layout:

- cookbook and artifact HTTP routes: [internal/api/cookbook_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes.go)
- environment cookbook views: [internal/api/environment_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/environment_routes.go)
- cookbook HTTP coverage: [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go)
- cookbook state and normalization logic: [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
- environment cookbook filtering: [internal/bootstrap/environments.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/environments.go)
- depsolver cookbook reads: [internal/bootstrap/depsolver.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/depsolver.go)
- bootstrap organization state that still owns the cookbook maps: [internal/bootstrap/service.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/service.go)
- PostgreSQL scaffold: [internal/store/pg/store.go](/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/store.go)
- application wiring: [internal/app/app.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go)

The important architectural constraint is that cookbook persistence is not only a cookbook-route concern. The current in-memory cookbook maps are also read by:

- cookbook collection, named, latest, recipe, universe, and artifact routes
- environment cookbook collection and named-cookbook views
- cookbook depsolver reads and candidate filtering

That means this slice needs a storage seam extraction step before we cut over to PostgreSQL. A route-only swap would leave environment and depsolver behavior split across two sources of truth.

## Compatibility Contract

For this slice, we will treat the following as the compatibility contract:

- cookbook and cookbook-artifact HTTP responses stay byte-shape compatible with the current pinned behavior
- signed blob URLs, checksum preconditions, and `blob_unavailable` behavior stay unchanged
- released-checksum reporting after cookbook and artifact mutation stays unchanged so visible blob cleanup semantics do not drift
- environment cookbook views and depsolver continue reading the same cookbook source as the cookbook routes
- default-org, explicit-org, and configured-default-org behavior stays unchanged
- auth decisions stay where they are today; this slice swaps persistence, not authorization
- the rest of the bootstrap object state may remain in memory for now

## Definition Of Done

- cookbook versions and cookbook artifacts are persisted through a PostgreSQL-backed repository instead of the bootstrap in-memory maps
- cookbook routes, environment cookbook views, and depsolver reads all use the same cookbook persistence seam
- the current cookbook/blob compatibility suite stays green without contract changes
- released-checksum reporting and visible blob cleanup behavior are preserved
- roadmap and milestone docs describe PostgreSQL-backed cookbook persistence as completed rather than a vague future bucket

## Task 1 Inventory Snapshot

The current cookbook persistence source of truth is still embedded in `organizationState`:

- `org.cookbooks` in [internal/bootstrap/service.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/service.go)
- `org.cookbookArtifacts` in [internal/bootstrap/service.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/service.go)

The current bootstrap cookbook surface that sits directly on those maps is:

- artifact list/get/create/delete in [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
- cookbook version list/get/upsert/delete in [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
- universe rendering support in [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
- environment cookbook filtering in [internal/bootstrap/environments.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/environments.go)
- depsolver candidate lookup and dependency traversal in [internal/bootstrap/depsolver.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/depsolver.go)

The current API consumers are:

1. Cookbook routes in [internal/api/cookbook_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes.go).
- collection reads call `ListCookbookVersions`
- `_latest` and `_recipes` call `ListCookbookVersions`, then `GetCookbookVersion`
- named and version reads call `ListCookbookVersionsByName` and `GetCookbookVersion`
- version writes call `UpsertCookbookVersionWithReleasedChecksums`
- version deletes call `DeleteCookbookVersionWithReleasedChecksums`
- artifact reads call `ListCookbookArtifacts`, `ListCookbookArtifactsByName`, and `GetCookbookArtifact`
- artifact writes call `CreateCookbookArtifact` and `DeleteCookbookArtifactWithReleasedChecksums`
- universe reads call `CookbookUniverse`

2. Environment cookbook views in [internal/api/environment_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/environment_routes.go).
- collection reads call `ListEnvironmentCookbookVersions`
- named-cookbook reads call `GetEnvironmentCookbookVersions`
- environment recipe reads call `ListEnvironmentCookbookVersions`, then `GetCookbookVersion`

3. Depsolver reads in [internal/bootstrap/depsolver.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/depsolver.go).
- root candidate lookup reads `org.cookbooks` directly
- dependency expansion reads `org.cookbooks` directly
- role expansion still lives in bootstrap because roles remain in memory for now

That means the minimum cookbook persistence seam for this slice is not just CRUD. It needs to cover:

- cookbook version list/get/upsert/delete
- artifact list/get/create/delete
- released-checksum reporting on cookbook and artifact mutations
- universe entry enumeration
- environment-filtered cookbook refs
- cookbook version reads needed by `_recipes` and environment recipe views
- depsolver candidate enumeration and resolved cookbook lookup

The seam should also preserve the current split of responsibilities:

- validation and normalization can stay close to the current logic in [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go) for the first cut, because that is where the current compatibility exactness already lives
- blob existence checks should stay at the route layer, because the current routes already own request-context-aware blob access and `blob_unavailable` mapping
- blob cleanup should stay at the route layer, with the repository returning released checksums but not performing blob deletion itself
- auth stays in the API layer and should not move into the repository

The likely first seam shape after extraction is:

- an in-memory cookbook repository that preserves the current behavior behind an interface
- a PostgreSQL cookbook repository that implements the same interface
- API and bootstrap consumers depending on that interface rather than directly on `organizationState`
- [internal/app/app.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go) wiring both the still-in-memory bootstrap service and the new cookbook repository side by side during the transition

The main risk surfaced by this inventory is split-brain reads. If cookbook routes move to PostgreSQL before environment cookbook views and depsolver do, the compatibility surface will silently diverge. So the slice should treat those readers as part of the same cutover, not follow-up cleanup.

## Task List

### Task 1: Freeze The Persistence Contract And Consumer Inventory

Goal:

- turn the PostgreSQL cookbook bucket into a concrete seam map before we start extracting storage logic

Changes:

- inventory the cookbook reads and writes currently implemented in:
  - [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
  - [internal/bootstrap/environments.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/environments.go)
  - [internal/bootstrap/depsolver.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/depsolver.go)
  - [internal/api/cookbook_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes.go)
  - [internal/api/environment_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/environment_routes.go)
- decide the minimum repository surface needed for:
  - cookbook version list/get/upsert/delete
  - artifact list/get/create/delete
  - universe reads
  - environment-filtered cookbook refs
  - depsolver candidate lookup
  - released-checksum reporting

Exit condition:

- every cookbook consumer is mapped to an explicit repository method instead of an implicit `org.cookbooks` or `org.cookbookArtifacts` map read

### Task 2: Extract A Cookbook Persistence Seam With An In-Memory Implementation

Goal:

- decouple the current cookbook contract from the bootstrap maps before introducing PostgreSQL

Changes:

- introduce a cookbook persistence interface and shared read/write models in the storage layer
- move the current in-memory cookbook implementation behind that interface
- update cookbook routes, environment cookbook views, and depsolver helpers to depend on the new seam instead of reaching directly into `organizationState`
- keep the broader bootstrap state in place for non-cookbook objects

Exit condition:

- cookbook behavior still runs entirely in memory by default, but direct reads from `org.cookbooks` and `org.cookbookArtifacts` are isolated to the in-memory repository implementation

### Task 3: Add PostgreSQL Schema And Repository Scaffolding

Goal:

- define a reviewable persistent model for cookbook versions and artifacts without widening the public contract

Changes:

- add the initial PostgreSQL schema/migration assets for:
  - cookbook versions
  - cookbook files
  - cookbook artifacts
  - artifact files
- preserve the current canonical fields and sort behavior needed by:
  - collection and named reads
  - universe rendering
  - environment filtering
  - depsolver candidate selection
- extend [internal/store/pg/store.go](/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/store.go) with a cookbook-focused repository scaffold instead of the current status-only placeholder

Exit condition:

- PostgreSQL can round-trip the current cookbook and artifact shapes in repository-level tests

### Task 4: Wire Cookbook And Artifact Write Paths Through The New Seam

Goal:

- move the visible mutation paths onto the extracted repository while keeping validation and blob behavior unchanged

Changes:

- route cookbook version create/update/delete through the new cookbook repository seam
- route cookbook artifact create/delete through the same seam
- keep validation and normalization behavior aligned with the current logic from [internal/bootstrap/cookbooks.go](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap/cookbooks.go)
- preserve released-checksum reporting so the current visible cleanup and retention behavior does not drift

Exit condition:

- cookbook and artifact writes no longer depend on the bootstrap cookbook maps
- repeated-`PUT`, frozen overwrite, conflict, and no-mutation behavior stay unchanged

### Task 5: Wire Cookbook Read Paths, Environment Cookbook Views, And Depsolver Through The Same Backend

Goal:

- eliminate split-brain cookbook reads by making every cookbook-derived surface use the same storage source

Changes:

- move cookbook collection, `_latest`, `_recipes`, named, version, universe, and artifact reads to the new seam
- move environment cookbook collection and named-cookbook filtering to the same seam
- move depsolver cookbook candidate lookup and dependency traversal to the same seam
- keep current sort order, filtering, and conflict detail behavior intact

Exit condition:

- cookbook routes, environment cookbook views, and depsolver all read from the same repository-backed source instead of a mix of route logic and in-memory maps

### Task 6: Cut Over The Application To PostgreSQL-Backed Cookbook Persistence Safely

Goal:

- activate the new persistence path without forcing the rest of the bootstrap object model to move in the same slice

Changes:

- update [internal/app/app.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go) and the API dependencies so cookbook persistence can come from PostgreSQL while the rest of the bootstrap service remains in memory
- keep the in-memory cookbook repository available for tests and non-PostgreSQL configurations where needed during the transition
- add repository and route-level coverage that proves the PostgreSQL path preserves the current cookbook contract

Exit condition:

- when PostgreSQL is configured, the app serves cookbook and cookbook-artifact behavior from PostgreSQL-backed persistence without changing unrelated object storage

### Task 7: Sync Docs And Close The Bucket

Goal:

- mark the PostgreSQL cookbook persistence slice complete and make the next roadmap bucket unambiguous

Changes:

- sync:
  - [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md)
  - [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
  - [docs/compatibility-matrix-template.md](/Users/oberon/Projects/coding/go/OpenCook/docs/compatibility-matrix-template.md)
  - [AGENTS.md](/Users/oberon/Projects/coding/go/OpenCook/AGENTS.md)
- update the status text in the app/router if it still points at cookbook persistence as the next bucket
- update this plan document with completion status and a short summary

Exit condition:

- the roadmap no longer describes PostgreSQL-backed cookbook persistence as an open bucket

## Suggested Commit Sequence

1. `docs: inventory cookbook postgres persistence seam`
2. `refactor: extract cookbook persistence interface`
3. `pg: scaffold cookbook persistence schema`
4. `pg: persist cookbook write paths`
5. `pg: persist cookbook reads and depsolver sources`
6. `app: wire postgres-backed cookbook persistence`
7. `docs: close postgres cookbook persistence bucket`

## Verification

Minimum verification for the seam-extraction and route-integration tasks:

```bash
gofmt -w internal/api/cookbook_routes.go internal/api/environment_routes.go internal/bootstrap/cookbooks.go internal/bootstrap/depsolver.go internal/bootstrap/environments.go internal/store/pg/store.go internal/app/app.go
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

For repository or schema tasks, add focused coverage alongside the full suite:

```bash
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg ./internal/api ./internal/bootstrap
```

If a task is docs-only, note that explicitly and skip the test pass only when no code or test behavior changed.
