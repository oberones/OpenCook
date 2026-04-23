# Cookbook/Blob Pedant Compatibility Plan

Status: completed on 2026-04-22

## Purpose

This document breaks the remaining cookbook/blob compatibility bucket into one implementation slice with concrete, commit-sized tasks.

The goal is to finish the remaining cookbook and cookbook-artifact pedant coverage on the current HTTP contract without mixing in PostgreSQL persistence or unrelated surface-area work.

## Slice Boundary

In scope:

- remaining cookbook and cookbook-artifact pedant-shaped HTTP behavior on the current in-memory and blob-backed implementation
- route semantics, validation exactness, response shaping, and no-mutation guarantees on cookbook-facing routes
- blob-linked cookbook flows where they are already part of the current public contract
- auth parity and org/default-org route parity where cookbook behavior is still uneven
- tests and docs needed to close the bucket honestly

Out of scope:

- PostgreSQL-backed cookbook persistence
- new blob backends or more provider-operational hardening
- broader search, node, role, or migration work
- speculative behavior changes not supported by Chef docs or `oc-chef-pedant`

## Existing Seam

This slice is intentionally aligned to the current code layout:

- cookbook HTTP routes: [internal/api/cookbook_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes.go)
- cookbook HTTP coverage: [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go)
- cookbook state and compatibility shaping: [internal/bootstrap](/Users/oberon/Projects/coding/go/OpenCook/internal/bootstrap)
- blob abstractions used by cookbook downloads and checksum checks: [internal/blob](/Users/oberon/Projects/coding/go/OpenCook/internal/blob)
- compatibility tracking docs: [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md), [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md), [docs/compatibility-matrix-template.md](/Users/oberon/Projects/coding/go/OpenCook/docs/compatibility-matrix-template.md), and [AGENTS.md](/Users/oberon/Projects/coding/go/OpenCook/AGENTS.md)

## Compatibility Contract

For this slice, we will treat the following as the compatibility contract:

- existing cookbook and cookbook-artifact endpoints keep their current route shapes and auth model
- pedant-shaped validation and response exactness should be pinned before any implementation widening
- invalid writes must preserve current no-mutation guarantees
- signed blob downloads and checksum-backed cookbook flows must keep the current public contract where it already exists
- default-org and explicit-org parity should only diverge where upstream behavior actually differs

## Definition Of Done

- the remaining cookbook pedant gaps outside the current environment-filtered, named-filter, latest, version, and depsolver contract are explicitly pinned in tests
- cookbook and cookbook-artifact route semantics are explicit for the remaining unpinned edge cases
- validation and no-mutation guarantees are explicit for the remaining malformed and mismatch cases
- any remaining cookbook-facing blob-linked behaviors on the current contract are pinned
- roadmap and milestone docs no longer describe this as a vague pending cookbook/blob bucket

## Task 1 Inventory Snapshot

Audit sources:

- current OpenCook coverage in [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go)
- roadmap and milestone wording in [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md) and [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
- upstream pedant cookbook families in:
  - `oc-chef-pedant/spec/api/cookbooks/read_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/named_filters_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/version_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/create_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/update_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/delete_spec.rb`
  - `oc-chef-pedant/spec/api/cookbooks/version_conversion_spec.rb`

Current coverage already looks strong for the default-org cookbook contract:

- cookbook collection, `_latest`, `_recipes`, named version, named `_latest`, and universe reads
- cookbook create, update, delete, and API-version-sensitive version conversion
- pedant-shaped validation around top-level fields, metadata, omitted defaults, and invalid file collections
- frozen cookbook handling including `force=true` and `force=false`
- no-mutation guarantees for invalid, outside-user, and invalid-user cookbook and artifact mutations
- signed cookbook and artifact downloads plus checksum-retention behavior
- provider-backed `blob_unavailable` degradation on cookbook and artifact flows

The remaining bucket is now concrete. The main unpinned families are:

1. Cookbook route-semantics parity.
- Explicit tests are still missing for trailing slash, extra-path `404`, and `405` plus `Allow` behavior on the cookbook routes.
- This applies to `/cookbooks`, `/cookbooks/_latest`, `/cookbooks/_recipes`, `/cookbooks/{name}`, `/cookbooks/{name}/{version}`, `/cookbooks/{name}/_latest`, and `/universe`.

2. Explicit-org cookbook parity.
- Current cookbook coverage is heavily default-org-biased; the test file only has a narrow org-scoped universe check today.
- We still need org-scoped parity on `/organizations/{org}/cookbooks`, `/organizations/{org}/cookbooks/_latest`, `/organizations/{org}/cookbooks/_recipes`, `/organizations/{org}/cookbooks/{name}`, `/organizations/{org}/cookbooks/{name}/{version}`, `/organizations/{org}/cookbooks/{name}/_latest`, and `/organizations/{org}/universe`.
- That includes read shaping, create/update/delete behavior, malformed-input handling, and no-mutation guarantees.

3. Explicit-org cookbook-artifact parity.
- Cookbook-artifact coverage is also default-org-biased today.
- We still need the same create/read/delete/conflict/auth/no-mutation families on `/organizations/{org}/cookbook_artifacts`, `/organizations/{org}/cookbook_artifacts/{name}`, and `/organizations/{org}/cookbook_artifacts/{name}/{identifier}`.
- API-version-sensitive artifact shaping should be pinned there too.

4. Collection and named-filter auth parity.
- Default-org cookbook version and artifact read/write auth is covered, but the collection and named-filter surfaces still need explicit actor-path coverage.
- The remaining family here is normal-user, outside-user, and invalid-user behavior on collection, named-filter, and org-scoped collection routes.

5. Org-scoped blob-linked cookbook flows.
- Signed cookbook and artifact download behavior is covered on the default-org routes.
- The remaining parity work is to pin the same observable blob-linked behavior on the explicit-org cookbook and artifact routes, including any current checksum-precondition and `blob_unavailable` surfaces those routes expose.

6. Remaining route-adjacent validation exactness.
- The broad field-validation families from pedant are substantially covered on the default-org cookbook routes already.
- What remains is mainly carrying those exact same malformed-name, malformed-version, route-versus-payload mismatch, and failed-mutation no-change guarantees across the explicit-org aliases and the still-unpinned route-shape cases.

This means the slice is still one coherent vertical pass, but the work is narrower than the old bucket wording suggested:

- finish cookbook route semantics
- bring cookbook and cookbook-artifact parity to the explicit-org aliases
- finish the remaining auth and blob-linked parity on those same routes

## Task List

### Task 1: Freeze The Remaining Contract Inventory

Goal:

- turn the remaining cookbook/blob bucket into a concrete checklist before we start widening coverage

Changes:

- audit [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) against:
  - [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md)
  - [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
  - `oc-chef-pedant` in the local upstream checkout
- record the remaining test families this slice is responsible for

Exit condition:

- the remaining bucket is expressed as specific cookbook test families rather than broad prose

### Task 2: Finish Cookbook Route-Semantics Parity

Goal:

- close the remaining cookbook route-shape gaps before deeper validation work

Changes:

- extend [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for any still-missing combinations of:
  - trailing-slash acceptance
  - extra-path `404`
  - method-not-allowed and `Allow` header behavior
  - named, `_latest`, `_recipes`, and org-scoped variants where still unpinned
  - default-org versus explicit-org precedence where the route surface is shared

Exit condition:

- route semantics are explicit and not left implicit in router setup

### Task 3: Finish Cookbook Validation Exactness And No-Mutation Coverage

Goal:

- close the remaining pedant-style malformed-write and mismatch cases on cookbook version routes

Changes:

- extend [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for remaining gaps in:
  - route-versus-payload mismatch behavior
  - malformed cookbook names and version strings
  - top-level field exactness
  - metadata field validation not already pinned
  - file-collection exactness and invalid file payloads
  - no-mutation behavior after failed create or update attempts

Exit condition:

- the remaining malformed cookbook write behavior is pinned with explicit pre- and post-state assertions

### Task 4: Finish Cookbook-Artifact Pedant Parity

Goal:

- close the remaining artifact-specific coverage gaps without changing the current public contract

Status:

- completed on 2026-04-22

Changes:

- extend [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for any remaining gaps in:
  - create, repeated `PUT`, read, and delete edge cases
  - route semantics and mismatch behavior
  - API-version-sensitive response shaping
  - omission and default behavior on artifact reads and writes
  - no-mutation guarantees for failed artifact mutations

Exit condition:

- cookbook-artifact compatibility no longer depends on a few broad happy-path tests

### Task 5: Finish Cookbook-Facing Blob-Linked Flow Parity

Goal:

- pin the remaining cookbook behaviors that depend on checksum blobs or signed download URLs

Status:

- completed on 2026-04-22

Changes:

- extend [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for any remaining gaps in:
  - signed recipe or artifact download behavior
  - checksum-backed mutation preconditions
  - cleanup and retention behavior that is already part of the visible cookbook contract
  - any cookbook-facing `blob_unavailable` case that belongs to the existing public surface

Exit condition:

- cookbook/blob integration behavior is explicit where clients can actually observe it

### Task 6: Finish Auth And Actor Parity On Cookbook Routes

Goal:

- make the remaining cookbook read and mutation behavior consistent across admin, normal member, outside user, and invalid user paths

Status:

- completed on 2026-04-22

Changes:

- extend [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for any remaining gaps in:
  - successful in-org normal-user cookbook and artifact actions
  - outside-user and invalid-user `401` and `403` coverage
  - no-mutation guarantees on failed cookbook and artifact actions
  - default-org and explicit-org actor parity where one side is still ahead

Exit condition:

- the remaining cookbook auth surface is explicit rather than inferred from other object types

### Task 7: Sync Docs And Close The Bucket

Goal:

- mark the cookbook/blob pedant slice complete and make the next roadmap bucket unambiguous

Status:

- completed on 2026-04-22

Changes:

- sync:
  - [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md)
  - [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
  - [docs/compatibility-matrix-template.md](/Users/oberon/Projects/coding/go/OpenCook/docs/compatibility-matrix-template.md)
  - [AGENTS.md](/Users/oberon/Projects/coding/go/OpenCook/AGENTS.md)
- update this plan document with completion status and a short summary

Exit condition:

- the roadmap no longer treats cookbook/blob pedant work as an open compatibility bucket

## Completion Summary

This slice is complete.

It closed the remaining cookbook and cookbook-artifact compatibility gaps on the current HTTP contract, including:

- cookbook route semantics and org-scoped validation/no-mutation parity
- cookbook-artifact route semantics, org-scoped validation/no-mutation parity, and API v2 read shaping
- explicit-org blob-linked cookbook and artifact parity for signed downloads, checksum preconditions, provider-backed `blob_unavailable`, and visible checksum cleanup/retention
- remaining cookbook and cookbook-artifact auth parity on collection, named-filter, named-collection, and explicit-org mutation paths

The next roadmap bucket is no longer “remaining cookbook pedant coverage.” It is the post-compatibility follow-on work around PostgreSQL-backed cookbook persistence and the broader storage/provider path.

## Suggested Commit Sequence

1. `docs: inventory remaining cookbook pedant coverage`
2. `test: pin cookbook route semantics parity`
3. `test: pin cookbook validation and no-mutation parity`
4. `test: pin cookbook artifact pedant parity`
5. `test: pin cookbook blob-linked flow parity`
6. `test: pin cookbook auth parity`
7. `docs: close cookbook pedant compatibility bucket`

## Verification

Minimum verification for each task:

```bash
gofmt -w internal/api/cookbook_routes_test.go
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

For docs-only updates:

```bash
git diff -- docs/
```
