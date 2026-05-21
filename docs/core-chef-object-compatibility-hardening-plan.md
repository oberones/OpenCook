# Core Chef Object Compatibility Hardening Plan

Status: planned

## Summary

This bucket hardens the remaining Chef-facing object compatibility gaps across
the object surfaces OpenCook already implements. It comes after the persistence,
search, migration, live-source extraction, maintenance, operational, and
API-version buckets, so the goal is not to add a new storage architecture or a
new migration pipeline. The goal is to mine upstream Chef Server behavior and
the recent functional/live-source evidence for the highest-risk remaining
object mismatches, then pin them with focused route, service, persistence, and
functional coverage.

The slice should cover implemented object families only:

- nodes
- environments
- roles
- data bags and data bag items
- policy revisions, policy groups, and policy assignments
- sandboxes and checksum-reference metadata
- cookbook/cookbook-artifact object edge cases that are not already covered by
  the dedicated cookbook/blob buckets
- ACL-linked object reads and writes for those families

Use this file as the reference checklist for this bucket.

## Current State

OpenCook already has:

- Signed Chef request verification and API-version negotiation coverage.
- PostgreSQL-backed bootstrap core persistence for users, organizations,
  clients, keys, groups, containers, and ACL documents.
- PostgreSQL-backed core object persistence for nodes, environments, roles,
  data bags/items, policies, sandboxes, checksum references, object ACLs, and
  cookbook metadata.
- Provider-backed blob storage in memory, filesystem, and S3-compatible modes.
- Active OpenSearch-backed search for implemented Chef-searchable families.
- Operational reindex/check/repair, maintenance-mode write gating, migration
  backup/restore/import/sync/shadow/cutover tooling, and direct live Chef
  source extraction into normalized bundles.
- Broad API-version-specific object semantics coverage for implemented routes.
- Deep depsolver coverage for environment cookbook constraints, role expansion,
  run-list normalization, solver graph behavior, and auth precedence.

Remaining gap:

- Several implemented object surfaces still have compatibility edges that were
  not systematically mined from `oc-chef-pedant`, local Chef Server source, and
  functional/live-source findings.
- Some cross-surface behaviors still need a shared checklist: default-org
  aliases, explicit-org aliases, trailing slashes, extra path segments,
  method-not-allowed `Allow` headers, auth/ACL precedence, invalid-write
  no-mutation, and PostgreSQL restart/rehydration parity.
- Some linked-object reads, ACL-filtered reads, search projection updates, and
  persistence rollback cases are covered per surface but not yet stress-tested
  as one compatibility bucket.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, payloads, status codes, signed-auth
  behavior, API-version behavior, or ACL semantics except to match upstream Chef
  behavior on already implemented routes.
- Do not add licensing, license enforcement, license telemetry, or
  license-management endpoints.
- Do not add new public object families in this bucket. If an upstream behavior
  depends on an unimplemented family, record it as deferred instead of guessing.
- Keep PostgreSQL-backed state authoritative when configured and preserve the
  in-memory fallback behavior when PostgreSQL is absent.
- Keep OpenSearch documents derived from PostgreSQL-backed authoritative state.
  Do not treat search provider documents as source of truth.
- Keep provider-backed blob behavior and signed blob URL shapes stable. Cookbook
  and sandbox blob changes in this bucket should be limited to compatibility
  edges that are visible through already implemented object routes.
- Preserve the completed migration/live-source pipeline. This bucket may add
  functional fixtures or assertions, but it must not create a parallel migration
  path.
- Keep `/status`, admin command output, and operational runbook shapes stable
  unless wording must change to stay truthful.
- Prefer route-level compatibility tests first, then focused service/store
  tests where rollback, rehydration, search projection, or ACL state needs a
  narrower assertion.

## Upstream And Local Evidence Sources

Primary references:

- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/`
- `~/Projects/coding/ruby/chef-server/src/oc_bifrost/`
- `~/Projects/coding/ruby/chef-server/src/bookshelf/`
- `~/Projects/coding/ruby/chef-server/dev-docs/API_VERSIONING.md`
- `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`
- `~/Projects/coding/ruby/chef-server/dev-docs/BOOKSHELF.md`

Local OpenCook references:

- `internal/api`
- `internal/bootstrap`
- `internal/store/pg`
- `internal/search`
- `internal/blob`
- `cmd/opencook`
- `test/compat`
- `test/functional`
- `docs/compatibility-matrix-template.md`
- `docs/chef-infra-server-rewrite-roadmap.md`

## Task Breakdown

### Task 1: Build The Remaining Object Compatibility Inventory

Task status: complete.

- Inventory `oc-chef-pedant` object specs and local Chef Server code for nodes,
  environments, roles, data bags/items, policies, sandboxes, cookbooks,
  cookbook artifacts, and ACL-linked reads.
- Compare upstream behaviors against current OpenCook route and service tests.
- Create a task-local checklist of gaps grouped by:
  - route semantics
  - payload exactness
  - validation and error precedence
  - auth/ACL behavior
  - persistence rollback and rehydration
  - search projection
  - default-org versus explicit-org parity
- Record intentionally deferred behaviors and why they stay out of scope.
- Add lightweight inventory tests or table-driven fixtures where useful so the
  gap list does not drift during implementation.

Implementation notes:

- Added a lightweight inventory fixture in
  `internal/compat/core_object_inventory_test.go` so the bucket's planned
  families, upstream evidence classes, local coverage pointers, gap categories,
  and follow-on task mapping stay explicit.
- Upstream pedant evidence was found for nodes, environments, roles, data
  bags, policies, sandboxes, cookbooks, cookbook artifacts, and ACL-linked
  behaviors.
- Upstream source evidence was found in `oc_erchef` Webmachine resources,
  `chef_objects` modules, RAML schemas, SQL tests/schema files, and Bifrost ACL
  schema/resource code.
- Current OpenCook coverage is strongest for API-version behavior,
  PostgreSQL-backed restart/rehydration, OpenSearch projection, encrypted data
  bag opacity, depsolver behavior, and cookbook/blob lifecycle edges.
- The highest-value implementation work is therefore not broad new API design;
  it is filling cross-surface gaps for route semantics, payload exactness,
  validation/error precedence, auth/ACL no-mutation behavior, default-org
  parity, and representative functional object coverage.

Inventory matrix:

| Family | Primary upstream evidence | Current OpenCook anchors | Follow-on tasks |
| --- | --- | --- | --- |
| Nodes | `oc-chef-pedant/spec/api/nodes*`, knife node specs, `chef_wm_nodes.erl`, `chef_wm_named_node.erl`, `chef_node.erl`, node RAML schema | `node_api_version_routes_test.go`, `core_object_pg_persistence_routes_test.go`, search document/rebuild tests | Tasks 2, 3, 10, 11, 12 |
| Environments | pedant environment create/read/update/delete/cookbook/role/recipe specs, `chef_wm_environments.erl`, `chef_wm_named_environment.erl`, `chef_environment.erl` | `role_environment_api_version_routes_test.go`, `environment_cookbook_routes_test.go`, `environment_depsolver_routes_test.go`, core-object persistence tests | Tasks 2, 4, 10, 11, 12 |
| Roles | pedant role endpoint and knife role specs, `chef_wm_roles.erl`, `chef_wm_named_role.erl`, `chef_role.erl`, role RAML schema | `role_environment_api_version_routes_test.go`, `bootstrap/roles_test.go`, depsolver route tests, core-object persistence tests | Tasks 2, 5, 10, 11, 12 |
| Data bags/items | pedant data bag endpoint and knife data bag specs, `chef_wm_data.erl`, `chef_wm_named_data.erl`, `chef_wm_named_data_item.erl`, data bag object modules and RAML schemas | `data_bag_routes_test.go`, `data_bag_api_version_routes_test.go`, `bootstrap/data_bags_test.go`, core-object persistence tests | Tasks 2, 6, 10, 11, 12 |
| Policyfiles | pedant policy endpoint specs, policy Webmachine resources, policy authz modules, policy RAML examples | `policy_routes_test.go`, `policy_sandbox_api_version_routes_test.go`, `bootstrap/policies_test.go`, core-object persistence tests | Tasks 2, 7, 10, 11, 12 |
| Sandboxes/checksums | pedant sandbox endpoint specs, `chef_wm_sandboxes.erl`, `chef_wm_named_sandbox.erl`, `chef_sandbox.erl`, sandbox RAML schemas | `sandbox_routes_test.go`, `policy_sandbox_api_version_routes_test.go`, `bootstrap/sandboxes_test.go`, core-object persistence tests | Tasks 2, 8, 10, 11, 12 |
| Cookbooks/artifacts | pedant cookbook and artifact specs, knife cookbook specs, cookbook Webmachine resources, cookbook RAML docs | `cookbook_routes_test.go`, `cookbook_api_version_routes_test.go`, `cookbook_pg_provider_routes_test.go`, `bootstrap/cookbooks_test.go` | Tasks 2, 9, 10, 11, 12 |
| Object ACLs | pedant ACL specs, `oc_chef_wm_acl*.erl`, Bifrost ACL schema/functions/resources | validator/bootstrap ACL tests, maintenance repair tests, bootstrap service ACL tests, PostgreSQL core object tests | Tasks 2, 10, 11, 12 |

Gap groups frozen for Tasks 2-12:

- Route semantics: default-org aliases, explicit-org aliases,
  configured-default-org resolution, ambiguous default-org failures, missing
  organization precedence, trailing slashes, extra path segments, and exact
  method-not-allowed `Allow` headers.
- Payload exactness: omitted/defaulted fields, Chef metadata fields, route/name
  canonicalization, nested arbitrary JSON preservation, policyfile rich payloads,
  sandbox checksum maps, and cookbook/artifact read-shape leftovers.
- Validation and error precedence: invalid JSON, trailing JSON, invalid names,
  route/payload mismatches, missing linked objects, conflicts, and malformed
  payload subdocuments.
- Auth/ACL behavior: outside users, invalid users, normal org members, admins,
  clients, validators, container ACL checks, object ACL checks, and no-mutation
  guarantees on denied writes.
- Persistence/search behavior: active PostgreSQL rollback, restart/rehydration,
  OpenSearch projection updates, stale projection removal, unsupported-index
  stability, and provider-unavailable degradation where already part of the
  object contract.

Deferred from this bucket:

- New Chef object families that OpenCook has not implemented yet.
- Native Bifrost REST API compatibility outside the Chef-facing ACL/object
  surfaces OpenCook already exposes.
- Knife bulk-delete command parity as a command-level workflow. The underlying
  object delete semantics remain in scope.
- Broader online direct PostgreSQL repair mutations beyond the current
  maintenance-gated default ACL repair path.
- oc-id/OAuth, Redis runtime state, reporting/data-collector telemetry,
  licensing, and supervisor workflows.

### Task 2: Add A Cross-Surface Route Semantics Harness

Task status: complete.

- Add reusable route test helpers for implemented object surfaces covering:
  - default-org aliases
  - explicit-org aliases
  - configured default-org resolution
  - ambiguous default-org failures
  - missing organization precedence
  - trailing slash acceptance
  - extra path segment `404`s
  - method-not-allowed responses and exact `Allow` headers
- Use the harness to pin any missing route semantics for nodes, environments,
  roles, data bags/items, policies, sandboxes, and remaining cookbook/artifact
  object reads.
- Keep helper behavior test-only. Do not add runtime route abstractions unless
  implementation duplication creates a real bug risk.

Implementation notes:

- Added `internal/api/core_object_route_semantics_test.go` as a shared
  cross-surface route-semantics harness.
- The harness now pins default-org aliases, explicit-org aliases, trailing
  slash acceptance, configured default-org resolution, ambiguous default-org
  `400 organization_required` failures, missing-organization precedence, extra
  path `404`s, and exact `405 Allow` headers across nodes, environments,
  roles, data bags/items, policies, policy groups, sandboxes, cookbooks,
  cookbook artifacts, and universe reads.
- Added a narrow runtime helper for the node/environment/role collection-vs-
  named object `Allow` header split because those three handlers duplicated
  the same route-shape decision and had drifted.
- Tightened environment linked-read method responses so environment nodes,
  cookbooks, and recipes now return Chef-style `Allow` headers while preserving
  existing route and payload shapes.

### Task 3: Harden Node Object Compatibility

Task status: complete.

- Pin node create/read/update/delete payload exactness for omitted/defaulted
  fields, normal attributes, automatic attributes, override attributes,
  default attributes, run lists, policy fields, and Chef metadata fields.
- Cover invalid payload shapes, route/payload name mismatch, invalid names,
  bad JSON, trailing JSON, and unsupported top-level fields where upstream
  behavior is known.
- Prove invalid writes, auth failures, and ACL failures do not mutate in-memory
  state, PostgreSQL state, verifier/search state, or object ACLs.
- Prove node search projection updates and stale projection removal after
  create/update/delete and after PostgreSQL restart/rehydration.

Implementation notes:

- Existing API-version node coverage already pinned create/read/update/delete
  payload exactness across omitted/defaulted fields, all four attribute maps,
  run lists, policy fields, Chef metadata fields, default-org aliases, explicit
  org aliases, active PostgreSQL restart/rehydration, and API-version failure
  no-mutation behavior.
- Added targeted validation coverage for invalid names, missing names,
  unsupported top-level fields, malformed JSON, trailing JSON, route/payload
  name mismatches, invalid map attributes, invalid run lists, invalid
  `json_class`, and invalid policy fields.
- Added active PostgreSQL plus OpenSearch coverage proving rejected validation,
  authentication, and ACL write paths preserve PostgreSQL-backed node state,
  OpenSearch documents, verifier behavior, and object ACLs.
- Added restart-backed OpenSearch lifecycle coverage proving node projection
  updates and stale projection deletes continue to work after PostgreSQL
  rehydration.

### Task 4: Harden Environment Object Compatibility

Task status: complete.

- Pin environment create/read/update/delete payload exactness for cookbook
  constraints, default/override attributes, `_default` immutability, and
  Chef metadata fields.
- Cover environment rename behavior, route/payload name mismatch, invalid
  cookbook constraint shapes, invalid names, bad JSON, trailing JSON, and
  no-mutation failures.
- Reconfirm linked routes:
  - `/environments/{name}/cookbooks`
  - `/environments/{name}/cookbook_versions`
  - `/environments/{name}/nodes`
  - `/environments/{name}/recipes`
- Preserve the existing depsolver contract and only add cases where upstream
  object behavior changes the environment surface itself.

Implementation notes:

- Existing API-version environment coverage already pinned create/read/update,
  rename, delete, omitted/defaulted fields, cookbook constraints,
  default/override attributes, `_default` immutability, Chef metadata fields,
  linked empty routes, and active PostgreSQL restart/rehydration.
- Added a focused validation/no-mutation matrix for invalid names, missing
  names, unsupported top-level keys, malformed JSON, trailing JSON,
  rename-to-existing conflicts, invalid cookbook constraint maps, invalid
  cookbook names/constraints, invalid attribute maps, and invalid
  `json_class`.
- Added linked-route coverage with real cookbook versions, recipe files, and
  environment-scoped node membership so `/cookbooks`, named cookbooks,
  `/nodes`, `/recipes`, and the empty depsolver path stay aligned with the
  environment object contract without expanding the depsolver scope.
- Added active PostgreSQL plus OpenSearch coverage proving rejected validation,
  authentication, and ACL write paths preserve persisted environment state,
  search documents, verifier behavior, and object ACLs.
- Added restart-backed rename, update, and delete coverage proving environment
  ACLs move on rename and stale OpenSearch projections are removed after
  update/delete and PostgreSQL rehydration.

### Task 5: Harden Role And Run-List Object Compatibility

Task status: complete.

- Pin role create/read/update/delete payload exactness for `run_list`,
  `env_run_lists`, descriptions, metadata fields, omitted/defaulted fields, and
  normalization/deduplication behavior.
- Cover invalid run-list entries, invalid environment-specific run lists,
  route/payload name mismatch, invalid names, bad JSON, trailing JSON, and
  no-mutation failures.
- Reconfirm linked role routes:
  - `/roles/{name}/environments`
  - `/roles/{name}/environments/{environment}`
  - `/environments/{name}/roles/{role}`
- Preserve the existing depsolver role-expansion contract and add only the
  missing role-object compatibility edges.

Implementation notes:

- Existing API-version role coverage already pinned create/read/update/delete
  payload exactness, descriptions, Chef metadata fields, omitted/defaulted
  fields, run-list normalization/deduplication, environment-specific run-list
  normalization, active PostgreSQL restart/rehydration, and the core linked
  role routes.
- Added a focused validation/no-mutation matrix for invalid names, missing
  names, unsupported top-level keys, malformed JSON, trailing JSON,
  route/payload name mismatches, invalid `run_list` shapes/items, invalid
  `env_run_lists` shapes/items/environment names, invalid attribute maps,
  invalid descriptions, and invalid `chef_type`.
- Added linked-route coverage proving normalized top-level and
  environment-specific run lists are preserved across
  `/roles/{name}/environments`, `/roles/{name}/environments/{environment}`,
  and `/environments/{name}/roles/{role}` including explicit-org aliases,
  empty environment overrides, and `null` for existing environments with no
  role override.
- Added active PostgreSQL plus OpenSearch coverage proving rejected validation,
  authentication, and ACL write paths preserve persisted role state, search
  documents, verifier behavior, and object ACLs.
- Added restart-backed update/delete coverage proving role search projections,
  linked environment run lists, and object ACL cleanup remain correct after
  PostgreSQL rehydration.

### Task 6: Harden Data Bag And Data Bag Item Compatibility

- Pin data bag list/create/read/delete behavior, including empty bags,
  duplicate creates, invalid names, bad JSON, trailing JSON, and method
  semantics.
- Pin data bag item create/read/update/delete behavior for item `id`, route
  item name, route/payload mismatch, missing data bag, invalid item shape,
  encrypted-looking payload opacity, nested arbitrary JSON, and no-mutation
  failures.
- Prove ordinary and encrypted-looking data bag search projections remain
  correct after create/update/delete and after PostgreSQL restart/rehydration.
- Preserve encrypted payload opacity. Do not add decrypt/reencrypt behavior in
  this bucket.

### Task 7: Harden Policyfile Object Compatibility

- Pin policy revision create/read/delete behavior for identifier, name,
  cookbook locks, default attributes, override attributes, named run lists,
  solution dependencies, and metadata fields.
- Pin policy group list/read/delete and policy assignment create/update/delete
  behavior for route/payload mismatch, missing policy revision, missing group,
  invalid revision identifiers, bad JSON, trailing JSON, and no-mutation
  failures.
- Reconfirm default-org and explicit-org alias parity for `/policies` and
  `/policy_groups`.
- Preserve the current contract that policyfile state is not publicly
  searchable through Chef search indexes except for node policy compatibility
  fields.

### Task 8: Harden Sandbox And Checksum Metadata Compatibility

- Pin sandbox create/commit/read behavior for route semantics, checksum maps,
  upload URL shape, completed/failed commit behavior, bad JSON, trailing JSON,
  missing checksum references, invalid checksums, and no-mutation failures.
- Prove sandbox-held checksums still prevent blob deletion when cookbook or
  artifact metadata is removed.
- Reconfirm PostgreSQL-backed sandbox/checksum restart/rehydration and live
  source import/export compatibility.
- Keep blob provider behavior stable and avoid adding new blob repair/admin
  flows in this bucket.

### Task 9: Sweep Cookbook And Cookbook-Artifact Object Edges

- Mine pedant and local Chef references for cookbook/cookbook-artifact object
  edges not already covered by the cookbook/blob hardening buckets.
- Prioritize remaining read-shape, validation, auth, route-semantics,
  API-version, and PostgreSQL restart/rehydration gaps over broad new cookbook
  behavior.
- Reconfirm shared-checksum retention and cleanup behavior only where a newly
  added object edge could affect it.
- Keep signed download URL shape and provider-backed blob behavior unchanged.

### Task 10: Pin Cross-Surface Auth, ACL, And No-Mutation Precedence

- Add a matrix that proves outside users, invalid users, normal org members,
  admins, clients, validators, and missing ACL grants behave consistently on
  implemented object routes.
- Pin auth-before-body or body-before-auth precedence only where upstream Chef
  evidence is known.
- Prove failed auth, failed ACL, invalid payload, missing linked object, and
  persistence failure paths do not partially mutate:
  - in-memory service state
  - PostgreSQL rows
  - OpenSearch documents
  - blob/checksum references
  - object ACLs
- Preserve current Chef-shaped `401`, `403`, `404`, `405`, `409`, and `400`
  response shapes unless upstream evidence requires a targeted correction.

### Task 11: Add Active PostgreSQL And OpenSearch Regression Coverage

- For each object family touched in Tasks 3-9, add restart/rehydration coverage
  on the active PostgreSQL path.
- Reconfirm search rebuild, mutation indexing, stale document handling, and
  unsupported-index behavior for object families that should or should not be
  searchable.
- Add focused store tests for rollback or rehydration gaps found during the
  route-level work.
- Keep OpenSearch provider documents derived from PostgreSQL; do not add
  direct provider-as-truth reads.

### Task 12: Extend Functional Coverage For The Hardened Object Paths

- Extend the Docker functional harness with a focused object-compatibility
  phase that exercises representative node, environment, role, data bag,
  policy, sandbox, cookbook, search, restart, and rehydration flows.
- Keep the phase deterministic and small enough to run alongside existing
  migration/search/operational phases.
- Include remote Docker guidance in `docs/functional-testing.md` if the new
  phase needs extra invocation examples.
- Avoid requiring a full upstream Chef Server container; live-source fixtures
  and local functional checks remain enough unless a later task explicitly
  needs upstream shadow evidence.

### Task 13: Sync Docs And Close The Bucket

- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `AGENTS.md`
  - this plan file
- Mark this bucket complete once route, persistence, search, functional, and
  documentation coverage land.
- Point the next bucket at the highest-risk remaining Chef compatibility gap
  found during this slice, or at broader online repair/admin mutation parity if
  object compatibility hardening does not expose a higher-risk blocker.

## Test Plan

Focused tests:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
```

Full verification:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```text
scripts/functional-compose.sh create verify query-compat search-update verify-search-updated restart
scripts/functional-compose.sh migration-live-source-all
scripts/functional-compose.sh object-compat
```

Required scenarios:

- Default-org, configured-default-org, ambiguous-default-org, and explicit-org
  route parity across touched object families.
- Trailing-slash, extra-path, and method-not-allowed route semantics with exact
  `Allow` headers where applicable.
- Payload exactness for reads and successful writes across nodes,
  environments, roles, data bags/items, policies, sandboxes, and remaining
  cookbook/artifact object edges.
- Invalid JSON, trailing JSON, invalid payload, route/payload mismatch,
  missing linked object, auth failure, ACL failure, conflict, and persistence
  failure no-mutation behavior.
- Active PostgreSQL restart/rehydration for every touched object family.
- OpenSearch projection correctness for searchable families and
  unsupported-index stability for non-searchable families.
- Encrypted-looking data bag item opacity.
- Sandbox-held checksum retention and cookbook/artifact shared-checksum
  retention where newly covered edges touch checksum references.
- Functional Docker coverage for a representative end-to-end object lifecycle
  and restart flow.

## Assumptions

- This bucket should harden implemented object surfaces, not add brand-new Chef
  object families.
- PostgreSQL remains the durable source of truth when configured, and the
  in-memory fallback remains supported.
- OpenSearch remains derived from PostgreSQL-backed state.
- Blob providers remain behind the existing blob interfaces and signed URL
  shapes.
- Any compatibility correction should be backed by upstream pedant evidence,
  local Chef Server source, functional evidence, or an explicit documented
  inference.
- Licensing, license telemetry, oc-id/OAuth, Redis runtime state, and upstream
  supervisor workflows remain out of scope.
