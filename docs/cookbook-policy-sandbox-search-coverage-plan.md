# Cookbook, Policy, And Sandbox Search Coverage Plan

Status: completed

## Summary

This bucket resolves the roadmap item for cookbook, policy, and sandbox search coverage after the broader Lucene/query-string compatibility work.

The goal is not to add new search indexes just because OpenCook now persists more object families. The goal is to pin Chef-compatible behavior for cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum-related state across memory-backed search and active OpenSearch-backed search. If upstream Chef exposes a public search index for one of those families, OpenCook should implement that index with the same ACL-filtered response shape. If upstream does not expose it, OpenCook should freeze the negative contract so those objects do not appear in `/search` index listings and do not become queryable through invented `/search/*` surfaces.

Use this file as the reference plan for the cookbook/policy/sandbox search coverage bucket.

## Current State

OpenCook already has:

- `/search` and `/organizations/{org}/search` index listing routes.
- `/search/{client,environment,node,role}` and org-scoped aliases.
- Dynamic per-data-bag search indexes.
- GET full search responses with `start`, `total`, and `rows`.
- POST partial search responses with `url` and `data`.
- ACL filtering after search provider matches.
- In-memory fallback search when OpenSearch is not configured.
- Active OpenSearch-backed search when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Startup rebuild, mutation upsert/delete hooks, stale-ID ignoring, operational reindex/check/repair, and Docker functional coverage for the implemented indexes.
- PostgreSQL-backed persistence for cookbooks, cookbook artifacts, policies, policy groups, sandboxes, checksum references, nodes, environments, roles, clients, and data bags/items.
- Node policy fields that remain searchable as node document fields.

The current public search object families are clients, environments, nodes, roles, and data bags. Cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum state is persisted and API-visible through its own routes, but is not currently exposed as public search indexes.

## Upstream Compatibility Signals

Primary local upstream references:

- `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_index_expand.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_index_query.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_objects/src/chef_object.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/oc_chef_object_db.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/chef_wm_search.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/chef_wm_search_index.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/chef_reindex.erl`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/search/search_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/search_util.rb`

Confirmed upstream signals from Task 1:

- Chef's search index listing route describes the standard indexes as `client`, `environment`, `node`, and `role`, plus one index for each data bag (`chef_wm_search_index.erl:67-78`).
- Chef's reindex path uses the same effective surface: clients, data bag items, environments, nodes, and roles (`chef_reindex.erl:29-68`).
- Chef's search response hydration only accepts `client`, `environment`, `node`, `role`, and data bag index types in its bulk-get path (`chef_wm_search.erl:245-256`).
- Pedant search helpers treat only `environment`, `role`, `node`, and `client` as valid built-in object types (`search_util.rb:174-181`) and list only those built-ins plus data bags (`search_util.rb:631-638`).
- Chef's write path only sends objects to search when `chef_object:is_indexed/1` is true (`oc_chef_object_db.erl:151-165`).
- Upstream cookbook versions, cookbook artifacts, cookbook artifact versions, policies, policy revisions, policy groups, and policy-group revision associations currently report `is_indexed` as false.
- Upstream sandbox records are fetched through sandbox-specific callbacks and are not a full `chef_object` indexing target in the same way nodes, roles, environments, clients, and data bag items are.
- Pedant explicitly pins node searches by `policy_name` and `policy_group`, which makes policyfile state searchable as node document fields rather than as separate policy indexes (`search_spec.rb:117-156`).
- `SEARCH_AND_INDEXING.md` uses broad language about indexed Chef objects, so this bucket should still do a deliberate inventory before closing the question.

## Final Searchability Matrix

Task 1 completed this matrix from the local Chef Server checkout. The contract is that this bucket should harden unsupported-index behavior for cookbook, policy, sandbox, and checksum families rather than exposing new public `/search/*` indexes.

| Object family | Final classification | Evidence | Completed action |
| --- | --- | --- | --- |
| Cookbook versions | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and `chef_cookbook_version:is_indexed/1` returns false (`chef_cookbook_version.erl:763-767`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Cookbook artifacts | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and artifact records/versions return false from `is_indexed/1` (`oc_chef_cookbook_artifact.erl:94-98`, `oc_chef_cookbook_artifact_version.erl:99-104`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Policies | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and `oc_chef_policy:is_indexed/1` returns false (`oc_chef_policy.erl:158-162`) | Preserved policy API routes and node policy search fields only |
| Policy revisions | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and `oc_chef_policy_revision:is_indexed/1` returns false (`oc_chef_policy_revision.erl:181-185`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Policy groups | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and `oc_chef_policy_group:is_indexed/1` returns false (`oc_chef_policy_group.erl:124-128`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Policy-group assignments | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and `oc_chef_policy_group_revision_association:is_indexed/1` returns false (`oc_chef_policy_group_revision_association.erl:225-229`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Sandboxes | Not publicly searchable | Not in the public index list, not in reindex surfaces, not accepted by search hydration, and sandbox handling does not provide a search-indexing callback (`chef_sandbox.erl:21-32`, `chef_wm_sandboxes.erl:65-129`) | Pinned unsupported public index behavior across memory, OpenSearch, admin, and functional paths |
| Checksum references | Not publicly searchable | No public index or reindex signal; checksum values are sandbox/cookbook lifecycle metadata, not an accepted search hydration type (`chef_wm_search.erl:245-256`) | Pinned unsupported public index behavior and continued to avoid blob-content indexing |
| Node policy refs | Public through node documents | Pedant searches nodes by `policy_name` and `policy_group`; `chef_node:is_indexed/1` returns true (`search_spec.rb:117-156`, `chef_node.erl:147-151`) | Preserved node-index behavior without inventing policy joins |

## Task 1 Decision

Task 1 confirms that this bucket is primarily a negative-compatibility hardening slice. OpenCook should not add public cookbook, cookbook-artifact, policy, policy-group, sandbox, or checksum search indexes in this bucket. The strongest next implementation work is Task 2: pin the unsupported-index contract across memory-backed and active OpenSearch-backed search while those persisted object families exist.

## Interfaces And Behavior

- Do not add, remove, or rename Chef-facing search routes unless upstream evidence proves OpenCook is missing a public Chef route.
- Preserve default-org and explicit-org route alias behavior.
- Preserve current full-search response keys: `start`, `total`, and `rows`.
- Preserve current partial-search response row shape: `url` and `data`.
- Preserve missing-index and missing-organization error shapes unless upstream inventory proves a mismatch.
- Preserve ACL filtering after search provider matching.
- Preserve PostgreSQL-backed state as the source of truth for hydrated results when PostgreSQL is configured.
- Preserve memory-backed search as the no-OpenSearch fallback.
- Preserve active OpenSearch as a derived index, not as the authoritative object store.
- Do not expose `/search/cookbook`, `/search/cookbook_artifacts`, `/search/policy`, `/search/policy_group`, `/search/sandbox`, `/search/checksum`, or similar indexes unless upstream evidence proves Chef exposes them.
- Do not index cookbook file content, blob content, encrypted data bag secrets, private keys, or provider-internal fields.
- Keep node `policy_name` and `policy_group` searchable as node compatibility fields, not as foreign-key joins to policy objects.
- Keep `/status`, `/readyz`, root payload keys, and public route shapes stable. Human-readable status or next-step wording may change only during bucket closeout.

## Compatibility Contract To Freeze

This slice should freeze the object-family search contract before changing implementation behavior:

- Search index listing:
  - built-in indexes
  - data bag indexes
  - behavior when cookbooks, cookbook artifacts, policies, policy groups, sandboxes, and checksum references exist
- Unsupported index behavior:
  - GET full search for unsupported object-family names
  - POST partial search for unsupported object-family names
  - default-org and org-scoped route aliases
  - missing-organization and ambiguous-default-org precedence
- Searchable fields that already exist:
  - node policy fields
  - role run-list fields
  - environment cookbook constraints
  - data bag item fields
- Any upstream-confirmed new indexes:
  - document IDs
  - index names
  - returned row payload shape
  - partial-search field extraction
  - URL generation
  - ACL resource mapping
  - create, update, delete, restart, and rehydration behavior
  - provider-unavailable degradation
  - stale-document check/repair behavior
- Operational behavior:
  - memory and OpenSearch parity
  - admin reindex/check/repair scope
  - status wording
  - Docker functional coverage

## Task Breakdown

### Task 1: Inventory Upstream Object-family Searchability

Status:

- Completed. The final searchability matrix above confirms that cookbook versions, cookbook artifacts, policies, policy revisions, policy groups, policy-group assignments, sandboxes, and checksum references are not public Chef search indexes.
- Completed. The only policy-related public search behavior found in upstream evidence is node-field search for `policy_name` and `policy_group`.
- Completed. The rest of this bucket should harden negative compatibility and preserve existing indirect fields rather than add new public indexes.

- Finalize the searchability matrix in this plan for:
  - cookbook versions
  - cookbook artifacts
  - policies
  - policy revisions
  - policy groups
  - policy-group assignments
  - sandboxes
  - checksum references
- Confirm the public index list from `chef_wm_search_index.erl`, `chef_wm_search.erl`, `chef_reindex.erl`, pedant search specs, and object `is_indexed` callbacks.
- Record whether each object family is:
  - publicly searchable
  - indexed only internally
  - not indexed
  - visible only as fields on another searchable object
- Decide, with upstream evidence cited in this file, whether this bucket is mostly a negative-compatibility hardening slice or whether one or more new public indexes are required.

### Task 2: Pin Negative Compatibility For Unsupported Public Indexes

Status:

- Completed. Memory-backed route tests now create cookbook versions, cookbook artifacts, policies, policy groups, policy assignments, sandboxes, and checksum references before asserting those families do not appear in `/search` or `/organizations/{org}/search` index listings.
- Completed. Default-org and org-scoped GET/POST search routes now pin the Chef-style `404` message for unsupported cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum index names.
- Completed. Active PostgreSQL plus OpenSearch-backed route coverage now proves the same unsupported-index contract after restart/rehydration, without consulting the provider for invented indexes.
- Completed. Route precedence coverage now pins missing-organization handling ahead of unsupported-index handling, and unsupported-index handling ahead of read authorization.

- Add route-level coverage proving unsupported cookbook/policy/sandbox/checksum search indexes stay unsupported even when those objects exist.
- Cover default-org and org-scoped aliases for GET full search and POST partial search.
- Cover memory-backed search and active OpenSearch-backed search.
- Preserve current unknown-index error shape and status code.
- Prove `/search` and `/organizations/{org}/search` listings do not include unsupported object families.
- Include auth and org-resolution precedence cases where existing search routes already define the contract.

### Task 3: Preserve Existing Indirect Search Fields

Status:

- Completed. Memory-backed and active PostgreSQL plus OpenSearch-backed route tests now seed cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum-reference state before querying the existing `node`, `role`, `environment`, and data bag indexes.
- Completed. Policyfile node fields, run-list-derived `recipe`/`role` fields, environment cookbook constraints, and data bag fields remain searchable only through their existing supported indexes.
- Completed. Partial search coverage now selects policyfile node fields, node run lists, and nested data bag fields without introducing joins to unsupported object-family indexes.
- Completed. Provider-backed coverage returns both allowed and denied node IDs from OpenSearch and verifies ACL filtering removes the denied result before full or partial response shaping.

- Add coverage proving policy-related node fields remain searchable on the node index.
- Add coverage proving role run lists, environment constraints, and data bag fields retain their existing query behavior while cookbook/policy/sandbox objects exist.
- Verify partial search can still select the existing fields without accidentally introducing joins to unsupported object families.
- Verify ACL filtering still happens after provider matching and before response shaping.

### Task 4: Implement Cookbook Or Cookbook-artifact Search Only If Upstream Requires It

Status:

- Completed as a no-op implementation. Upstream confirms cookbook versions and cookbook artifacts are not public Chef search indexes, so OpenCook must not add cookbook or cookbook-artifact search document builders, OpenSearch emission, URL shaping, or ACL mappings.
- Completed. The compatibility evidence is the public index list containing only `client`, `environment`, `node`, `role`, and data bags; the reindex path covering only clients, data bag items, environments, nodes, and roles; and search hydration accepting only those same types (`chef_wm_search_index.erl:67-78`, `chef_reindex.erl:29-68`, `chef_wm_search.erl:245-256`).
- Completed. Cookbook version and cookbook-artifact records report `is_indexed/1` as false and their indexing serializers are unavailable or explicitly `not_indexed` (`chef_cookbook_version.erl:763-767`, `oc_chef_cookbook_artifact.erl:94-98`, `oc_chef_cookbook_artifact_version.erl:99-104`).
- Completed. Task 2 route tests are the implementation guardrail for this decision: cookbook and cookbook-artifact records can exist in memory-backed and active PostgreSQL plus OpenSearch-backed modes, but `/search/cookbook`, `/search/cookbooks`, and `/search/cookbook_artifacts` remain unsupported and absent from index listings.
- Completed. Task 3 preserves the only cookbook-adjacent public search behavior found in this bucket: environment cookbook constraints and run-list-derived recipe fields remain searchable through `environment`, `node`, and `role` documents without indexing cookbook metadata, checksum rows, blob bytes, or provider-internal state.

- If upstream confirms cookbook or cookbook-artifact indexes are not public Chef indexes, mark this task complete by keeping negative tests from Task 2 and documenting the decision here.
- If upstream confirms one of these indexes is public, add the narrowest compatible implementation:
  - search document builders
  - memory-backed query support
  - OpenSearch document emission
  - startup rebuild from PostgreSQL-backed cookbook state
  - create/update/delete mutation indexing hooks around the existing cookbook store seam
  - URL generation and partial-search shaping
  - ACL resource mapping to the existing cookbook or cookbook-artifact permissions
- Cover shared checksum references without indexing blob content or provider-internal state.

### Task 5: Implement Policy Or Policy-group Search Only If Upstream Requires It

Status:

- Completed as a no-op implementation. Upstream confirms policies, policy revisions, policy groups, and policy-group assignments are not public Chef search indexes, so OpenCook must not add policy or policy-group search document builders, OpenSearch emission, URL shaping, or ACL mappings.
- Completed. The same public search surface evidence applies here: Chef lists and reindexes only `client`, `environment`, `node`, `role`, and data bag indexes, and search hydration accepts only those types (`chef_wm_search_index.erl:67-78`, `chef_reindex.erl:29-68`, `chef_wm_search.erl:245-256`).
- Completed. Policy-related persisted records report `is_indexed/1` as false (`oc_chef_policy.erl:158-162`, `oc_chef_policy_revision.erl:181-185`, `oc_chef_policy_group.erl:124-128`, `oc_chef_policy_group_revision_association.erl:225-229`).
- Completed. Task 2 route tests are the implementation guardrail for this decision: policy and policy-group records can exist in memory-backed and active PostgreSQL plus OpenSearch-backed modes, but `/search/policy`, `/search/policies`, `/search/policy_group`, and `/search/policy_groups` remain unsupported and absent from index listings.
- Completed. Task 3 preserves the upstream-pinned public policyfile search behavior: `policy_name` and `policy_group` remain searchable fields on node documents, matching pedant coverage, without joining to or exposing policy objects as their own search indexes (`search_spec.rb:117-156`).

- If upstream confirms policy and policy-group objects are not public Chef search indexes, mark this task complete by keeping negative tests from Task 2 and documenting the decision here.
- If upstream confirms one of these indexes is public, add the narrowest compatible implementation:
  - policy or policy-group document builders
  - memory-backed query support
  - OpenSearch document emission
  - startup rebuild from PostgreSQL-backed policy state
  - policy revision, policy group, and assignment mutation indexing hooks
  - URL generation and partial-search shaping
  - ACL resource mapping to existing policy or policy-group permissions
- Keep node `policy_name` and `policy_group` search as node-field behavior regardless of whether policy objects get their own index.

### Task 6: Implement Sandbox Or Checksum Search Only If Upstream Requires It

Status:

- Completed as a no-op implementation. Upstream confirms sandboxes and checksum references are not public Chef search indexes, so OpenCook must not add sandbox/checksum search document builders, OpenSearch emission, URL shaping, or ACL mappings.
- Completed. Chef's public search list and reindex paths still expose only `client`, `environment`, `node`, `role`, and data bag indexes, and search hydration accepts only those types (`chef_wm_search_index.erl:67-78`, `chef_reindex.erl:29-68`, `chef_wm_search.erl:245-256`).
- Completed. Sandbox handling is lifecycle-specific upload metadata: `chef_sandbox.erl` exports parse/fetch helpers but no `is_indexed/1` callback, and `chef_wm_sandboxes.erl` builds sandbox responses directly from checksum upload state instead of indexing those records (`chef_sandbox.erl:21-32`, `chef_wm_sandboxes.erl:87-129`).
- Completed. Task 2 route tests are the implementation guardrail for this decision: sandbox and checksum-reference state can exist in memory-backed and active PostgreSQL plus OpenSearch-backed modes, but `/search/sandbox`, `/search/sandboxes`, `/search/checksum`, and `/search/checksums` remain unsupported and absent from index listings.
- Completed. This bucket must continue to avoid indexing raw blob bytes, cookbook file contents, checksum-upload URLs, S3-compatible object paths, credentials, or provider-internal metadata.

- If upstream confirms sandboxes and checksum references are not public Chef search indexes, mark this task complete by keeping negative tests from Task 2 and documenting the decision here.
- If upstream confirms one of these indexes is public, add the narrowest compatible implementation:
  - sandbox or checksum document builders
  - memory-backed query support
  - OpenSearch document emission
  - startup rebuild from PostgreSQL-backed sandbox/checksum state
  - sandbox create/commit/delete mutation indexing hooks
  - URL generation and partial-search shaping
  - ACL resource mapping to existing sandbox permissions
- Do not index raw blob bytes, cookbook file content, S3 paths, credentials, upload URLs, or private provider metadata.

### Task 7: Keep Search Rebuild And Mutation Indexing Truthful

Status:

- Completed. Search rebuild tests now seed unsupported policy, policy-group, sandbox, and checksum-reference state and prove `DocumentsFromBootstrapState` still emits only public Chef search documents.
- Completed. Active PostgreSQL plus OpenSearch mutation coverage now proves cookbook versions, cookbook artifacts, policy assignments, and sandbox create/commit lifecycle mutations do not enqueue provider bulk upserts, deletes, refreshes, or document changes.
- Completed. Unsupported-index route coverage now proves a provider-unavailable active OpenSearch backend does not turn unsupported cookbook/policy/sandbox/checksum searches into `503 search_unavailable`; those routes keep the Chef-style unsupported-index `404` contract.
- Completed. Existing active OpenSearch hydration coverage continues to preserve stale-ID ignoring and provider-unavailable `503 search_unavailable` behavior for supported indexes.

- Ensure startup rebuild emits exactly the public Chef search documents and no invented cookbook/policy/sandbox/checksum documents.
- Ensure mutation hooks update or delete any upstream-confirmed new documents transactionally with the PostgreSQL-backed state.
- If no new public indexes are exposed, add tests proving cookbook, policy, and sandbox mutations do not enqueue OpenSearch documents.
- Preserve stale-ID ignoring after PostgreSQL hydration.
- Preserve provider-unavailable `503 search_unavailable` behavior for supported indexes only.

### Task 8: Extend Admin Reindex, Check, And Repair Coverage

Status:

- Completed. Admin reindex tests now seed unsupported policy, policy-group, and sandbox state plus stale provider IDs for cookbook, cookbook-artifact, and checksum scopes, proving reindex emits only supported public Chef search documents.
- Completed. Admin reindex and admin search scoped unsupported indexes now fail with `ErrIndexNotFound` before provider mutation.
- Completed. Admin search check reports unsupported provider document scopes as drift without adding them to public object-count summaries.
- Completed. Admin search repair deletes stale unsupported provider IDs without upserting unsupported documents, keeping repair sourced from PostgreSQL-backed supported search state.

- Ensure `opencook admin search reindex`, `check`, and `repair` report only supported public search indexes.
- If new upstream-confirmed indexes are added, include them in reindex/check/repair with create/update/delete/restart coverage.
- If no new indexes are added, add explicit coverage proving admin tooling does not advertise or repair unsupported cookbook/policy/sandbox/checksum indexes.
- Keep repair sourced from PostgreSQL-backed state, not from stale OpenSearch documents.

### Task 9: Add Functional Coverage For The Resolved Contract

Status:

- Completed. Docker functional create/verify phases now persist cookbook versions, cookbook artifacts, policy revisions, policy groups, sandboxes, and checksum-backed blob state before exercising active OpenSearch-backed search after restart.
- Completed. Functional search checks now prove unsupported cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum indexes remain absent from default-org and org-scoped index listings and return unsupported-index responses for full and partial search.
- Completed. Functional query compatibility now includes node `policy_name` and `policy_group` full and partial search through the supported node index, without exposing policy objects as standalone search indexes.
- Completed. Operational functional coverage now rejects unsupported admin reindex/check/repair scopes after restart and proves unscoped repair deletes stale unsupported provider documents without upserting unsupported search documents.

- Extend the Docker functional test scope to create cookbooks, cookbook artifacts, policies, policy groups, sandboxes, and checksum references before exercising search.
- Cover memory-backed behavior in unit or route tests and active OpenSearch-backed behavior in app/API/functional tests.
- Verify restart/rehydration before search and before admin check/repair.
- Verify partial search behavior for existing supported fields.
- Verify unsupported indexes remain unsupported in a deployed container if upstream inventory confirms they are not Chef public indexes.
- If upstream-confirmed new indexes are implemented, verify create/update/delete/restart parity against both memory and OpenSearch-backed modes.

### Task 10: Sync Docs And Close The Bucket

Status:

- Completed. Roadmap, milestone, compatibility-matrix, functional testing, and agent guidance docs now mark cookbook/policy/sandbox/checksum search coverage as resolved through negative compatibility rather than new search indexes.
- Completed. This plan is marked complete and its final matrix records the completed unsupported-index guardrails.
- Completed. The next recommended bucket now points at deeper API-version-specific object semantics, with OpenSearch provider capability/version hardening and migration/cutover tooling kept visible as follow-on operational work.

- Update `docs/chef-infra-server-rewrite-roadmap.md`.
- Update `docs/milestones.md`.
- Update `docs/compatibility-matrix-template.md`.
- Update `AGENTS.md`.
- Update this plan with the final searchability matrix and completion status.
- Mark this bucket complete without implying unsupported object families became searchable.
- Point the next bucket at deeper API-version-specific object semantics, OpenSearch provider capability/version hardening, or migration/cutover tooling, depending on what remains most urgent after this slice lands.

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/admin`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

Functional verification:

- Run the existing Docker functional stack with PostgreSQL and OpenSearch enabled.
- Run the remote functional test script against the OpenCook container.

Required scenarios:

- `/search` listing remains Chef-compatible when cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum records exist.
- Unsupported cookbook/policy/sandbox/checksum indexes return the existing missing-index contract if upstream confirms they are unsupported.
- Existing client, environment, node, role, and data bag search behavior remains unchanged.
- Node policy fields remain searchable through the node index.
- Memory-backed and active OpenSearch-backed search agree for supported indexes.
- Active OpenSearch restart/rehydration does not create unsupported documents.
- Admin reindex/check/repair includes only supported indexes, or includes any newly upstream-confirmed index with correct stale-document behavior.
- Provider-unavailable behavior remains `503 search_unavailable` only for routes that actually consult the provider.
- ACL filtering and partial-search shaping remain API-layer behavior after candidate hydration.

## Assumptions And Defaults

- Treat the local Chef Server checkout as the primary compatibility source.
- Treat pedant as a contract source, especially where prose docs use broad language.
- Do not add a public search index unless upstream route, reindex, object indexing, or pedant evidence supports it.
- Store and hydrate search responses from OpenCook's current PostgreSQL-backed state when PostgreSQL is configured.
- Keep OpenSearch as a derived acceleration layer.
- Prefer negative compatibility tests over speculative feature expansion when upstream evidence says an object family is not searchable.
- If upstream evidence contradicts the preliminary signals, implement the confirmed behavior narrowly and document the source.

## Out Of Scope

- Deeper API-version-specific object payload semantics.
- OpenSearch provider capability/version negotiation.
- Migration/cutover tooling.
- New admin endpoints beyond existing reindex/check/repair behavior.
- New storage abstractions.
- Cross-process cache invalidation redesign.
- Searching blob bytes, cookbook file contents, encrypted data bag plaintext, private keys, or provider-internal metadata.
