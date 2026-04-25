# OpenSearch-backed Indexing And Query Parity Plan

Status: completed

## Summary

This bucket replaced the current in-memory-only search compatibility implementation with an OpenSearch-backed indexing and query path when OpenSearch and PostgreSQL are configured, while preserving the Chef-facing search API already pinned in OpenCook.

The compatibility goal is not to invent a new search model. Chef clients should continue to use the existing `/search`, `/search/{index}`, `/organizations/{org}/search`, and `/organizations/{org}/search/{index}` routes with the same request and response shapes. PostgreSQL-backed bootstrap/core-object state remains the system of record. OpenSearch is the external search index used to find matching object IDs, and OpenCook still hydrates response payloads from the current service state before applying ACL filtering and partial-search shaping.

The configured Docker functional stack now proves that OpenSearch is active, useful, and consistent enough for the currently implemented search surfaces.

## Current State

OpenCook now has:

- `internal/search.Index` with `Indexes` and `Search` query methods.
- `search.MemoryIndex`, which derives documents directly from `bootstrap.Service` when OpenSearch is not configured.
- `search.OpenSearchIndex`, which asks OpenSearch for matching document IDs, hydrates current objects from PostgreSQL-backed `bootstrap.Service` state, and preserves API-layer ACL filtering and partial-search shaping.
- `search.NoopIndex`, which is retained only for explicitly unavailable/unconfigured edge paths.
- Search routes for:
  - `/search`
  - `/search/{client,environment,node,role}`
  - `/search/{bag}`
  - `/organizations/{org}/search`
  - `/organizations/{org}/search/{client,environment,node,role}`
  - `/organizations/{org}/search/{bag}`
- GET full search responses.
- POST partial search responses.
- ACL filtering in the API layer after search results are produced.
- Dynamic data bag indexes.
- Basic query support for `*:*`, field terms, unqualified terms, prefix wildcards, `OR`, `AND`, `NOT`, `-term`, and escaped `:`, `[`, `]`, `@`, and `/`.
- PostgreSQL-backed persistence for the object state that feeds search: clients, environments, nodes, roles, data bags/items, policies, policy groups, sandboxes, and ACLs.
- Active OpenSearch startup rebuild/reconciliation from PostgreSQL-backed state when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Successful mutation indexing for clients, environments, nodes, roles, and data bag items on the active OpenSearch path.
- Stable `503 search_unavailable` route degradation for provider failures where the current route contract surfaces search unavailability.
- A functional Docker stack where PostgreSQL and OpenSearch share the Compose network and search routes prove active OpenSearch behavior across restart, update, and delete phases.

Upstream Chef behavior to preserve:

- Chef expands objects into search documents before indexing.
- The search provider returns matching IDs, not final API payloads.
- Erchef then fetches current object state from PostgreSQL and filters by READ ACLs.
- Partial search is shaped by the API after object hydration.
- OpenSearch uses a `chef` index, `_bulk` writes, `/chef/_search`, refresh behavior, and delete-by-query or equivalent deletion behavior.
- Search failures should not leak provider internals into Chef-facing responses.

## Interfaces And Behavior

- No Chef-facing route, method, trailing-slash, payload, response, or signed-auth changes on search routes.
- Preserve default-org and explicit-org route alias behavior.
- Preserve current search response keys: `start`, `total`, and `rows`.
- Preserve current partial-search response row shape: `url` and `data`.
- Preserve current Chef-style missing data bag index error text.
- Preserve ACL filtering in the API layer after search provider results are returned.
- Preserve PostgreSQL-backed state as the source of truth for hydrated result payloads.
- Keep the in-memory search path as the default when `OPENCOOK_OPENSEARCH_URL` is absent.
- Do not make OpenSearch the only copy of any Chef object.
- Do not add public reindex, repair, or admin endpoints in this bucket.
- Do not broaden this bucket into encrypted data bags, cookbook search, operational admin tooling, or migration tooling unless a task explicitly calls it out.
- Keep `/status`, `/readyz`, and root payload keys stable. Human-readable backend wording may change.

## Compatibility Contract To Freeze

Task 1 status: completed.

Task 1 froze the current memory-backed search contract as the baseline that active OpenSearch mode must match before behavior is widened. The upstream inventory confirms the compatibility model: Chef expands objects into provider documents, the search provider returns IDs, erchef fetches the current object rows from PostgreSQL, and ACL filtering plus partial-search shaping happen after hydration.

The route-level baseline now explicitly pins:

- default-org and explicit-org search index URL shaping
- dynamic data bag index URL shaping
- default paging values of `start: 0` and all matching rows when `rows` is omitted or `0`
- deterministic current ordering for memory-backed results
- org-scoped query alias behavior
- current method behavior for index listing and query routes
- invalid `start` and `rows` query errors
- Chef-style missing data bag index errors on default-org and explicit-org aliases
- ACL-filtered search `total` and rows after denied resources are removed

Current out-of-scope behavior remains:

- cookbook search
- policy, policy group, and sandbox search indexes
- encrypted data bag semantics
- public reindex, repair, or admin endpoints
- migration tooling
- broader Lucene/query-string behavior beyond the compatibility subset already pinned

### Indexed Surfaces

This bucket targets the currently implemented search surfaces first:

- clients
- environments
- nodes
- roles
- data bag items, one dynamic index per data bag

Cookbook, policy, sandbox, and encrypted-data-bag search behavior stays out of scope unless upstream inventory proves one is required for the current search contract.

### Document Identity

OpenSearch documents need stable IDs that can be converted back into a current OpenCook object lookup:

- client: organization plus client name
- environment: organization plus environment name
- node: organization plus node name
- role: organization plus role name
- data bag item: organization plus bag name plus item ID

The route layer should not return OpenSearch source documents directly. It should hydrate from `bootstrap.Service` and silently ignore IDs whose object rows no longer exist, matching the upstream "search returns IDs, database returns objects" model.

### Document Expansion

The document expansion contract should remain shared between memory and OpenSearch paths:

- nested fields are searchable by joined path names and leaf names
- node attributes merge default, normal, override, and automatic data for partial search
- node `run_list` supports `recipe` and `role` derived search fields
- environment `cookbook_versions` remains searchable
- role run lists and attributes remain searchable
- data bag item search indexes raw item fields without requiring `raw_data_` prefixes for common queries
- policy fields on nodes remain searchable compatibility fields, not relational foreign keys

### Query Semantics

The first OpenSearch-backed pass should preserve existing query behavior before widening it:

- empty query and `*:*`
- exact field terms
- unqualified terms over any indexed field
- prefix wildcard terms
- `OR`
- `AND`
- `NOT` and `-`
- escaped `:`, `[`, `]`, `@`, and `/`
- stable pagination with `start` and `rows`
- deterministic ordering compatible with the current route tests

Additional Lucene-style semantics should be added only after the existing behavior is pinned against both memory and OpenSearch-backed paths.

### Consistency Model

PostgreSQL remains authoritative. OpenSearch is a derived index.

For this bucket:

- startup activation should rebuild or reconcile the OpenSearch index from PostgreSQL-backed state before serving active OpenSearch search traffic
- successful object writes should update the OpenSearch index for the affected object when active OpenSearch mode is enabled
- successful object deletes should remove the affected OpenSearch document when active OpenSearch mode is enabled
- invalid writes must not create, update, or delete OpenSearch documents
- stale OpenSearch IDs should not produce stale API rows if the PostgreSQL-backed object no longer exists
- cross-process cache invalidation and public reindex commands remain follow-on operational work

## Task Breakdown

### Task 1: Freeze The Current Search Contract

Status:

- Completed. The plan now records the upstream search/provider inventory and current OpenCook baseline. Route tests now pin memory-backed default-org and explicit-org search index URL shaping, dynamic data bag indexes, default pagination and current ordering, org-scoped query alias behavior, method and paging errors, Chef-style missing-index errors across aliases, and ACL-filtered `total`/row semantics.

- Add or extend tests that capture the current memory-backed search behavior as the compatibility baseline.
- Inventory upstream search route behavior from:
  - `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`
  - `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_opensearch.erl`
  - `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_solr.erl`
  - `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/search_util.rb`
  - search-related pedant specs for nodes, roles, environments, clients, and data bags
- Pin the in-scope indexes, route aliases, method behavior, pagination defaults, missing-index errors, and ACL-filtered response behavior.
- Record known out-of-scope behavior explicitly: cookbook search, encrypted data bag search, reindex tooling, admin commands, and broader migration tooling.

### Task 2: Extract Shared Document Expansion And Query Translation

Status:

- Completed. Search document construction now flows through a shared `DocumentBuilder`, with table-driven coverage for client, environment, node, role, and data bag item expansion. The tests pin nested path and leaf aliases, node attribute merge precedence, derived run-list fields, environment cookbook version fields, data bag raw-field indexing, and node policy fields. Query handling now has a `QueryPlan` seam that preserves the memory matcher contract while producing an OpenSearch `query_string` body for the future adapter.

- Move document construction and field expansion behind a reusable internal search document builder.
- Keep memory search and future OpenSearch search using the same expansion code to avoid drift.
- Add table-driven tests for client, environment, node, role, and data bag document expansion.
- Add tests for:
  - nested field flattening
  - leaf-name field aliases
  - node attribute merge precedence
  - run-list `recipe` and `role` fields
  - environment cookbook version fields
  - data bag raw item fields
  - policy fields on node documents
- Add a query translation seam that can produce both the current memory matcher behavior and an OpenSearch `query_string` request body.
- Preserve current exact query behavior before adding deeper Lucene-style coverage.

### Task 3: Add The OpenSearch Client And Request Contract Scaffold

Status:

- Completed. The OpenSearch provider request layer now has a validated endpoint constructor, injectable HTTP transport, active/unavailable status helpers, and stable internal error classification for transport, timeout, 4xx, 429, and 5xx failures. Unit tests pin the request shapes for ping, chef-index creation, bulk upsert, `_search` ID queries, refresh, single-document delete, and delete-by-query without requiring a real OpenSearch network dependency. App startup now rejects malformed `OPENCOOK_OPENSEARCH_URL` values before serving, while the current Chef-facing search routes continue to use the memory fallback until the later activation tasks wire indexing events and hydration.

- Add an OpenSearch-backed implementation under `internal/search`.
- Keep HTTP details behind a small transport/client interface so tests can use an `httptest.Server` or fake transport instead of a real network.
- Support the configured `OPENCOOK_OPENSEARCH_URL` endpoint without adding mandatory new environment variables.
- Validate malformed endpoints clearly during app startup or search activation.
- Implement and test request shapes for:
  - ping/status capability checks
  - index/template creation for the `chef` index if needed
  - bulk upsert
  - search IDs through `/chef/_search`
  - refresh behavior
  - single-document delete
  - delete-by-query or per-document fallback where needed
- Classify OpenSearch transport, timeout, 4xx, and 5xx failures into stable internal errors without leaking provider internals into API responses.
- Add capability/status reporting for active OpenSearch, memory fallback, and configured-but-unavailable modes.

### Task 4: Wire Indexing Events From Successful Mutations

Status:

- Completed. Bootstrap-core and core-object persistence can now be wrapped with indexing stores that diff successfully persisted state and emit shared search-document upserts/deletes for clients, environments, nodes, roles, and data bag items without changing route handlers. The wrappers do not emit for invalid writes or failed delegate persistence writes, and memory search remains derived directly from bootstrap state when OpenSearch indexing is not active. Tests pin successful mutation events, failed-write no-event behavior, and a route-level invalid node update that leaves search-visible state unchanged.

- Add a narrow internal indexing seam for bootstrap/core object mutations without changing Chef-facing route handlers.
- Emit index upserts after successful creates and updates for:
  - clients
  - environments
  - nodes
  - roles
  - data bag items
- Emit index deletes after successful deletes for the same objects.
- Ensure invalid writes and failed persistence writes do not emit index changes.
- Ensure route-level no-mutation tests cover search-visible state as well as PostgreSQL-backed object state.
- Keep memory search behavior unchanged when OpenSearch is not active.

### Task 5: Rebuild OpenSearch From Persisted State At Startup

Status:

- Completed. Startup now validates and activates an internal OpenSearch rebuild path only when PostgreSQL-backed state and `OPENCOOK_OPENSEARCH_URL` are both configured. The rebuild pings OpenSearch, ensures the `chef` index, clears stale derived documents with delete-by-query, bulk upserts the current hydrated client/environment/node/role/data-bag search documents, and refreshes the index before the app is returned. Tests pin document collection for generated validator clients, `_default` environments, and dynamic data bag indexes; stale-document cleanup through `match_all` delete-by-query; and idempotent repeated app construction against the same active PostgreSQL state.

- During app startup, when PostgreSQL-backed state and OpenSearch are both configured, rebuild or reconcile the active OpenSearch index from loaded bootstrap/core-object state.
- Include default bootstrap state that should be searchable, such as generated clients and `_default` environments.
- Include dynamic data bag indexes.
- Make startup rehydration idempotent across repeated app construction against the same PostgreSQL and OpenSearch state.
- Prove stale OpenSearch documents are removed or ignored after persisted object deletes.
- Keep public reindex/repair commands out of this bucket; this task is internal startup activation only.

### Task 6: Pin Active OpenSearch Read And Query Parity

Status:

- Completed. Successful OpenSearch startup activation now returns an active `OpenSearchIndex` provider after rebuild, so app search routes use OpenSearch for matching document IDs while hydrating Chef-facing payloads from PostgreSQL-backed bootstrap/core-object state. Tests now pin active PostgreSQL plus OpenSearch-backed default-org and explicit-org search index listings, client/environment/node/role/data-bag full search, data bag partial search, stale-ID ignoring, pagination/order preservation, ACL filtering after OpenSearch returns candidate IDs, and query forwarding for `*:*`, field terms, unqualified terms, prefix wildcards, `OR`, `AND`, `NOT`, escaped slash terms, and bracket-like terms.

- Add route-level coverage where search metadata and object rows are persisted in PostgreSQL and search IDs come from an active OpenSearch-backed provider.
- Cover default-org and explicit-org aliases for:
  - `/search`
  - `/search/client`
  - `/search/environment`
  - `/search/node`
  - `/search/role`
  - `/search/{data_bag}`
  - `/organizations/{org}/search`
  - `/organizations/{org}/search/{index}`
- Cover GET full search and POST partial search.
- Cover ACL-filtered search responses after OpenSearch returns matching IDs.
- Cover pagination and deterministic ordering.
- Cover query strings already supported by the memory path:
  - `*:*`
  - field terms
  - unqualified terms
  - prefix wildcards
  - `OR`
  - `AND`
  - `NOT`
  - escaped slash and bracket-like terms
- Include restart/rehydration cases where the first request after app construction sees OpenSearch-backed search results for PostgreSQL-backed object state.

### Task 7: Pin Active OpenSearch Mutation And Deletion Parity

Status:

- Completed. Active PostgreSQL plus OpenSearch mode now wires successful bootstrap/core-object persistence mutations into the OpenSearch document indexer, and the active app search provider uses the same OpenSearch client after startup rebuild. Route coverage now proves validator and normal client creation, client deletion, environment create/update/rename/delete, node create/update/delete, role create/update/delete, and data-bag item create/update/delete are reflected through active OpenSearch-backed search. Invalid node and data-bag-item updates do not create stale searchable terms, updated objects stop matching old terms, deleted objects disappear from search, and stale OpenSearch IDs are ignored when the PostgreSQL-backed row is gone.

- Add route-level coverage proving successful object mutations become searchable through active OpenSearch mode.
- Cover create, update, rename where currently supported, and delete for:
  - clients
  - environments
  - nodes
  - roles
  - data bag items
- Prove deleted objects no longer appear in search after OpenSearch refresh/reconciliation.
- Prove object updates replace old searchable fields and do not leave stale query matches.
- Prove invalid writes do not create or update search documents.
- Prove stale OpenSearch IDs are ignored if a matching PostgreSQL-backed object is gone.
- Keep existing object route response and error shapes unchanged.

### Task 8: Pin OpenSearch-unavailable Degradation And Operational Truthfulness

Status:

- Completed. Active OpenSearch startup failures now fail app construction clearly, rebuild failures stop before serving active OpenSearch traffic, and provider bodies or endpoint details are not copied into Chef-facing errors. Search listing/query unavailable paths now degrade to stable `503 search_unavailable` responses, while `/status` reports configured-but-unavailable OpenSearch truthfully when the active provider cannot serve. The mutation contract is pinned as PostgreSQL-authoritative: successful object persistence is not rolled back when OpenSearch upsert/delete fails; invalid writes and failed persistence writes still do not emit index events; reconciliation/rebuild remains the recovery path for missed derived-index events.

- Add tests for OpenSearch unavailable during:
  - app activation
  - startup index rebuild
  - search index listing
  - search query
  - index upsert
  - index delete
- Decide and pin the active-mode mutation contract from upstream evidence:
  - whether search-index failures fail the object mutation, or
  - whether object persistence succeeds while search returns unavailable until reconciliation catches up
- In either case, prove invalid writes do not create index documents and failed persistence writes do not create index documents.
- Preserve existing API payload keys while updating human-readable `/status` and root/status wording.
- Ensure provider details such as URLs, transport errors, and raw OpenSearch bodies are not leaked in Chef-facing errors.
- Keep absent `OPENCOOK_OPENSEARCH_URL` behavior on the memory adapter unchanged.

### Task 9: Expand Functional Docker Coverage

Status:

- Completed. The functional Compose flow now proves active OpenSearch-backed search behavior instead of reachability only. The black-box functional suite asserts `/_status` reports the active OpenSearch backend, creates searchable clients, environments, nodes, roles, and data bag items, verifies search after OpenCook restart, updates searchable terms and proves stale terms disappear, and deletes search fixtures before verifying no search rows remain after restart. The phase runner now exposes `search-update` and `verify-search-updated` so the OpenSearch-heavy portion can be run independently when the full stack flow is slow.

- Update the functional Docker smoke flow so the compose stack proves active OpenSearch search, not only OpenSearch reachability.
- Verify `/status` reports an active OpenSearch-backed search mode when `OPENCOOK_OPENSEARCH_URL` and PostgreSQL are configured.
- Create searchable clients, environments, nodes, roles, and data bag items.
- Restart OpenCook and verify search results still come back through active OpenSearch mode.
- Update objects and verify old query terms disappear while new terms match.
- Delete objects and verify search no longer returns them after restart.
- Keep OpenSearch, PostgreSQL, and OpenCook on the shared Compose network.
- Document how to run only the OpenSearch-heavy functional phases if the full flow becomes slow.

### Task 10: Sync Docs And Close The Bucket

Status:

- Completed. Roadmap, milestone, compatibility matrix, functional testing, AGENTS, README, root/status next-step wording, and this plan now describe active OpenSearch-backed search when PostgreSQL plus `OPENCOOK_OPENSEARCH_URL` are configured. The bucket is closed with public reindex/repair and operational admin tooling called out as the next recommended bucket, while encrypted data bag compatibility remains the main possible compatibility-priority detour.

- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `AGENTS.md`
  - `README.md` if current-status wording still says OpenSearch is placeholder-only
  - this plan file
- Mark this bucket complete after tests pass.
- Point the next bucket at operational admin/reindex/repair tooling unless encrypted data bag compatibility becomes more urgent.
- Update status text that still implies configured OpenSearch is unused once active OpenSearch mode lands.

## Bucket Outcome

This bucket is complete for the currently implemented search surfaces:

- Memory search remains the no-OpenSearch fallback.
- Active PostgreSQL plus OpenSearch mode rebuilds the `chef` index at startup and serves search routes through OpenSearch document ID matches hydrated from PostgreSQL-backed service state.
- Successful client, environment, node, role, and data bag item mutations update or delete derived OpenSearch documents.
- Search route payloads, partial-search shaping, ACL filtering, default-org and explicit-org aliases, and missing-index behavior remain Chef-facing compatible.
- Provider failures avoid leaking backend details and surface stable `search_unavailable` responses where the route contract allows degradation.
- Functional Docker coverage proves active OpenSearch status, restart/rehydration, update/stale-term removal, and delete/no-result behavior.

Next bucket: operational admin plus OpenSearch reindex/repair tooling, unless encrypted data bag compatibility becomes the more urgent Chef-contract gap.

## Likely Implementation Touchpoints

- `internal/search/search.go`: expand interfaces and shared errors/status.
- `internal/search/memory.go`: reuse shared expansion/query code.
- `internal/search/opensearch.go`: add active OpenSearch implementation.
- `internal/search/*_test.go`: add request-shape, expansion, query, and failure tests.
- `internal/bootstrap`: add a narrow indexing event sink or mutation hook while keeping validation and persistence ownership unchanged.
- `internal/api/search_routes.go`: preserve route shape while hydrating and ACL-filtering OpenSearch-backed IDs.
- `internal/api/search_routes_test.go`: add active OpenSearch route parity and failure coverage.
- `internal/app/app.go`: activate OpenSearch-backed search only when configuration and dependencies make that truthful.
- `internal/app/app_test.go`: add startup/status/activation coverage.
- `deploy/functional/docker-compose.yml`: keep the existing OpenSearch service and add any needed health/capability env defaults.
- `test/functional`: expand black-box OpenSearch search coverage.

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

Functional verification:

- `scripts/functional-compose.sh`
- targeted phase runs if added for OpenSearch-specific coverage

Required scenarios:

- memory search remains unchanged when `OPENCOOK_OPENSEARCH_URL` is absent
- active PostgreSQL plus active OpenSearch search returns the same Chef-facing payloads as memory search
- startup rebuild/reconciliation makes persisted objects searchable after app construction
- successful object writes update OpenSearch-backed search results
- successful object deletes remove search results
- invalid writes and failed persistence writes do not mutate OpenSearch-visible state
- ACL-filtered results are filtered after OpenSearch returns matching IDs
- partial search remains API-shaped and hydrated from current object state
- data bag dynamic indexes work through OpenSearch-backed search
- OpenSearch unavailable errors preserve stable Chef-facing error shapes and truthful status wording

## Assumptions And Defaults

- PostgreSQL remains the source of truth for objects and ACLs.
- OpenSearch stores derived search documents and IDs only.
- The first active OpenSearch mode should target the currently implemented search surfaces, not every historical Chef index.
- The local Docker OpenSearch service is enough for functional coverage; request-shape unit tests should avoid network dependence.
- The memory adapter remains the no-configuration developer fallback.
- Startup reconciliation is acceptable for this bucket; public reindex and repair commands are follow-on operational work.
- Cross-process cache invalidation is out of scope for this bucket.
- OpenSearch security and credential variants should not be expanded beyond what the current config can truthfully support unless tests require it.
- If upstream evidence conflicts with a proposed OpenSearch failure behavior, prefer Chef compatibility and document the decision before implementation.
