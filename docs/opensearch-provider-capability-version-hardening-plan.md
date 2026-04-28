# OpenSearch Provider Capability And Version Hardening Plan

Status: complete

## Summary

This bucket hardens OpenCook's OpenSearch provider adapter as a versioned external dependency while preserving the Chef-facing search contract already pinned in previous buckets. PostgreSQL remains the authoritative source of truth. OpenSearch remains a derived ID index used by the existing `/search`, partial search, startup rebuild, mutation indexing, and `opencook admin reindex/check/repair` flows.

The goal is not to change search route behavior or add new searchable object families. The goal is to make provider discovery, capability flags, index creation, mapping updates, query execution, delete behavior, refresh behavior, failure classification, status wording, and functional coverage explicit enough that OpenCook can run against supported OpenSearch versions without brittle ad hoc branches.

Use this file as the reference checklist for the OpenSearch provider capability/version hardening bucket.

## Completion State

OpenCook now has:

- Active OpenSearch-backed search when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Memory-backed search fallback when OpenSearch is not configured.
- Startup rebuild of the `chef` index from PostgreSQL-backed bootstrap/core-object state.
- Mutation indexing for clients, environments, nodes, roles, and data bag items.
- Shared document expansion and query planning across memory and OpenSearch paths.
- Search route coverage for full search, partial search, ACL filtering, pagination, stale-ID ignoring, provider-unavailable `503 search_unavailable`, and broader Lucene/query-string behavior.
- `opencook admin reindex`, `opencook admin search check`, and `opencook admin search repair` from PostgreSQL-backed state.
- Functional Docker coverage with PostgreSQL, OpenSearch, and filesystem-backed blobs.
- Provider discovery with a cached identity/capability model for known OpenSearch, known Elasticsearch, and compatible unknown providers.
- Status wording that reports discovered provider distribution, version, search-after support, delete-by-query mode, and total-hit response shape without changing status payload keys.
- Versioned `chef` index mapping metadata plus idempotent create/update behavior.
- Direct delete-by-query and fallback delete behavior for provider/version drift.
- Hardened search, bulk, refresh, delete, mapping, discovery, and startup failure classification with provider-body redaction.
- Functional coverage that observes provider capability wording and an opt-in provider image matrix, plus package-level capability-mode coverage for direct and fallback delete providers.

## Upstream Signals To Preserve

Inventory sources:

- `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_opensearch.erl`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/search_util.rb`
- Search-related pedant specs for clients, environments, nodes, roles, and data bags

Contract signals:

- The provider returns matching document IDs; Chef hydrates current object rows from PostgreSQL before shaping route responses.
- Missing PostgreSQL objects for returned provider IDs are ignored.
- ACL filtering happens after object hydration.
- Partial search shaping happens after object hydration.
- Provider failures must not leak internal provider bodies or cluster details through Chef-facing routes or admin command summaries.
- Provider `hits.total` may be an integer or an object with a `value` field.
- Upstream has provider-version-specific delete behavior notes, especially around delete-by-query availability.
- Search route and payload compatibility matters more than matching upstream's internal provider request body byte-for-byte.

## Interfaces And Behavior

- Do not change Chef-facing routes, methods, trailing-slash behavior, payload keys, response keys, status codes, or signed-auth semantics for `/search` routes.
- Do not expose cookbook, cookbook artifact, policy, policy group, sandbox, or checksum indexes unless future upstream evidence proves they are public Chef search indexes.
- Preserve PostgreSQL as the source of truth for search result hydration, startup rebuilds, consistency checks, and repair.
- Preserve memory search fallback when `OPENCOOK_OPENSEARCH_URL` is absent.
- Preserve current `search_unavailable` route error shape and status code for provider failures.
- Preserve `opencook admin` JSON output shape where already pinned; additive capability/status fields are acceptable only where they do not break existing checks.
- Keep `/status`, `/readyz`, and root payload keys stable. Human-readable wording may become more specific.
- Do not add migration/cutover workflows to this bucket beyond documenting them as the next operational follow-on.

## Task 1 Contract Snapshot

Status: complete.

Upstream inventory:

- `dev-docs/SEARCH_AND_INDEXING.md` documents search as a derived index: provider hits identify candidate IDs, Erchef hydrates PostgreSQL rows, ACL checks run after hydration, and partial-search projection is shaped from hydrated objects.
- `chef_opensearch.erl` pings the provider, writes through `/_bulk`, searches `/chef/_search`, refreshes `/chef/_refresh`, and carries explicit comments that delete-by-query behavior varies across provider versions.
- Upstream search request bodies are not a byte-for-byte compatibility target. Chef-facing routes and payloads are the compatibility target.

Current OpenCook provider request shapes now frozen by `TestOpenSearchClientProviderContractRequestSequence`:

| Operation | Request Shape | Notes |
| --- | --- | --- |
| Ping | `GET /{endpoint-prefix}` | Accepts `200`; endpoint prefixes are preserved. |
| Index exists | `HEAD /{endpoint-prefix}/chef` | Existing indexes proceed to mapping update. |
| Index create | `PUT /{endpoint-prefix}/chef` | Body includes current settings plus dynamic mapping for `document_id` and `compat_terms`. |
| Mapping update | `PUT /{endpoint-prefix}/chef/_mapping` | Body is the current dynamic mapping descriptor. |
| Bulk upsert | `POST /{endpoint-prefix}/_bulk` | NDJSON `index` actions target `_index: chef`; `_id` is `{org}/{index}/{name}`. |
| Search IDs | `POST /{endpoint-prefix}/chef/_search` | Body sets `_source:false`, filters `compat_terms` for `__org` and `__index`, compiles the shared query AST, sorts by `document_id`, and paginates with `search_after`. |
| Refresh | `POST /{endpoint-prefix}/chef/_refresh` | Used after indexing/deletion flows that need provider visibility. |
| Delete document | `DELETE /{endpoint-prefix}/chef/_doc/{path-escaped-id}` | Document IDs containing `/` remain path-escaped. |
| Delete by query | `POST /{endpoint-prefix}/chef/_delete_by_query?refresh=true` | Body filters exact `organization` and `index`, or uses `match_all` only when no scope is provided. |

Chef-facing route boundaries now frozen by active OpenSearch route tests:

- `/search` and `/organizations/{org}/search` list only supported public indexes: `client`, `environment`, `node`, `role`, and data bag names.
- Cookbook, cookbook artifact, policy, policy group, sandbox, and checksum state remain unsupported as public search indexes even when those objects exist in PostgreSQL.
- Cookbook, policy, and role/cookbook concepts remain searchable only as fields on supported object families, such as node policy references, role run lists, environment cookbook constraints, and data bag attributes.
- Provider `_source` documents are ignored. Search routes hydrate PostgreSQL state by returned provider IDs before shaping full or partial rows, so provider-only fields and stale provider payloads cannot leak to Chef clients.
- Missing PostgreSQL rows for returned provider IDs are ignored, and ACL filtering still runs after hydration.

Provider-hardening boundaries for the rest of this bucket:

- No public route changes.
- No new public search indexes.
- No migration or cutover workflow in this slice.
- No provider source documents returned directly to clients.
- No licensing or license-telemetry endpoints.

## Task 2 Discovery Snapshot

Status: complete.

Discovery model:

- `search.OpenSearchProviderInfo` records distribution, raw version, parsed major/minor/patch when available, tagline, node/cluster names, build flavor/type/hash, and capability flags.
- `search.OpenSearchCapabilities` records the operations OpenCook depends on: index existence checks, index creation, mapping updates, bulk indexing, ID search, search-after pagination, refresh, document delete, delete-by-query, delete-by-query fallback requirement, and total-hits object response support.
- `OpenSearchClient.DiscoverProvider(ctx)` runs a non-mutating discovery sequence and caches the successful snapshot on the client for later status/admin work.
- `OpenSearchClient.ProviderInfo()` returns the cached snapshot when discovery has completed successfully.

Discovery behavior:

- `GET /{endpoint-prefix}` parses provider identity from the root payload.
- `HEAD /{endpoint-prefix}/chef` accepts `200` or `404` as a working index-existence capability; `404` is valid because the activation flow may create the index later.
- OpenSearch and Elasticsearch root payloads are recognized; unknown future providers are accepted when the required root, index-existence, and inferred capability checks behave correctly.
- Elasticsearch versions before 7 are marked as not returning total-hits object responses by default.
- Elasticsearch versions before 5 are rejected as unsupported because OpenCook's search and safe fallback-delete paths require `search_after` pagination.
- Failed discovery does not cache partial provider details.
- Discovery errors are classified through the existing OpenSearch error model and do not leak raw provider bodies or cluster details.

## Task 3 Status Snapshot

Status: complete.

Status behavior:

- `/_status`, `/_health`, and `/_ready` keep the existing payload shape and dependency keys.
- The `dependencies.opensearch` object still contains only `backend`, `configured`, and `message`.
- Active OpenSearch status now reports discovered provider identity through `message`, for example `opensearch 2.12.0`.
- Unknown but compatible providers use `OpenSearch-compatible search provider active` wording while preserving the `opensearch` backend value.
- Degraded capability mode is visible through `message`, including delete-by-query fallback and legacy total-hit behavior.
- Configured-but-unavailable OpenSearch still reports the existing `OpenSearch is configured but unavailable` message.
- Memory fallback wording now explicitly says memory search fallback is active, and distinguishes configured-but-not-activated OpenSearch from an unconfigured provider.
- `opencook admin status` continues to proxy the server status JSON and therefore surfaces the provider wording without a new admin output shape.

## Task 4 Mapping Snapshot

Status: complete.

Mapping behavior:

- The public index name remains `chef`.
- The `chef` index mapping is defined through one internal descriptor.
- Mapping `_meta` records `opencook_mapping_version: 1`.
- Indexed fields remain compatible with prior behavior: `document_id` is `keyword`, `compat_terms` is `keyword`, and dynamic fields remain enabled for expanded search terms.
- Missing index creation still sends `PUT /chef` with settings plus the versioned mapping descriptor.
- Existing indexes now use `GET /chef/_mapping` before `PUT /chef/_mapping`.
- Existing compatible mappings skip `PUT /chef/_mapping`, making activation idempotent.
- Older compatible mappings are updated so mapping metadata can be recorded.
- Mapping conflicts classify through the existing redacted OpenSearch error model and do not expose provider bodies.
- Create-index races where another process creates `chef` after `HEAD /chef` are treated as success only after the resulting mapping is read and proven compatible.

## Task 5 Bulk And Refresh Snapshot

Status: complete.

Bulk and refresh behavior:

- Empty bulk upserts remain a no-op and do not contact the provider.
- Bulk upserts still use deterministic NDJSON `index` actions against the public `chef` index.
- Bulk payloads keep the final newline required by strict NDJSON bulk parsers.
- Successful bulk responses with `errors:false` remain accepted without requiring provider-specific item details.
- Bulk responses with `errors:true` now decode item-level action results.
- Item-level `429` or `5xx` failures classify as `search backend unavailable` so route/admin callers can treat them as retryable provider states.
- Item-level non-retryable `4xx` failures classify as `search backend rejected request`.
- Malformed bulk responses classify as provider unavailable rather than leaking provider response bodies.
- Refresh accepts `200 OK` and `202 Accepted`.
- Refresh `429`/`5xx` responses classify as unavailable, while other `4xx` responses classify as rejected, with provider bodies redacted.
- Active PostgreSQL plus OpenSearch route coverage continues to prove successful client, environment, node, role, and data bag writes index provider documents, while invalid Chef object writes do not enqueue provider mutations.

## Task 6 Search Query Snapshot

Status: complete.

Search query behavior:

- Search request construction still uses the shared AST compiler and `compat_terms` org/index filters.
- Search-after pagination remains the default strategy and no low 10,000 result cap has been introduced.
- Provider `hits.total` may be an integer or an object; OpenCook still treats provider totals as advisory and hydrates current PostgreSQL rows by returned IDs.
- Missing or empty `hits.hits` arrays return no provider IDs.
- Hits with missing `_id` are ignored rather than returned to Chef-facing search responses.
- Unexpected provider `_source` content is ignored.
- Pagination decisions now use the raw provider hit count rather than only accepted IDs, so a full provider page with missing IDs cannot accidentally hide a missing `sort`.
- A missing `sort` on a non-final provider page classifies as provider unavailable because OpenCook cannot safely continue the search-after sequence.
- Malformed provider search responses classify as unavailable without leaking raw provider bodies.
- Existing route/index coverage continues to prove returned provider IDs are hydrated from PostgreSQL-backed state and stale provider IDs are ignored.

## Task 7 Delete Fallback Snapshot

Status: complete.

Delete behavior:

- Direct `POST /chef/_delete_by_query?refresh=true` remains the active path when provider discovery reports delete-by-query support.
- `OpenSearchClient.DeleteByQuery` now discovers provider capabilities before delete-by-query if no cached discovery snapshot exists.
- Providers discovered as requiring delete-by-query fallback now search the target scope and delete matching document IDs one by one only when `search_after` pagination is available.
- Direct unsupported responses (`404`, `405`, or `501`) fall back to search plus per-document delete instead of failing the operation.
- Retryable direct delete-by-query failures (`429` or `5xx`) still classify as provider unavailable and do not fall back.
- Fallback scope searches preserve the direct delete-by-query filter semantics:
  - all organizations use `match_all`
  - organization scope filters exact `organization`
  - organization plus index scope filters exact `organization` and `index`
- Fallback deletion uses deterministic `document_id` sorting and search-after pagination so large scopes are deleted page by page.
- Named-document deletion continues to use exact per-document `DELETE /chef/_doc/{id}` calls through the existing `DeleteDocument`/`DeleteDocuments` path.
- Partial fallback delete failures stop the fallback and surface the existing redacted provider classification without leaking provider bodies.
- Fallback delete refreshes the provider after successful per-document deletes to preserve the visibility behavior of direct delete-by-query with `refresh=true`.

## Task 8 Failure Classification Snapshot

Status: complete.

Failure behavior:

- OpenSearch status classification now runs through a centralized response/status helper used by ping, discovery, mapping, bulk, search, refresh, delete, and fallback paths.
- Required JSON response decoding now runs through one helper that classifies empty and malformed response bodies as provider unavailable without preserving provider body text.
- Transport failure classification now distinguishes:
  - context cancellation
  - context deadline expiration
  - timeout errors
  - DNS lookup failures
  - connect/dial failures
  - generic transport failures
- HTTP status classification now distinguishes:
  - `400`, `404`, `405`, and `501` malformed or unsupported request/API rejections
  - `401` and `403` provider authentication/configuration rejections
  - `409` mapping/index conflicts
  - `429` throttling as provider unavailable
  - `5xx` provider outages as provider unavailable
- Route-level unavailable provider states still map to `503 search_unavailable`.
- Provider rejections still preserve the existing non-`search_unavailable` route/admin behavior so Chef-facing contracts do not shift silently.
- Error messages remain redacted against raw provider bodies, cluster names, endpoint internals, and secret-looking strings.
- Existing admin reindex/search JSON error structures and stderr summaries remain stable because they continue to key off the public search error sentinels.

## Task 9 Startup Source Of Truth Snapshot

Status: complete.

Startup behavior:

- Active PostgreSQL plus configured OpenSearch still activates as a mixed durable stack rather than using memory search.
- Startup discovery runs before rebuild work so provider identity and capability flags are cached before delete-by-query selection.
- Startup rebuild order is now pinned as discovery, ping, index exists check, mapping check/update, stale-document delete, bulk upsert, and refresh.
- Compatible unknown provider versions follow the same activation and rebuild path as recognized OpenSearch distributions.
- Configured-but-broken OpenSearch fails app construction instead of silently falling back to memory search.
- Provider-unavailable and required-capability rejection errors preserve stable redacted classifications.
- Startup rebuild continues to bulk upsert supported PostgreSQL-backed client, environment, node, role, and data bag documents only.
- Unsupported object families remain excluded during startup rebuild and mutation indexing even when persisted PostgreSQL rows exist.
- Existing route coverage continues to prove stale provider IDs and spoofed provider `_source` documents are ignored after PostgreSQL hydration.

## Task 10 Functional And Matrix Snapshot

Status: complete.

Functional behavior:

- The Docker functional Go phase now requires status text to expose provider capability details, including search-after pagination, delete-by-query mode, and total-hit shape.
- The operational functional shell phase now verifies the same capability wording through `opencook admin status` before and after restart.
- `scripts/functional-compose.sh` supports an opt-in `OPENCOOK_FUNCTIONAL_OPENSEARCH_MATRIX` list that runs the full flow once per provider image in isolated Compose projects.
- The default functional path remains unchanged when no matrix is configured.
- Package-level operational coverage now runs admin reindex/check/repair through a real `OpenSearchClient` and fake capability-mode provider for:
  - direct delete-by-query
  - safe search-after-backed delete fallback after direct delete-by-query is unsupported
- Encrypted data bag functional and operational reindex/check/repair coverage remains intact and fixture-gated where appropriate.

## Task Breakdown

### Task 1: Freeze The Provider Contract And Compatibility Boundaries

Status: complete.

- Inventory upstream OpenSearch/Elasticsearch provider assumptions from the local Chef Server checkout.
- Record the exact OpenCook request shapes currently emitted for ping, index ensure, mapping update, bulk upsert, search IDs, refresh, delete document, and delete-by-query.
- Add focused tests that freeze the current Chef-facing route behavior before provider internals change.
- Record the unsupported public search index contract for cookbook, cookbook artifact, policy, policy group, sandbox, and checksum state.
- Capture the provider-hardening boundaries in this plan:
  - no public route changes
  - no new public search indexes
  - no migration/cutover workflow in this slice
  - no provider source documents returned directly to clients

### Task 2: Add Provider Discovery And Capability Flags

Status: complete.

- Add an internal provider discovery model, for example:
  - distribution
  - version string
  - parsed major/minor/patch when available
  - server tagline/build metadata when useful for diagnostics
  - capabilities
- Discover capabilities through a small, tested sequence that can run during OpenSearch activation and admin command setup.
- Include capability flags for:
  - index exists checks
  - create index
  - put mapping
  - bulk indexing
  - search IDs
  - search-after pagination
  - refresh
  - delete document
  - delete-by-query
  - delete-by-query fallback required
  - total-hits object responses
- Make discovery tolerant of unknown future versions when required APIs behave correctly.
- Keep discovery failures redacted and classified as provider unavailable or invalid configuration, depending on the failure.

### Task 3: Report Provider Truthfully Without Changing Status Shapes

Status: complete.

- Thread discovered provider details into internal status reporting.
- Keep existing `/status` payload keys stable.
- Update human-readable status messages to distinguish:
  - memory search fallback
  - OpenSearch configured but unavailable
  - active OpenSearch with known distribution/version
  - active OpenSearch with unknown but compatible distribution/version
  - active OpenSearch with degraded capabilities and safe fallbacks
- Add app/status tests for configured PostgreSQL plus OpenSearch discovery outcomes.
- Add admin status coverage where provider details are visible through existing operational surfaces.

### Task 4: Version The Chef Index Mapping Internally

Status: complete.

- Define a single internal `chef` index mapping descriptor with an OpenCook mapping version stored in mapping metadata if supported.
- Keep the public index name `chef`.
- Preserve current indexed fields and compatibility terms.
- Make index creation idempotent when the index already exists.
- Make mapping updates idempotent and safe when the current mapping is already compatible.
- Classify incompatible mapping conflicts clearly without leaking provider bodies.
- Add request-shape and response-classification tests for:
  - missing index creation
  - existing compatible index
  - mapping update success
  - mapping update conflict
  - malformed mapping response
  - race where another process creates the index between check and create

### Task 5: Harden Bulk Upsert And Refresh Semantics

Status: complete.

- Keep NDJSON bulk request shape deterministic and compatible.
- Ensure bulk payloads include a trailing newline if OpenSearch requires it.
- Decode item-level bulk failures and classify them without leaking raw provider responses.
- Treat retryable provider states as unavailable and non-retryable bad requests as rejected.
- Preserve no-mutation/no-indexing behavior for invalid Chef object writes.
- Preserve successful write indexing behavior for clients, environments, nodes, roles, and data bag items.
- Add tests for:
  - empty bulk as no-op
  - mixed bulk item success/failure
  - malformed bulk response
  - refresh accepted/success statuses
  - refresh unavailable/rejected statuses

### Task 6: Harden Search ID Query Execution Across Provider Variants

Status: complete.

- Preserve the shared AST query compiler and compatibility-term filtering.
- Keep search-after pagination as the default large-result strategy.
- Pin behavior for provider hits that include:
  - `hits.total` as an integer
  - `hits.total` as an object
  - missing or empty hits arrays
  - missing `_id`
  - missing `sort` on a non-final page
  - unexpected `_source` content
- Preserve high-result pagination without a low 10,000 cap.
- Add tests that prove returned provider IDs are still hydrated from current PostgreSQL-backed state and stale IDs are ignored.
- Keep provider parse errors classified as unavailable/rejected in a way that preserves current route/admin error shapes.

### Task 7: Add Delete-By-Query Capability Fallbacks

Status: complete.

- Use provider discovery to decide whether direct delete-by-query is safe.
- Preserve direct delete-by-query when the provider supports it.
- Add a fallback that searches the target scope and deletes matching document IDs one by one when delete-by-query is unavailable.
- Preserve scoped deletion semantics for:
  - all organizations
  - one organization
  - one organization plus one index
  - one organization plus one index plus named documents
- Make fallback deletion deterministic and paginated for large scopes.
- Add tests for:
  - direct delete-by-query success
  - delete-by-query unsupported response
  - fallback search plus per-document delete
  - partial fallback delete failure
  - refresh after direct and fallback deletion
  - admin reindex/check/repair behavior with fallback mode

### Task 8: Tighten Failure Classification And Redaction

Status: complete.

- Centralize provider status/body classification for all OpenSearch operations.
- Ensure route-level provider failures still degrade to `503 search_unavailable` where currently pinned.
- Ensure admin commands still emit stable JSON error structures and useful stderr summaries.
- Classify at least:
  - transport failures
  - context cancellation/deadline
  - DNS/connect failures
  - 400/404 malformed request or unsupported API
  - 401/403 auth/configuration failure
  - 409 mapping/index conflict
  - 429 throttling
  - 5xx provider outage
  - malformed JSON response
  - empty response bodies where a JSON body is required
- Add redaction tests that include raw provider bodies, cluster names, and secret-looking strings.

### Task 9: Preserve Startup Activation And PostgreSQL Source Of Truth

Status: complete.

- Ensure app startup discovery, index ensure, mapping ensure, and rebuild happen in a clear order.
- Preserve startup failure behavior when configured OpenSearch is unreachable or incompatible.
- Keep memory fallback available only when OpenSearch is not configured, not when configured OpenSearch is broken.
- Preserve startup rebuild from PostgreSQL-backed state for supported indexes.
- Preserve unsupported object-family exclusion during startup rebuild and mutation indexing.
- Add restart/rehydration tests for:
  - active PostgreSQL plus active OpenSearch
  - provider unavailable at startup
  - compatible unknown provider version
  - incompatible required capability
  - stale provider IDs after hydration

### Task 10: Extend Functional And Matrix Coverage

Status: complete.

- Extend the Docker functional harness so provider capability/status details are observable in the active stack.
- Keep the default functional OpenSearch image path working.
- Add an optional functional/provider matrix entry for another supported OpenSearch image, guarded behind an opt-in environment variable if runtime cost is high.
- Add a fake/capability-mode functional or package-level harness for provider variants that are hard to reproduce with public images.
- Cover operational admin reindex/check/repair in both direct delete-by-query and fallback-delete modes.
- Keep encrypted data bag search/reindex/repair coverage intact when the fixture exists.

### Task 11: Sync Docs And Close The Bucket

Status: complete.

- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `AGENTS.md`
  - this plan file
- Mark OpenSearch provider capability/version hardening complete.
- Point the next recommended bucket at migration/cutover tooling unless deployment testing identifies a higher-risk compatibility gap.
- Keep the completed API-version, search route, unsupported-index, encrypted-data-bag, and PostgreSQL-source-of-truth contracts visible as preserved boundaries.

## Bucket Closeout

OpenSearch provider capability/version hardening is complete for the current compatibility-foundation scope. OpenCook now keeps Chef-facing search routes stable while making the provider adapter explicit about discovery, capability flags, index/mapping versioning, delete-by-query fallback, search response variants, failure classification, startup activation, PostgreSQL-source-of-truth hydration, and functional/provider-matrix coverage.

The next recommended bucket is migration/cutover tooling: backup/restore, import/export, shadow/cutover rehearsal, operational runbooks, and validation helpers for moving real Chef Infra Server installations onto OpenCook. If deployment testing uncovers a higher-risk compatibility gap first, that gap should take priority, but this bucket intentionally leaves the API-version, search-route, unsupported-index, encrypted-data-bag, and PostgreSQL-source-of-truth contracts closed unless upstream pedant evidence requires revisiting them.

## Test Plan

Focused verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```sh
scripts/functional-compose.sh
```

Recommended targeted functional phases:

```sh
KEEP_STACK=1 scripts/functional-compose.sh create restart verify query-compat operational restart operational-verify
```

Required scenarios:

- active PostgreSQL plus OpenSearch startup discovery and rebuild
- provider distribution/version/capability reporting
- compatible unknown provider version
- incompatible required provider capability
- idempotent index creation and mapping update
- mapping conflict redaction
- bulk item failure redaction
- search response variants for integer/object total hits and missing sort values
- large result pagination beyond 10,000 provider hits
- direct delete-by-query and fallback search-plus-delete
- admin reindex/check/repair under direct and fallback delete behavior
- route-level `503 search_unavailable` degradation without provider body leakage
- unsupported cookbook/policy/sandbox/checksum search scopes remain unsupported
- encrypted-looking data bag search/reindex/repair behavior remains intact

## Assumptions

- The default functional stack can continue to use the current configured OpenSearch image unless this bucket explicitly adds an opt-in matrix.
- PostgreSQL remains authoritative; OpenSearch never becomes the source of truth for Chef objects.
- Unknown future OpenSearch versions should be accepted if the required capability probes pass.
- Elasticsearch compatibility can be inventoried, but this bucket should not promise Elasticsearch support without tested request/response coverage.
- This slice should not introduce public admin HTTP endpoints or migration/cutover workflows.
- This slice should not add licensing endpoints or license-related compatibility behavior.

## Completion Criteria

- Provider discovery and capability flags are implemented and covered.
- Status/admin wording reports active provider details truthfully without payload-key churn.
- Index creation and mapping updates are idempotent, version-aware, and failure-classified.
- Bulk, search, refresh, and delete flows have provider-variant and redaction coverage.
- Delete-by-query fallback exists and is exercised.
- Startup rebuild, mutation indexing, admin reindex/check/repair, and functional Docker flows still preserve the existing Chef-facing search contract.
- Docs point the next bucket at migration/cutover tooling.
- Completed API-version, encrypted-data-bag, unsupported-index, and PostgreSQL-source-of-truth contracts remain preserved boundaries for follow-on work.
