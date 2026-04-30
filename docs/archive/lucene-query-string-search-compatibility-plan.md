# Lucene Query-string Search Compatibility Plan

Status: completed

## Summary

This bucket widens OpenCook's Chef search query compatibility beyond the currently pinned query subset while preserving the Chef-facing search API shape.

The goal is not to expose a new search language or make OpenSearch the public contract. Chef and Cinc clients already send Lucene-style query strings to `/search/{index}` and `/organizations/{org}/search/{index}`. OpenCook should accept the upstream-compatible query forms those clients rely on, evaluate them consistently in both memory-backed and active OpenSearch-backed modes, hydrate results from current PostgreSQL-backed state when configured, and keep ACL filtering plus partial-search response shaping in the API layer.

Use this file as the reference plan for the broader search query compatibility bucket.

## Current State

OpenCook already has:

- `/search` and `/organizations/{org}/search` index listing routes.
- `/search/{client,environment,node,role}` and org-scoped aliases.
- Dynamic per-data-bag search indexes.
- GET full search responses with `start`, `total`, and `rows`.
- POST partial search responses with `url` and `data`.
- ACL filtering after search-provider matches.
- In-memory fallback search when OpenSearch is not configured.
- Active OpenSearch-backed search when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Startup rebuild, mutation upsert/delete hooks, stale-ID ignoring, operational reindex/check/repair, and Docker functional coverage for the implemented indexes.
- Search coverage for `*:*`, exact field terms, unqualified terms, prefix and word-break wildcards, lexicographic field ranges, `OR`, `AND`, `NOT`, unary `-`, parenthesized grouping, quoted phrases, escaped Lucene-reserved punctuation, pagination, data bag rows, partial search, and encrypted-looking data bag payloads.
- A shared query parser/AST used by memory-backed search and active OpenSearch request planning.
- Stable invalid-query and provider-error route shapes that avoid leaking provider internals.

Remaining search follow-on work after this bucket:

- Complete word-break and special-character behavior from pedant.
- Richer OpenSearch provider capability/version negotiation.
- Search index coverage beyond clients, environments, nodes, roles, and data bags, especially cookbook, policy, policy group, and sandbox search surfaces.

## Upstream Compatibility Signals

Primary local upstream references:

- `~/Projects/coding/ruby/chef-server/dev-docs/SEARCH_AND_INDEXING.md`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_index_expand.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_index_query.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_lucene.peg`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_opensearch.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/chef_index/src/chef_solr.erl`
- `~/Projects/coding/ruby/chef-server/src/oc_erchef/apps/oc_chef_wm/src/chef_wm_search_index.erl`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/search/search_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/search/word_break_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/search_util.rb`

Important signals for this bucket:

- Chef expands objects into search-provider documents, but provider matches are only candidate IDs.
- Chef-facing responses are hydrated from current object state, not returned directly from OpenSearch.
- READ ACL filtering happens after provider matching.
- Partial search is API-shaped after object hydration.
- OpenSearch-backed Chef Server sends query-string queries to the provider, combined with organization/type filtering and stable ID ordering.
- Pedant has dedicated coverage for basic search, partial search, ACL-filtered search, limited rows, run-list recipe matching, policy fields on nodes, and word-break behavior around special characters.

## Interfaces And Behavior

- Do not add, remove, or rename Chef-facing search routes.
- Preserve default-org and explicit-org route alias behavior.
- Preserve current full-search response keys: `start`, `total`, and `rows`.
- Preserve current partial-search response row shape: `url` and `data`.
- Preserve current missing-index and missing-organization route behavior unless upstream inventory proves a mismatch.
- Preserve ACL filtering after search provider matching.
- Preserve PostgreSQL-backed state as the source of truth for hydrated results when PostgreSQL is configured.
- Preserve memory-backed search as the no-OpenSearch fallback.
- Preserve active OpenSearch as a derived index, not as the authoritative object store.
- Preserve encrypted data bag opacity; do not add data bag secret or decryption behavior.
- Do not broaden this bucket into new cookbook, policy, policy group, or sandbox search indexes.
- Do not introduce arbitrary low result caps. If provider constraints require limits, prefer Chef-compatible pagination behavior and document any operational ceiling explicitly.
- Keep `/status`, `/readyz`, and root payload keys stable. Human-readable status or next-step wording may change only during bucket closeout.

## Compatibility Contract To Freeze

This slice should freeze the query-language contract before changing implementation behavior:

- Query defaults:
  - empty query
  - omitted `q`
  - `*:*`
- Fielded queries:
  - exact field/value terms
  - nested-path fields
  - leaf-name aliases
  - field-name wildcards where supported by upstream behavior
  - unknown fields and missing values
- Unqualified terms:
  - matching across expanded searchable fields
  - no accidental matches against internal-only organization/index fields
- Boolean logic:
  - `AND`
  - `OR`
  - `NOT`
  - unary `-`
  - grouped expressions
  - precedence and associativity
- String handling:
  - escaped Lucene-reserved characters
  - quoted phrases
  - literal colons and slashes
  - bracket-like, at-sign, underscore, dash, and other word-break cases from pedant
- Wildcards:
  - trailing wildcard
  - leading wildcard
  - infix wildcard
  - wildcard field name
  - wildcard-only existence checks
- Ranges:
  - numeric-looking values
  - string values
  - inclusive and exclusive forms, if upstream evidence proves they are accepted
- Search route behavior:
  - GET full search
  - POST partial search
  - `start` and `rows`
  - deterministic ordering
  - large result sets
  - invalid query strings
  - provider-unavailable degradation
- Search surfaces:
  - clients
  - environments
  - nodes
  - roles
  - ordinary data bag items
  - encrypted-looking data bag items as opaque stored JSON

## Task Breakdown

### Task 1: Inventory Upstream Query Semantics And Freeze OpenCook Baseline

Status:

- Completed. The upstream inventory confirms the relevant contract sources are Chef Server's search expansion/query modules, OpenSearch/Solr adapters, the search webmachine route, pedant search specs, and pedant word-break coverage.
- Added a provider-independent baseline test proving the current memory matcher and the OpenSearch compatibility clause agree for the already-supported query subset.
- Recorded unsupported or not-yet-pinned upstream behaviors here as explicit follow-on work instead of guessing at generic Lucene behavior.

Current OpenCook baseline frozen by this task:

- Empty query, omitted `q`, whitespace-only query, and `*:*` behave as match-all.
- Exact field terms match the expanded document field map.
- Unqualified terms match any expanded field value through the synthetic OpenSearch `__any` compatibility tokens.
- Value `*` on a known field behaves as a field-existence check.
- Trailing value wildcards behave as prefix checks.
- `OR`, `AND`, leading `NOT`, and unary `-` are supported only through the current simple split-based grammar.
- Escaped `:`, `[`, `]`, `@`, and `/` are unescaped before matching.
- Unknown fields and missing field values do not match.
- Empty `OR` clauses do not accidentally become match-all.
- Memory-backed matching and active OpenSearch query planning both operate through the same `CompileQuery` seam.

Explicitly pending for later tasks:

- Full Chef/Lucene parser behavior from `chef_lucene.peg` and `chef_index_query.erl`.
- Parenthesized grouping and verified operator precedence.
- Quoted phrase behavior.
- Leading and infix wildcards.
- Wildcard field-name matching such as the pedant `*:value` cases.
- Complete word-break behavior from `oc-chef-pedant/spec/api/search/word_break_spec.rb`.
- Stable invalid-query route response shapes across memory and OpenSearch-backed modes.
- Large-result paging/order/total behavior beyond the current route tests.

- Read the upstream query parser, provider adapters, search webmachine route, pedant search specs, and word-break specs.
- Record the current OpenCook search behavior in this plan before widening it.
- Build a query fixture matrix grouped by behavior: defaults, field terms, unqualified terms, booleans, grouping, escaping, quoting, wildcards, ranges, paging, invalid queries, and provider failures.
- Add baseline tests proving current memory-backed and OpenSearch-backed behavior still agrees for the already supported subset.
- Explicitly mark unsupported upstream features as pending with source references rather than guessing.

### Task 2: Introduce A Shared Query Parser And AST

Status:

- Completed. `CompileQuery` now builds a shared internal AST used by both memory search matching and OpenSearch compat_terms query planning.
- Added `QueryPlan.Err()` plus stable `search.ErrInvalidQuery` handling so invalid parser syntax can map to Chef-facing search errors without exposing parser or provider internals.
- Preserved the Task 1 query subset exactly: match-all, field terms, unqualified terms, existence checks, trailing wildcards, `OR`, `AND`, leading `NOT`, unary `-`, and the existing escape handling.
- Added parser-focused coverage for tokenization, current AST shapes, grouping rejection, quoting rejection, invalid syntax, and memory/OpenSearch clause parity.
- Kept grouping and quoting deliberately rejected for now; Tasks 3 and 4 will enable those semantics only after their Chef-compatible behavior is pinned.

- Replace ad hoc string splitting in `internal/search/query.go` with a small internal parser that produces a query AST.
- Keep `CompileQuery` as the public internal seam used by memory search and OpenSearch query planning.
- Support parse errors as stable internal errors that the API can map to Chef-shaped route responses.
- Add parser-only tests for tokenization, escaping, grouping, quoting, fielded terms, wildcards, and invalid syntax.
- Preserve existing behavior for the currently supported subset before enabling new semantics.

### Task 3: Pin Boolean Logic, Grouping, And Precedence

Status:

- Completed. The shared query parser now supports `AND`, `OR`, `NOT`, unary `-`, and parenthesized groups with pinned precedence: groups first, unary negation, `AND`, then `OR`.
- Added parser/query coverage for grouped disjunctions combined with required terms, grouped negation, dash-negated groups, and memory/OpenSearch compatibility-clause parity.
- Added memory-backed and active OpenSearch-backed route coverage for grouped boolean candidate hydration, plus an ACL-filtered grouped query assertion proving filtering still happens after query expansion.

- Implement Chef-compatible `AND`, `OR`, `NOT`, unary `-`, and parenthesized grouping.
- Pin precedence and associativity against upstream evidence.
- Cover mixed expressions such as grouped disjunctions combined with required and negated terms.
- Prove memory-backed and OpenSearch-backed routes return the same candidate objects for the same query fixtures.
- Preserve ACL filtering after the expanded query result is produced.

### Task 4: Harden Escaping, Quoting, And Word-break Behavior

Status:

- Completed. Quoted phrase terms are now accepted and evaluated as exact keyword phrases unless combined with explicit wildcard syntax.
- Escaped Lucene-reserved punctuation is normalized as literal text before matching, including path-like values, recipe names with colons, bracketed data bag fields, punctuation-heavy node attributes, and encrypted-looking envelope values.
- Added memory/OpenSearch clause parity coverage for exact matches, no-match partial word searches, wildcard-around-punctuation searches, wildcarded quoted phrases, invalid unterminated quotes/trailing escapes, and active OpenSearch route hydration for escaped, quoted, and punctuation-sensitive queries.

- Add focused fixtures for node attributes and data bag fields containing Lucene-reserved and word-break-sensitive characters.
- Pin exact-match, no-match, quoted-string, escaped-character, and wildcard-around-character behavior from pedant.
- Ensure literal path-like values, recipe names, policy names, encrypted envelope values, and data bag IDs do not require provider-specific escaping surprises.
- Keep invalid or unsupported escape sequences from leaking provider internals.
- Add parity tests for memory and active OpenSearch-backed modes.

### Task 5: Add Chef-compatible Wildcard And Existence Semantics

Status:

- Completed. Field-value wildcards now cover trailing, leading, infix, and single-character `?` patterns while preserving efficient prefix clauses for simple trailing wildcards.
- Wildcard field names now match expanded Chef search keys, including nested-path aliases, leaf aliases, ordinary data bag fields, and encrypted-looking envelope field names.
- Added parser parity, memory route, active OpenSearch route, broad wildcard ACL-filtering, and post-filter pagination coverage for `*:*`, `field:*`, `*:value`, wildcard field names, run-list projections, policy fields, data bag fields, and encrypted-looking envelope fields.

- Implement trailing, leading, and infix wildcard matching where upstream supports it.
- Implement wildcard field-name matching where upstream supports it.
- Preserve `*:*` as match-all and field `*` value behavior as existence-style matching.
- Cover wildcard behavior on nested-path fields, leaf aliases, run-list fields, policy fields, ordinary data bag fields, and encrypted-looking data bag envelope fields.
- Prove broad wildcard searches still obey ACL filtering and pagination after filtering.

### Task 6: Decide And Pin Range Query Behavior

Status:

- Completed. Upstream `chef_lucene.peg` accepts `field:[start TO end]` and `field:{start TO end}` range forms, so OpenCook now supports field ranges instead of rejecting them as provider-specific syntax.
- Implemented inclusive/exclusive and open `*` bounds as lexicographic keyword ranges over expanded search values, matching the `compat_terms` OpenSearch request shape used by active provider-backed search.
- Added parser parity, OpenSearch request-shape, memory route, active OpenSearch route, boolean-composed range, malformed-range, string/date-like, and numeric-looking range coverage.

- Inventory whether Chef Server accepts range queries on expanded search documents for supported object types.
- If range queries are part of the Chef contract, implement inclusive/exclusive range parsing and matching for the relevant string or numeric-looking values.
- If range queries are rejected or effectively provider-dependent upstream, preserve that behavior and document it.
- Cover malformed ranges and ensure invalid-query responses stay stable.
- Keep the behavior identical between memory and active OpenSearch-backed search.

### Task 7: Align OpenSearch Query Planning With The Shared AST

Status:

- Completed. Active OpenSearch search requests are compiled from the shared `CompileQuery` AST into `compat_terms` clauses; the legacy raw query-string view has been removed from the query plan.
- Added request-shape coverage for boolean, grouped, quoted phrase, existence, wildcard, wildcard-field, and range queries while preserving org/index filters, `_source: false`, sort, and `search_after` pagination.
- Added coverage proving invalid parser syntax fails before any provider request, provider-rejected search bodies are redacted, provider-unavailable behavior stays `search_unavailable`, and Chef-facing routes still hydrate IDs from current state before responding.

- Compile the shared AST into OpenSearch request bodies instead of forwarding raw query strings or maintaining a separate parser path.
- Preserve organization/index filtering and hydrated-result behavior.
- Pin OpenSearch request-shape tests for boolean, grouped, quoted, wildcard, existence, and range cases.
- Ensure OpenSearch-backed search still returns IDs only and the API still hydrates current objects before partial search and ACL filtering.
- Keep provider parse errors and provider unavailable errors mapped to stable Chef-facing shapes.

### Task 8: Pin Route-level Query Parity Across Search Surfaces

Status:

- Completed. Added a memory-backed route matrix covering default-org and explicit-org aliases for clients, environments, nodes, roles, ordinary data bag items, and encrypted-looking data bag items.
- Added GET full-search and POST partial-search assertions across widened query forms including grouped booleans, quoted phrases, escaped paths, wildcard field names, wildcard values, and ranges.
- Expanded active PostgreSQL plus OpenSearch-backed restart coverage so persisted client/environment/node/role/data-bag/encrypted-data-bag state is rehydrated before route-level search and partial-search responses are shaped.
- Added active non-admin org-member coverage proving ACL filtering still happens after provider matches for broad wildcard and grouped boolean queries.

- Add route-level matrix coverage for memory-backed and active PostgreSQL plus OpenSearch-backed modes.
- Cover default-org and explicit-org aliases for clients, environments, nodes, roles, ordinary data bag items, and encrypted-looking data bag items.
- Cover GET full search and POST partial search for widened query forms.
- Cover non-admin org-member ACL filtering after matches, including denied rows in broad wildcard and grouped boolean queries.
- Include restart/rehydration cases so active OpenSearch query parity is proven against persisted state.

### Task 9: Harden Paging, Ordering, Totals, And Large-result Behavior

Status:

- Completed. Route coverage now pins `start`/`rows` behavior for widened grouped/range queries after stale provider IDs are ignored, objects are hydrated, and ACL filtering has completed.
- Omitted `rows`, `rows=0`, and very large `rows` are documented by tests as uncapped result requests; OpenCook does not add an arbitrary low Chef-facing cap.
- Memory-backed search service coverage now verifies deterministic sorted ordering across a larger fixture set, while active OpenSearch route coverage preserves provider ID ordering after hydration/filtering.
- OpenSearch client coverage now proves `search_after` pagination continues past the default provider page size and ignores provider `hits.total` as a Chef-facing total source.

- Pin `start` and `rows` behavior for widened query forms after ACL filtering.
- Preserve deterministic ordering compatible with current tests and upstream ID ordering signals.
- Decide how to handle omitted `rows`, `rows=0`, very large `rows`, and provider total-hit behavior.
- Avoid adding arbitrary low caps that surprise large Chef installations.
- Add route tests and, where practical, search-service tests for large enough fixture sets to catch ordering and paging drift.

### Task 10: Extend Functional Docker Coverage

Status:

- Completed. Added a targeted `query-compat` functional phase to the Docker harness and included it in the default Compose flow after restart-backed verification.
- The phase now exercises representative widened query forms against active PostgreSQL plus OpenSearch: grouped booleans, quoted phrases, escaped slash values, wildcard field names, wildcard values, ranges, full search, and POST partial search.
- Existing update and deletion phases now also assert widened old-term absence so stale OpenSearch terms are checked beyond simple exact queries.
- Functional docs now show how to run the targeted query compatibility phase independently.

- Add black-box functional phases for representative widened query forms against active PostgreSQL plus OpenSearch.
- Cover grouped boolean, quoted or escaped values, wildcard field/value behavior, encrypted-looking data bag values, partial search, restart verification, and update/delete stale-term removal.
- Keep the functional set small enough to remain practical, with unit and route tests carrying the exhaustive matrix.
- Document how to run any new targeted query-compatibility phase independently.

### Task 11: Sync Docs And Close The Bucket

Status:

- Completed. README, roadmap, milestones, compatibility matrix, functional testing docs, AGENTS guidance, and this plan now describe broader Lucene/query-string compatibility as complete for the implemented search surfaces.
- The next recommended bucket is cookbook/policy/sandbox search coverage, with deeper API-version-specific object semantics, OpenSearch provider capability/version hardening, and migration/cutover tooling kept visible as follow-on options.

- Update:
  - `README.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `AGENTS.md`
  - this plan file
- Mark broader Lucene/query-string search compatibility complete after tests pass.
- Point the next bucket at the strongest remaining compatibility gap, likely one of:
  - cookbook/policy/sandbox search coverage
  - deeper API-version-specific object semantics
  - OpenSearch provider capability/version hardening
  - migration/cutover tooling

## Likely Implementation Touchpoints

- `internal/search/query.go`: parser, AST, memory matcher, OpenSearch query planning.
- `internal/search/query_test.go`: parser and matcher matrix.
- `internal/search/documents.go`: field expansion fixes surfaced by query tests.
- `internal/search/opensearch.go`: request-body compilation and provider error classification if needed.
- `internal/search/opensearch_test.go`: request-shape tests for widened query forms.
- `internal/api/search_routes.go`: invalid-query response mapping, paging/order behavior, and route-level invariants if needed.
- `internal/api/search_routes_test.go`: memory-backed route parity.
- `internal/api/search_opensearch_routes_test.go`: active OpenSearch route parity.
- `internal/app`: status wording only if the bucket exposes a new truthful operational distinction.
- `test/functional/functional_test.go`: black-box query compatibility coverage.
- `scripts/functional-compose.sh`: optional targeted phase wiring.

## Test Plan

Focused verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
scripts/functional-compose.sh
```

Required scenarios:

- current query subset remains unchanged
- memory-backed and active OpenSearch-backed search agree for widened query forms
- default-org and explicit-org search aliases behave the same
- GET full search and POST partial search preserve response shapes
- grouped boolean queries obey Chef-compatible precedence
- escaped and quoted values match the upstream word-break contract
- wildcard field and value queries match upstream-supported cases
- range queries are either implemented or explicitly rejected according to upstream behavior
- ACL filtering happens after provider matching
- pagination and ordering remain deterministic after ACL filtering
- invalid queries return stable Chef-facing errors without provider detail leaks
- encrypted-looking data bag values remain opaque and searchable only as stored JSON
- restart/rehydration preserves active OpenSearch-backed query behavior
- successful updates remove stale query matches and successful deletes remove rows

## Assumptions And Defaults

- PostgreSQL remains the source of truth for persisted objects and ACLs.
- OpenSearch remains a derived provider for candidate IDs.
- Memory search remains the no-OpenSearch fallback and must match active OpenSearch semantics for this bucket's query forms.
- The bucket should prefer a shared parser/AST over relying on OpenSearch-native parsing in one path and custom matching in another.
- Pedant and local Chef Server source win over generic Lucene expectations when they disagree.
- Cookbook, policy, policy group, and sandbox search index coverage stays out of scope unless it is needed to prove query-language behavior.
- No licensing behavior is in scope.
