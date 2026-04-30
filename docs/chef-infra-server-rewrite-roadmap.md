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

As of 2026-04-29, OpenCook has moved past pure scaffolding and into the first compatibility and operational hardening slices:

- Chef request signing verification is implemented in Go and enforced on the first authenticated routes
- user, organization, client, group, container, key, and ACL bootstrap core flows are working and can now persist through PostgreSQL when configured
- org bootstrap creates validator clients with default key material
- generated `<org>-validator` clients can now register normal clients through the stock default-org and explicit-org client bootstrap routes
- actor key lifecycle now supports list, create, update, delete, and expiration-aware authentication behavior
- the first migration/cutover tooling slice is now live with redacted preflight checks, OpenCook logical backup create/inspect, offline restore preflight/apply, read-only Chef Server source artifact inventory, restored-target complete reindex, cutover rehearsal, and Docker functional coverage
- `chef-server-ctl`-style operational parity is now live for config validation, service status/doctor, Prometheus-compatible metrics, request IDs, structured logs, log discovery, redacted diagnostics bundles, runbook discovery, service-management docs, and Docker functional coverage
- the core object API persistence slice is now live with PostgreSQL-backed durability for nodes, environments, roles, data bags/items, policy revisions/groups/assignments, sandbox metadata/checksum references, and object ACLs when PostgreSQL is configured, while preserving the existing in-memory fallback
- the adjacent environment slice is now live with `_default`, list/get/head/create/update/delete, and rename-capable `PUT`
- environment-scoped node listing is implemented via `/environments/{name}/nodes`
- environment-scoped cookbook and recipe views are now implemented via `/environments/{name}/cookbooks`, `/environments/{name}/cookbooks/{cookbook}`, and `/environments/{name}/recipes`
- the current environment depsolver slice is now live via `/environments/{name}/cookbook_versions` and `/organizations/{org}/environments/{name}/cookbook_versions`
- depsolver HTTP coverage now also pins environment-read auth parity, including short-circuiting before cookbook-container and role-container checks on both default-org and org-scoped routes plus the `_default` aliases
- depsolver invalid-JSON handling now also wins before missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver invalid-run-list handling now also wins before missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver malformed-item handling is now also explicitly pinned before missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver trailing-JSON handling is now also explicitly pinned before missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver empty-payload invalid-JSON handling is now also explicitly pinned before missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- org-scoped depsolver missing-organization handling is now also explicitly pinned before malformed-request bodies, including invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item inputs on both named-environment and `_default` paths
- default-org depsolver ambiguous-organization handling is now also explicitly pinned before malformed-request bodies, including invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item inputs on both named-environment and `_default` paths
- configured default-org depsolver resolution is now also explicitly pinned before malformed-request bodies and environment-read auth, and on named-environment routes also before missing-environment lookup, including invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item inputs
- configured default-org depsolver resolution now also has explicit multi-org route-semantics coverage for trailing slashes, method-not-allowed with `Allow: POST`, and extra-path `404`s on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org environment-read auth parity, including role-expanded short-circuiting before role-container auth on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org cookbook-container read auth parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org roles-container read auth parity for role-expanded requests on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org missing-role and recursive-role `400` parity for role-expanded requests on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org role-expanded success parity, including environment-specific role run-list selection on named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org explicit-empty environment-specific role run-list parity on named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit multi-org role-expanded equivalent-root deduplication parity on named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit named-environment missing-environment `404` parity on the resolved default-org route
- configured default-org depsolver resolution now also has explicit empty-`run_list` success parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit omitted-`run_list` success parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit single missing-root, single no-version-root, and mixed missing-vs-no-version root precedence parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit plural missing-root and plural no-version-root detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit named-environment filtered-root no-version detail parity when environment cookbook constraints exclude every candidate version
- configured default-org depsolver resolution now also has explicit named-environment impossible-dependency detail parity when environment cookbook constraints make a dependency unsatisfiable
- configured default-org depsolver resolution now also has explicit named-environment environment-respected root-selection parity for both the older-root fallback and newer-root-allowed branches
- configured default-org depsolver resolution now also has explicit named-environment combined environment-plus-dependency constraint success parity
- configured default-org depsolver resolution now also has explicit named-environment stability parity showing unrelated environment cookbook constraints do not perturb either conflict detail or successful selection
- configured default-org depsolver resolution now also has explicit upstream conflicting-failing graph parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit missing-dependency, later-root missing-dependency attribution, unsatisfied-dependency, and impossible-dependency detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit transitive-conflict, complex-dependency, and multi-root conflict detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit pinned/dependent success and dependency-metadata shaping parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit recipe-qualified success, equivalent-root deduplication, and pinned equivalent-form selection parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit upstream first-graph, pinned-root-no-solution, and second-graph selection parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit pessimistic major/minor and major/minor/patch constraint parity, repeated-root pinned-selection and first-label attribution parity, and circular-dependency parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit named-environment datestamp-version parity
- configured default-org depsolver resolution now also has explicit non-admin org-member dependency-metadata shaping parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member pinned-and-dependent success parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member recipe-qualified success, equivalent-root deduplication, and pinned equivalent-form selection parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member single missing-root, single no-version-root, and mixed missing-vs-no-version root precedence parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member plural missing-root and plural no-version-root detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member missing-dependency, later-root missing-dependency attribution, unsatisfied-dependency, and impossible-dependency detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member transitive-conflict, complex-dependency, and multi-root conflict detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member upstream first-graph, pinned-root-no-solution, and second-graph selection parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member pessimistic major/minor and major/minor/patch constraints, repeated-root pinned selection and first-label attribution, and circular dependency handling parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment datestamp-version parity
- configured default-org depsolver resolution now also has explicit non-admin org-member role-expanded missing-role, recursive-role, environment-specific success, explicit-empty environment override, and equivalent-root deduplication parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment filtered-root no-version detail parity when environment cookbook constraints exclude every candidate version
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment impossible-dependency detail parity when environment cookbook constraints make a dependency unsatisfiable
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment environment-respected root-selection parity for both the older-root fallback and newer-root-allowed branches
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment combined environment-plus-dependency constraint success parity
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment conflict and success stability parity when unrelated environment cookbook constraints are present
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment upstream third-graph and conflicting-passing graph parity
- configured default-org depsolver resolution now also has explicit non-admin org-member conflicting-failing graph detail parity on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member environment-read auth parity on both named-environment and `_default` paths, including role-expanded short-circuiting before role-container auth
- configured default-org depsolver resolution now also has explicit non-admin org-member cookbook-container read auth parity on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver resolution now also has explicit non-admin org-member roles-container read auth parity for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver resolution now also has explicit non-admin org-member missing-role and recursive-role parity for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver resolution now also has explicit non-admin org-member empty- and omitted-`run_list` success parity on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver resolution now also has explicit non-admin org-member route-semantics parity for trailing-slash acceptance, `405` plus `Allow: POST`, and extra-path `404`s on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver resolution now also has explicit non-admin org-member named-environment missing-environment `404` parity
- configured default-org depsolver resolution now also has explicit non-admin org-member malformed-request precedence parity before environment-read auth on both named-environment and `_default` paths
- configured default-org depsolver resolution now also has explicit non-admin org-member malformed-request precedence parity before named-environment missing-environment lookup
- org-scoped depsolver routes now also have explicit non-admin org-member dependency-metadata shaping and pinned-and-dependent success parity on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member root-form success parity for recipe-qualified items, equivalent-root deduplication, and pinned equivalent-form selection on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member root-failure detail parity for single missing-root, single no-version-root, and mixed missing-vs-no-version precedence on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member plural-root detail parity for plural missing-root and plural no-version-root shaping on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member dependency-detail parity for missing-dependency, later-root attribution, unsatisfied-dependency, and impossible-dependency shaping on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member richer-conflict detail parity for transitive conflict, complex dependency, and multi-root conflict shaping on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member graph-selection parity for the upstream first graph, pinned-root-no-solution graph, and second graph on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member solver-mechanics parity for pessimistic constraints, repeated-root pinned selection and first-label attribution, and circular dependency handling on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member named-environment datestamp-version parity
- org-scoped depsolver routes now also have explicit non-admin org-member role-expansion parity for missing-role, recursive-role, environment-specific success, explicit-empty environment override, and equivalent-root deduplication on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member environment-read auth parity on both named-environment and `_default` paths, including role-expanded short-circuiting before role-container auth
- org-scoped depsolver routes now also have explicit non-admin org-member cookbook-container read auth parity on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member roles-container read auth parity for role-expanded requests on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member empty- and omitted-`run_list` success plus route-semantics parity on both named-environment and `_default` paths
- org-scoped depsolver routes now also have explicit non-admin org-member missing-environment `404` parity on the named-environment path
- org-scoped depsolver routes now also have explicit non-admin org-member malformed-request precedence parity before environment-read auth on both named-environment and `_default` paths, and before named-environment missing-environment lookup
- default-org and explicit-org node routes now resolve against the same org-scoped compatibility state
- default-org and explicit-org environment routes now resolve against the same org-scoped compatibility state
- the first role slice is now live with in-memory list/get/head/create/update/delete behavior plus Chef-style run-list and env-run-list normalization/deduplication
- role environment endpoints are implemented via `/roles/{name}/environments`, `/roles/{name}/environments/{environment}`, and `/environments/{name}/roles/{role}`, including linked-missing-environment list-vs-read parity, pinned ambiguous/configured default-org handling, missing-organization and missing-role `404`s, missing-role-over-missing-environment precedence, trailing-slash, method-not-allowed with `Allow: GET`, extra-path route semantics, and role-read-only auth parity on the `/roles/{name}/environments*` routes, Chef-style environment-linked role read behavior, role-read-only auth parity on the environment-linked role route, and pinned route semantics for missing-organization, default-org resolution, trailing-slash, method-not-allowed, extra-path, and outside-user cases
- default-org and explicit-org role routes now resolve against the same org-scoped compatibility state
- default-org client routes are now live for `/clients`, `/clients/{name}`, and `/clients/{name}/keys`
- the first data bag slice is now live with `/data`, `/data/{bag}`, and `/data/{bag}/{item}` on both default-org and explicit-org routes
- data bag item create, update, and delete flows now reproduce Chef-style response wrapping and not-found/conflict messages
- encrypted data bag compatibility is now explicitly pinned as a server-side payload opacity contract: encrypted-looking item JSON is stored, returned, cloned, persisted through PostgreSQL, searched and partial-searched through memory and OpenSearch, reindexed/repaired operationally, and covered by functional Docker tests without server-side secrets or crypto validation
- the first search-facing slice is now live with `/search` and `/search/{client,environment,node,role}` plus per-data-bag indexes on both default-org and explicit-org routes
- the default no-OpenSearch path keeps the in-memory compatibility adapter, while configured PostgreSQL plus `OPENCOOK_OPENSEARCH_URL` activates OpenSearch-backed search for clients, environments, nodes, roles, and data bag items
- active OpenSearch mode rebuilds the `chef` index from PostgreSQL-backed state at startup, updates and deletes derived search documents after successful object mutations, ignores stale provider IDs after hydration, and keeps PostgreSQL as the source of truth
- partial search, ACL-filtered responses, default-org client URLs, ordinary and encrypted-looking data bag wrapper rows, pagination, and broader Lucene/query-string semantics now have parity coverage across memory and active OpenSearch-backed paths for the implemented search indexes
- OpenSearch provider capability/version behavior is now hardened with discovery, cached provider identity/capability flags, versioned mapping metadata, direct and fallback delete-by-query behavior, stable failure classification/redaction, and status/admin wording that reports provider distribution, version, search-after support, delete mode, and total-hit shape without payload-key churn
- OpenSearch provider failures now degrade through stable `503 search_unavailable` route responses where applicable, and status reporting distinguishes memory fallback, active OpenSearch, provider capability details, and configured-but-unavailable OpenSearch
- the functional Docker stack now proves active OpenSearch search lifecycle behavior across create, restart, representative widened query compatibility, update, stale-term removal, delete, and post-restart absence, including encrypted-looking data bag item coverage and provider capability/status wording; an opt-in provider image matrix plus package-level capability-mode harness cover direct delete-by-query and fallback-delete provider paths
- cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum-related state now has explicit negative search compatibility coverage: those persisted object families remain absent from `/search` index listings, unsupported for full and partial search, excluded from startup rebuild and mutation indexing, rejected by admin reindex/check/repair scoped commands, and cleaned up only as stale unsupported provider documents during unscoped repair; node `policy_name` and `policy_group` remain searchable through the supported node index
- the policyfile slice is now live on both default-org and explicit-org routes with `/policies`, `/policies/{name}`, `/policies/{name}/revisions`, `/policies/{name}/revisions/{revision}`, `/policy_groups`, and `/policy_groups/{group}/policies/{name}`
- policy revision create/get/delete, policy-group listing, policy-group get/delete, and policy-group assignment flows are now working in the in-memory compatibility layer
- policy payload normalization now preserves richer canonical structures like `named_run_lists`, nested cookbook-lock metadata, and `solution_dependencies`, with deeper validation around cookbook lock versions and shapes
- node `policy_name` and `policy_group` semantics remain compatibility-safe searchable fields rather than newly enforced foreign keys
- the first sandbox/blob slice is now live with signed sandbox create/commit flows, absolute checksum upload URLs, and in-memory checksum blob storage
- the first provider-backed blob seam is now live with backend selection, a filesystem adapter for local dev/test persistence, and S3-compatible request-path blob operations for configured endpoints and static credentials
- the S3-compatible blob path now also has pinned transport/status classification, retry/backoff and `Retry-After` behavior, request-construction coverage for path-style, virtual-hosted, session-token, and TLS-disabled cases, plus config/status validation for malformed endpoints and missing credentials
- sandbox and cookbook routes now also pin provider-backed `blob_unavailable` degradation for checksum upload/download and checksum-existence failures through the S3-compatible path instead of surfacing generic internal blob failures
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
- cookbook artifact route semantics now also pin trailing-slash acceptance, method-not-allowed plus exact `Allow` headers, and extra-path `404`s, and explicit-org artifact coverage now also pins org-scoped collection URL shaping, create/update validation and no-mutation behavior, wrong-identifier delete no-mutation, normal-user/outside/invalid auth outcomes, and API v2 `all_files` read shaping on `/organizations/{org}/cookbook_artifacts...`
- explicit-org cookbook/blob coverage now also pins org-scoped signed cookbook and artifact download usability, missing-uploaded-checksum artifact parity, provider-backed `blob_unavailable` create/update/download behavior, and visible checksum cleanup/retention on `/organizations/{org}/cookbooks...` and `/organizations/{org}/cookbook_artifacts...`
- cookbook auth coverage now also pins normal-user/outside-user/invalid-user outcomes on cookbook and cookbook-artifact collection, named-filter, and named-collection read routes on both default-org and explicit-org aliases, and explicit-org cookbook mutation coverage now also pins normal-user create/update/delete success plus outside/invalid no-mutation behavior on `/organizations/{org}/cookbooks/{name}/{version}`
- the active PostgreSQL-backed cookbook path is now also hardened against provider-backed blob behavior, with filesystem-backed read/download/mutation parity, restart/rehydration coverage, shared-checksum cleanup and retention, provider-unavailable `blob_unavailable` degradation, cleanup-failure tolerance, and more truthful active backend/status reporting
- PostgreSQL-backed bootstrap core persistence is now live for users, organizations, clients, user/client keys, groups, containers, and ACL documents, with startup rehydration into the existing bootstrap service/cache model and request verifier key cache
- PostgreSQL-backed core object API persistence is now live for nodes, environments, roles, data bags/items, policies, policy groups, sandbox metadata, checksum references, and object ACL documents, with restart/rehydration coverage for route reads, search-facing state, depsolver-visible state, invalid-write no-mutation behavior, and persistence failure rollback
- API-version-specific object semantics are now pinned across `/server_api_version`, invalid-version precedence, signed-header verification, v0/v1 user and client key behavior, v0/v2 cookbook and cookbook-artifact file shapes, nodes, roles, environments, data bags, policies, sandboxes, and OpenSearch-facing node policy fields, with Docker functional coverage against the PostgreSQL plus OpenSearch stack
- the current depsolver slice now validates cookbook run lists with upstream-style item-shape exactness including numeric names, reserved `recipe`/`role` cookbook names, `x.y` and `x.y.z` version suffixes, and stricter one-part-version or malformed-colon rejection on both default-org and org-scoped routes, accepts recipe-qualified and version-pinned run-list items on both default-org and org-scoped routes, has invalid-JSON, trailing-JSON, invalid-run-list, malformed-item, missing-environment, missing-organization, ambiguous-default-org, configured-default-org, method-not-allowed, extra-path-segment, and trailing-slash acceptance coverage on the depsolver routes including both named-environment and `_default` paths, with empty-payload invalid-JSON, trailing-JSON, invalid-run-list, malformed-item, missing-organization, ambiguous-default-org, configured-default-org, method-not-allowed, and extra-path-segment parity now also pinned on the `_default` aliases, normalizes and deduplicates equivalent cookbook roots across plain and `recipe[...]` forms on both default-org and org-scoped routes, expands `role[...]` items server-side, honors environment cookbook constraints and version pins, uses environment-specific role run lists when present including explicit empty environment overrides on both default-org and org-scoped routes, returns `200 {}` for both explicit empty run lists and requests that omit `run_list` on the default-org and org-scoped routes, with that same explicit-empty and missing-run-list behavior now also pinned on the `_default` aliases, expands nested roles and recursive cookbook dependencies, backtracks across compatible sibling and multi-root dependency alternatives, covers Chef-style pessimistic `~>` constraint behavior for both major/minor and major/minor/patch forms on both default-org and org-scoped HTTP routes, with that same pessimistic-constraint behavior now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, covers broader upstream solver-graph parity including combined environment-plus-dependency constraint ranges on both default-org and org-scoped HTTP routes, circular dependency handling now also pinned on the `_default` aliases for both default-org and org-scoped routes, and upstream first/second/complex-dependency/conflicting-failing/pinned-root-no-solution graph selection coverage now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, while the environment-constrained third and conflicting-passing graphs remain named-environment cases because `_default` cannot be modified, preserves root invalid-item precedence plus single and plural missing/no-version root error shaping and mixed missing-vs-no-version root precedence on both default-org and org-scoped routes, now also pins that same root failure detail on the `_default` aliases for both default-org and org-scoped paths, now also pins missing-dependency, unsatisfied-dependency, impossible-dependency, later-root missing-dependency attribution, transitive-conflict detail, and multi-root conflict detail on the `_default` aliases for both default-org and org-scoped paths, covers impossible dependency failures caused by environment cookbook constraints plus environment-driven root version selection including both upstream environment-respected branches on both default-org and org-scoped routes, preserves both repeated-root first-label attribution and successful repeated-root pinned selection on both default-org and org-scoped routes, with that same repeated-root behavior now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, now also pins pinned-root dependency resolution, recipe-qualified success, equivalent-root deduplication, pinned equivalent-form selection, and role-expanded equivalent-root deduplication on the `_default` aliases for both default-org and org-scoped HTTP routes, now also preserves missing- and later-root dependency culprit attribution on both default-org and org-scoped routes including unsatisfied-version failure detail, now also has explicit org-scoped alias parity for impossible, transitive, and multi-root conflict cases, now also preserves conflict-result stability and successful-selection stability when unrelated environment cookbook constraints are present on both default-org and org-scoped routes, enforces cookbook-container read auth alongside environment read on both default-org and org-scoped routes including the `_default` aliases, enforces roles-container read for role-expanded run lists on both default-org and org-scoped routes including the `_default` aliases, now also pins missing-role and recursive-role `400` responses on the `_default` aliases for both default-org and org-scoped paths, returns the upstream-style minimal cookbook payloads that omit `metadata.attributes` and `metadata.long_description` while preserving solved cookbook dependency metadata on both named-environment and `_default` success paths, now has explicit org-scoped alias parity coverage for missing, filtered, plural missing-root, plural no-version, mixed missing-vs-no-version, `_default`, datestamp, environment-driven, major/minor and major/minor/patch pessimistic-constraint, combined-constraint, equivalent-root, recipe-qualified, and role-expanded success cases, and returns the current Chef-shaped `400`/`404`/`412` responses for invalid, missing, filtered, datestamp, missing-role, recursive-role, and richer transitive or multi-root dependency-failure cookbook cases with fuller constraint-path detail
- compatibility tracking docs and route inventory are in place and being updated alongside code

Current focus:

- plan Chef Infra Server source import/sync beyond read-only source inventory now that PostgreSQL persistence, provider-backed blobs, validator bootstrap, core object persistence, encrypted data bag compatibility, operational admin/reindex/repair tooling, migration/cutover tooling, broader Lucene/query-string compatibility, cookbook/policy/sandbox/checksum negative search compatibility, API-version-specific object semantics, OpenSearch provider capability/version hardening, and `chef-server-ctl`-style operational parity are pinned
- preserve the completed API-version, search-route, unsupported-index, encrypted-data-bag, provider capability, migration/cutover, operational parity, and PostgreSQL-source-of-truth contracts while designing source import/sync and shadow-read cutover checks
- treat deployment-test compatibility gaps as interrupt-worthy if they are higher-risk than source import/sync hardening

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
- Current status: active OpenSearch-backed search is live when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured, including startup rebuild, mutation indexing, hydration from PostgreSQL-backed state, ACL filtering, partial search, broader Lucene/query-string semantics for the implemented indexes, encrypted-looking data bag search/reindex/repair coverage, provider-unavailable degradation, Docker functional coverage for the implemented client/environment/node/role/data-bag indexes, and `opencook admin` reindex/check/repair tooling. Cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum-related state is now explicitly pinned as not publicly searchable unless future upstream evidence proves otherwise; node policy refs remain searchable only as node document fields. Provider capability/version behavior is now explicit through discovery, status/admin wording, versioned mapping handling, direct and fallback delete-by-query behavior, provider response/failure classification, startup activation hardening, and opt-in functional/provider-mode coverage.

### Blob/object storage layer

- Support S3-compatible storage as the primary production mode
- Provide local dev/test filesystem mode
- Emulate Bookshelf upload/download contracts, including signed URL behavior expected by clients
- Current status: provider selection now exists, the filesystem adapter is live for local dev/test persistence, and the S3-compatible path now supports real request-time blob operations for configured endpoints and static credentials, with pinned status/transport classification, retry and `Retry-After` behavior, request-construction parity for path-style and virtual-hosted flows, malformed-endpoint and missing-credential diagnostics, and sandbox/cookbook `blob_unavailable` degradation on the current provider-backed paths

### Operations layer

- Health/readiness endpoints for API, PostgreSQL, OpenSearch, and object storage
- Metrics compatible with Prometheus/OpenTelemetry
- Structured logs with request IDs
- Admin tooling for org/user/group/container/ACL management plus reindex, consistency checks, and data repair
- Current status: the `opencook admin` surface is live, with signed HTTP-backed user/org/key/group/container/ACL inspection workflows, offline-gated direct PostgreSQL repair commands, OpenSearch reindex/check/repair from PostgreSQL-backed state including encrypted data bag indexes, unsupported-index rejection for cookbook/policy/sandbox/checksum scopes, JSON/human output modes, destructive-command confirmation gates, OpenCook-to-OpenCook migration preflight/backup/restore/reindex/rehearsal tooling, read-only Chef Server source artifact inventory, config validation, service status/doctor, log path discovery, redacted diagnostics bundles, runbook discovery, service-management docs, Prometheus-compatible `/metrics`, request IDs, structured operational logs, and Docker functional coverage against PostgreSQL plus OpenSearch plus filesystem-backed blobs

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
- validator-based client bootstrap registration compatibility

Exit criteria:

- bootstrap flows, validator bootstrap registration compatibility, and core authz tests pass

## Phase 4: Core Chef object APIs

Deliverables:

- nodes
- roles
- environments
- data bags
- encrypted data bag payload compatibility and coverage
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
- chef-server-ctl-style admin flows for orgs, users, groups, containers, and ACLs
- full Chef-style documentation for admin and operational workflows, with the final CLI/API packaging left open for now
- backup/restore guidance and first OpenCook logical backup/restore tooling
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
- first cutover rehearsal/runbook tooling for restored OpenCook targets
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

Plan and implement Chef Infra Server source import/sync plus shadow-read/cutover hardening now that PostgreSQL persistence, provider-backed blobs, validator bootstrap, core object persistence, encrypted data bag compatibility, operational admin/reindex/repair tooling, first migration/cutover tooling, broader Lucene/query-string compatibility, cookbook/policy/sandbox/checksum negative search compatibility, API-version-specific object semantics, OpenSearch provider capability/version hardening, and `chef-server-ctl`-style operational parity are pinned.

The recommended next bucket should:

1. Inventory the upstream Chef Infra Server source artifacts and APIs needed to import users, organizations, clients, keys, groups, containers, ACLs, core objects, cookbooks, sandboxes/checksums, policies, and search-relevant state without relying on unsupported licensing endpoints.
2. Extend the current read-only source inventory into a safe import plan with dry-run validation, compatibility normalizers, resumable progress metadata, and no-mutation guarantees on failed imports.
3. Define source-to-target sync and conflict behavior for PostgreSQL metadata, provider-backed blobs, and derived OpenSearch documents while preserving PostgreSQL as the restored target source of truth.
4. Deepen shadow-read and cutover rehearsal checks so restored OpenCook responses can be compared against read-only source Chef responses with documented compatibility normalizers and rollback guidance.

That sequence builds on the completed identity, cookbook/blob, core object, validator bootstrap, active OpenSearch, operational tooling, migration/cutover, encrypted data bag, Lucene/query-string, API-version, cookbook/policy/sandbox search, provider capability, and operational parity contracts without reopening their Chef-facing behavior.
