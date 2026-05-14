# OpenCook Milestones

## Milestone 1: Contract Inventory

Status: in progress

- initial route inventory captured in the compatibility registry and roadmap docs
- `oc-chef-pedant` coverage mapped to the first compatibility surfaces
- golden request and response fixtures still need to be captured
- undocumented behavior still needs a dedicated compatibility inventory pass

## Milestone 2: Auth Compatibility Slice

Status: in progress

- Chef header signing verification is implemented and enforced on the first real endpoints
- in-memory key lookup is implemented for signed request verification
- `/keys` list, create, update, and delete behavior is implemented for users and clients, including default-org and org-scoped client key routes
- key expiration is now enforced during request verification
- API version edge cases and broader actor/resource compatibility semantics are still pending
- fixture-based canonical request coverage still needs to be expanded

## Milestone 3: Org and ACL Bootstrap

Status: in progress

- users, organizations, clients, groups, containers, user/client keys, and ACL documents are now persisted through the PostgreSQL bootstrap core store when PostgreSQL is configured, with the in-memory adapter preserving default behavior otherwise
- Bifrost-style ACL permission checks are implemented in the bootstrap layer
- org bootstrap and validator client creation flows are working, including returned validator key material
- startup rehydration now restores bootstrap core state and request-verifier keys from PostgreSQL
- generated `<org>-validator` clients can now register normal clients through the stock default-org and explicit-org client bootstrap routes, with PostgreSQL restart coverage and functional Docker smoke coverage
- organization membership and broader association workflows still need follow-on slices

## Milestone 4: Core Object APIs

Status: in progress

- in-memory node list/get/head/create/update/delete behavior is implemented
- in-memory environment list/get/head/create/update/delete behavior is implemented, including `_default`
- environment `PUT` now supports Chef-style full replacement and rename semantics
- `/environments/{name}/nodes` is live and filters the current node compatibility state
- `/environments/{name}/cookbooks`, `/environments/{name}/cookbooks/{cookbook}`, and `/environments/{name}/recipes` are now live and apply Chef-style environment cookbook constraints
- in-memory role list/get/head/create/update/delete behavior is implemented
- `/roles/{name}/environments`, `/roles/{name}/environments/{environment}`, and `/environments/{name}/roles/{role}` are live, including `_default` run-list resolution plus Chef-style environment-linked role reads, role-read-only auth parity on the environment-linked role route, and pinned route semantics for missing-org/default-org resolution, trailing slashes, method-not-allowed, extra-path `404`s, and outside-user auth
- `/roles/{name}/environments` and `/roles/{name}/environments/{environment}` now also pin ambiguous/configured default-org handling, missing-organization and missing-role `404`s, missing-role-over-missing-environment precedence, trailing-slash acceptance, method-not-allowed with `Allow: GET`, extra-path `404`s, and role-read-only auth parity on both default-org and org-scoped routes
- in-memory data bag list/get/create/delete behavior is implemented for both default-org and explicit-org routes
- in-memory data bag item get/create/update/delete behavior is implemented with Chef-style response shapes and error messages
- encrypted-looking data bag item payloads are now pinned as opaque JSON across create/read/update/delete, invalid-write no-mutation, ACL behavior, PostgreSQL rehydration, memory/OpenSearch search, operational reindex/check/repair, and functional Docker coverage
- default-org and explicit-org client read/create/delete routes are now available too
- default-org and explicit-org node routes are both available for the first object slice
- default-org and explicit-org environment routes are now available too
- default-org and explicit-org role routes are now available too
- default-org and explicit-org data bag routes are now available too
- creator-aware node ACLs now allow clients to manage their own node objects
- PostgreSQL-backed persistence for nodes, environments, roles, data bags/items, policies, policy groups, sandbox metadata, checksum references, and object ACLs is now live when PostgreSQL is configured, with restart/rehydration coverage for route reads, search-facing state, and depsolver-visible state
- invalid writes and failed persistence writes now have no-mutation/rollback coverage across the active PostgreSQL path and the bootstrap object-store seam
- API-version-specific object semantics are now pinned for `/server_api_version`, v0/v1 actor and client key behavior, v0/v2 cookbook and artifact file shapes, nodes, roles, environments, data bags, policies, sandboxes, OpenSearch-facing node policy fields, default-org/explicit-org aliases, invalid-version precedence, and active PostgreSQL restart/rehydration
- the rest of the object surface still needs follow-on compatibility slices where upstream behavior is not yet pinned, especially broader depsolver and linked-object edge cases

## Milestone 5: Search Compatibility

Status: in progress

- in-memory compatibility search is live for `client`, `environment`, `node`, `role`, and per-data-bag indexes
- `/search` and `/organizations/{org}/search` now advertise the currently implemented built-in indexes plus live data bag indexes
- GET search and POST partial search now support client, environment, node, role, and data bag queries
- search results are filtered through current read authz before pagination is applied
- node partial search now reflects merged attribute precedence for search-facing behavior
- default-org client search results now point at live `/clients/...` routes instead of org-only URLs
- data bag search now mirrors Chef-style wrapper results, encrypted-looking item opacity, and raw-item partial search behavior
- broader Lucene/query-string compatibility is now pinned for the implemented indexes, including grouped booleans, precedence, unary negation, quoted phrases, escaped punctuation, wildcard field names, wildcard values, existence checks, lexicographic ranges, invalid-query shaping, deterministic paging/order, and partial search
- active OpenSearch-backed search is now live when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured, with PostgreSQL-backed state remaining authoritative
- active OpenSearch startup rebuilds the `chef` index from persisted clients, environments, nodes, roles, and data bag items, and successful object mutations update/delete derived search documents
- route coverage now pins OpenSearch-backed full search, partial search, widened query planning through the shared AST, pagination/order, ACL filtering after provider matches, stale-ID ignoring, active status reporting, and stable `503 search_unavailable` degradation for provider failures
- the functional Docker stack now proves active OpenSearch search lifecycle behavior across restart, representative query-string compatibility, update/stale-term removal, delete, and post-restart absence, including encrypted-looking data bag item coverage
- `opencook admin` now provides OpenSearch reindex, consistency check, and consistency repair commands that rebuild and compare derived documents from PostgreSQL-backed authoritative state, with encrypted data bag index coverage and functional Docker coverage for stale-document detection, dry-run repair, actual repair, and post-restart clean verification
- cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum-related state now has explicit negative compatibility coverage: these persisted object families remain absent from search index listings, return unsupported-index responses for full and partial search, are excluded from startup rebuild and mutation indexing, and are rejected by scoped admin reindex/check/repair commands while unscoped repair may delete stale unsupported provider documents
- policyfile routes are now live for both default-org and explicit-org `/policies` and `/policy_groups`
- policy revision storage, revision lookup, policy-group listing, policy-group assignment, and richer canonical payload round-tripping are implemented and now persist through PostgreSQL when configured
- policy payload validation now covers more cookbook-lock and solution-dependency structure, while node policy refs remain compatibility-safe searchable fields instead of enforced foreign keys
- Docker functional coverage now also sends v0, v1, and v2 API-version headers through representative object and OpenSearch-backed search flows after PostgreSQL restart/rehydration
- OpenSearch provider capability/version hardening is now pinned with provider discovery, truthful status/admin capability wording, versioned mapping lifecycle, direct and fallback delete-by-query behavior, provider response/failure redaction, startup activation hardening, and opt-in provider matrix coverage
- migration/cutover tooling is now complete for OpenCook-to-OpenCook logical backup/restore, normalized Chef Server source inventory/normalize/import/sync, source-to-target shadow-read comparison, restored-target reindex, and cutover rehearsal
- `chef-server-ctl`-style operational parity is now complete for config validation, service status/doctor, metrics, request IDs, structured logs, log discovery, diagnostics bundles, runbook discovery, and Docker functional coverage
- maintenance-mode request blocking, shared maintenance state, cache-safe reload seams, and the first narrow online repair path are now pinned
- production-scale migration validation and cutover readiness are now pinned with deterministic scale fixtures, small/medium/large profiles, scale-aware functional phases, operator reports, retry-safe guidance, and remote Docker workflows; direct live Chef Infra Server source extraction beyond normalized artifacts is the next recommended operational bucket unless deployment testing identifies a higher-risk Chef compatibility gap

## Milestone 6: Cookbook and Blob Workflows

Status: in progress

- in-memory sandbox create and commit behavior is implemented on default-org and explicit-org routes
- signed checksum upload and download URLs now point at a live in-memory blob store with content-hash validation and upload-size limits
- in-memory cookbook artifact list/get/create/delete behavior is implemented on default-org and explicit-org routes
- cookbook version create/update/delete behavior is now implemented on default-org and explicit-org `/cookbooks/{name}/{version}` routes
- `/cookbooks`, `/cookbooks/_latest`, `/cookbooks/_recipes`, named cookbook reads, and `/universe` are now live on default-org and explicit-org routes
- cookbook version responses now preserve `json_class`, `cookbook_name`, legacy segment views, and API v2 `all_files` shaping
- frozen cookbook versions now return Chef-style `409` conflicts unless `?force=` is used, and forced updates keep the version frozen
- cookbook PUT responses now preserve pedant-shaped omission of optional top-level fields like `version`, `json_class`, and `chef_type`, and explicit `?force=false` now has its own HTTP coverage
- cookbook create/update HTTP coverage now includes omitted-default exactness, top-level `json_class`/`chef_type`/`version` validation, invalid request-key rejection, metadata-name write-vs-read canonicalization, permissive `metadata.providing` writes, exact no-mutation behavior for invalid metadata payloads, and malformed route-path handling for invalid cookbook names and version strings
- cookbook metadata validation now covers more pedant-shaped string and constraint-map failures, including the update-specific missing-checksum error shape
- cookbook reads now filter metadata down to the Chef-returned subset and inflate upstream defaults without changing exact PUT response bodies
- cookbook version conversion is now covered across v0 and v2 upload/download paths, including segment-aware `all_files[].name` behavior for root files
- cookbook named filters and latest/version reads now reflect manifest-derived recipe names, including Chef-style default recipe qualification on `/cookbooks/_recipes`
- environment-filtered cookbook and recipe views now honor cookbook constraints for both collection and named-cookbook routes
- cookbook create-path validation now matches Chef’s `Field 'name' invalid` behavior for route/payload name-version mismatches, while update-path validation remains field-specific
- cookbook collection and latest-version reads now have explicit pedant-style coverage for `num_versions` edge cases and `_latest` not-found behavior
- cookbook mutation coverage now includes pedant-style v0/v2 file-collection presence and omission exactness on successful update responses
- cookbook version updates/deletes and artifact deletes now clean up unreferenced checksum blobs while preserving shared checksums still referenced by other cookbooks, artifacts, or live sandboxes
- cookbook HTTP coverage now explicitly exercises multi-version shared-checksum retention, successful in-org normal-user cookbook read/delete/create/update behavior, usable signed recipe download URLs, create/update no-mutation guarantees for failed outside-user and invalid-user cookbook mutations, full file-set replacement behavior that deletes all or some cookbook files on update, invalid-checksum update rejection without mutating the existing cookbook file set, malformed negative/overflow route-version handling, plus the expected 401/403 cookbook auth behavior for invalid and outside users
- cookbook artifact HTTP coverage now also exercises wrong-identifier delete no-mutation behavior, successful normal-user artifact reads/deletes with usable signed recipe download URLs, and the expected 401/403 artifact auth behavior for invalid and outside users
- cookbook artifact read coverage now also exercises empty and multi-identifier collections, named-artifact collection views, and explicit API v2 `all_files` response shaping
- cookbook artifact create/update coverage now also exercises large-component and prerelease versions, invalid route name/identifier rejection, exact route/payload name and identifier mismatch errors, repeated-`PUT` `409` conflict behavior, and no-mutation behavior for failed outside-user and invalid-user updates
- cookbook artifact create coverage now also exercises metadata default overrides and multi-identifier create behavior for the same cookbook name
- cookbook artifact validation HTTP coverage now also exercises missing metadata versions, invalid legacy segment shapes, and invalid metadata dependency/platform payloads
- cookbook artifact create auth coverage now also exercises normal-user create success plus 401/403 no-mutation behavior for invalid and outside users
- cookbook artifact route semantics now also exercise trailing-slash acceptance, method-not-allowed plus exact `Allow` headers, and extra-path `404`s, and explicit-org artifact coverage now also exercises org-scoped collection URL shaping, create/update validation and no-mutation behavior, wrong-identifier delete no-mutation, normal-user/outside/invalid auth outcomes, and API v2 `all_files` read shaping on `/organizations/{org}/cookbook_artifacts...`
- explicit-org cookbook/blob coverage now also exercises org-scoped signed cookbook and artifact download usability, missing-uploaded-checksum artifact parity, provider-backed `blob_unavailable` create/update/download behavior, and visible checksum cleanup/retention on `/organizations/{org}/cookbooks...` and `/organizations/{org}/cookbook_artifacts...`
- cookbook auth coverage now also exercises normal-user/outside-user/invalid-user outcomes on cookbook and cookbook-artifact collection, named-filter, and named-collection read routes on both default-org and explicit-org aliases, and explicit-org cookbook mutation coverage now also exercises normal-user create/update/delete success plus outside/invalid no-mutation behavior on `/organizations/{org}/cookbooks/{name}/{version}`
- blob backend selection now supports the existing in-memory compatibility mode, a provider-backed filesystem adapter for local dev/test persistence, and a real S3-compatible adapter for request-path `PUT`/`GET`/`HEAD`/`DELETE` blob operations when endpoint and credentials are configured
- provider-backed blob hardening now also includes pinned S3 status/transport classification, exact retry exhaustion plus `Retry-After` and cancellation behavior, request-construction parity for path-style, virtual-hosted, session-token, and TLS-disabled cases, and clearer malformed-endpoint plus missing-credential diagnostics
- sandbox and cookbook routes now also pin S3-backed `blob_unavailable` degradation for provider-backed upload, download, and checksum-existence failures where that compatibility contract already exists
- role create/update/get coverage now also preserves Chef-style normalization and deduplication for top-level `run_list` and `env_run_lists`, including canonicalized `/roles/{name}/environments/{environment}` payloads
- role environment HTTP coverage now also preserves the current Chef behavior where `/roles/{name}/environments` lists linked environment names even if the environment object is missing, while direct reads of that missing environment still return `404`
- the current depsolver slice is now live on `/environments/{env}/cookbook_versions` and the org-scoped alias, with empty-payload invalid-JSON, trailing-JSON, invalid-run-list, malformed-item, missing-environment, missing-organization, ambiguous-default-org, configured-default-org, method-not-allowed, extra-path-segment, and trailing-slash acceptance coverage on the depsolver routes including both named-environment and `_default` paths, with empty-payload invalid-JSON, trailing-JSON, invalid-run-list, malformed-item, missing-organization, ambiguous-default-org, configured-default-org, method-not-allowed, and extra-path-segment parity now also pinned on the `_default` aliases, upstream-style run-list validator exactness including numeric names, reserved `recipe`/`role` cookbook names, `x.y` and `x.y.z` version suffixes, and stricter one-part-version or malformed-colon rejection, recipe-qualified and version-pinned run-list item support on both default-org and org-scoped routes, explicit malformed run-list item coverage including non-string array elements, equivalent-root normalization and deduplication coverage across plain and `recipe[...]` forms on both default-org and org-scoped routes, server-side `role[...]` expansion plus roles-container-read coverage on both default-org and org-scoped routes including the `_default` aliases, environment-aware cookbook selection, environment-specific role run-list support including explicit empty environment overrides on both default-org and org-scoped routes, `200 {}` parity for explicit empty run lists and requests that omit `run_list` on both default-org and org-scoped routes, with that same explicit-empty and missing-run-list behavior now also pinned on the `_default` aliases, nested-role expansion, version pins, recursive dependency expansion, sibling-aware and multi-root backtracking across compatible dependency alternatives, Chef-style pessimistic `~>` constraint coverage for both major/minor and major/minor/patch forms on both default-org and org-scoped HTTP routes, with that same pessimistic-constraint behavior now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, broader upstream solver-graph parity including combined environment-plus-dependency constraint ranges on both default-org and org-scoped HTTP routes, circular dependency handling now also pinned on the `_default` aliases for both default-org and org-scoped routes, and upstream first/second/complex-dependency/conflicting-failing/pinned-root-no-solution graph selection coverage now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, while the environment-constrained third and conflicting-passing graphs remain named-environment cases because `_default` cannot be modified, root invalid-item precedence plus single and plural missing/no-version root error shaping and mixed missing-vs-no-version root precedence on both default-org and org-scoped routes, with that same root failure detail now also pinned on the `_default` aliases for both default-org and org-scoped paths, missing-dependency, unsatisfied-dependency, impossible-dependency, later-root missing-dependency attribution, transitive-conflict detail, and multi-root conflict detail now also pinned on the `_default` aliases for both default-org and org-scoped paths, impossible dependency coverage caused by environment cookbook constraints plus environment-driven root version selection including both upstream environment-respected branches on both default-org and org-scoped routes, repeated-root first-label error attribution plus successful repeated-root pinned selection on both default-org and org-scoped routes, with that same repeated-root behavior now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, pinned-root dependency resolution, recipe-qualified success, equivalent-root deduplication, pinned equivalent-form selection, and role-expanded equivalent-root deduplication now also pinned on the `_default` aliases for both default-org and org-scoped HTTP routes, missing- and later-root dependency culprit attribution on both default-org and org-scoped routes including unsatisfied-version failure detail, explicit org-scoped alias parity for richer impossible, transitive, and multi-root conflict cases, explicit coverage that unrelated environment cookbook constraints do not perturb an otherwise identical conflict or successful selection result on both default-org and org-scoped routes, environment-read auth parity including short-circuiting before cookbook-container and role-container checks on both default-org and org-scoped routes including the `_default` aliases, cookbook-container read auth parity on both default-org and org-scoped routes including the `_default` aliases, minimal Chef-style success payloads with preserved solved dependency metadata on both named environments and `_default`, explicit org-scoped alias parity coverage for the main missing, filtered, plural missing-root, plural no-version, mixed missing-vs-no-version, `_default`, datestamp, environment-driven, major/minor and major/minor/patch pessimistic-constraint, and combined-constraint success cases, explicit `400` handling for missing and recursive role inputs on both default-org and org-scoped routes including the `_default` aliases, and the current Chef-shaped `400`/`404`/`412` responses for missing, filtered, and richer dependency-failure cookbook cases
- depsolver invalid-JSON precedence now also wins over missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver invalid-run-list precedence now also wins over missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver malformed-item precedence now also wins over missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver trailing-JSON precedence now also wins over missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- depsolver empty-payload invalid-JSON precedence now also wins over missing-environment lookup and environment-read auth on both default-org and org-scoped routes, including the `_default` auth cases
- org-scoped depsolver missing-organization precedence is now also pinned ahead of invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item request bodies on both named-environment and `_default` paths
- default-org depsolver ambiguous-organization precedence is now also pinned ahead of invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item request bodies on both named-environment and `_default` paths
- configured default-org depsolver resolution is now also pinned ahead of invalid JSON, empty payload, trailing JSON, invalid `run_list`, and malformed-item request bodies plus environment-read auth on both named-environment and `_default` paths, and on named-environment routes also ahead of missing-environment lookup
- configured default-org depsolver route semantics are now also pinned for trailing-slash acceptance, `405` plus `Allow: POST`, and extra-path `404`s on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver environment-read auth is now also pinned on both named-environment and `_default` paths in the multi-org case, including role-expanded short-circuiting before role-container auth
- configured default-org depsolver cookbook-container read auth is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver roles-container read auth is now also pinned for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver missing-role and recursive-role `400`s are now also pinned for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver role-expanded success is now also pinned on both named-environment and `_default` paths in the multi-org case, including environment-specific role run-list selection
- configured default-org depsolver explicit-empty environment-specific role run-list behavior is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver role-expanded equivalent-root deduplication is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver missing-environment `404` is now also pinned on the named-environment default-org route in the multi-org case
- configured default-org depsolver empty-`run_list` success is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver omitted-`run_list` success is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver single missing-root, single no-version-root, and mixed missing-vs-no-version root precedence are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver plural missing-root and plural no-version-root detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver named-environment filtered-root no-version detail is now also pinned when environment cookbook constraints exclude every candidate version
- configured default-org depsolver named-environment impossible-dependency detail is now also pinned when environment cookbook constraints make a dependency unsatisfiable
- configured default-org depsolver named-environment environment-respected root selection is now also pinned for both the older-root fallback and newer-root-allowed branches
- configured default-org depsolver named-environment combined environment-plus-dependency constraint success is now also pinned
- configured default-org depsolver named-environment conflict and success stability are now also pinned when unrelated environment cookbook constraints are present
- configured default-org depsolver upstream conflicting-failing graph detail is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver missing-dependency, later-root missing-dependency attribution, unsatisfied-dependency, and impossible-dependency detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver transitive-conflict, complex-dependency, and multi-root conflict detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver pinned/dependent success and dependency-metadata shaping are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver recipe-qualified success, equivalent-root deduplication, and pinned equivalent-form selection are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver upstream first-graph, pinned-root-no-solution, and second-graph selection are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver pessimistic major/minor and major/minor/patch constraints, repeated-root pinned selection and first-label attribution, and circular dependency handling are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver named-environment datestamp-version support is now also pinned
- configured default-org depsolver non-admin org-member dependency-metadata shaping is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member pinned-and-dependent success is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member recipe-qualified success, equivalent-root deduplication, and pinned equivalent-form selection are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member single missing-root, single no-version-root, and mixed missing-vs-no-version root precedence are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member plural missing-root and plural no-version-root detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member missing-dependency, later-root missing-dependency attribution, unsatisfied-dependency, and impossible-dependency detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member transitive-conflict, complex-dependency, and multi-root conflict detail are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member upstream first-graph, pinned-root-no-solution, and second-graph selection are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member pessimistic major/minor and major/minor/patch constraints, repeated-root pinned selection and first-label attribution, and circular dependency handling are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member named-environment datestamp-version support is now also pinned
- configured default-org depsolver non-admin org-member role-expanded missing-role, recursive-role, environment-specific success, explicit-empty environment override, and equivalent-root deduplication are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member named-environment filtered-root no-version detail is now also pinned when environment cookbook constraints exclude every candidate version
- configured default-org depsolver non-admin org-member named-environment impossible-dependency detail is now also pinned when environment cookbook constraints make a dependency unsatisfiable
- configured default-org depsolver non-admin org-member named-environment environment-respected root selection is now also pinned for both the older-root fallback and newer-root-allowed branches
- configured default-org depsolver non-admin org-member named-environment combined environment-plus-dependency constraint success is now also pinned
- configured default-org depsolver non-admin org-member named-environment conflict and success stability are now also pinned when unrelated environment cookbook constraints are present
- configured default-org depsolver non-admin org-member named-environment upstream third-graph and conflicting-passing graph selection are now also pinned
- configured default-org depsolver non-admin org-member conflicting-failing graph detail is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member environment-read auth is now also pinned on both named-environment and `_default` paths in the multi-org case, including role-expanded short-circuiting before role-container auth
- configured default-org depsolver non-admin org-member cookbook-container read auth is now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member roles-container read auth is now also pinned for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member missing-role and recursive-role parity are now also pinned for role-expanded requests on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member empty- and omitted-`run_list` success are now also pinned on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member route semantics are now also pinned for trailing-slash acceptance, `405` plus `Allow: POST`, and extra-path `404`s on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member missing-environment `404` is now also pinned on the named-environment path in the multi-org case
- configured default-org depsolver non-admin org-member malformed-request precedence is now also pinned before environment-read auth on both named-environment and `_default` paths in the multi-org case
- configured default-org depsolver non-admin org-member malformed-request precedence is now also pinned before named-environment missing-environment lookup in the multi-org case
- org-scoped depsolver non-admin org-member dependency-metadata shaping and pinned-and-dependent success are now also pinned on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member root-form success is now also pinned for recipe-qualified items, equivalent-root deduplication, and pinned equivalent-form selection on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member root-failure detail is now also pinned for single missing-root, single no-version-root, and mixed missing-vs-no-version precedence on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member plural-root detail is now also pinned for plural missing-root and plural no-version-root shaping on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member dependency detail is now also pinned for missing-dependency, later-root attribution, unsatisfied-dependency, and impossible-dependency shaping on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member richer conflict detail is now also pinned for transitive conflict, complex dependency, and multi-root conflict shaping on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member graph selection is now also pinned for the upstream first graph, pinned-root-no-solution graph, and second graph on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member solver mechanics are now also pinned for pessimistic constraints, repeated-root pinned selection and first-label attribution, and circular dependency handling on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member named-environment datestamp-version support is now also pinned
- org-scoped depsolver non-admin org-member role-expanded missing-role, recursive-role, environment-specific success, explicit-empty environment override, and equivalent-root deduplication are now also pinned on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member environment-read auth is now also pinned on both named-environment and `_default` paths, including role-expanded short-circuiting before role-container auth
- org-scoped depsolver non-admin org-member cookbook-container read auth is now also pinned on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member roles-container read auth is now also pinned for role-expanded requests on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member empty- and omitted-`run_list` success plus route semantics are now also pinned on both named-environment and `_default` paths
- org-scoped depsolver non-admin org-member missing-environment `404` is now also pinned on the named-environment path
- org-scoped depsolver non-admin org-member malformed-request precedence is now also pinned before environment-read auth on both named-environment and `_default` paths, and before named-environment missing-environment lookup
- PostgreSQL-backed cookbook persistence is now live for cookbook versions and cookbook artifacts
- mixed PostgreSQL-backed cookbook metadata plus provider-backed blob lifecycle hardening is now also complete
- PostgreSQL-backed bootstrap core persistence is now live for identity and authorization root state
- PostgreSQL-backed core object API persistence is now live for nodes, environments, roles, data bags/items, policy state, sandbox metadata/checksum references, and object ACLs
- S3-compatible blob storage remains the target production mode after the compatibility contract settles

## Milestone 7: Validator Bootstrap Compatibility

Status: complete

- org bootstrap returns generated `<org>-validator` key material
- same-org generated validator clients can register normal clients through default-org and explicit-org client create routes
- generated-key and explicit-public-key registration flows, no-mutation failures, ACL/group side effects, and PostgreSQL restart/rehydration behavior are covered
- the functional Docker flow now exercises validator-authenticated client registration and follow-up signed client requests against the PostgreSQL-backed stack

## Milestone 8: Operations and Migration

Status: in progress

- the first `opencook admin` path is live for signed HTTP-backed user/org/key/group/container/ACL inspection and live-safe management workflows without changing Chef-facing API contracts
- direct PostgreSQL repair-style commands for org membership, server-admin membership, and group membership remain offline-gated until each has a narrow live seam and cache/search safety coverage; default ACL repair now has a maintenance-gated online path through the live service
- OpenSearch reindex, consistency check, and repair commands can rebuild and compare derived documents from PostgreSQL-backed state, including encrypted data bag indexes
- functional Docker coverage now proves admin status, live-safe user/org/key operations, group/container/ACL inspection, complete org reindex, encrypted data bag scoped reindex/check/repair, stale OpenSearch detection, dry-run repair, repair, and post-restart verification
- OpenSearch provider capability details are visible through status/admin status and functional coverage, with package-level direct and fallback delete provider-mode coverage for admin reindex/check/repair
- migration/cutover tooling now provides redacted preflight checks, OpenCook logical backup create/inspect, offline restore preflight/apply, normalized Chef Server source inventory/normalize/import/sync, source-to-target shadow-read comparison, restored-target complete reindex, and live cutover rehearsal against a temporary restored OpenCook target
- functional Docker coverage now also exercises migration preflight, backup create/inspect, restore into a fresh PostgreSQL/blob target, complete reindex from restored state, normalized source import/sync, shadow-read comparison, hardened source cutover rehearsal, and clear success footers
- `chef-server-ctl`-style operational parity now includes config validation, service status/doctor, safe `/metrics`, request IDs, structured request logs, log path discovery, redacted diagnostics bundles, runbook list/show, service-management docs, and Docker functional coverage
- full Chef-style operational documentation is now anchored in the README, functional test guide, and `docs/chef-server-ctl-operational-runbooks.md`
- live maintenance mode now blocks mutating Chef-facing writes during operator-controlled windows while preserving reads, read-like POST routes, and signed blob downloads
- maintenance state is PostgreSQL-shared in durable deployments, process-local in standalone mode, and visible through admin maintenance commands, `/status`, service doctor, metrics, structured logs, functional tests, and operator runbooks
- cache-invalidation and reload seams now exist for PostgreSQL-backed bootstrap/core/cookbook state, with default ACL repair as the first narrow online repair path under active maintenance
- production-scale validation now covers migration, reindex, source sync, shadow-read comparison, rollback readiness, cutover rehearsal guidance, and operator-facing report summaries; direct live Chef Infra Server source extraction beyond normalized artifacts is the next recommended follow-on bucket
