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

- users, organizations, clients, groups, containers, and default ACLs are implemented in memory
- Bifrost-style ACL permission checks are implemented in the bootstrap layer
- org bootstrap and validator client creation flows are working
- organization membership and broader association workflows still need follow-on slices

## Milestone 4: Core Object APIs

Status: in progress

- in-memory node list/get/head/create/update/delete behavior is implemented
- in-memory environment list/get/head/create/update/delete behavior is implemented, including `_default`
- environment `PUT` now supports Chef-style full replacement and rename semantics
- `/environments/{name}/nodes` is live and filters the current node compatibility state
- `/environments/{name}/cookbooks`, `/environments/{name}/cookbooks/{cookbook}`, and `/environments/{name}/recipes` are now live and apply Chef-style environment cookbook constraints
- in-memory role list/get/head/create/update/delete behavior is implemented
- `/roles/{name}/environments` and `/roles/{name}/environments/{environment}` are live, including `_default` run-list resolution
- in-memory data bag list/get/create/delete behavior is implemented for both default-org and explicit-org routes
- in-memory data bag item get/create/update/delete behavior is implemented with Chef-style response shapes and error messages
- default-org and explicit-org client read/create/delete routes are now available too
- default-org and explicit-org node routes are both available for the first object slice
- default-org and explicit-org environment routes are now available too
- default-org and explicit-org role routes are now available too
- default-org and explicit-org data bag routes are now available too
- creator-aware node ACLs now allow clients to manage their own node objects
- the rest of the object surface still needs follow-on slices
- PostgreSQL-backed persistence for object APIs is still pending

## Milestone 5: Search Compatibility

Status: in progress

- in-memory compatibility search is live for `client`, `environment`, `node`, `role`, and per-data-bag indexes
- `/search` and `/organizations/{org}/search` now advertise the currently implemented built-in indexes plus live data bag indexes
- GET search and POST partial search now support client, environment, node, role, and data bag queries
- search results are filtered through current read authz before pagination is applied
- node partial search now reflects merged attribute precedence for search-facing behavior
- default-org client search results now point at live `/clients/...` routes instead of org-only URLs
- data bag search now mirrors Chef-style wrapper results and raw-item partial search behavior
- simple `AND`/`NOT` matching and escaped-slash prefix handling are now covered for the in-memory compatibility layer
- policyfile routes are now live for both default-org and explicit-org `/policies` and `/policy_groups`
- in-memory policy revision storage, revision lookup, policy-group listing, policy-group assignment, and richer canonical payload round-tripping are implemented
- policy payload validation now covers more cookbook-lock and solution-dependency structure, while node policy refs remain compatibility-safe searchable fields instead of enforced foreign keys
- OpenSearch-backed indexing, deeper query translation, provider capability handling, and reindex tooling are still pending

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
- production S3-compatible blob storage, PostgreSQL-backed cookbook persistence, and broader cookbook pedant coverage beyond the current environment-filtered/named-filter/latest/version read-write contract are still pending
- S3-compatible blob storage remains the target production mode after the compatibility contract settles

## Milestone 7: Operations and Migration

- add health, metrics, repair, backup, and reindex commands
- define migration path from existing Chef Infra Server installs
- rehearse shadow traffic and cutover workflows
