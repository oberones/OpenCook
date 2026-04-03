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
- the first default-org policyfile slice is now live for `/policies` and `/policy_groups`
- in-memory policy revision storage, revision lookup, policy-group listing, and policy-group assignment flows are implemented
- OpenSearch-backed indexing, deeper query translation, richer policyfile validation, provider capability handling, and reindex tooling are still pending

## Milestone 6: Cookbook and Blob Workflows

- implement sandboxes and checksum lifecycle
- integrate S3-compatible blob storage
- support cookbook artifacts and universe endpoints

## Milestone 7: Operations and Migration

- add health, metrics, repair, backup, and reindex commands
- define migration path from existing Chef Infra Server installs
- rehearse shadow traffic and cutover workflows
