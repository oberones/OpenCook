# Compatibility Matrix Template

Use this document as the working checklist for each Chef Infra Server behavior area.

| Surface | Upstream source | Pedant coverage | OpenCook package | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Request signing | `oc_chef_wm_base`, Bookshelf SigV4 flow | Partial | `internal/authn` | In progress | Chef request verification is live on initial routes; canonical fixture depth still needs work |
| Keys and API versions | `API_VERSIONING.md`, pedant versioned behaviors | Partial | `internal/authn`, `internal/api` | In progress | Actor `/keys` list/create/update/delete flows and expiration-aware auth are live for users and clients, including default-org client key aliases; deeper v0/v1 semantics still need a dedicated pass |
| Organizations and ACLs | Bifrost, org bootstrap specs | Partial | `internal/authz` | In progress | In-memory bootstrap and default ACL generation are live; persistence and wider membership flows remain |
| Core Chef objects | `oc_erchef`, pedant object specs | Partial | `internal/api`, `internal/bootstrap`, `internal/store/pg` | In progress | Nodes, environments, roles, and data bags now have in-memory read/write slices, and clients now have default-org plus org-scoped read/create/delete routes; deeper run-list/object edge cases and PostgreSQL persistence still need follow-on work |
| Search | `SEARCH_AND_INDEXING.md`, search specs | Partial | `internal/search`, `internal/api` | In progress | In-memory compatibility search is live for clients, environments, nodes, roles, and per-data-bag indexes, including partial search, ACL filtering, live default-org client URLs, and Chef-style data bag search result shaping; OpenSearch-backed indexing and deeper query translation still need follow-on work |
| Policies and policy groups | policyfile specs, pedant policy API | Partial | `internal/api`, `internal/bootstrap` | In progress | Default-org and explicit-org `/policies` and `/policy_groups` now support revision create/get/delete, list endpoints, policy-group assignment flows, and richer canonical payload round-tripping in memory; PostgreSQL persistence and the remaining policyfile edge cases still need follow-on work |
| Sandboxes and cookbooks | Bookshelf, cookbook specs | Partial | `internal/blob`, `internal/api`, `internal/bootstrap` | In progress | Sandbox create/commit flows, signed checksum upload/downloads, cookbook artifact create/read/delete, cookbook read views, and universe responses are live against the in-memory compatibility store; deeper cookbook mutation parity and production S3-compatible behavior still need follow-on work |
| Operations | status and ctl flows | Pending | `internal/api`, future admin package | Scaffolded | Add reindex and repair later |

OpenCook policy note:

- licensing, license enforcement, and license telemetry are intentionally excluded and should not be added as compatibility targets
- node `policy_name` and `policy_group` should remain compatibility fields, not new referential-integrity constraints, unless upstream behavior proves otherwise
