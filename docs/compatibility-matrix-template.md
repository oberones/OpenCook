# Compatibility Matrix Template

Use this document as the working checklist for each Chef Infra Server behavior area.

| Surface | Upstream source | Pedant coverage | OpenCook package | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Request signing | `oc_chef_wm_base`, Bookshelf SigV4 flow | Partial | `internal/authn` | In progress | Chef request verification is live on initial routes; canonical fixture depth still needs work |
| Keys and API versions | `API_VERSIONING.md`, pedant versioned behaviors | Partial | `internal/authn`, `internal/api` | In progress | Actor `/keys` list/create/update/delete flows and expiration-aware auth are live; deeper v0/v1 semantics still need a dedicated pass |
| Organizations and ACLs | Bifrost, org bootstrap specs | Partial | `internal/authz` | In progress | In-memory bootstrap and default ACL generation are live; persistence and wider membership flows remain |
| Core Chef objects | `oc_erchef`, pedant object specs | Partial | `internal/api`, `internal/bootstrap`, `internal/store/pg` | In progress | Nodes and environments now have in-memory read/write slices, including `_default` environments and `/environments/{name}/nodes`; wider object coverage and PostgreSQL persistence still need follow-on work |
| Search | `SEARCH_AND_INDEXING.md`, search specs | Pending | `internal/search` | Scaffolded | Preserve expansion format |
| Sandboxes and cookbooks | Bookshelf, cookbook specs | Pending | `internal/blob`, `internal/api` | Scaffolded | S3-compatible mode is important |
| Operations | status and ctl flows | Pending | `internal/api`, future admin package | Scaffolded | Add reindex and repair later |

OpenCook policy note:

- licensing, license enforcement, and license telemetry are intentionally excluded and should not be added as compatibility targets
