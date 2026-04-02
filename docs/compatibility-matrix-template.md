# Compatibility Matrix Template

Use this document as the working checklist for each Chef Infra Server behavior area.

| Surface | Upstream source | Pedant coverage | OpenCook package | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Request signing | `oc_chef_wm_base`, Bookshelf SigV4 flow | Pending | `internal/authn` | Scaffolded | Canonical request fixtures still needed |
| Keys and API versions | `API_VERSIONING.md`, pedant versioned behaviors | Pending | `internal/authn`, `internal/api` | Scaffolded | Preserve v0 and v1 semantics |
| Organizations and ACLs | Bifrost, org bootstrap specs | Pending | `internal/authz` | Scaffolded | Default ACL generation required |
| Core Chef objects | `oc_erchef`, pedant object specs | Pending | `internal/api`, `internal/store/pg` | Scaffolded | Start with nodes |
| Search | `SEARCH_AND_INDEXING.md`, search specs | Pending | `internal/search` | Scaffolded | Preserve expansion format |
| Sandboxes and cookbooks | Bookshelf, cookbook specs | Pending | `internal/blob`, `internal/api` | Scaffolded | S3-compatible mode is important |
| Operations | status and ctl flows | Pending | `internal/api`, future admin package | Scaffolded | Add reindex and repair later |

OpenCook policy note:

- licensing, license enforcement, and license telemetry are intentionally excluded and should not be added as compatibility targets

