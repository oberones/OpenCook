# API-Version-Specific Object Semantics Plan

Status: complete

## Summary

This bucket hardens Chef Server API-version behavior across the object surfaces OpenCook already implements. The goal is to make `X-Ops-Server-API-Version` negotiation and version-sensitive payload semantics predictable for Chef/Cinc clients, `knife`, and `chef-server-ctl` without reopening storage, search, blob, or licensing decisions that are already settled.

The slice should pin:

- global server API version validation and discovery
- v0/v1 actor and key behavior for users and clients
- v0/v2 cookbook and cookbook-artifact payload shapes
- object payload exactness for nodes, roles, environments, data bags, policies, sandboxes, and related search-facing fields
- default-org and explicit-org parity
- in-memory and active PostgreSQL-backed parity
- invalid-write no-mutation behavior

Use this file as the completed reference record for the deeper API-version-specific object semantics bucket.

## Current State

OpenCook already has:

- signed request verification that includes `X-Ops-Server-API-Version` in the canonical string
- default signed request behavior for the admin CLI and functional tests
- PostgreSQL-backed bootstrap core persistence for users, organizations, clients, actor keys, groups, containers, and ACLs
- PostgreSQL-backed core object persistence for nodes, environments, roles, data bags/items, policies, sandboxes, and object ACLs
- active PostgreSQL-backed cookbook metadata plus provider-backed cookbook/sandbox blobs
- active OpenSearch-backed search with PostgreSQL rebuild, mutation hooks, ACL filtering, and repair tooling
- route-level cookbook and cookbook-artifact v0/v2 coverage for legacy file segments versus `all_files`
- validator bootstrap registration and default-org client compatibility coverage

This bucket closes the systematic compatibility pass over API-version negotiation, version-specific fields, omitted/defaulted fields, response exactness, and error precedence across the implemented object surfaces. Remaining follow-on work should avoid reopening these Chef-facing contracts unless upstream pedant evidence requires a targeted correction.

## Upstream Compatibility Signals

Primary local upstream references:

- `~/Projects/coding/ruby/chef-server/dev-docs/API_VERSIONING.md`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/server_api_version_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/versioned_behaviors/server_api_v1_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/auth_headers_util.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/client_util.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/cookbook_util.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/cookbooks/*`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/cookbook_artifacts/*`

Important signals already identified:

- API v0 allows user public key management through `/users`, while API v1 requires actor keys to be managed through `/keys`.
- API v1 actor/client create responses use `chef_key` for generated or supplied default keys, while legacy v0 client create responses expose `private_key` and `public_key` at the top level.
- API v1 actor/client reads should not include top-level `public_key`.
- API v1 actor/client create accepts `create_key: true` or an explicit `public_key`, rejects conflicting `create_key: true` plus `public_key`, and does not create a default key when neither is provided.
- API v1 actor/client update rejects `create_key`, `private_key`, and direct `public_key` updates.
- API v2 cookbook and cookbook-artifact payloads use `all_files`; API versions below v2 use legacy file collections such as `recipes`, `files`, `templates`, `attributes`, `definitions`, `libraries`, `providers`, `resources`, and `root_files`.
- Pedant validates `/server_api_version` discovery and rejects invalid, too-low, or too-high `X-Ops-Server-API-Version` values before most route or body validation.
- Pedant uses licensing routes for some version-validation probes, but OpenCook intentionally does not implement licensing endpoints. This bucket must prove the same version-validation contract on implemented Chef-compatible routes without adding licensing routes.

## Task 1 Inventory Results

Status:

- Completed. The first inventory pass confirms that OpenCook has several version-sensitive behaviors implemented locally, but they are not yet governed by one shared server API-version negotiation layer.
- Completed. `/server_api_version` is present in the compatibility registry as part of the `api-versioning-and-ops` surface, but it is not yet registered as a live API route in `internal/api/router.go`.
- Completed. Signed authentication already includes `X-Ops-Server-API-Version` in the canonical string and defaults a missing value before verification, but supported-version range validation is not yet centralized.
- Completed. Cookbook and cookbook-artifact v0/v2 file-shape behavior is the strongest already-pinned area; the actor/client/key v0/v1 contract and global invalid-version precedence remain the highest-risk gaps.
- Completed. The licensing-free boundary is explicit: upstream pedant uses `/license` for some generic version-validation probes, but OpenCook should pin the same negotiation behavior on implemented non-licensing routes.

### Version-Surface Matrix

| Surface | Upstream version signal | Local inventory | Follow-on task |
| --- | --- | --- | --- |
| Global discovery and validation | `/server_api_version` returns `min_api_version` and `max_api_version`; invalid, too-low, and too-high `X-Ops-Server-API-Version` requests return the upstream invalid-version error before most route/body validation | Registered in `internal/compat/registry.go`, but no live `mux.HandleFunc("/server_api_version", ...)` route was found. Missing API-version headers are accepted through authn defaults. Invalid-version range checks are not centralized. | Task 2 |
| Signed request verification | Pedant signs `X-Ops-Server-API-Version` as part of the canonical request string, and missing API version is accepted | `internal/authn` verifies the header in the canonical string, `internal/admin` signs it, and functional tests send it. Mismatched signed-versus-forwarded version cases still need explicit route-level coverage. | Task 2 |
| Users and user keys | API v0 allows public-key management through `/users`; API v1+ makes keys the source of truth and rejects direct `/users` key mutation fields | Version-gated in Task 3. API v0 user reads expose the default `public_key`; API v1+ user creates only create default keys when `create_key` or `public_key` asks for one, reads omit top-level `public_key`, and direct key mutation fields on user update are rejected. User key CRUD and PostgreSQL verifier-cache rehydration remain covered by existing key and active-Postgres route tests. | Task 3 |
| Clients and client keys | API v0 client create returns top-level key fields; API v1+ create uses nested `chef_key`; v1+ actor reads and updates need stricter key-field semantics | Version-gated in Task 3. API v0 client create/read preserves top-level key fields; API v1+ client create uses nested `chef_key` only for generated or explicit default keys, omitted key fields do not create a default key, reads omit top-level `public_key`, and direct key mutation fields on client update are rejected for both default-org and explicit-org aliases. Active-Postgres restart coverage proves persisted client keys rehydrate into the verifier cache. | Task 3 |
| Organizations and validator keys | API v1 org create confirms generated validator client/key behavior and default key accessibility | Version-gated in Task 4. Organization create responses are pinned across v0, v1, and v2 with the upstream legacy `clientname` plus top-level `private_key` validator shape, not the client-create `chef_key` shape. Active-Postgres restart coverage proves validator client/key metadata, signed validator authentication, default groups, containers, ACLs, and `_default` environment bootstrap rehydrate intact. Licensing endpoints remain intentionally excluded. | Task 4 |
| Nodes | No narrow `API_VERSIONING.md` signal found, but global version validation applies and node payload fields feed search | Version-gated in Task 5. Node collection, get, head, create, update, and delete response shapes are pinned across v0, v1, and v2, including rich payload fields, minimal/defaulted field behavior, default-org and explicit-org aliases, active-PostgreSQL restart/rehydration, invalid-version no-mutation guarantees, and OpenSearch node-field projection after versioned writes. | Task 5 |
| Roles | No narrow `API_VERSIONING.md` signal found, but global version validation applies and role payload fields feed depsolver/search behavior | Version-gated in Task 6. Role collection, get, head, create, update, delete, omitted/defaulted fields, run-list and env-run-list normalization, environment-linked role reads, active-PostgreSQL restart/rehydration, invalid-version no-mutation, authz no-mutation, and OpenSearch role-field projection are pinned across v0, v1, and v2. | Task 6 |
| Environments | No narrow `API_VERSIONING.md` signal found, but environment fields affect cookbook/depsolver routes and search | Version-gated in Task 6. Environment collection, get, head, create, update, rename, delete, omitted/defaulted fields, `_default` immutability, cookbook constraints, attribute maps, environment-linked node/cookbook/recipe/depsolver aliases, active-PostgreSQL restart/rehydration, invalid-version no-mutation, authz no-mutation, and OpenSearch environment-field projection are pinned across v0, v1, and v2. | Task 6 |
| Data bags and items | Pedant treats data bag items as arbitrary JSON plus Chef-shaped wrappers; global version validation applies | Version-gated in Task 7. Data bag collection, item-list, create, delete, item create/read/update/delete, wrapper-vs-raw item payloads, encrypted-looking opaque payload preservation, defaulted path-derived item IDs, memory search, OpenSearch search, ACL filtering, parent-data-bag authorization, active-PostgreSQL restart/rehydration, invalid-version no-mutation, and authz no-mutation are pinned across v0, v1, and v2. | Task 7 |
| Cookbooks and cookbook artifacts | API v2 uses `all_files`; API versions below v2 use legacy file collections. Pedant runs create/read/update/delete specs for v0 and v2. | Version-gated in Task 8. Cookbook collection, named cookbook, version, `_latest`, `_recipes`, `/universe`, cookbook-artifact collection/name/identifier, create/update/delete response exactness, signed download URL shape, default-org and explicit-org aliases, active PostgreSQL plus filesystem-blob restart/rehydration, invalid-version no-mutation, and provider-unavailable degradation are pinned around v0 legacy segments versus v2 `all_files`. Existing checksum existence, cleanup, shared-retention, and sandbox-held-retention tests remain part of the frozen contract. | Task 8 |
| Policies and policy groups | No narrow `API_VERSIONING.md` signal found, but policy payload canonicalization and node policy refs affect compatibility | Version-gated in Task 9. Policy revision create/get/delete, policy collection/named reads, policy-group collection/named reads, policy assignment create/get/delete, canonical `named_run_lists`, cookbook locks, `solution_dependencies`, default-org and explicit-org aliases, invalid-version no-mutation, and active-PostgreSQL restart/rehydration are pinned across v0, v1, and v2. | Task 9 |
| Sandboxes and checksums | No narrow `API_VERSIONING.md` signal found, but global version validation applies and upload URL shape is Chef-facing | Version-gated in Task 9. Sandbox create and commit response exactness, absolute checksum upload URL query shape, default-org and explicit-org aliases, invalid-version precedence, committed-checksum reuse, active-PostgreSQL restart/rehydration, and no change to unsigned blob upload/download routes are pinned across v0, v1, and v2. | Task 9 |
| Search-facing object fields | Search rows should reflect persisted object state while unsupported object-family indexes stay unsupported | Version-gated in Task 9. OpenSearch-backed coverage proves versioned node writes keep `policy_name` and `policy_group` searchable, while policy and sandbox mutations remain absent from provider documents, unsupported public search indexes, and `/search` index listings. | Task 9 |
| Licensing routes | Upstream pedant uses `/license` for some version-validation probes | Intentionally excluded by ADR and product stance. This bucket must not add licensing endpoints; use implemented Chef-compatible routes for equivalent validation coverage. | Task 2 |

## Task 1 Decision

The next strongest implementation slice is Task 2: centralize server API-version negotiation before changing per-object payload behavior. That gives every later object task one shared contract for `/server_api_version`, invalid-version error shape, supported range checks, and validation precedence.

Per-object tasks should not add ad hoc API-version parsing unless Task 2 leaves a deliberate extension point. Cookbook routes currently parse version headers locally for v0/v2 file shape; Task 2 should either preserve that behavior through a shared helper or wrap it so malformed/unsupported versions get the global Chef-style response before cookbook-specific handling.

## Interfaces And Behavior

- Preserve Chef-facing route shapes for all implemented routes unless upstream inventory proves OpenCook is missing a non-licensing Chef compatibility route.
- Do not add licensing, license-management, license-enforcement, or license-telemetry endpoints.
- Preserve signed request verification and keep `X-Ops-Server-API-Version` in the canonical signed string.
- Preserve default-org and explicit-org alias behavior.
- Preserve in-memory fallback behavior when PostgreSQL is not configured.
- Preserve PostgreSQL as the source of truth when configured, with existing repository/cache activation behavior.
- Preserve OpenSearch as a derived search projection, not an object source of truth.
- Preserve provider-backed blob behavior and signed blob URL shapes.
- Preserve `/status`, `/readyz`, and root payload keys. Human-readable text may be corrected only if a task needs truthful API-version status wording.
- Do not broaden this bucket into new storage abstractions, cross-process cache invalidation, provider capability negotiation, or migration/cutover tooling.

## Version Contract To Freeze

This bucket should freeze the following cross-cutting contract before and during implementation:

- Missing `X-Ops-Server-API-Version` behavior.
- Valid supported versions, including at least v0, v1, and v2 where upstream behavior exists.
- Invalid, non-numeric, too-low, and too-high version error status and body.
- Version-validation precedence relative to method validation, org lookup, authz, request JSON parsing, and object validation.
- `/server_api_version` discovery payload shape, excluding licensing-specific surfaces.
- Header behavior for signed requests, including mismatched signed-versus-forwarded version values.
- Version-specific request fields that are accepted, rejected, ignored, defaulted, or echoed.
- Version-specific response fields that are present, omitted, renamed, or nested.
- No-mutation behavior when a version-specific request is rejected.
- Restart/rehydration behavior for version-sensitive persisted fields.
- Search-facing document fields that are derived from version-sensitive object payloads.

## Task Breakdown

### Task 1: Inventory The API-Version Contract

Status:

- Completed. The version-surface matrix above covers global version discovery/validation, users, user keys, clients, client keys, organizations, validator keys, nodes, roles, environments, data bags/items, cookbooks/artifacts, policies, policy groups, sandboxes, checksum upload flows, search-facing fields, and the licensing-free exclusion.
- Completed. The inventory checked upstream pedant specs, `API_VERSIONING.md`, and local OpenCook route/test seams before changing behavior.
- Completed. The matrix records which behaviors are confirmed upstream, already implemented, missing, intentionally excluded, or still under-specified.
- Completed. `/server_api_version` is currently compatibility-registered but not live-routed; Task 2 should implement or tighten that discovery route with an OpenCook-compatible `min_api_version`/`max_api_version` payload.

### Task 2: Centralize Server API Version Negotiation

Status:

- Completed. Added a shared server API-version parser/validator for implemented Chef routes with Chef-style `406 invalid-x-ops-server-api-version` responses for non-numeric, too-low, and too-high requested versions.
- Completed. Added live `/server_api_version` and `/server_api_version/` discovery routes returning `min_api_version: 0` and `max_api_version: 2` without adding licensing endpoints.
- Completed. Route tests now pin invalid-version precedence before method validation, body validation, object lookup, and authorization on implemented non-licensing routes.
- Completed. Route tests now cover missing, v0, v1, and v2 headers reaching authenticated routes successfully.
- Completed. Route tests now prove supported but mismatched `X-Ops-Server-API-Version` values still participate in v1.3 signature verification and fail authentication rather than bypassing the signed-header contract.

- Add or tighten a shared API-version parser/validator for implemented Chef routes.
- Add route-test helpers for signed requests with missing, v0, v1, v2, invalid, too-low, and too-high `X-Ops-Server-API-Version` values.
- Pin invalid-version responses before method validation, body validation, object lookup, and authorization on representative implemented routes.
- Preserve request-signature compatibility by proving the signed version header is the value verified by authn.
- Add `/server_api_version` route coverage if the route is absent or under-specified.
- Avoid adding licensing endpoints; use existing Chef-compatible routes for validation precedence.

### Task 3: Pin Actor, Client, And Key Version Semantics

Status:

- Completed. API v0 user and client reads preserve legacy top-level default `public_key` exposure, while API v1+ actor reads omit top-level `public_key`.
- Completed. API v1+ user and client creates now distinguish `create_key: true`, explicit `public_key`, `create_key: false` plus `public_key`, conflicting `create_key: true` plus `public_key`, omitted key fields, and invalid `private_key: true`.
- Completed. API v1+ omitted-key actor creates no longer create an implicit default key; API v0 actor creates retain legacy implicit default-key generation.
- Completed. API v1+ actor updates reject direct `public_key`, `create_key`, and `private_key` mutation fields so `/keys` remains the key source of truth.
- Completed. Route coverage pins user key and client key list/read/authentication behavior around the version gate, including generated material, explicit public keys, default key names, and empty key lists for v1+ omitted-key actor creates.
- Completed. Active PostgreSQL restart coverage proves an API v1+ generated client key rehydrates into `authn.MemoryKeyStore` and authenticates a signed request after app reconstruction.
- Completed. Client coverage includes both `/clients` default-org aliases and `/organizations/{org}/clients` explicit-org routes.

- Pin API v0 user public-key behavior through `/users` without breaking the API v1+ key-source-of-truth model.
- Pin API v1+ user and client create behavior for:
  - `create_key: true`
  - explicit `public_key`
  - `create_key: false` plus `public_key`
  - conflicting `create_key: true` plus `public_key`
  - omitted key fields
  - invalid `private_key` generation requests
- Pin API v1+ actor reads omitting top-level `public_key`.
- Pin API v1+ actor updates rejecting direct key mutation fields.
- Cover user keys and client keys for list/get/create/update/rename/delete, expiration, generated material, explicit public keys, and default key names.
- Prove `authn.MemoryKeyStore` stays synchronized and PostgreSQL restart/rehydration preserves signed authentication with persisted keys.
- Cover default-org and explicit-org client aliases.

### Task 4: Pin Organization And Validator Response Semantics

Status:

- Completed. Organization create response tests now pin v0, v1, and v2 behavior for generated validator `clientname`, top-level `private_key`, stable `uri`, and absence of client-create `chef_key` nesting.
- Completed. Validator default key accessibility and metadata are pinned after organization creation, including `default` name, `infinity` expiration, and a PEM public key.
- Completed. Active PostgreSQL restart coverage proves the generated validator key rehydrates into `authn.MemoryKeyStore` and can authenticate a signed request after app reconstruction.
- Completed. Active PostgreSQL restart coverage also proves validator client metadata, key list metadata, default containers, default groups, organization/client ACL defaults, and the `_default` environment survive rehydration.
- Completed. Invalid `X-Ops-Server-API-Version` on `POST /organizations` now has explicit coverage proving global version rejection wins before malformed organization payload validation.
- Completed. Upstream organization creation still returns validator key material through legacy top-level `private_key`; this task preserves that Chef contract and keeps licensing-specific upstream behavior intentionally out of scope.

- Pin organization create responses for API-version-sensitive validator client/key payload fields.
- Prove default validator client and key metadata persist across PostgreSQL restart/rehydration.
- Preserve default containers, default groups, default ACLs, and `_default` environment bootstrap behavior.
- Cover invalid-version precedence before organization payload validation.
- Preserve licensing-free behavior by documenting any upstream organization/license coupling as intentionally excluded.

### Task 5: Pin Node Payload Semantics

Status:

- Completed. Node collection, get, head, create, update, and delete response shapes are now pinned across supported API versions v0, v1, and v2.
- Completed. Rich node payload coverage now includes `name`, `chef_type`, `json_class`, `chef_environment`, `run_list`, attribute maps, `policy_name`, and `policy_group`.
- Completed. Minimal node coverage pins omitted/defaulted fields on the explicit-org alias, including empty attribute maps, omitted policy fields, and the current default `run_list: null` read behavior.
- Completed. Default-org and explicit-org aliases are covered for memory-backed behavior and active PostgreSQL-backed restart/rehydration, including update and delete after restart.
- Completed. Invalid version-specific node create, update, and delete attempts now prove no mutation of live state, persisted PostgreSQL-backed state, or OpenSearch-visible node documents.
- Completed. OpenSearch-backed search coverage now proves version-sensitive node writes project the expected policy, run-list, and attribute fields into full and partial search responses.

- Pin node list/get/head/create/update/delete response shape across supported API versions.
- Cover `name`, `chef_type`, `json_class`, `run_list`, attribute maps, `policy_name`, `policy_group`, and omitted/defaulted fields.
- Prove invalid version-specific writes do not mutate live state, persisted PostgreSQL state, or search-visible node documents.
- Cover default-org and explicit-org aliases.
- Cover memory-backed and active PostgreSQL-backed behavior, including restart/rehydration.
- Verify OpenSearch-backed search still sees the correct node fields after version-sensitive writes.

### Task 6: Pin Role And Environment Payload Semantics

Status:

- Completed. Role collection, get, head, create, update, and delete response shapes are now pinned across supported API versions v0, v1, and v2.
- Completed. Role payload coverage now includes `name`, `description`, `chef_type`, `json_class`, default and override attributes, normalized/deduplicated `run_list`, normalized/deduplicated `env_run_lists`, omitted/defaulted field behavior, and the current minimal-role `run_list: null` read behavior.
- Completed. Environment collection, get, head, create, update, rename-capable `PUT`, and delete response shapes are now pinned across supported API versions v0, v1, and v2.
- Completed. Environment payload coverage now includes `_default` immutability, `name`, `description`, `chef_type`, `json_class`, cookbook version constraints, default and override attributes, omitted/defaulted field behavior, and rename `Location` shaping.
- Completed. Environment-linked node, cookbook, recipe, depsolver, and role-read aliases now have version-header coverage where the header affects successful response shaping or invalid-version precedence.
- Completed. Active PostgreSQL restart coverage proves roles, environments, and environment-linked role reads rehydrate, update, delete, and remain absent after deletion.
- Completed. OpenSearch-backed coverage proves version-sensitive role and environment writes project expected fields and that invalid-version or authz-rejected writes do not mutate live state, persisted state, or search-visible documents.

- Pin role list/get/head/create/update/delete response shape across supported API versions.
- Cover role `run_list` and `env_run_lists` normalization, deduplication, omitted/defaulted fields, and environment-linked role reads.
- Pin environment list/get/head/create/update/delete response shape across supported API versions.
- Cover `_default` environment immutability, cookbook version constraints, default/override attributes, rename-capable `PUT`, and environment-linked cookbook/node/recipe/depsolver routes where the version header affects payload or error behavior.
- Prove invalid writes and unauthorized writes do not mutate live state, persisted state, or search-visible documents.
- Cover default-org and explicit-org aliases plus restart/rehydration.

### Task 7: Pin Data Bag And Data Bag Item Payload Semantics

Status:

- Completed. Data bag collection, data bag item-list, create, delete, and item create/read/update/delete response shapes are now pinned across supported API versions v0, v1, and v2.
- Completed. Data bag item coverage now freezes the Chef compatibility distinction between create/update wrapper responses, raw item reads, and delete envelopes with `raw_data`.
- Completed. Opaque item payload coverage now includes `id`, route-added `chef_type` and `data_bag`, nested arbitrary JSON, defaulted path-derived item IDs on update, omitted metadata, and encrypted-looking payloads.
- Completed. Default-org and explicit-org aliases are covered for data bag and item lifecycle routes.
- Completed. Memory-search and OpenSearch-backed search coverage proves partial-search fields and encrypted-looking payload fields remain searchable without decrypting or reshaping raw item JSON.
- Completed. Rejected invalid-version writes and parent-data-bag authz failures now prove no mutation of live state, PostgreSQL-backed persisted state, memory-search rows, or OpenSearch documents.
- Completed. Active PostgreSQL restart coverage proves data bags and data bag items rehydrate, update, delete, and remain absent after deletion.
- Completed. ACL coverage now pins both normal parent-data-bag authorization rejection and post-query search result filtering for matched encrypted-looking documents.

- Pin data bag list/create/delete and data bag item get/create/update/delete response shapes across supported API versions.
- Preserve data bag item opacity, including encrypted-looking payloads from the encrypted data bag bucket.
- Cover `id`, `chef_type`, `data_bag`, wrapper/raw response differences, partial-search fields, and defaulted or omitted metadata.
- Prove invalid version-specific writes do not mutate live state, persisted PostgreSQL state, memory search, or OpenSearch documents.
- Cover default-org and explicit-org aliases, restart/rehydration, ACL filtering, and parent-data-bag authorization.

### Task 8: Consolidate Cookbook And Cookbook-Artifact Version Exactness

Status:

- Completed. The cookbook/cookbook-artifact inventory confirmed strong existing local coverage for checksum existence gating, provider-backed failures, cleanup, shared-checksum retention, sandbox-held retention, and baseline v0/v2 file-shape conversion.
- Completed. Added a compact API-version matrix covering cookbook collection reads, named cookbook reads, cookbook version reads, `_latest`, `_recipes`, `/universe`, cookbook-artifact collection/name/identifier reads, create/update/delete response exactness, and signed file download URL shape.
- Completed. Added default-org and explicit-org alias coverage for cookbook and cookbook-artifact v0/v2 reads, including signed blob URL query shape and usable downloads.
- Completed. Added active PostgreSQL plus filesystem-backed blob restart/rehydration coverage proving legacy file segments convert to v2 `all_files`, v2 `all_files` converts back to legacy segments, and update/delete decisions remain correct after restart.
- Completed. Added invalid-version and provider-unavailable no-mutation coverage for cookbook creates, updates, deletes, cookbook-artifact creates, and signed cookbook file downloads while preserving the current `blob_unavailable` error shape.
- Completed. Preserved existing checksum cleanup and retention behavior; this task tightened API-version exactness around that subsystem rather than redesigning cookbook storage or blob cleanup.

- Inventory existing cookbook and cookbook-artifact v0/v2 coverage before adding tests.
- Fill gaps for:
  - cookbook collection reads
  - named cookbook reads
  - cookbook version reads
  - `_latest`
  - `_recipes`
  - `/universe`
  - cookbook artifact collection/name/identifier reads
  - create/update/delete response exactness
  - signed file download URL shapes
  - provider-backed blob unavailable behavior
- Pin conversion between legacy file segments and v2 `all_files` on create, update, get, delete, and restart/rehydration paths.
- Preserve checksum existence gating, cleanup, shared checksum retention, and sandbox-held checksum retention.
- Cover default-org and explicit-org aliases.

### Task 9: Pin Policy, Sandbox, And Search-Facing Semantics

Status:

- Completed. Added v0, v1, and v2 route coverage for policy revision create/read/delete, policy collection and named reads, policy-group collection and named reads, policy assignment create/read/delete, and default-org plus explicit-org alias URI shaping.
- Completed. Canonical policy payload coverage now pins `named_run_lists`, nested cookbook-lock metadata, `solution_dependencies`, assignment responses without `policy_group_list`, and revision reads with current `policy_group_list` membership.
- Completed. Added v0, v1, and v2 sandbox create/commit coverage, including absolute checksum upload URL path/query shape, upload/commit success, committed-checksum reuse, and default-org plus explicit-org aliases.
- Completed. Added active PostgreSQL restart coverage proving versioned policy and sandbox state rehydrate before revision reads, group reads, sandbox commit, and committed-checksum reuse.
- Completed. Added invalid-version no-mutation coverage for policy assignment updates and sandbox commits, plus invalid-version precedence before unsupported search-index handling.
- Completed. Added OpenSearch-backed coverage proving versioned node policy refs remain searchable while policy and sandbox mutations do not create provider documents, do not enqueue provider mutations, and remain absent from unsupported public search indexes.

- Pin policy revision, policy-group, and policy assignment response shapes across supported API versions.
- Preserve canonical payload round-tripping for `named_run_lists`, cookbook locks, `solution_dependencies`, and node policy refs.
- Pin sandbox create/commit response shapes, checksum upload URL shapes, and version-validation precedence without changing blob routes.
- Prove policy and sandbox state remains absent from unsupported public search indexes while node policy fields remain searchable.
- Cover memory-backed and active PostgreSQL-backed behavior, restart/rehydration, OpenSearch-backed derived fields, and invalid-write no-mutation behavior.

### Task 10: Functional Coverage, Documentation, And Bucket Closeout

Status:

- Completed. Docker functional coverage now exercises `/server_api_version` discovery and invalid-version rejection on both unsigned discovery and signed Chef routes.
- Completed. Functional coverage now proves v0/v1 actor key behavior for persisted users and clients after the PostgreSQL-backed startup path has rehydrated verifier keys.
- Completed. Functional coverage now proves v0 legacy cookbook file segments versus v2 `all_files` shape, including a usable signed cookbook file download through the configured filesystem blob backend.
- Completed. Functional coverage now sends v0, v1, and v2 headers across representative node, role, environment, data bag, policy, policy-group, sandbox create/commit, and OpenSearch node-field search flows.
- Completed. The functional checks run during the verify phase, so the Compose flow exercises persisted PostgreSQL state and OpenSearch startup rebuild behavior after the create phase.
- Completed. Roadmap, milestone, compatibility-matrix, agent-guidance, and this plan document now mark the API-version object semantics bucket complete and point the next bucket at OpenSearch provider capability/version hardening, with migration/cutover tooling still visible as the next operational follow-on.

- Extend Docker functional tests to exercise API-version headers against representative surfaces:
  - global version validation and discovery
  - v0/v1 actor key behavior
  - v0/v2 cookbook file shape behavior
  - nodes, roles, environments, data bags, policies, and sandboxes
  - PostgreSQL restart/rehydration
  - OpenSearch search-facing fields
- Update:
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `AGENTS.md`
  - this plan file
- Mark this bucket complete once all required scenarios are pinned.
- Point the next bucket at OpenSearch provider capability/version hardening or migration/cutover tooling unless deployment testing identifies a more urgent compatibility gap.

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/authn`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./test/functional`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

Required scenarios:

- missing, v0, v1, v2, invalid, too-low, and too-high API-version headers
- invalid-version precedence before method, body, lookup, and authorization failures
- signed request verification with API-version headers included in the canonical string
- API v0 versus v1 actor and key behavior
- API v0/v2 cookbook and cookbook-artifact file-shape behavior
- memory-backed and active PostgreSQL-backed parity
- restart/rehydration before reads, updates, deletes, and search rebuilds
- invalid-write and unauthorized-write no-mutation behavior
- default-org and explicit-org alias parity
- OpenSearch-backed search field parity for version-sensitive object writes
- provider-backed blob behavior unchanged by API-version handling

## Assumptions And Non-goals

- OpenCook should support the Chef API versions required by existing Chef/Cinc clients and tooling, with v0/v1/v2 behavior pinned where upstream exposes distinct semantics.
- Public key source-of-truth behavior should follow the upstream v1+ key-table model while preserving v0 compatibility where clients still rely on it.
- Cookbook v2 `all_files` behavior is already partially implemented; this bucket should inventory and fill gaps rather than rewrite that subsystem.
- Licensing endpoints remain intentionally out of scope even when upstream pedant uses them as generic API-version probes.
- OpenSearch provider capability/version negotiation is the next recommended operational bucket.
- Migration/cutover tooling remains a later operational bucket.
- This bucket should not introduce a new persistence abstraction or convert repository/cache reads to live SQL queries.

## Completion Definition

This bucket is complete. OpenCook now has a documented API-version compatibility matrix, route-level and persistence-backed coverage for each in-scope object family, functional coverage for representative versioned flows, and updated roadmap/milestone docs identifying OpenSearch provider capability/version hardening as the recommended next operational bucket.
