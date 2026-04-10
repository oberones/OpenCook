# OpenCook

OpenCook is a Go-based rewrite of Chef Infra Server with a compatibility-first architecture. The goal is to remain wire-compatible with Chef and Cinc clients, `knife`, and existing automation workflows while modernizing the server internals and operations model.

OpenCook is intended to be fully free and open source. Licensing, license enforcement, license telemetry, and related product mechanics are intentionally out of scope.

The project is currently in its compatibility-foundation phase. This repository now includes:

- a buildable Go application layout centered on `cmd/opencook`
- compatibility surface inventory wired into the HTTP server
- Chef request-signing verification with test coverage
- in-memory bootstrap state for users, organizations, clients, groups, containers, and ACLs
- initial authenticated endpoints for users, organizations, clients, ACL inspection, actor key lifecycle, nodes, environments, roles, data bags, and compatibility search
- policyfile compatibility routes for policies, revisions, and policy-group assignments on both default-org and explicit-org paths
- sandbox/blob compatibility with signed checksum uploads/downloads plus selectable in-memory, filesystem, and S3-compatible blob backends
- the cookbook compatibility slice with cookbook artifact lifecycle, writable cookbook versions, pedant-shaped write exactness, frozen/force update behavior, cookbook read views, environment depsolver routes, and universe responses
- docs for architecture decisions, milestones, and compatibility tracking
- a starting test layout for contract-driven development

## Current Layout

```text
.
|-- cmd/opencook
|-- configs
|-- docs
|   |-- adr
|   `-- chef-infra-server-rewrite-roadmap.md
|-- internal
|   |-- api
|   |-- app
|   |-- authn
|   |-- authz
|   |-- blob
|   |-- bootstrap
|   |-- compat
|   |-- config
|   |-- search
|   |-- store/pg
|   `-- version
`-- test/compat
```

## Planned Vertical Slices

1. Request signing and key management compatibility
2. Org, user, client, and ACL bootstrap flows
3. Remaining core object CRUD and deeper object parity
4. Search provider and indexing compatibility on OpenSearch
5. Cookbook, artifact, and universe compatibility on top of the sandbox/blob slice
6. Persistence and provider-backed compatibility on PostgreSQL, OpenSearch, and S3-compatible storage

## Local Development

The scaffold uses environment variables for configuration. See [configs/opencook.env.example](/Users/oberon/Projects/coding/go/OpenCook/configs/opencook.env.example).

To exercise the first authenticated HTTP endpoints locally, configure a bootstrap requestor with a public key:

```bash
export OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=silent-bob
export OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user
export OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/path/to/public.pem
export OPENCOOK_DEFAULT_ORGANIZATION=ponyville
export OPENCOOK_MAX_AUTH_BODY_BYTES=8388608
export OPENCOOK_MAX_BLOB_UPLOAD_BYTES=67108864
export OPENCOOK_BLOB_BACKEND=filesystem
export OPENCOOK_BLOB_STORAGE_URL=file:///tmp/opencook-objects
```

With that in place, signed requests can successfully hit:

- `/users`
- `/users/{name}`
- `/users/{name}/keys`
- `/users/{name}/keys/{key}`
- `/clients`
- `/clients/{name}`
- `/clients/{name}/keys`
- `/clients/{name}/keys/{key}`
- `/data`
- `/data/{bag}`
- `/data/{bag}/{item}`
- `/organizations`
- `/organizations/{org}`
- `/environments`
- `/environments/{name}`
- `/environments/{name}/cookbooks`
- `/environments/{name}/cookbooks/{cookbook}`
- `/environments/{name}/cookbook_versions`
- `/environments/{name}/nodes`
- `/environments/{name}/recipes`
- `/roles`
- `/roles/{name}`
- `/roles/{name}/environments`
- `/roles/{name}/environments/{environment}`
- `/sandboxes`
- `/sandboxes/{id}`
- `/cookbook_artifacts`
- `/cookbook_artifacts/{name}`
- `/cookbook_artifacts/{name}/{identifier}`
- `/cookbooks`
- `/cookbooks/_latest`
- `/cookbooks/_recipes`
- `/cookbooks/{name}`
- `/cookbooks/{name}/{version}`
- `/universe`
- `/search`
- `/search/{index}`
- `/organizations/{org}/environments`
- `/organizations/{org}/environments/{name}`
- `/organizations/{org}/environments/{name}/cookbooks`
- `/organizations/{org}/environments/{name}/cookbooks/{cookbook}`
- `/organizations/{org}/environments/{name}/cookbook_versions`
- `/organizations/{org}/environments/{name}/nodes`
- `/organizations/{org}/environments/{name}/recipes`
- `/nodes`
- `/nodes/{name}`
- `/policies`
- `/policies/{name}`
- `/policies/{name}/revisions`
- `/policies/{name}/revisions/{revision}`
- `/policy_groups`
- `/policy_groups/{group}`
- `/policy_groups/{group}/policies/{name}`
- `/organizations/{org}/policies`
- `/organizations/{org}/policies/{name}`
- `/organizations/{org}/policies/{name}/revisions`
- `/organizations/{org}/policies/{name}/revisions/{revision}`
- `/organizations/{org}/policy_groups`
- `/organizations/{org}/policy_groups/{group}`
- `/organizations/{org}/policy_groups/{group}/policies/{name}`
- `/organizations/{org}/data`
- `/organizations/{org}/data/{bag}`
- `/organizations/{org}/data/{bag}/{item}`
- `/organizations/{org}/search`
- `/organizations/{org}/search/{index}`
- `/organizations/{org}/roles`
- `/organizations/{org}/roles/{name}`
- `/organizations/{org}/roles/{name}/environments`
- `/organizations/{org}/roles/{name}/environments/{environment}`
- `/organizations/{org}/sandboxes`
- `/organizations/{org}/sandboxes/{id}`
- `/organizations/{org}/cookbook_artifacts`
- `/organizations/{org}/cookbook_artifacts/{name}`
- `/organizations/{org}/cookbook_artifacts/{name}/{identifier}`
- `/organizations/{org}/cookbooks`
- `/organizations/{org}/cookbooks/_latest`
- `/organizations/{org}/cookbooks/_recipes`
- `/organizations/{org}/cookbooks/{name}`
- `/organizations/{org}/cookbooks/{name}/{version}`
- `/organizations/{org}/universe`
- `/organizations/{org}/nodes`
- `/organizations/{org}/nodes/{name}`
- `/organizations/{org}/clients`
- `/organizations/{org}/clients/{name}`
- `/organizations/{org}/clients/{name}/keys`
- `/organizations/{org}/clients/{name}/keys/{key}`

The in-memory search compatibility layer currently exposes the built-in Chef indexes for `client`, `environment`, `node`, and `role`, plus per-data-bag indexes, with GET search and POST partial search support across those object types. Broader Lucene-style query parity and OpenSearch-backed indexing are still pending.

The current policyfile slice is live on both the default-org and explicit-org routes for `/policies` and `/policy_groups`. It now round-trips richer upstream-shaped policy payloads, validates more of the cookbook-lock and solution-dependency structure, and keeps node `policy_name` and `policy_group` behavior compatibility-safe as searchable fields rather than hard foreign keys.

The sandbox compatibility slice is live too. Signed callers can create and commit sandboxes through `/sandboxes` and `/organizations/{org}/sandboxes`, and the returned checksum entries expose absolute signed upload URLs under `/_blob/checksums/{checksum}`. Blob storage is now provider-selectable: the default in-memory compatibility store still works for tests, a real filesystem-backed adapter is wired for local dev/test persistence, and the S3-compatible adapter now supports real request-time `PUT`/`GET`/`HEAD`/`DELETE` blob operations for configured endpoints and static credentials while keeping the same OpenCook upload/download HTTP contract. That S3-compatible path now also has configurable request timeout and retry settings, and transient/provider-auth failures now flow through the existing `blob_unavailable` compatibility path instead of surfacing as generic internal blob errors.

The cookbook compatibility slice is also live. Signed callers can create, update, read, and delete cookbook versions through `/cookbooks` and `/organizations/{org}/cookbooks`, create/read/delete cookbook artifacts through `/cookbook_artifacts` and `/organizations/{org}/cookbook_artifacts`, resolve environment-scoped run lists through `/environments/{name}/cookbook_versions` and `/organizations/{org}/environments/{name}/cookbook_versions`, and fetch `/universe` metadata. Cookbook version responses now preserve Chef-style `json_class` and `cookbook_name` fields plus API-version-sensitive v0/v2 file shaping, including segment-aware `all_files[].name` values like `root_files/CHANGELOG`, frozen cookbook versions now reject mutation unless `?force=` is used and remain frozen even after forced updates, explicit `?force=false` now preserves the same Chef conflict behavior, and cookbook PUT responses now preserve pedant-shaped omission and presence rules for optional top-level fields and legacy/v2 file collections. Cookbook create and update HTTP coverage now also exercises omitted-default exactness, top-level `json_class`/`chef_type`/`version` validation, invalid request-key rejection, metadata-name write-vs-read canonicalization, permissive `metadata.providing` writes, exact validation/no-mutation behavior for invalid metadata payloads, and malformed route-path handling for invalid cookbook names and version strings, including negative and overflowing version components. Cookbook reads now return the narrower Chef-style metadata view with upstream defaults inflated on read, `/cookbooks/_recipes` now derives recipe names from the latest cookbook manifests with Chef-style default-recipe qualification, environment-scoped cookbook and recipe routes now honor environment cookbook constraints on both default-org and explicit-org paths, and the current depsolver slice now validates cookbook run lists with upstream-style item-shape exactness, including numeric names, reserved `recipe`/`role` cookbook names, `x.y` and `x.y.z` versioned recipe forms, and stricter rejection of one-part-version and malformed-colon items on both default-org and org-scoped routes, has empty-payload invalid-JSON, trailing-JSON, invalid-run-list, malformed-item, missing-environment, missing-organization, ambiguous-default-org, configured-default-org, method-not-allowed, extra-path-segment, and trailing-slash acceptance coverage on the depsolver routes including both named-environment and `_default` paths, with empty-payload invalid-JSON, trailing-JSON, invalid-run-list, and malformed-item parity now also pinned on the `_default` aliases, expands `role[...]` items server-side, honors environment constraints and version pins, uses environment-specific role run lists when present including explicit empty environment overrides on both default-org and org-scoped routes, returns `200 {}` for explicit empty run lists and requests that omit `run_list` on the default-org and org-scoped routes including the `_default` aliases, expands nested roles and recursive cookbook dependencies, backtracks across compatible sibling and multi-root dependency alternatives, preserves Chef-style pessimistic `~>` constraint behavior for both major/minor and major/minor/patch forms on both default-org and org-scoped HTTP routes, now has broader upstream solver-graph parity including combined environment-plus-dependency constraint ranges on both default-org and org-scoped HTTP routes, circular dependency handling on both default-org and org-scoped routes, and the current upstream first-, second-, third-, complex-dependency, conflicting-passing, conflicting-failing, and pinned-root no-solution graph coverage on both default-org and org-scoped HTTP routes, preserves root invalid-item precedence, single and plural missing/no-version root error shaping, and mixed missing-vs-no-version root precedence on both default-org and org-scoped routes, now also pins that same root failure detail on the `_default` aliases for both default-org and org-scoped paths, covers impossible dependency failures caused by environment cookbook constraints plus environment-driven root version selection including both upstream environment-respected branches on both default-org and org-scoped routes, preserves first-run-list-item attribution and successful pinned-version selection when repeated cookbook roots appear on both default-org and org-scoped routes, now has explicit malformed run-list item coverage including non-string array elements plus repeated-root successful pinned-version selection, now also has equivalent-root normalization coverage across `foo`, `recipe[foo]`, and `foo::default` forms on both default-org and org-scoped routes including deduped role-expanded duplicates and pinned equivalent-form selection, now also locks in missing- and later-root dependency culprit attribution on both default-org and org-scoped routes including unsatisfied-version failure detail, now also has explicit org-scoped alias parity for richer impossible, transitive, and multi-root conflict cases, and now also locks in that unrelated environment cookbook constraints do not perturb an otherwise identical depsolver conflict or successful selection result on both default-org and org-scoped routes, rejects missing and recursive role inputs with Chef-shaped `400` responses on both default-org and org-scoped routes including the `_default` aliases, enforces cookbook-container read auth alongside environment read on both default-org and org-scoped routes including the `_default` aliases, enforces roles-container read for role-expanded run lists on both default-org and org-scoped routes including the `_default` aliases, returns minimal Chef-style cookbook payloads on success with `metadata.attributes` and `metadata.long_description` omitted while preserving solved cookbook dependency metadata on both named-environment and `_default` success paths, and now has explicit org-scoped alias parity coverage for the main missing, filtered, plural missing-root, plural no-version, mixed missing-vs-no-version, `_default`, datestamp, environment-driven, major/minor and major/minor/patch pessimistic-constraint, combined-constraint, equivalent-root, recipe-qualified, and role-expanded success cases. Create-path cookbook validation now mirrors Chef’s `Field 'name' invalid` behavior for route/payload tuple mismatches while update-path validation stays field-specific, and returned cookbook file URLs are signed direct blob URLs backed by the current blob adapter. Cookbook artifact create/update coverage now also exercises large-component and prerelease versions, route/payload name and identifier mismatch errors, invalid route name/identifier rejection, exact `409` conflict behavior on repeated `PUT`, no-mutation behavior for failed outside-user and invalid-user artifact updates, explicit metadata default overrides, multi-identifier create behavior for the same cookbook name, broader HTTP validation coverage for missing metadata versions, invalid legacy segment shapes, and invalid metadata dependency/platform payloads, plus normal-user create success with 401/403 no-mutation coverage for invalid and outside users. Cookbook artifact collection reads now have explicit coverage for empty collections, multiple identifiers, named-artifact collection views, and API v2 `all_files` shaping. Cookbook version updates/deletes and artifact deletes now also reclaim unreferenced checksum blobs while preserving shared checksum content referenced by other cookbooks, artifacts, or live sandboxes, and the current HTTP coverage now explicitly exercises shared-checksum retention across multiple cookbook versions, successful in-org normal-user cookbook reads/deletes/creates/updates, usable signed recipe download URLs, create/update no-mutation guarantees for failed outside-user and invalid-user cookbook mutations, full file-set replacement behavior that deletes all or some cookbook files on update, invalid-checksum update rejection without mutating the existing cookbook file set, cookbook-artifact wrong-identifier delete no-mutation behavior, successful normal-user cookbook-artifact reads/deletes with usable signed recipe URLs, and cookbook-artifact 401/403 auth coverage for invalid and outside users. Provider-backed blob behavior now also has configurable S3 timeout/retry settings and availability classification for transient or credential/backend failures. Remaining cookbook work is now the broader pedant edge cases beyond the current environment-filtered/named-filter/latest/version read contract plus deeper depsolver parity around broader upstream run-list semantics and deeper operational hardening around provider-backed blob behavior.

Typical commands once a Go toolchain is available:

```bash
make build
make test
make run
```

## Reference Docs

- [Rewrite roadmap](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md)
- [Milestones](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
- [Compatibility matrix template](/Users/oberon/Projects/coding/go/OpenCook/docs/compatibility-matrix-template.md)
- [ADR 0001](/Users/oberon/Projects/coding/go/OpenCook/docs/adr/0001-compatibility-first-architecture.md)
- [ADR 0002](/Users/oberon/Projects/coding/go/OpenCook/docs/adr/0002-external-stateful-dependencies.md)
