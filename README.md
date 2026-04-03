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
- sandbox/blob compatibility with in-memory sandbox lifecycle plus signed checksum uploads and downloads
- the first cookbook compatibility slice with cookbook artifact lifecycle, cookbook read views, and universe responses
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
- `/environments/{name}/nodes`
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
- `/organizations/{org}/environments/{name}/nodes`
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

The sandbox compatibility slice is live too. Signed callers can create and commit sandboxes through `/sandboxes` and `/organizations/{org}/sandboxes`, and the returned checksum entries expose absolute signed upload URLs under `/_blob/checksums/{checksum}` backed by the current in-memory blob store.

The first cookbook compatibility slice is also live. Signed callers can create, read, and delete cookbook artifacts through `/cookbook_artifacts` and `/organizations/{org}/cookbook_artifacts`, browse cookbook read views through `/cookbooks` and `/organizations/{org}/cookbooks`, and fetch `/universe` metadata. Returned cookbook file URLs are signed direct blob URLs backed by the same in-memory compatibility store. Deeper cookbook mutation parity and production S3-compatible blob storage are still pending.

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
