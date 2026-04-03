# OpenCook

OpenCook is a Go-based rewrite of Chef Infra Server with a compatibility-first architecture. The goal is to remain wire-compatible with Chef and Cinc clients, `knife`, and existing automation workflows while modernizing the server internals and operations model.

OpenCook is intended to be fully free and open source. Licensing, license enforcement, license telemetry, and related product mechanics are intentionally out of scope.

The project is currently in its compatibility-foundation phase. This repository now includes:

- a buildable Go application layout centered on `cmd/opencook`
- compatibility surface inventory wired into the HTTP server
- Chef request-signing verification with test coverage
- in-memory bootstrap state for users, organizations, clients, groups, containers, and ACLs
- initial authenticated endpoints for users, organizations, clients, ACL inspection, actor key lifecycle, and nodes
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
3. Roles, environments, data bags, and remaining core object CRUD
4. Search/indexing compatibility on OpenSearch
5. Cookbook, sandbox, and blob storage flows

## Local Development

The scaffold uses environment variables for configuration. See [configs/opencook.env.example](/Users/oberon/Projects/coding/go/OpenCook/configs/opencook.env.example).

To exercise the first authenticated HTTP endpoints locally, configure a bootstrap requestor with a public key:

```bash
export OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=silent-bob
export OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user
export OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/path/to/public.pem
export OPENCOOK_DEFAULT_ORGANIZATION=ponyville
export OPENCOOK_MAX_AUTH_BODY_BYTES=8388608
```

With that in place, signed requests can successfully hit:

- `/users`
- `/users/{name}`
- `/users/{name}/keys`
- `/users/{name}/keys/{key}`
- `/organizations`
- `/organizations/{org}`
- `/nodes`
- `/nodes/{name}`
- `/organizations/{org}/nodes`
- `/organizations/{org}/nodes/{name}`
- `/organizations/{org}/clients`
- `/organizations/{org}/clients/{name}`
- `/organizations/{org}/clients/{name}/keys`
- `/organizations/{org}/clients/{name}/keys/{key}`

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
