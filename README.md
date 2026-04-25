# OpenCook

OpenCook is a compatibility-first Go rewrite of Chef Infra Server.

Its goal is to remain wire-compatible with existing Chef and Cinc clients, `knife`, and surrounding automation while moving the server to a simpler, modern Go codebase.

OpenCook is free and open source software released under the MIT License. License enforcement, license telemetry, and license-management endpoints are intentionally out of scope.

## Goals

- Preserve compatibility with existing Chef and Cinc tooling.
- Modernize the server implementation and operational model.
- Keep the project fully open source under the MIT License.
- Build compatibility incrementally, with explicit tests and behavior tracking.

## Current Status

OpenCook is in active development and already provides a meaningful compatibility foundation for local development, experimentation, and integration work.

Today, it should be viewed as an early server implementation rather than a production-ready drop-in replacement. Some subsystems still rely on in-memory state, and deeper operational tooling, search edge-case coverage, and broader compatibility hardening are still in progress.

Org bootstrap can mint validator key material, and generated `<org>-validator` clients can now register normal clients through the stock client bootstrap routes.
Administrative object management is also still API-first today; a first-class `chef-server-ctl`-style replacement for orgs, users, groups, and ACLs remains future work.
Data bag CRUD is live, but encrypted data bag compatibility is not yet an explicitly tracked/tested compatibility slice.

## Current Capabilities

- Authentication and bootstrap: Chef request-signing verification, bootstrap users, organizations, clients, groups, containers, ACLs, actor key lifecycle, and validator-authenticated client registration.
- Core objects: nodes, environments, roles, and data bags, with default-org and explicit-org routing where implemented.
- Cookbook flows: sandboxes, signed checksum upload/download flows, cookbook artifacts, cookbook versions, cookbook read views, `universe`, and environment depsolver behavior.
- Search and policy: built-in Chef search indexes with an in-memory fallback and active OpenSearch-backed mode when PostgreSQL plus OpenSearch are configured, partial search support, and policy/policy-group compatibility routes.
- Blob backends: in-memory, filesystem-backed, and S3-compatible storage for sandbox and cookbook content.

## Installation

### Prerequisites

- Go 1.22 or newer
- A public key for the bootstrap requestor used in local development
- Optional: local filesystem or S3-compatible object storage for blob content
- Optional: PostgreSQL and OpenSearch for durable state plus provider-backed search

### Build From Source

```bash
git clone https://github.com/oberones/OpenCook.git
cd OpenCook
make build
```

This produces `bin/opencook`.

If you prefer plain Go commands:

```bash
go build -o bin/opencook ./cmd/opencook
```

## Running Locally

Review the example configuration in [configs/opencook.env.example](configs/opencook.env.example).

A minimal local setup looks like this:

```bash
export OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=silent-bob
export OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user
export OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/path/to/public.pem
export OPENCOOK_DEFAULT_ORGANIZATION=ponyville
export OPENCOOK_BLOB_BACKEND=filesystem
export OPENCOOK_BLOB_STORAGE_URL=file:///tmp/opencook-objects
```

Then start the server:

```bash
make run
```

By default, OpenCook listens on `:4000`.

## Testing

Run the test suite with:

```bash
make test
```

For a fuller local verification pass:

```bash
make verify
```

`make verify` runs formatting, `go vet`, and the test suite.

To exercise OpenCook with PostgreSQL, OpenSearch, and filesystem-backed blob storage on a shared Docker network:

```bash
scripts/functional-compose.sh
```

See [Functional Docker Stack](docs/functional-testing.md) for phase-by-phase and remote Docker usage.

## Contributing

Contributions are welcome.

When contributing:

- Keep changes compatibility-first.
- Prefer small vertical slices with focused tests.
- Update relevant docs when behavior changes.
- Run `make verify` before opening a pull request.

For project background and deeper implementation details, see:

- [Rewrite roadmap](docs/chef-infra-server-rewrite-roadmap.md)
- [Milestones](docs/milestones.md)
- [Compatibility-first architecture](docs/adr/0001-compatibility-first-architecture.md)
- [External stateful dependencies](docs/adr/0002-external-stateful-dependencies.md)
- [No licensing subsystem](docs/adr/0003-no-licensing-subsystem.md)

## License

OpenCook is released under the [MIT License](LICENSE).
