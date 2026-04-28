# OpenCook

OpenCook is a compatibility-first Go rewrite of Chef Infra Server.

The goal is to remain wire-compatible with existing Chef and Cinc clients, `knife`, and surrounding automation while replacing the legacy server internals with a simpler, modern Go implementation.

OpenCook is free and open source software under the MIT License. Licensing, license enforcement, license telemetry, and license-management endpoints are intentionally out of scope.

## Project Status

OpenCook is in active development. It is useful for local development, compatibility testing, integration experiments, and early operational workflow work, but it is not yet a production-ready drop-in replacement for Chef Infra Server.

The durable deployment path is now PostgreSQL plus provider-backed blob storage, with OpenSearch used as a derived search index. When PostgreSQL is not configured, OpenCook falls back to in-memory compatibility state for fast standalone experiments.

## What Works Today

- Chef-style request signing and authenticated routing.
- Bootstrap users, organizations, clients, user/client keys, groups, containers, ACLs, and validator-authenticated client registration.
- PostgreSQL-backed persistence for the implemented identity, authorization, cookbook, sandbox, checksum, node, environment, role, data bag, policy, policy group, and object ACL state.
- Cookbook and sandbox flows, including signed checksum upload/download URLs, cookbook artifacts, cookbook versions, cookbook read views, `universe`, and environment cookbook depsolver behavior.
- Data bag CRUD, including encrypted-looking data bag payload opacity. OpenCook stores, returns, searches, reindexes, and repairs encrypted-looking JSON without decrypting it or managing data bag secrets.
- Built-in Chef search indexes for clients, environments, nodes, roles, and data bags, with memory fallback and active OpenSearch-backed mode when PostgreSQL and OpenSearch are configured.
- Blob storage backends for in-memory, local filesystem, and S3-compatible providers.
- `opencook admin` workflows for signed live inspection/management, offline-gated repair commands, and PostgreSQL-backed OpenSearch reindex/check/repair.

## Current Limitations

- OpenCook is not production-ready yet.
- Migration/cutover tooling, backup/restore, broader `chef-server-ctl` parity, metrics, and service-management hardening are still in progress.
- Some Chef object edge cases and less common compatibility surfaces still need additional pedant-backed hardening.
- OpenSearch is intentionally a derived index. PostgreSQL is the source of truth.
- Public search indexes are limited to Chef-supported object families that OpenCook has implemented: clients, environments, nodes, roles, and data bags. Cookbook, cookbook-artifact, policy, policy-group, sandbox, and checksum state is intentionally not exposed as public Chef search indexes.
- The standalone no-PostgreSQL mode is volatile. Use it for development and experiments, not durable deployments.
- Licensing endpoints are intentionally not implemented.

## Quickstart: Standalone Local Mode

Standalone mode runs a single OpenCook process without PostgreSQL or OpenSearch. It is the fastest way to try the API, but metadata is in memory; a filesystem blob backend can persist blob bytes only.

Prerequisites:

- Go 1.22 or newer
- OpenSSL, or another way to generate an RSA key pair

Build the server and create a bootstrap key pair:

```bash
make build
mkdir -p .local
openssl genrsa -out .local/bootstrap_private.pem 2048
openssl rsa -in .local/bootstrap_private.pem -pubout -out .local/bootstrap_public.pem
```

Start OpenCook:

```bash
export OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=pivotal
export OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user
export OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID=default
export OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH="$PWD/.local/bootstrap_public.pem"
export OPENCOOK_DEFAULT_ORGANIZATION=ponyville
export OPENCOOK_BLOB_BACKEND=filesystem
export OPENCOOK_BLOB_STORAGE_URL="file://$PWD/.local/blobs"

bin/opencook serve
```

In another terminal, point the admin CLI at the same bootstrap identity:

```bash
export OPENCOOK_ADMIN_SERVER_URL=http://127.0.0.1:4000
export OPENCOOK_ADMIN_REQUESTOR_NAME=pivotal
export OPENCOOK_ADMIN_REQUESTOR_TYPE=user
export OPENCOOK_ADMIN_PRIVATE_KEY_PATH="$PWD/.local/bootstrap_private.pem"
export OPENCOOK_ADMIN_DEFAULT_ORG=ponyville

bin/opencook admin --json status
bin/opencook admin orgs create ponyville --full-name "Ponyville" --validator-key-out .local/ponyville-validator.pem
```

Health and status endpoints are available without signed auth:

```bash
curl http://127.0.0.1:4000/readyz
curl http://127.0.0.1:4000/_status
```

## Quickstart: Docker Compose

For a turnkey local stack with PostgreSQL persistence, filesystem-backed blobs,
and OpenSearch-backed search, use the root Compose file:

```bash
docker compose up --build -d
```

If `.local/bootstrap_private.pem` and `.local/bootstrap_public.pem` do not
already exist, the stack generates them on startup and reuses them on later
`docker compose up` runs:

- `.local/bootstrap_private.pem`
- `.local/bootstrap_public.pem`

Check that OpenCook is healthy:

```bash
curl http://127.0.0.1:4000/readyz
curl http://127.0.0.1:4000/_status
```

The `opencook` container is preconfigured so you can run admin commands directly
inside it with the generated bootstrap identity:

```bash
docker compose exec opencook opencook admin --json status
docker compose exec opencook opencook admin orgs create ponyville --full-name "Ponyville" --validator-key-out /var/lib/opencook/bootstrap/ponyville-validator.pem
```

If you also want to run the admin CLI from your host and have built
`bin/opencook` locally, point it at the generated private key:

```bash
export OPENCOOK_ADMIN_SERVER_URL=http://127.0.0.1:4000
export OPENCOOK_ADMIN_REQUESTOR_NAME=pivotal
export OPENCOOK_ADMIN_REQUESTOR_TYPE=user
export OPENCOOK_ADMIN_PRIVATE_KEY_PATH="$PWD/.local/bootstrap_private.pem"
export OPENCOOK_ADMIN_DEFAULT_ORG=ponyville

bin/opencook admin --json status
```

Shut the stack down but keep PostgreSQL, OpenSearch, and blob data:

```bash
docker compose down
```

Remove the containers and the named volumes backing PostgreSQL, OpenSearch, and
blob storage:

```bash
docker compose down -v
```

The root `compose.yml` is the user-facing reference stack. The existing
`deploy/functional/docker-compose.yml` and `scripts/functional-compose.sh`
remain the black-box functional test harness.

## Quickstart: PostgreSQL And OpenSearch

Use this path when you want durable OpenCook state and active provider-backed search. The example below runs PostgreSQL and OpenSearch as external local containers while OpenCook runs from your checked-out source tree.

Start PostgreSQL:

```bash
docker run --rm -d \
  --name opencook-postgres \
  -p 5432:5432 \
  -e POSTGRES_USER=opencook \
  -e POSTGRES_PASSWORD=opencook \
  -e POSTGRES_DB=opencook \
  postgres:17-alpine
```

Start OpenSearch:

```bash
docker run --rm -d \
  --name opencook-opensearch \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e DISABLE_INSTALL_DEMO_CONFIG=true \
  -e DISABLE_SECURITY_PLUGIN=true \
  -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
  --ulimit memlock=-1:-1 \
  --ulimit nofile=65536:65536 \
  opensearchproject/opensearch:3.5.0
```

Then start OpenCook with PostgreSQL, OpenSearch, and filesystem-backed blobs:

```bash
export OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=pivotal
export OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user
export OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID=default
export OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH="$PWD/.local/bootstrap_public.pem"
export OPENCOOK_DEFAULT_ORGANIZATION=ponyville

export OPENCOOK_POSTGRES_DSN="postgres://opencook:opencook@localhost:5432/opencook?sslmode=disable"
export OPENCOOK_OPENSEARCH_URL="http://localhost:9200"
export OPENCOOK_BLOB_BACKEND=filesystem
export OPENCOOK_BLOB_STORAGE_URL="file://$PWD/.local/blobs"

bin/opencook serve
```

Check dependency status:

```bash
bin/opencook admin --json status
```

The status payload should report PostgreSQL persistence, filesystem blob storage, and OpenSearch-backed search. If OpenSearch is unavailable while configured, startup fails instead of silently falling back to memory search.

If OpenCook itself runs in a container on the same Docker network as PostgreSQL and OpenSearch, use container DNS names instead of `localhost`, for example:

```bash
OPENCOOK_POSTGRES_DSN="postgres://opencook:opencook@postgres:5432/opencook?sslmode=disable"
OPENCOOK_OPENSEARCH_URL="http://opensearch:9200"
```

For a complete Docker-based functional stack, use:

```bash
scripts/functional-compose.sh
```

See [Functional Docker Stack](docs/functional-testing.md) for phase-by-phase and remote Docker usage.

## Container Image

Build a local runtime image:

```bash
docker build -t opencook:dev .
```

Run it in standalone mode with your own bootstrap public key:

```bash
docker run --rm -p 4000:4000 \
  -v "$PWD/.local/bootstrap_public.pem:/etc/opencook/bootstrap_public.pem:ro" \
  -e OPENCOOK_BOOTSTRAP_REQUESTOR_NAME=pivotal \
  -e OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE=user \
  -e OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID=default \
  -e OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH=/etc/opencook/bootstrap_public.pem \
  -e OPENCOOK_DEFAULT_ORGANIZATION=ponyville \
  opencook:dev
```

For durable container deployments, also provide `OPENCOOK_POSTGRES_DSN`, `OPENCOOK_OPENSEARCH_URL`, and blob backend settings that are reachable from inside the container.

## Configuration

OpenCook is configured with `OPENCOOK_*` environment variables. See [configs/opencook.env.example](configs/opencook.env.example) for a full example.

Common server settings:

- `OPENCOOK_LISTEN_ADDRESS`: bind address, default `:4000`
- `OPENCOOK_DEFAULT_ORGANIZATION`: optional default org for default-org routes
- `OPENCOOK_BOOTSTRAP_REQUESTOR_NAME`: bootstrap user/client name
- `OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE`: usually `user`
- `OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID`: bootstrap key id, default `default`
- `OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH`: public key used to seed the bootstrap requestor
- `OPENCOOK_POSTGRES_DSN`: PostgreSQL DSN for durable persistence
- `OPENCOOK_OPENSEARCH_URL`: OpenSearch-compatible endpoint for active search
- `OPENCOOK_BLOB_BACKEND`: `memory`, `filesystem`, or `s3`
- `OPENCOOK_BLOB_STORAGE_URL`: blob target, such as `file:///var/lib/opencook/blobs` or `s3://bucket/prefix`

Common admin CLI settings:

- `OPENCOOK_ADMIN_SERVER_URL`: server URL, default `http://127.0.0.1:4000`
- `OPENCOOK_ADMIN_REQUESTOR_NAME`: signing actor name
- `OPENCOOK_ADMIN_REQUESTOR_TYPE`: signing actor type, usually `user` or `client`
- `OPENCOOK_ADMIN_PRIVATE_KEY_PATH`: private key PEM for signed requests
- `OPENCOOK_ADMIN_DEFAULT_ORG`: default org for admin commands
- `OPENCOOK_ADMIN_SERVER_API_VERSION`: `X-Ops-Server-API-Version`, default `1`

## Admin Operations

The first `opencook admin` CLI supports:

- status inspection
- user and user-key inspection/management
- organization creation and inspection
- client key inspection/management
- group, container, and ACL inspection
- offline-gated membership and ACL repair commands
- OpenSearch reindex/check/repair from PostgreSQL-backed state

Show command help:

```bash
bin/opencook help
bin/opencook admin help
bin/opencook admin reindex help
```

## Testing

Run the Go test suite:

```bash
make test
```

For a fuller local verification pass:

```bash
make verify
```

`make verify` runs formatting, `go vet`, and the test suite.

Run the Docker functional stack with PostgreSQL, OpenSearch, filesystem-backed blobs, and black-box functional tests:

```bash
scripts/functional-compose.sh
```

## Development Notes

OpenCook is compatibility-first. When behavior differs from upstream Chef Infra Server, the difference should be intentional, documented, and tested.

Useful project docs:

- [Rewrite roadmap](docs/chef-infra-server-rewrite-roadmap.md)
- [Milestones](docs/milestones.md)
- [Compatibility matrix](docs/compatibility-matrix-template.md)
- [Functional Docker Stack](docs/functional-testing.md)
- [Compatibility-first architecture](docs/adr/0001-compatibility-first-architecture.md)
- [External stateful dependencies](docs/adr/0002-external-stateful-dependencies.md)
- [No licensing subsystem](docs/adr/0003-no-licensing-subsystem.md)

## License

OpenCook is released under the [MIT License](LICENSE).
