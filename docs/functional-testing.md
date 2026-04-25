# Functional Docker Stack

This harness spins up OpenCook, PostgreSQL, and OpenSearch on one Docker network, then runs black-box functional tests from a separate test container.

The tests intentionally talk to OpenCook over HTTP with Chef-style signed requests. PostgreSQL is used for the active persistence path, the blob layer uses the filesystem provider backend, and OpenSearch is present on the shared network while search compatibility routes continue to use the current in-memory adapter.

## Run The Full Flow

```sh
scripts/functional-compose.sh
```

The default flow builds the images, starts the stack, creates compatibility objects, restarts OpenCook, verifies rehydration, runs invalid-write/no-mutation checks, deletes the objects, restarts again, and verifies deletion persisted.

By default the script removes containers and volumes on exit. Keep the stack for inspection with:

```sh
KEEP_STACK=1 scripts/functional-compose.sh
```

Then clean it up manually with:

```sh
docker compose -p opencook-functional -f deploy/functional/docker-compose.yml down -v --remove-orphans
```

## Run Individual Phases

```sh
KEEP_STACK=1 scripts/functional-compose.sh create restart verify
KEEP_STACK=1 scripts/functional-compose.sh invalid restart verify
KEEP_STACK=1 scripts/functional-compose.sh delete restart verify-deleted
```

Supported phase names are `create`, `verify`, `invalid`, `delete`, `verify-deleted`, and `restart`.

## Remote Docker

The Compose stack does not rely on bind mounts, so it can run against a remote Docker daemon as long as your Docker client can send the build context.

```sh
DOCKER_HOST=ssh://example-host scripts/functional-compose.sh
```

Useful overrides:

```sh
POSTGRES_IMAGE=postgres:17-alpine
OPENSEARCH_IMAGE=opensearchproject/opensearch:3.5.0
OPENCOOK_FUNCTIONAL_PORT=4000
OPENCOOK_FUNCTIONAL_ORG=ponyville
OPENCOOK_FUNCTIONAL_ACTOR_NAME=pivotal
```

## What It Covers

- OpenCook can boot with PostgreSQL and OpenSearch reachable on the Compose network.
- Bootstrap auth works with a fixture RSA key and Chef request-signing headers.
- Organization bootstrap rehydrates groups, containers, ACLs, and the validator client shape.
- Environments, nodes, roles, data bags, policy groups, and policy revisions survive OpenCook restarts.
- Filesystem-backed blob uploads survive restart and can be reused by a later sandbox.
- Invalid writes return compatibility errors without mutating persisted state.
- Deletes persist across restart.

The fixture RSA key pair under `test/functional/fixtures` is for this Docker harness only and must not be reused as a production bootstrap key.
