# Functional Docker Stack

This harness spins up OpenCook, PostgreSQL, and OpenSearch on one Docker network, then runs black-box functional tests from a separate test container.

The tests intentionally talk to OpenCook over HTTP with Chef-style signed requests. PostgreSQL is used for the active persistence path, the blob layer uses the filesystem provider backend, and OpenSearch is active on the shared network for the implemented search indexes.

## Run The Full Flow

```sh
scripts/functional-compose.sh
```

The default flow builds the images, starts the stack, creates compatibility objects including encrypted-looking data bag items, restarts OpenCook, verifies rehydration through active OpenSearch-backed search, runs the targeted Lucene/query-string compatibility phase, runs invalid-write/no-mutation checks, updates searchable fields and verifies old search terms disappear, restarts again, reruns the query compatibility phase against updated persisted state, runs the operational admin/reindex/search-repair phases, restarts after repair, deletes the objects, restarts one more time, and verifies deletion persisted.

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
KEEP_STACK=1 scripts/functional-compose.sh query-compat
KEEP_STACK=1 scripts/functional-compose.sh invalid restart verify
KEEP_STACK=1 scripts/functional-compose.sh search-update verify-search-updated restart verify-search-updated
KEEP_STACK=1 scripts/functional-compose.sh operational restart operational-verify
KEEP_STACK=1 scripts/functional-compose.sh delete restart verify-deleted
```

Supported phase names are `create`, `verify`, `query-compat`, `invalid`, `search-update`, `verify-search-updated`, `operational`, `operational-verify`, `delete`, `verify-deleted`, and `restart`.

To run just the OpenSearch-heavy compatibility phases after a stack already has created fixtures, use:

```sh
KEEP_STACK=1 REBUILD=0 scripts/functional-compose.sh verify query-compat search-update verify-search-updated restart verify-search-updated query-compat
```

To run only the Lucene/query-string search compatibility phase against the current fixtures, use:

```sh
KEEP_STACK=1 REBUILD=0 scripts/functional-compose.sh query-compat
```

To run only the operational admin/reindex/search-repair phases, use:

```sh
KEEP_STACK=1 REBUILD=0 scripts/functional-compose.sh operational restart operational-verify
```

## Remote Docker

The Compose stack does not rely on bind mounts, so it can run against a remote Docker daemon as long as your Docker client can send the build context.

```sh
DOCKER_HOST=ssh://example-host scripts/functional-compose.sh
```

To run only the operational phases against a remote Docker daemon:

```sh
DOCKER_HOST=ssh://example-host KEEP_STACK=1 scripts/functional-compose.sh operational restart operational-verify
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
- `/_status` reports an active OpenSearch-backed search provider when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Bootstrap auth works with a fixture RSA key and Chef request-signing headers.
- Organization bootstrap rehydrates groups, containers, ACLs, and the validator client shape.
- Validator-authenticated bootstrap registration uses the generated `<org>-validator` key from organization creation to create normal clients through both explicit-org and configured default-org client routes.
- Validator-created clients persist key material across restart, authenticate signed follow-up requests, retain default key metadata, appear in the `clients` group, expose their client ACL read side effect, and show up in client search rows.
- Searchable clients, environments, nodes, roles, ordinary data bag items, and encrypted-looking data bag items are visible through active OpenSearch-backed search after OpenCook restarts.
- The targeted `query-compat` phase covers representative grouped boolean, quoted phrase, escaped slash, wildcard field, wildcard value, range, full search, and partial search behavior against active PostgreSQL plus OpenSearch.
- Encrypted-looking data bag partial search can select encrypted envelope fields and clear metadata without requiring a data bag secret.
- Searchable environments, nodes, roles, ordinary data bag items, and encrypted-looking data bag items update OpenSearch-visible terms, removing old terms and matching new terms.
- `opencook admin` can sign live HTTP admin requests from the test container to the OpenCook container over the shared Compose network.
- Live-safe operational commands cover admin status, user/org creation, user key creation, a follow-up signed request with the generated key, group/container/ACL inspection, and complete org reindex.
- Operational search consistency detects injected stale OpenSearch documents, including encrypted data bag index drift, dry-runs repair, repairs the stale documents, and verifies clean state after an OpenCook restart.
- Deleted clients, environments, nodes, roles, ordinary data bag items, and encrypted-looking data bag items stop appearing in search after restart.
- Environments, nodes, roles, data bags, policy groups, and policy revisions survive OpenCook restarts.
- Filesystem-backed blob uploads survive restart and can be reused by a later sandbox.
- Invalid writes return compatibility errors without mutating persisted state.
- Deletes persist across restart.

The fixture RSA key pair under `test/functional/fixtures` is for this Docker harness only and must not be reused as a production bootstrap key.
