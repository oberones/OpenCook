# Operational Admin And Reindex/Repair Plan

Status: complete

## Summary

This bucket added the first operational administration surface for OpenCook after the PostgreSQL, provider-backed blob, validator bootstrap, core object persistence, and active OpenSearch search cutovers.

The goal is to give operators a safe way to manage root identity/authz state and to repair derived OpenSearch state without changing Chef-facing API contracts. OpenCook should remain compatible with Chef and Cinc clients, `knife`, and existing signed HTTP traffic; operational commands are for administrators and should not become a new public Chef API surface.

The completed slice covers:

- a stable `opencook` CLI command structure that preserves today's default server startup behavior
- admin workflows for users, organizations, org membership, groups, containers, ACL inspection/repair, and actor keys
- OpenSearch reindexing from PostgreSQL-backed state
- consistency checks and repair reports for PostgreSQL-to-OpenSearch drift
- functional Docker coverage proving the operational commands work against the PostgreSQL plus OpenSearch stack

## Current State

OpenCook currently has:

- `cmd/opencook`, which always starts the HTTP server.
- PostgreSQL-backed cookbook metadata, bootstrap core state, and implemented core object API state.
- Active OpenSearch-backed search when PostgreSQL and `OPENCOOK_OPENSEARCH_URL` are configured.
- Startup OpenSearch rebuild from PostgreSQL-backed state.
- Mutation-time OpenSearch indexing hooks for clients, environments, nodes, roles, and data bag items.
- Stable Chef-facing `503 search_unavailable` degradation for provider failures.
- Functional Docker coverage for PostgreSQL, filesystem blobs, and OpenSearch lifecycle behavior.

OpenCook now has:

- a first-class `opencook admin` operational CLI
- signed HTTP-backed live-safe admin commands for the currently implemented user, organization, actor-key, group, container, and ACL surfaces
- public OpenSearch reindex, consistency check, and repair commands backed by PostgreSQL state
- offline-gated direct PostgreSQL repair commands where live routes would bypass running-server cache state
- functional Docker coverage for admin, reindex, consistency repair, and post-restart verification

OpenCook does not yet have:

- online cross-process cache invalidation for direct PostgreSQL admin mutations
- full `chef-server-ctl` parity
- backup/restore, secret rotation, HA, service-management, or migration/cutover tooling

## Upstream Inventory

The upstream Chef Server references for this bucket are:

- `src/chef-server-ctl/plugins/wrap-knife.rb`
  - wraps user and organization commands such as `user-create`, `user-delete`, `user-list`, `user-show`, `org-create`, `org-delete`, `org-list`, `org-show`, `org-user-add`, and `org-user-remove`
- `src/chef-server-ctl/plugins/server_admins.rb`
  - manages the global `server-admins` group through `grant-server-admin-permissions`, `remove-server-admin-permissions`, and `list-server-admins`
- `src/chef-server-ctl/plugins/key_control.rb`
  - manages user and client key lifecycle commands
- `src/chef-server-ctl/plugins/reindex.rb`
  - implements `chef-server-ctl reindex`, including `--all-orgs`, `--disable-api`, and `--with-timing`
- `src/oc_erchef/priv/reindex-opc-organization`
  - supports org-level `drop`, `reindex`, and `complete`
- `src/oc_erchef/priv/reindex-opc-piecewise`
  - supports narrower org/index/item reindexing for clients, environments, nodes, roles, and data bag indexes
- `src/oc_erchef/apps/oc_chef_wm/src/chef_reindex.erl`
  - treats PostgreSQL as authoritative, rebuilds search documents in batches, returns failed and missing items, and keeps full-org drop separate from reindex

OpenCook should reuse the operational intent, not the omnibus packaging model. Licensing commands, package install commands, omnibus service control, and proprietary notice/license flows remain out of scope.

Task 1 freezes this inventory as an implementation guide, not a byte-for-byte clone:

| Upstream area | Upstream behavior | OpenCook target |
| --- | --- | --- |
| Wrapped knife-opc user/org commands | `chef-server-ctl` shells out to `knife` for user and org management | Prefer live-safe signed HTTP calls to existing OpenCook routes so the running server updates its own cache and persistence seams |
| Key-control commands | user/client key add, list, delete, and update flow through Chef key APIs | Wrap existing key routes and preserve generated key material handling, expiration parsing, conflict/not-found behavior, and no license-specific branches |
| Server-admin commands | direct database plus Bifrost group mutation for `server-admins` | Model the operational intent as server-admin membership management, but gate any direct PostgreSQL mutation as offline until cache invalidation exists |
| Full-org reindex | `drop`, `reindex`, and `complete` rebuild an organization's search documents from database state | Rebuild OpenSearch from PostgreSQL-backed OpenCook state, with `complete` as drop then reindex and PostgreSQL remaining authoritative |
| Piecewise reindex | org/index/item targeting for clients, environments, nodes, roles, and data bag indexes | Support org, built-in index, data bag index, and optional named-item scopes where OpenCook document identity supports it |
| Reindex results | returns success, missing items, and failed items without exposing raw provider internals | Return structured counts and redacted failure summaries in human and JSON output modes |

## Interfaces And Behavior

- Keep the existing no-argument `opencook` behavior: it starts the server.
- Add an explicit `opencook serve` alias for server startup.
- Add admin/reindex commands under `opencook admin ...` or an equivalent subcommand tree.
- Do not change Chef-facing HTTP routes, payloads, methods, signed-auth behavior, or error shapes in this bucket.
- Do not add licensing behavior, licensing telemetry, or licensing endpoints.
- Keep PostgreSQL as the system of record.
- Keep OpenSearch as a derived query projection.
- Keep the memory search fallback unchanged when OpenSearch is not configured.
- Use existing signed HTTP APIs for live-safe admin operations where the API already exists.
- Treat direct PostgreSQL admin mutations as offline maintenance until OpenCook has cross-process cache invalidation or an explicit admin control plane.
- Make destructive commands require clear intent, such as `--yes`, `--force`, or a command-specific confirmation flag.
- Provide machine-readable `--json` output for reports and human-readable output for normal CLI use.
- Keep provider internals out of operator-facing error messages unless a diagnostic flag explicitly requests lower-level detail.

## Scope Boundaries

In scope for this bucket:

- `opencook` command dispatch and an explicit `opencook serve` alias.
- Signed HTTP-backed admin commands for surfaces that already exist as stable OpenCook routes.
- Offline-gated direct PostgreSQL repair for root state that does not yet have live-safe admin routes.
- User, organization, user key, client key, group, container, ACL, org-membership, and server-admin operational workflows.
- OpenSearch drop, reindex, complete reindex, consistency check, and consistency repair from PostgreSQL-backed state.
- Functional Docker coverage for operational commands against the PostgreSQL plus OpenSearch stack.

Out of scope for this bucket:

- Chef-facing route, payload, authentication, authorization, signed URL, or response-shape changes.
- Licensing behavior, licensing telemetry, license-management commands, or license endpoints.
- Omnibus package install, service supervisor control, `reconfigure`, HA role management, Redis maintenance, and notice-generation commands.
- Backup/restore, secrets rotation, migration/cutover tooling, and production upgrade playbooks.
- Encrypted data bag compatibility.
- Broader Lucene/query-string semantics beyond the search behavior already pinned.
- Cookbook, policy, and sandbox search indexes beyond consistency reporting for currently indexed surfaces.
- Online direct PostgreSQL admin mutation that bypasses a running server's in-process state.

## Initial Command Shape

The exact names can tighten during implementation, but the first implementation should aim for this shape:

```text
opencook serve
opencook admin status
opencook admin users list
opencook admin users show USER
opencook admin users create USER --first-name FIRST --last-name LAST --email EMAIL [--key-name NAME] [--public-key PATH] [--private-key-out PATH]
opencook admin users delete USER --yes
opencook admin users keys list USER [--verbose]
opencook admin users keys add USER --key-name NAME [--public-key PATH] [--expiration-date DATE] [--private-key-out PATH]
opencook admin users keys delete USER KEY --yes
opencook admin orgs list
opencook admin orgs show ORG
opencook admin orgs create ORG --full-name NAME [--association-user USER] [--validator-key-out PATH]
opencook admin orgs delete ORG --yes
opencook admin orgs add-user ORG USER [--admin]
opencook admin orgs remove-user ORG USER [--force]
opencook admin groups list ORG
opencook admin groups show ORG GROUP
opencook admin groups add-actor ORG GROUP ACTOR
opencook admin groups remove-actor ORG GROUP ACTOR
opencook admin containers list ORG
opencook admin containers show ORG CONTAINER
opencook admin acls get RESOURCE
opencook admin acls repair-defaults [--org ORG] [--dry-run] [--yes]
opencook admin server-admins list
opencook admin server-admins grant USER
opencook admin server-admins revoke USER
opencook admin reindex --org ORG [--complete|--drop|--no-drop] [--with-timing] [--json]
opencook admin reindex --all-orgs [--complete|--drop|--no-drop] [--with-timing] [--json]
opencook admin reindex --org ORG --index INDEX [--name NAME ...] [--json]
opencook admin search check [--org ORG] [--index INDEX] [--json]
opencook admin search repair [--org ORG] [--index INDEX] [--dry-run] [--yes] [--json]
```

Admin command names do not need to be perfect Chef clones, but common upstream names and flags should be easy to map in docs.

## Exit Codes And Output Contract

The first implementation should keep exit codes intentionally small and stable:

| Exit code | Meaning |
| --- | --- |
| `0` | Success, including clean `search check` results and no-op dry-runs |
| `1` | Usage error, invalid flags, malformed input, or missing required confirmation |
| `2` | Requested organization, user, client, group, container, ACL, index, or named item was not found |
| `3` | Operation ran but reported drift, missing items, failed items, or partial repair/reindex failure |
| `4` | Required dependency is unavailable or misconfigured, such as PostgreSQL, OpenSearch, the server URL, or signing credentials |
| `5` | Unsafe online operation refused because it would require direct PostgreSQL mutation without an explicit offline/force contract |

Human-readable output:

- Write normal summaries and generated public metadata to stdout.
- Write warnings, redacted provider failures, and usage errors to stderr.
- Print generated private keys only when the command explicitly requests stdout output.
- Prefer writing private key material to an operator-provided path with restrictive permissions.
- Never print raw PostgreSQL connection strings, private key paths with file contents, provider response bodies, or request signatures.

JSON output:

- `--json` should write a single JSON object to stdout and reserve stderr for fatal usage/runtime errors.
- Common fields should include `ok`, `command`, `mode`, `target`, `counts`, `warnings`, `errors`, and `duration_ms` when applicable.
- Reindex and repair counts should use stable keys: `scanned`, `upserted`, `deleted`, `skipped`, `missing`, `stale`, `failed`, and `clean`.
- Errors should use stable codes such as `usage_error`, `not_found`, `dependency_unavailable`, `unsafe_online_mutation`, `reindex_failed`, and `repair_failed`.

## Destructive And Offline Command Contract

- Commands that delete or overwrite state must require `--yes` or a command-specific force flag.
- Dry-run modes must not mutate PostgreSQL, OpenSearch, filesystem blob storage, or S3-compatible blob storage.
- Direct PostgreSQL repair commands must refuse to run unless PostgreSQL is configured and the command includes an explicit offline/force flag.
- Direct PostgreSQL repair output must state that running OpenCook server processes need a restart before repaired bootstrap/core-object state is visible.
- Search reindex and repair commands may run while the server is online because they modify only derived OpenSearch state.
- Complete drop-and-reindex commands should warn that concurrent Chef object writes can race with repair until a future maintenance gate exists.
- Provider failure summaries must be operationally useful but redacted by default; raw provider diagnostics can be a later explicit debug mode.

## Cache Safety Contract

OpenCook currently hydrates PostgreSQL-backed state into an in-process `bootstrap.Service` cache at startup. That makes direct PostgreSQL writes different from upstream Chef Server, where many reads go back through database-backed services.

For this bucket:

- Commands that mutate through existing signed HTTP routes are live-safe because the running server updates its own service state and persistence layer.
- Commands that mutate PostgreSQL directly must be marked offline-only or repair-only and must clearly tell the operator that running OpenCook processes need a restart before those changes are visible.
- Reindex and search repair commands may run online because they rebuild a derived OpenSearch projection from PostgreSQL. They should still warn that concurrent object writes can race with a complete drop-and-reindex unless a future maintenance gate is added.
- Do not silently add direct PostgreSQL writes that make a running server's verifier key cache, group memberships, ACLs, or object state stale.

## Task Breakdown

### Task 1: Freeze The Operational Contract

Status:

- Completed. The plan now records the upstream operational inventory, OpenCook-specific scope boundaries, command shape, cache-safety rules, exit-code targets, output contracts, destructive-command confirmation rules, and provider/error redaction requirements.

- Keep this plan as `docs/operational-admin-reindex-repair-plan.md`.
- Inventory upstream `chef-server-ctl`, reindex, key-control, server-admin, and wrapped knife-opc behavior.
- Separate in-scope OpenCook operational behavior from out-of-scope omnibus packaging, service supervision, licensing, backup/restore, and secrets rotation.
- Record the cache-safety contract for live HTTP-backed mutations versus offline direct PostgreSQL repair.
- Define initial command names, expected exit codes, output modes, destructive-command confirmations, and error redaction rules.

### Task 2: Add A CLI Framework Without Breaking Server Startup

Status:

- Completed. `cmd/opencook` now has a testable command runner; no-argument `opencook` still enters the existing server startup path, `opencook serve` is an explicit alias, help/version paths avoid server config loading, usage errors return the planned usage exit code, and config/server startup failures return the planned dependency-unavailable exit code.

- Refactor `cmd/opencook` so no arguments still start the server.
- Add `opencook serve` as an explicit server command.
- Add top-level help/version behavior and a testable command runner.
- Keep configuration loading consistent with the server path, but avoid requiring server-only values for commands that do not need them.
- Add unit tests for command parsing, usage errors, exit codes, and default server-mode compatibility.

### Task 3: Add An Admin Client And Signing Harness

Status:

- Completed. `internal/admin` now provides a small admin client/config surface with `OPENCOOK_ADMIN_*` environment defaults, flag binding for server URL, requestor identity, private key path, default org, and server API version, Chef 1.3 request signing, injectable HTTP transport, JSON request/response helpers, private-key loading, and redacted stable errors. Tests verify the signed headers through the existing authn verifier, request shape, JSON decoding, private-key loading, config flag overrides, unsafe path rejection, and provider/decode error redaction.

- Add an internal admin client that can send Chef-signed HTTP requests to a running OpenCook server.
- Reuse the existing authn signing/header behavior where possible so admin commands exercise the same route contracts as clients.
- Support configuration through environment variables and flags:
  - server URL
  - requestor name and type
  - private key path
  - default organization where needed
- Add fake-transport tests for request shape, auth headers, JSON decoding, private-key handling, and stable error redaction.
- Do not require OpenSearch or PostgreSQL for pure HTTP client construction tests.

### Task 4: Implement Live-safe User, Organization, And Key Admin Commands

- Completed. `opencook admin` now wraps live signed HTTP routes for status, user list/show/create, organization list/show/create, user key list/show/create/update/delete, and client key list/show/create/update/delete.
- Generated private keys are redacted from normal JSON output, can be written to an explicit `--private-key-out`/`--validator-key-out` path with `0600` permissions, and are printed to stdout only when the caller explicitly passes `-`.
- Key-add commands default `expiration_date` to `infinity` so the CLI preserves the current server validation contract while keeping the flag optional for operators.
- CLI tests now pin request methods, paths, JSON payloads, destructive `--yes` gating, private-key redaction/output behavior, and signed in-process HTTP behavior.
- App-level restart coverage now proves live HTTP-created users, orgs, generated validator clients, user keys, and client keys rehydrate from active PostgreSQL bootstrap core state.

- Wrap existing HTTP routes for user list/show/create and organization list/show/create.
- Wrap existing user and client key list/create/update/delete routes.
- Preserve generated private key behavior by printing to stdout only when explicitly requested or writing to a requested output path with restrictive permissions.
- Preserve current validation and route error shapes by letting the server handle compatibility logic.
- Add CLI tests using fake HTTP responses and app-level tests using an in-process server.
- Add PostgreSQL restart coverage proving CLI-created users, orgs, validator clients, and keys survive restart when commands go through the running server.

### Task 5: Add Safe Membership, Group, Container, And ACL Operations

- Completed. `opencook admin` now wraps live signed HTTP `GET` routes for group list/show, container list/show, and ACL inspection for users, orgs, groups, containers, and clients.
- Membership-changing operations are intentionally offline-only because there are not stable live HTTP write routes for them yet. They require `--offline --yes`, load PostgreSQL-backed bootstrap core state, save through the existing bootstrap core store seam, and emit a restart note for running OpenCook servers.
- Offline membership commands now cover org user add/remove, group actor add/remove for users/clients/groups, and server-admin intent mapped onto the current per-org `admins` groups until a global server-admins model exists.
- ACL repair now supports `opencook admin acls repair-defaults --offline`, including `--dry-run`, org scoping, bootstrap-core ACL defaults, and persisted core-object ACL defaults for currently implemented object maps.
- Tests pin live route paths, offline confirmation gating, no-mutation behavior for failed membership/repair operations, and in-process group/container/ACL reads.

- Implement read-only wrappers for existing group, container, and ACL GET routes.
- Add operational commands for org user association, org user removal, server-admin membership, and group actor membership.
- Prefer live-safe HTTP routes where they already exist.
- For behavior not yet exposed through stable HTTP routes, add an offline repair path that:
  - loads PostgreSQL-backed bootstrap/core state
  - applies the same bootstrap normalization/defaulting rules
  - saves through the existing store seams
  - refuses to run without an explicit offline/force flag
  - documents that running servers must be restarted
- Add ACL repair commands that can detect and restore missing default ACLs for organizations, default containers, default groups, clients, users, and currently implemented core objects.
- Add no-mutation tests for failed membership and ACL repair operations.

### Task 6: Extract A Reusable Reindex/Repair Service

- Completed. Startup OpenSearch rebuild now routes through a reusable `search.ReindexService` while preserving the existing full `complete` behavior: ping, ensure index, drop stale OpenSearch documents, upsert PostgreSQL-derived documents, and refresh.
- The service supports all-org, single-org, built-in-index, data-bag-index, and named-item plans, plus `drop`, `reindex`, `complete`, and dry-run modes.
- Reindex results now carry structured `scanned`, `upserted`, `deleted`, `skipped`, `missing`, `failed`, redacted failure summaries, and duration fields.
- Tests pin full startup-equivalent scope, org/index/data-bag/named filtering, drop and dry-run behavior, missing org/index validation, provider failure redaction, and unchanged startup request shape.

- Move the startup OpenSearch rebuild behavior behind a reusable internal service, without changing startup behavior.
- Support reindex plans for:
  - all organizations
  - one organization
  - one organization plus one built-in index
  - one organization plus one data bag index
  - optional named items where the current document identity supports it
- Support modes matching upstream intent:
  - `drop`: remove derived search documents for the target scope
  - `reindex`: upsert derived search documents from PostgreSQL-backed state
  - `complete`: drop then reindex
- Return structured counts for scanned, upserted, deleted, skipped, missing, failed, and duration.
- Keep PostgreSQL-backed state as authoritative and never hydrate final search API payloads from OpenSearch source documents.

### Task 7: Implement Reindex CLI Commands

- Completed. `opencook admin reindex` now rebuilds derived OpenSearch documents from PostgreSQL-backed bootstrap/core-object state through the reusable `search.ReindexService`.
- The command supports all-org, single-org, built-in-index, data-bag-index, and named-item scopes, plus `complete`, `drop`, `no-drop`, dry-run, timing, JSON, PostgreSQL DSN, and OpenSearch URL flags.
- Active runs require both PostgreSQL and OpenSearch configuration, while dry-runs require only PostgreSQL and never construct or mutate an OpenSearch target.
- CLI tests now pin full-org, all-org, index-scoped, data-bag-scoped, named-item, dry-run, missing-org, missing-index, provider-unavailable, and config-validation behavior with redacted failure messages and stable exit codes.

- Add `opencook admin reindex` commands over the reusable reindex service.
- Require PostgreSQL and OpenSearch configuration for active reindex commands.
- Support `--org`, `--all-orgs`, `--index`, `--name`, `--complete`, `--drop`, `--no-drop`, `--dry-run`, `--with-timing`, and `--json` where applicable.
- Make missing orgs, missing indexes, provider-unavailable cases, and partial failures produce stable exit codes and redacted messages.
- Add tests for full-org, all-org, index-scoped, data-bag-scoped, named-item, dry-run, missing-org, missing-index, and provider-failure behavior.

### Task 8: Add Search Consistency Check And Repair

- Completed. `search.ConsistencyService` now compares PostgreSQL-derived expected search documents with OpenSearch-visible document IDs for all-org, org-scoped, built-in-index, and data-bag-index scopes.
- The service reports expected/observed/clean/missing/stale/unsupported/upserted/deleted/skipped/failed counts, per-org/index object counts, missing document IDs, stale provider IDs, unsupported scopes, provider-unavailable failures, and duration.
- `opencook admin search check` returns stable drift reports without mutating OpenSearch, while `opencook admin search repair` requires `--dry-run` or `--yes`, upserts missing documents, deletes stale documents, refreshes OpenSearch, and remains idempotent after a successful repair.
- Tests pin clean and drift reports, dry-run no-mutation behavior, idempotent repair, preserved PostgreSQL-derived state and Chef-facing search result shaping, missing org/index exit behavior, and redacted provider failures.

- Add a consistency checker that compares PostgreSQL-derived expected search documents with OpenSearch-visible document IDs for the same scope.
- Report:
  - missing documents
  - stale provider documents
  - unsupported indexes
  - provider-unavailable failures
  - object counts by org/index
- Add repair mode that upserts missing documents and deletes stale documents.
- Prefer idempotent repair behavior; repeated repair after success should report clean.
- Add tests proving repair does not mutate PostgreSQL state and does not change Chef-facing search response shapes.
- Add provider failure tests proving errors remain operationally useful without leaking provider internals.

### Task 9: Add Operational Functional Coverage

- Completed. The Docker functional harness now includes `operational` and `operational-verify` phases in the default flow after OpenSearch update verification and before cleanup.
- The functional test container builds the current `opencook` CLI and proves it can talk to the OpenCook container over the shared Compose network with signed admin requests.
- Operational coverage now includes admin status, live-safe user/org creation, user key creation plus a follow-up signed request with the generated key, group/container/ACL inspection, complete org reindex, stale OpenSearch document detection, dry-run repair, actual repair, clean consistency checks, and post-restart verification.
- The functional docs now document running only the operational phases locally or against a remote Docker daemon.

- Extend the Docker functional harness with admin/reindex phases.
- Prove the CLI can talk to the OpenCook container over the shared Compose network.
- Cover:
  - admin status
  - user/org create through live-safe commands
  - key creation and follow-up signed request
  - group/container/ACL inspection
  - complete org reindex
  - stale OpenSearch document detection and repair
  - restart after repair
- Add script documentation for running only the operational phases against a remote Docker daemon.

### Task 10: Sync Docs And Close The Bucket

- Completed. The roadmap, milestone, compatibility matrix, README, functional testing guide, AGENTS instructions, and this plan now describe the operational admin plus reindex/repair bucket as implemented.
- Encrypted data bag compatibility and broader Lucene/query-string search compatibility are now complete; cookbook/policy/sandbox search coverage is the next recommended bucket, with OpenSearch provider capability/version hardening and migration/cutover tooling left visible as follow-on work.

- Update:
  - `README.md`
  - `docs/functional-testing.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `AGENTS.md`
  - this plan file
- Mark the operational admin plus reindex/repair bucket complete.
- Point the next bucket at encrypted data bag compatibility unless implementation findings make deeper search semantics, cookbook/policy/sandbox search coverage, or migration/cutover tooling more urgent.

## Test Plan

Focused tests:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/admin
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
scripts/functional-compose.sh
```

Required scenarios:

- no-argument `opencook` still starts the server
- `opencook serve` starts the server through the same config path
- CLI admin commands sign requests correctly
- live-safe admin commands preserve existing HTTP route validation and error shapes
- direct PostgreSQL repair commands are explicitly offline/force-gated
- reindex can rebuild OpenSearch from PostgreSQL-backed state for one org and all orgs
- reindex can target built-in indexes and data bag indexes
- consistency check reports missing and stale OpenSearch documents
- repair fixes missing and stale documents idempotently
- OpenSearch provider failures do not leak provider internals
- functional Docker phases prove admin, reindex, repair, and restart behavior

## Assumptions And Defaults

- This bucket adds operational tooling, not new Chef-facing API contracts.
- The first CLI can live under the existing `opencook` binary rather than introducing a separate `opencook-ctl` binary.
- PostgreSQL is required for direct state repair and OpenSearch reindex/repair commands.
- OpenSearch reindex/repair commands are no-ops with clear errors when `OPENCOOK_OPENSEARCH_URL` is absent.
- Online direct PostgreSQL mutation is unsafe until cache invalidation or a live admin control plane exists.
- Backup/restore, secret rotation, HA service control, package installation, migration/cutover tooling, encrypted data bag compatibility, and richer Lucene/query-string behavior stay out of scope for this bucket.
- Licensing and license-management commands remain permanently out of scope for OpenCook.
