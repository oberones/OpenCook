# Chef-Server-CTL Operational Parity And Health Hardening Plan

Status: planned

## Summary

This bucket broadens OpenCook's operator-facing surface after the PostgreSQL,
provider-backed blob, active OpenSearch, admin/reindex/repair, and
migration/cutover buckets. The goal is to make OpenCook easier to operate in
standalone, containerized, and externally-managed deployments without changing
Chef-facing API behavior.

The target is `chef-server-ctl`-style operational confidence, not omnibus
packaging parity. OpenCook should provide truthful service status, configuration
validation, safe diagnostics, metrics, structured operational signals, and
runbooks that compose with the existing `opencook admin` and migration tooling.

Use this file as the reference checklist for the operational parity, health,
metrics, and service-management hardening bucket.

## Current State

OpenCook already has:

- `/readyz`, `/_status`, and admin status surfaces with PostgreSQL,
  OpenSearch, and blob backend reporting.
- Signed HTTP-backed `opencook admin` workflows for live-safe user, org, key,
  group, container, ACL, status, reindex, search check/repair, and migration
  operations.
- Offline-gated direct PostgreSQL repair commands for unsafe mutations until
  online cache invalidation exists.
- PostgreSQL as the source of truth for implemented persisted state.
- OpenSearch as a derived index with reindex/check/repair commands.
- Provider-backed blob storage with memory, filesystem, and S3-compatible
  modes.
- Docker functional coverage for PostgreSQL, OpenSearch, filesystem blobs,
  admin commands, migration backup/restore/rehearsal, and restart/rehydration.

OpenCook does not yet have:

- a broad `chef-server-ctl` workflow inventory mapped to OpenCook commands
- config validation beyond startup failure behavior
- a stable metrics endpoint or documented metrics vocabulary
- structured request logging with request IDs across all served requests
- operator-friendly service diagnostics and log discovery commands
- documented service-management runbooks for local binaries, Docker Compose,
  containers, and externally-managed PostgreSQL/OpenSearch/blob providers
- production-oriented diagnostic bundles that are safe to share

## Interfaces And Behavior

- Do not change Chef-facing routes, payloads, status codes, signed-auth
  semantics, or API-version behavior.
- Do not add Chef licensing, license enforcement, license telemetry, or
  license-management endpoints.
- Keep `/readyz`, `/_status`, root/status route shapes, and existing status
  payload keys stable. Additive fields may be introduced only when they are
  clearly operational and covered by tests.
- Keep PostgreSQL as authoritative state and OpenSearch as derived state.
- Keep direct PostgreSQL mutations offline-gated unless they go through existing
  live-safe HTTP routes.
- Redact PostgreSQL DSNs, OpenSearch credentials, blob credentials, private
  keys, signed URLs, request signatures, and provider response bodies from logs,
  status, diagnostics, and JSON command output.
- Prefer commands under the existing `opencook admin` namespace instead of
  adding a second operational CLI.
- Treat service start/stop/restart as deployment-environment actions unless a
  narrow helper can be implemented without pretending OpenCook controls systemd,
  Docker, Kubernetes, or external managed services.
- Keep metrics and diagnostics unauthenticated only if they expose no secrets
  and match explicitly documented deployment guidance. Otherwise require local
  CLI access, explicit opt-in, or signed admin access.

## Proposed Operator Surface

Task 1 should confirm or adjust this surface after upstream inventory, but this
is the initial target shape:

```sh
opencook admin status [--json] [--with-timing]
opencook admin config check [--offline] [--json] [--with-timing]
opencook admin service status [--json] [--with-timing]
opencook admin service doctor [--offline] [--json] [--with-timing]
opencook admin logs paths [--json]
opencook admin diagnostics collect --output PATH [--offline] [--yes] [--json]
opencook admin runbook list [--json]
opencook admin runbook show NAME [--json]
```

HTTP/runtime targets to evaluate:

```text
GET /readyz
GET /_status
GET /metrics
```

Command principles:

- `status` remains a live signed HTTP view of the running server when configured.
- `config check` validates runtime configuration and dependency reachability
  without serving traffic.
- `service status` summarizes process and dependency state without assuming a
  specific supervisor.
- `service doctor` combines safe config, dependency, status, storage, and search
  checks into one operator-friendly diagnosis.
- `logs paths` reports where OpenCook would write or read logs in the current
  deployment mode without collecting log contents.
- `diagnostics collect` emits a redacted, portable support bundle and requires
  explicit confirmation because it can include topology and object counts.
- `runbook list/show` exposes local operational guidance for backup, restore,
  reindex, cutover rehearsal, service restart, and troubleshooting.
- `/metrics` should be Prometheus-compatible if implemented as an HTTP endpoint,
  but must not expose object names, signed URLs, keys, secrets, or request body
  fragments.

## Upstream And Local Reference Points

Upstream Chef Server references to inspect during Task 1:

- `/Users/oberon/Projects/coding/ruby/chef-server/src/chef-server-ctl/`
- `/Users/oberon/Projects/coding/ruby/chef-server/src/chef-server-ctl/plugins/`
- `/Users/oberon/Projects/coding/ruby/chef-server/dev-docs/`
- `/Users/oberon/Projects/coding/ruby/chef-server/oc-chef-pedant/`

Local OpenCook seams to preserve and build on:

- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/command.go`
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_commands.go`
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_reindex.go`
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_search.go`
- `/Users/oberon/Projects/coding/go/OpenCook/cmd/opencook/admin_migration.go`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/api/router.go`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/config/config.go`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/blob/store.go`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/search/`
- `/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/`
- `/Users/oberon/Projects/coding/go/OpenCook/docs/functional-testing.md`

## Task Breakdown

### Task 1: Inventory Operator Workflows And Freeze Scope

- Inspect upstream `chef-server-ctl` commands and local OpenCook admin commands.
- Create a compatibility map for:
  - status and health
  - service status/start/stop/restart/reconfigure
  - config validation
  - log discovery
  - backup/restore/runbook discovery
  - diagnostics and support bundles
  - search reindex/check/repair
  - user/org/key/group/container/ACL administration
- Mark each workflow as:
  - implemented
  - implement as live-safe `opencook admin`
  - implement as offline-gated CLI
  - document-only because the deployment supervisor owns it
  - intentionally excluded
- Freeze non-goals:
  - no omnibus packaging clone
  - no systemd/Docker/Kubernetes control plane inside OpenCook
  - no Chef-facing route changes
  - no licensing endpoints
- Update this plan with the inventory results before implementing later tasks.

### Task 2: Harden Status And Readiness Truthfulness

- Audit `/readyz`, `/_status`, root/status helpers, and `opencook admin status`.
- Preserve existing payload keys while making human-readable wording consistent
  for:
  - in-memory fallback mode
  - active PostgreSQL mode
  - memory/filesystem/S3-compatible blob modes
  - active OpenSearch mode
  - OpenSearch unavailable or unconfigured mode
- Add tests for:
  - startup status with no PostgreSQL
  - active PostgreSQL plus filesystem blob plus OpenSearch
  - active PostgreSQL plus S3-compatible blob configuration using safe doubles
  - provider-unavailable degradation
  - redaction of credentials and signed URLs
- Ensure readiness reports dependency failures without exposing provider internals.

### Task 3: Add Configuration Validation Command

- Add `opencook admin config check`.
- Validate config structure without starting the HTTP server:
  - bootstrap requestor name/type/key path
  - default organization ambiguity risks
  - PostgreSQL DSN presence and optional reachability
  - OpenSearch URL presence and optional reachability
  - blob backend selection and provider-specific required fields
  - S3-compatible endpoint, region, path-style settings, and credential presence
  - filesystem blob path normalization and directory accessibility
- Support `--offline`, `--json`, and `--with-timing`.
- Return existing admin exit-code categories for success, usage errors, partial
  warnings, and dependency-unavailable failures.
- Prove redaction for DSNs, credentials, private-key paths where appropriate,
  and provider error bodies.

### Task 4: Add Service Status And Doctor Commands

- Add `opencook admin service status`.
- Add `opencook admin service doctor`.
- Report process-independent service state:
  - configured service name/environment
  - live server reachability when `OPENCOOK_ADMIN_SERVER_URL` is configured
  - dependency readiness for PostgreSQL, OpenSearch, and blob provider
  - search consistency summary when PostgreSQL and OpenSearch are configured
  - migration readiness summary hooks without running destructive checks
- Keep supervisor-specific start/stop/restart as documented runbook actions, not
  fake in-process control.
- Add tests for JSON/human output, partial status, unavailable providers, and
  redaction.

### Task 5: Add Prometheus-Compatible Metrics

- Decide whether `/metrics` is always enabled, opt-in, or bound by deployment
  config.
- Add a minimal Prometheus text endpoint if enabled:
  - build/version info with safe labels
  - process uptime
  - HTTP request totals by route family/status class/method
  - request duration histogram or summary buckets
  - dependency status gauges for PostgreSQL, OpenSearch, and blob backend
  - search reindex/check/repair counters where already tracked
- Avoid high-cardinality labels such as object names, org names, requestors,
  checksums, signed URLs, and raw paths.
- Add route tests for content type, payload stability, disabled behavior if
  opt-in, and no-secret output.

### Task 6: Add Structured Request Logging And Request IDs

- Add request ID propagation:
  - accept an inbound request ID header if safe
  - generate one when absent
  - include it in responses and structured logs
- Introduce structured logs for:
  - request start/finish
  - auth failure class without leaking signatures
  - dependency activation failures
  - provider-unavailable events with redacted messages
- Keep logs plain enough for container stdout while preserving machine parsing.
- Add tests for request ID response headers, log redaction, and stable status
  class/method/path-family fields.

### Task 7: Add Log Discovery And Safe Diagnostic Bundles

- Add `opencook admin logs paths`.
- Add `opencook admin diagnostics collect --output PATH --yes`.
- Include only redacted and safe diagnostics:
  - build/version info
  - redacted config
  - status/admin status output
  - dependency summaries
  - object-family inventory counts
  - search consistency summary
  - migration runbook pointers
  - recent local log excerpts only if a configured file path exists and secrets
    can be redacted predictably
- Exclude:
  - private keys
  - public/private key PEM material unless explicitly safe and necessary
  - signed URLs
  - bearer tokens or request signatures
  - raw data bag item bodies
  - provider response bodies
- Add archive integrity tests and redaction tests.

### Task 8: Add Runbook Discovery And Service-Management Docs

- Add local runbook markdown under `docs/runbooks/`.
- Add `opencook admin runbook list` and `opencook admin runbook show NAME`, or
  document why direct docs are better if CLI embedding would create maintenance
  churn.
- Cover:
  - standalone local binary startup
  - Docker Compose startup and restart
  - container image deployment with external PostgreSQL/OpenSearch/blob storage
  - config validation
  - health/readiness checks
  - OpenSearch reindex/check/repair
  - backup/restore/cutover rehearsal
  - safe shutdown and restart expectations
  - what OpenCook intentionally does not manage
- Link these runbooks from README and existing functional/migration docs.

### Task 9: Extend Functional Coverage

- Extend the Docker functional stack to cover the new operational surfaces:
  - config check
  - service status
  - service doctor
  - metrics endpoint if enabled
  - request ID/logging smoke checks where practical
  - diagnostics bundle generation and redaction checks
  - runbook list/show or docs presence
- Keep functional phases composable so operators can run the operational subset
  without always running migration restore/rehearsal.
- Ensure tests run against PostgreSQL, OpenSearch, and filesystem-backed blobs
  on the shared Docker network.

### Task 10: Sync Docs And Close The Bucket

- Update:
  - `README.md`
  - `docs/chef-infra-server-rewrite-roadmap.md`
  - `docs/milestones.md`
  - `docs/compatibility-matrix-template.md`
  - `docs/functional-testing.md`
  - `AGENTS.md`
  - this plan file
- Mark the bucket complete after tests and functional coverage pass.
- Point the next bucket at either:
  - live Chef Infra Server source import/sync and deeper shadow-read cutover
    guidance, or
  - deployment-test-discovered Chef compatibility gaps if those become higher
    risk.

## Test Plan

Focused tests:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/config
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/search
```

Full verification:

```sh
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```sh
scripts/functional-compose.sh operational
scripts/functional-compose.sh migration-preflight migration-backup
```

Additional checks:

- Verify no operational output leaks DSNs, credentials, private-key material,
  signed URLs, request signatures, or provider response bodies.
- Verify metrics labels are bounded and do not include object names, org names,
  requestors, checksums, or raw paths.
- Verify status and readiness payload keys remain stable.
- Verify existing Chef-facing route tests still pass unchanged.

## Assumptions

- The first implementation should prefer additive operator surfaces over
  changing existing status payload shapes.
- `opencook admin` remains the primary operator CLI namespace.
- Service lifecycle control belongs to deployment tooling unless OpenCook can
  provide a truthful helper without assuming a supervisor.
- Prometheus-compatible metrics are preferred over inventing a custom metrics
  format.
- Structured logs should be useful from container stdout without requiring a
  separate logging stack.
- Diagnostics are support bundles, not backups. They must not include secret
  material or authoritative object payloads.
- Live Chef Infra Server import/sync remains a follow-on bucket.
