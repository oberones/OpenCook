# Chef-Server-CTL Operational Parity And Health Hardening Plan

Status: complete

## Summary

This bucket adds the next layer of operator-facing parity after the PostgreSQL,
OpenSearch, blob, and migration foundations: practical `chef-server-ctl`-style
status, validation, diagnostics, metrics, and runbook workflows.

The goal is not to recreate Chef Infra Server's omnibus supervisor, Redis-backed
maintenance mode, or legacy HA tooling. OpenCook is deployed as a modern single
server process that composes with PostgreSQL, OpenSearch, and a blob provider.
This slice should make that model honest and easy to operate while preserving
Chef/Cinc client compatibility and the existing Chef-facing HTTP contract.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, payloads, authentication behavior, or
  error contracts.
- Keep the current `/`, `/_status`, `/healthz`, and `/readyz` route shapes
  stable. Existing payload keys should not be removed or renamed.
- New operational endpoints must be clearly non-Chef-facing and must not expose
  secrets, private keys, raw DSNs, request signatures, or provider credentials.
- Prefer additive admin CLI commands over changes to existing command behavior.
- Keep offline repair commands explicit with `--offline` and destructive changes
  gated by `--yes` or a dry-run default.
- Do not add licensing, license telemetry, or license-management commands.
- Do not implement live maintenance-mode request blocking in this bucket. That
  changes runtime traffic semantics and needs a separate compatibility decision.

## Current State

OpenCook already has a strong operator foundation:

- `opencook serve` starts the server and reports PostgreSQL/OpenSearch backend
  truth in the startup summary.
- `opencook version` prints build metadata.
- `opencook admin status` performs a signed status request against `/_status`.
- Live admin commands cover user, organization, key, group, container, ACL, and
  client-key inspection or safe mutation flows.
- Offline admin commands cover server-admin membership, organization membership,
  group membership, and ACL default repair against PostgreSQL state.
- `opencook admin reindex` and `opencook admin search check/repair` cover
  OpenSearch rebuild and consistency workflows.
- `opencook admin migration ...` covers backup, restore, source inventory,
  preflight, and cutover rehearsal workflows.
- Public health/status routes currently include `/`, `/_status`, `/healthz`,
  `/readyz`, and internal contract/capability routes.

The remaining gap is the operator affordance layer that Chef Infra Server users
expect from `chef-server-ctl`: configuration validation, service health
summaries, log discovery, safe diagnostics collection, runbook discovery, and
machine-scrapable metrics.

## Task 1 Inventory Snapshot

Status: complete.

This task freezes the compatibility inventory and the intended OpenCook
disposition before implementation begins. The inventory is based on the local
Chef Server checkout under `~/Projects/coding/ruby/chef-server`, especially
`src/chef-server-ctl/plugins`, and on the current OpenCook `opencook admin`
command set.

| Upstream/operator workflow | Current OpenCook support | Disposition for this bucket |
| --- | --- | --- |
| Process status, start, stop, restart, reconfigure | `opencook serve`; process supervision is external | Document service-manager runbooks and add status/doctor commands. Do not add an in-process supervisor. |
| `check-config` | Config loading and redaction exist; no dedicated validation command | Add `opencook admin config check` for environment/config validation without starting the server listener. |
| `gather-logs` | No dedicated log discovery or bundle command | Add log path discovery and a safe diagnostics bundle that excludes secrets by default. |
| `backup` and `restore` | Covered by `opencook admin migration backup/restore` | Keep existing migration commands and add runbook discovery/docs that point operators to them. |
| `reindex` | Covered by `opencook admin reindex` and `opencook admin search check/repair` | Keep commands; integrate into status/doctor/runbook docs where helpful. |
| User, organization, and key management wrappers | Live admin users/orgs/keys commands exist; some edit/delete workflows remain outside this bucket | Document current coverage and avoid inventing Chef-incompatible admin semantics. Remaining CRUD parity stays in API/admin follow-on work. |
| Server-admin management | Offline PostgreSQL commands exist | Keep offline-only until a live-safe invalidation story exists. Surface clearly in runbooks. |
| Group, container, and ACL inspection/repair | Live inspection and offline repair commands exist | Keep current command model and include in diagnostics/runbook output. |
| Maintenance mode | No runtime request-blocking mode | Defer. Upstream's Redis-backed 503/allow-list behavior changes live traffic and needs a separate compatibility decision. |
| Secrets and credential rotation | Environment/provider-managed config; no internal secret store | Defer. Do not add placeholder secret rotation commands that imply unsupported behavior. |
| `psql` helper | Direct database access is possible outside OpenCook | Document external PostgreSQL access patterns; do not add an interactive DB shell helper in this slice. |
| Pedant/test wrapper | Go unit tests and Docker functional scripts exist | Document supported test commands. Do not add a full oc-chef-pedant wrapper here. |
| Install, upgrade, cleanup, legacy HA, rebuild migration state | Not applicable to the current deployment model | Exclude or document as intentionally not implemented. |
| License notices | Apache-2.0 license and NOTICE/trademark docs exist | Do not add license-management commands or license enforcement endpoints. |
| Metrics | No `/metrics` endpoint yet | Add safe Prometheus-compatible operational metrics without secrets or high-cardinality payload values. |
| Request IDs and structured logging | Basic server logging only | Add request IDs and structured operational logs suitable for diagnostics. |

## Proposed Operator Surface

The rest of the bucket should converge on this additive surface:

```text
opencook admin status [--json] [--with-timing]
opencook admin config check [--offline] [--json] [--with-timing]
opencook admin service status [--json] [--with-timing]
opencook admin service doctor [--offline] [--json] [--with-timing]
opencook admin logs paths [--json] [--with-timing]
opencook admin diagnostics collect --output PATH [--offline] [--yes] [--json] [--with-timing]
opencook admin runbook list [--json]
opencook admin runbook show NAME [--json]
GET /metrics
```

Existing commands remain canonical for identity, ACL, reindex, search repair,
and migration operations. The new commands should mostly discover, validate, and
summarize rather than mutate state.

## Task Breakdown

### Task 1: Inventory And Freeze Scope

Status: complete.

- Inventory upstream `chef-server-ctl` plugin workflows.
- Map each workflow to existing OpenCook capabilities.
- Freeze explicit inclusions and exclusions for this bucket.
- Record the proposed additive operator surface in this plan.

### Task 2: Harden Status And Readiness Truthfulness

Status: complete.

- Review `/`, `/_status`, `/healthz`, and `/readyz` payloads and messages.
- Preserve existing keys and route shapes.
- Tighten human-readable wording for PostgreSQL, OpenSearch, blob, and in-memory
  fallback modes.
- Add route tests proving readiness reports dependency failures without leaking
  credentials or changing Chef-facing behavior.

### Task 3: Add Configuration Validation

Status: complete.

- Add `opencook admin config check`.
- Validate required and optional environment-driven configuration for server,
  PostgreSQL, OpenSearch, blob backends, S3-compatible settings, bootstrap
  requestors, limits, and timeout knobs.
- Support `--json` and `--with-timing`.
- Keep secret values redacted in all output.

### Task 4: Add Service Status And Doctor Commands

Status: complete.

- Add `opencook admin service status` for local config/backend reachability
  summaries.
- Add `opencook admin service doctor` for deeper non-mutating diagnostics.
- Include dependency checks for PostgreSQL, OpenSearch, and blob backend
  configuration.
- Keep offline mode explicit where direct PostgreSQL inspection is used.

### Task 5: Add Safe Metrics

Status: complete.

- Add a Prometheus-compatible `/metrics` endpoint.
- Include build/info, request counts/durations, status classes, dependency
  health, search operation counts, blob operation counts, and migration/admin
  command counters where practical.
- Avoid secret values, request signatures, private keys, raw URLs with
  credentials, and unbounded high-cardinality labels.
- Add tests proving `/metrics` is additive and does not affect Chef-facing
  routes.

Implementation note: admin and migration command counters are not exported from
the server process yet because those commands currently run as separate
short-lived CLI processes. This slice avoids fake counters until a shared
metrics sink exists.

### Task 6: Add Request IDs And Structured Operational Logs

Status: complete.

- Generate or preserve request IDs for HTTP traffic.
- Include request IDs in response headers and structured logs.
- Avoid logging request bodies, signatures, private keys, secrets, or raw
  provider credentials.
- Add focused tests for request ID propagation and redaction.

### Task 7: Add Log Discovery And Diagnostics Bundle

Status: complete.

- Add `opencook admin logs paths`.
- Add `opencook admin diagnostics collect --output PATH`.
- Include redacted config, status summaries, dependency summaries, selected
  runbook metadata, and optional log path references.
- Do not include raw private keys, request signatures, credentials, or full
  database/blob dumps.

### Task 8: Add Runbook Discovery And Service-Management Docs

Status: complete.

- Add `opencook admin runbook list` and `opencook admin runbook show NAME`.
- Document systemd, Docker Compose, and Kubernetes-style operational patterns.
- Point backup/restore/reindex/search/migration runbooks at existing commands.
- Document intentionally unsupported upstream omnibus workflows.

Implementation note: service-management docs live in
`docs/chef-server-ctl-operational-runbooks.md`, and the CLI runbook catalog is
also embedded in diagnostics bundles so support artifacts reference the same
operator guidance.

### Task 9: Extend Functional Coverage

Status: complete.

- Extend Docker functional scripts to cover the new config, service, metrics,
  diagnostics, and runbook commands.
- Keep the coverage compatible with standalone, PostgreSQL-backed, OpenSearch,
  and provider-backed blob modes.
- Ensure generated diagnostic artifacts are cleaned up unless `KEEP_STACK=1` or
  an equivalent debug mode is active.

Implementation note: the Docker operational phase now exercises config check,
service status/doctor, `/metrics`, log path discovery, diagnostics bundle
collection, and runbook list/show against the PostgreSQL plus OpenSearch plus
filesystem-blob functional stack. Generated diagnostics archives are removed by
default and retained only for explicit debug runs.

### Task 10: Sync Docs And Close The Bucket

Status: complete.

- Update `README.md`, `AGENTS.md`, `docs/chef-infra-server-rewrite-roadmap.md`,
  `docs/milestones.md`, and `docs/compatibility-matrix-template.md`.
- Mark this bucket complete in this plan.
- Point the next bucket at the remaining roadmap item with the strongest
  compatibility value after operational parity lands.

Implementation note: the operational parity bucket is now closed. The roadmap,
milestones, compatibility matrix, README, and agent guidance describe the
completed config/service/metrics/logging/diagnostics/runbook coverage and point
the next major bucket at Chef Infra Server source import/sync plus
shadow-read/cutover hardening.

## Test Plan

Focused verification:

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

Required scenarios:

- In-memory standalone mode remains usable and truthfully reported.
- PostgreSQL-backed mode reports active durable persistence without leaking DSNs.
- OpenSearch-backed mode reports reachability and search health without leaking
  credentials.
- Filesystem and S3-compatible blob configurations report backend truth without
  leaking credentials.
- Config validation catches missing/invalid required settings before serving.
- Metrics are scrapeable and do not include secrets or unbounded labels.
- Diagnostics bundles contain enough context for support while excluding secrets.
- Functional scripts print clear success messages when all checks complete.

## Assumptions

- Service supervision belongs to systemd, Docker Compose, Kubernetes, launchd, or
  another external process manager, not to the OpenCook server process.
- Maintenance mode is intentionally out of scope because it affects live request
  routing and needs a dedicated compatibility design.
- Secrets and credential rotation remain provider/deployment concerns until
  OpenCook has a formal secret-store abstraction.
- The first metrics endpoint should be Prometheus-compatible plain text because
  it is easy to scrape and does not force an OpenTelemetry collector dependency.
- Diagnostic bundles should prioritize redacted operational context over raw data
  export. Backup/restore remains the migration command family's responsibility.
