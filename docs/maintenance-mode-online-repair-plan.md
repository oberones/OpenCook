# Maintenance Mode And Online Repair Safety Plan

Status: complete

## Summary

This bucket adds an operator-visible maintenance mode and the process-safety
contract needed before OpenCook can safely perform controlled online repair and
cutover operations.

The goal is to prevent write races during backup, restore, source sync, reindex,
repair, and cutover windows without changing successful Chef-facing read
contracts. OpenCook should keep serving compatible reads while blocking only
mutating Chef-facing traffic and clearly reporting the maintenance state through
status, admin commands, logs, runbooks, and functional tests.

Use this file as the reference checklist for this bucket.

## Current State

OpenCook already has:

- PostgreSQL-backed persistence for implemented identity, authorization, core
  object, cookbook, sandbox/checksum, policy, and ACL state.
- Provider-backed blobs in memory, filesystem, and S3-compatible modes.
- Active OpenSearch-backed search for implemented Chef-searchable families.
- `opencook admin reindex`, `search check`, and `search repair` from
  PostgreSQL-backed state.
- OpenCook logical backup/restore, normalized Chef Server source
  inventory/normalize/import/sync, shadow-read comparison, and cutover
  rehearsal.
- `chef-server-ctl`-style operational parity for config validation, status,
  doctor, metrics, request IDs, structured logs, diagnostics, and runbooks.
- Offline-gated direct PostgreSQL repair commands for unsafe repair mutations.

Remaining gap:

- Operators must still freeze writes externally during backup, restore, source
  sync, reindex, repair, and cutover windows.
- Direct PostgreSQL repair mutations remain offline-only because live services
  and verifier/search/cache state do not yet have a safe invalidation contract.
- `/status`, admin status/doctor, and runbooks do not yet report maintenance
  state or write-blocking coverage.

## Interfaces And Behavior

- Do not change Chef-facing route shapes, successful read payloads, signed-auth
  behavior, API-version behavior, or established compatibility responses.
- Maintenance mode should block mutating Chef-facing writes, not every non-GET
  route. Chef-compatible read-like POST routes such as partial search and
  depsolver must remain allowed unless a task proves an upstream-compatible
  reason to block them.
- Keep root/status route payload keys stable. Additive fields are acceptable
  only where existing callers tolerate them; otherwise keep maintenance details
  in existing human-readable status text or admin output.
- Preserve existing `opencook admin` command shapes unless an additive
  subcommand is needed.
- Keep offline repair commands offline-gated until this bucket explicitly adds
  a maintenance-gated online path with cache invalidation and rollback rules.
- Do not add licensing, license enforcement, license telemetry, or
  license-management behavior.
- Do not invent a new Chef client protocol or require client configuration
  changes.

## Maintenance Contract

Maintenance mode should be explicit, auditable, and safe across the deployment
models OpenCook supports:

- In PostgreSQL-backed deployments, maintenance state should be stored in
  PostgreSQL so all OpenCook processes observe the same gate.
- In in-memory standalone mode, maintenance state may be process-local and must
  report that limitation truthfully.
- Maintenance state should include:
  - enabled/disabled state
  - reason
  - operator/requestor where available
  - creation time
  - optional expiration time or advisory duration
  - source command or mode such as `backup`, `source_sync`, `reindex`,
    `repair`, or `cutover`
- Mutating Chef-facing routes should return a stable maintenance error that
  does not leak operator notes, database details, provider failures, or internal
  lock implementation details.
- Successful reads should continue to work and should not gain maintenance-only
  payload changes.
- Admin commands that intentionally perform maintenance operations should either
  require maintenance mode, acquire/release an internal maintenance guard, or
  clearly report why an external guard is still required.

## Proposed Operator Surface

The exact command names can be adjusted during implementation, but the bucket
should converge on an additive surface like:

```text
opencook admin maintenance status [--json] [--with-timing]
opencook admin maintenance enable --reason TEXT [--mode MODE] [--expires-in DURATION] [--yes] [--json] [--with-timing]
opencook admin maintenance disable [--yes] [--json] [--with-timing]
opencook admin maintenance check [--json] [--with-timing]
```

Existing commands should be updated to consume this state where appropriate:

```text
opencook admin status [--json] [--with-timing]
opencook admin service status [--json] [--with-timing]
opencook admin service doctor [--offline] [--json] [--with-timing]
opencook admin reindex ...
opencook admin search repair ...
opencook admin migration ...
```

## Task Breakdown

### Task 1: Inventory And Freeze The Write-Gate Contract

Status: complete.

- Inventory every implemented Chef-facing route and classify it as read-only,
  read-like non-GET, mutating, blob upload/download, or operational-only.
- Explicitly preserve read-like POST routes such as partial search and cookbook
  depsolver.
- Define the maintenance error status code and JSON body by checking upstream
  Chef behavior where available and choosing the least disruptive compatible
  shape where upstream evidence is ambiguous.
- Add route-classification tests that fail if new mutating routes are added
  without a maintenance decision.
- Record the final route families and exceptions in this plan.

Task 1 completion notes:

- Added `internal/api/maintenance_contract.go` as the internal route
  classification source of truth for the future maintenance middleware.
- Added route-drift tests that parse concrete `mux.HandleFunc` patterns in
  `internal/api/router.go` and require each registered pattern to have a
  maintenance decision.
- Froze the Chef-facing maintenance response as HTTP `503` with the upstream
  static JSON shape:

```json
{ "error": "503 - Service Unavailable: Sorry, we are unavailable right now.  Please try again later." }
```

Frozen route-family decisions:

- Operational-only and status routes stay available:
  `/`, `/_status`, `/healthz`, `/readyz`, `/metrics`,
  `/server_api_version`, `/internal/contracts/routes`, and
  `/internal/authn/capabilities`.
- Signed blob downloads stay available, while signed checksum uploads are
  classified as writes:
  `GET /_blob/checksums/{checksum}` is allowed, and
  `PUT /_blob/checksums/{checksum}` is blocked.
- Read-only Chef views stay available, including cookbook collection views,
  cookbook artifact collection views, universe, environment cookbook/node/role
  views, environment recipes, role environment views, search index listings,
  group/container/ACL reads, and policy or policy-group collection reads.
- Chef read-like POST routes stay available:
  `POST /search/{index}`,
  `POST /organizations/{org}/search/{index}`,
  `POST /environments/{name}/cookbook_versions`, and
  `POST /organizations/{org}/environments/{name}/cookbook_versions`.
- Mutating Chef routes are classified for blocking by method, including user,
  organization, client, key, node, role, environment, data bag/item, policy
  revision/group assignment, sandbox create/commit, cookbook version, cookbook
  artifact, and checksum upload writes on both default-org and org-scoped
  aliases.

### Task 2: Add The Maintenance State Model And Store Seam

Status: complete.

- Add a small maintenance state model with enabled, mode, reason, actor,
  created-at, and optional expires-at fields.
- Add an internal store interface for reading, enabling, disabling, and checking
  maintenance state.
- Provide an in-memory implementation for standalone deployments and unit tests.
- Preserve default behavior: maintenance mode is disabled unless explicitly
  enabled.
- Add focused tests for state transitions, expiration handling, redaction, and
  invalid input normalization.

Task 2 completion notes:

- Added `internal/maintenance` with a small `Store` interface that supports
  `Read`, `Enable`, `Disable`, and `Check`.
- Added a process-local `MemoryStore` for standalone deployments and tests.
- Added normalized maintenance state fields for enabled state, mode, reason,
  actor, created time, and optional expiration time.
- Default mode is `manual`; empty mode normalizes to that value while explicit
  task modes such as `source sync` normalize to stable tokens like
  `source_sync`.
- Enable requires a non-empty reason and rejects invalid modes or expirations
  that are not after creation time.
- Expired maintenance windows are inactive for write-gate decisions but remain
  readable so status/admin output can explain what happened.
- `SafeStatus` returns a bounded display copy of operator-provided reason and
  actor text for future status/admin surfaces.
- Added tests for disabled defaults, normalized enable behavior, no mutation on
  invalid enable input, expiration handling, idempotent disable, timestamp copy
  safety, display redaction, and canceled context handling.

### Task 3: Persist Maintenance State In PostgreSQL

Status: complete.

- Add a PostgreSQL migration/table for singleton maintenance state or a small
  append-only state history plus current pointer.
- Wire PostgreSQL-backed maintenance storage into app startup when
  `OPENCOOK_POSTGRES_DSN` is configured.
- Make repeated startup idempotent and ensure every process observes the same
  active state.
- Add `internal/store/pg` tests for migrations, read/write round trips,
  disabled defaults, expiration, and malformed persisted state handling.

Task 3 completion notes:

- Added `internal/store/pg/schema/0004_maintenance_state.sql` with a singleton
  maintenance-state row for enabled state, mode, reason, actor, creation time,
  optional expiration time, and update time.
- Added `pg.MaintenanceRepository`, exposed through `pg.Store.Maintenance()`,
  implementing the maintenance store seam from Task 2.
- Active PostgreSQL maintenance reads and checks query PostgreSQL each time so
  multiple OpenCook processes observe the same current write gate.
- `Enable` upserts the singleton row; `Disable` deletes it and remains
  idempotent for retry-safe operator cleanup.
- App startup activation now applies and loads maintenance persistence together
  with cookbook, bootstrap core, and core object persistence.
- PostgreSQL status/readiness wording now reports active maintenance-state
  persistence while preserving the existing status payload keys.
- Added PostgreSQL repository tests for migration exposure, disabled defaults,
  inactive round trips, active cross-store visibility, expiration behavior, and
  malformed persisted-state activation failures.

### Task 4: Enforce Maintenance Blocking On Mutating Chef Routes

Status: complete.

- Add middleware or route-layer checks that block only classified mutating
  Chef-facing writes.
- Keep successful read routes unchanged, including GET/HEAD reads, partial
  search POST, depsolver POST, and signed cookbook/blob downloads.
- Cover sandbox upload and commit paths, cookbook/artifact mutations,
  user/org/client/key/group/container/ACL writes, node/role/environment/data bag
  writes, policy writes, and any implemented delete paths.
- Ensure blocked writes do not mutate PostgreSQL, memory stores, blob providers,
  search indexes, verifier caches, or progress metadata.
- Add route-level tests across default-org and explicit-org aliases.

Task 4 completion notes:

- Added a router-level maintenance gate that asks `http.ServeMux` for the
  matched concrete route pattern, then consults the Task 1 route contract before
  auth/body parsing or route execution.
- The gate blocks only contract-listed write methods and returns the frozen
  Chef-style HTTP `503` JSON body.
- Read-only routes, signed blob downloads, and read-like POST routes such as
  depsolver and partial search do not query maintenance state and continue to
  use their existing route behavior.
- Maintenance check failures fail closed for write candidates without leaking
  backend details, while reads still avoid the maintenance backend.
- Group, container, and ACL write attempts are now classified as blocked during
  maintenance even where the normal non-maintenance handler still reports
  unimplemented write flows.
- App startup now injects the shared PostgreSQL maintenance repository when
  active and a process-local in-memory maintenance store otherwise.
- Added route-level coverage for representative default-org and org-scoped
  writes, checksum uploads, read preservation, read-like POST preservation,
  no-mutation behavior for blocked writes, expired maintenance windows, and
  fail-closed maintenance-store errors.

### Task 5: Add Admin Maintenance Commands

Status: complete.

- Add `opencook admin maintenance status`.
- Add `maintenance enable` and `maintenance disable` with explicit `--yes`
  confirmation for state changes.
- Support JSON and human output plus `--with-timing`.
- Redact or omit sensitive details and do not print raw DSNs, private key paths,
  signatures, provider bodies, or long operator notes in unsafe contexts.
- Add command tests for success, validation errors, repeated enable/disable,
  PostgreSQL unavailable, in-memory fallback, and JSON shape stability.

Task 5 completion notes:

- Added `opencook admin maintenance status`, `check`, `enable`, and `disable`.
- State-changing commands require explicit `--yes`; enable also requires a
  non-empty `--reason`.
- Commands support `--json`, compact human output, `--with-timing`,
  `--postgres-dsn`, optional `--actor`, optional `--mode`, and optional
  `--expires-in` on enable.
- PostgreSQL-backed commands update the shared maintenance row so every active
  OpenCook process observes the same write gate.
- When PostgreSQL is not configured, the command reports a process-local memory
  fallback and warns that a separate CLI process cannot coordinate a running
  standalone server.
- Output uses the maintenance model's safe status view so operator notes are
  bounded and raw DSNs, private keys, signatures, and provider bodies are not
  emitted.
- Added command coverage for JSON status shape, memory fallback truthfulness,
  repeated enable/disable, validation no-mutation behavior, redacted backend
  open failures, and compact human output.

### Task 6: Surface Maintenance Truth In Status, Doctor, Metrics, And Logs

Status: complete.

- Update `/status` and existing status output wording without removing or
  renaming existing payload keys.
- Update `opencook admin status`, `service status`, and `service doctor` to
  report active maintenance state and backend limitations.
- Add safe Prometheus metrics for maintenance enabled state and blocked write
  counts without high-cardinality route labels or secret-bearing values.
- Add structured logs for maintenance enable/disable and blocked writes with
  request IDs.
- Add tests proving status keys remain stable and maintenance details are
  truthful in PostgreSQL and standalone modes.

Task 6 completion notes:

- `/status`, `/healthz`, and `/readyz` now include a bounded
  `dependencies.maintenance` block while preserving the stable top-level status
  envelope.
- Maintenance readiness reports whether the write gate can be checked; active
  maintenance stays ready because compatible reads continue to serve.
- `opencook admin status` inherits the live `/status` maintenance dependency
  details, while `opencook admin service status` reports whether maintenance is
  configured as process-local memory or PostgreSQL-shared state.
- `opencook admin service doctor` now checks maintenance state and reports
  active/expired/shared/backend details without exposing reason text, actors, or
  DSNs.
- `/metrics` now exposes `opencook_maintenance_enabled`,
  `opencook_maintenance_expired`, and
  `opencook_maintenance_blocked_writes_total` with only low-cardinality
  backend, shared, method, surface, and reason labels.
- Blocked writes emit a `maintenance_write_blocked` structured log event with
  request ID, safe path shape, bounded surface, pattern, reason, status class,
  and maintenance mode only.
- Added coverage for status envelope stability, active maintenance status,
  service doctor maintenance reporting, safe maintenance metrics, blocked-write
  metrics, and redacted blocked-write structured logs.

### Task 7: Gate Migration, Reindex, Repair, And Cutover Workflows

Status: complete.

- Decide which admin workflows should require pre-existing maintenance mode,
  acquire a temporary maintenance guard, or merely warn that an external write
  freeze is required.
- Apply the decision to backup, restore, source import/sync, complete reindex,
  search repair, diagnostics, and cutover rehearsal flows.
- Preserve existing dry-run and preflight behavior wherever possible.
- Ensure failed commands do not leave stale maintenance state unless the user
  explicitly requested a persistent manual maintenance window.
- Add command tests for missing maintenance, active maintenance, dry-run,
  failure cleanup, and warning text.

Task 7 completion notes:

- `opencook admin reindex` now requires a pre-existing active maintenance
  window for every non-dry-run execution before it opens the offline
  PostgreSQL snapshot or OpenSearch target.
- `opencook admin search repair --yes` now requires a pre-existing active
  maintenance window before it opens the offline PostgreSQL snapshot or
  OpenSearch target.
- Reindex dry-runs, search checks, and search-repair dry-runs preserve their
  read-only/preflight behavior and do not check or mutate maintenance state.
- Backup create, restore apply, source import apply, and source sync apply
  remain offline-gated instead of acquiring a temporary live maintenance guard;
  their output now says so explicitly.
- Cutover rehearsal remains read-only but now warns that source Chef writes
  should stay frozen through final sync, shadow reads, reindex checks, and
  smoke checks.
- Diagnostics collection remains read-only and now warns operators that it does
  not acquire maintenance mode for later repair/reindex/cutover work.
- Added command coverage for missing maintenance, active maintenance warnings,
  dry-run bypass, no temporary gate left behind after rejected commands, and
  preservation of caller-managed maintenance state after provider failures.

### Task 8: Define Cache Invalidation And Controlled Online Repair Safety

Status: complete.

- Inventory every mutable cache or derived state affected by direct PostgreSQL
  repair commands: bootstrap service state, verifier key cache, cookbook
  repository/cache, core object store state, search index state, and blob
  checksum references.
- Define the required invalidation/reload hooks before any offline-only repair
  command can become online.
- Add internal reload/invalidation seams where safe, but do not relax offline
  gates until tests prove process-local and PostgreSQL-backed behavior.
- Document which repair commands remain offline-only after this bucket and why.
- Add unit tests for reload hook idempotence and stale-cache prevention.

Task 8 completion notes:

- Mutable in-process state affected by direct PostgreSQL repair now has an
  explicit reload contract:
  - `bootstrap.Service` owns identity/authorization maps, core object maps, and
    the request verifier key cache.
  - `pg.BootstrapCoreRepository` and `pg.CoreObjectRepository` cache activated
    PostgreSQL snapshots for service startup and normal write-through saves.
  - `pg.CookbookRepository` caches activated cookbook organization, version,
    and artifact rows for Chef-facing cookbook reads and checksum-reference
    decisions.
  - OpenSearch remains an external derived projection; reload does not silently
    repair it. Operators must still use `admin reindex` or `admin search
    repair` under maintenance mode.
  - Blob providers remain external content stores; reload does not copy,
    delete, or reconcile provider objects. Blob checksum safety still depends
    on cookbook/sandbox metadata plus provider-specific repair logic.
- Added `bootstrap.Service.ReloadPersistedState()` as an all-or-nothing
  service reload seam. It loads persisted bootstrap/core-object state before
  mutating live maps, rebuilds the verifier key cache, and rolls back to the
  previous maps/cache if key hydration fails.
- Added `pg.Store.ReloadPersistence()` plus repository-level reload methods for
  cookbook, bootstrap-core, and core-object snapshots. The method is idempotent
  and a no-op before activation.
- Existing direct PostgreSQL mutation commands remain offline-only:
  `admin orgs add-user/remove-user`, `admin groups add-actor/remove-actor`,
  `admin server-admins grant/revoke`, and
  `admin acls repair-defaults`.
- Those commands still require process restart after mutation because Task 8
  only supplies the internal reload seams. Task 9 may relax one narrow command
  only after it routes through these seams under active maintenance and proves
  search/cache/blob behavior.
- Added unit coverage proving repository reload idempotence, stale snapshot
  refresh, service map refresh, verifier-key removal/addition, and rollback on
  invalid persisted key material.

### Task 9: Add Maintenance-Gated Online Repair For The Safest Narrow Case

Status: complete.

- Pick one low-risk repair flow, such as ACL default repair or group membership
  repair, only if Task 8 proves a safe invalidation path.
- Require active maintenance mode and explicit `--yes`.
- Apply the mutation through the normal service/store seam where possible rather
  than bypassing live caches.
- Rehydrate or invalidate affected caches before returning success.
- Add tests proving no stale reads, no stale verifier keys, and rollback or
  no-mutation behavior on failure.

Task 9 completion notes:

- Chose default ACL repair as the first online repair path because it mutates
  live authorization documents through `bootstrap.Service` and does not alter
  Chef-facing payloads, request-signing keys, cookbook/blob metadata, or search
  documents.
- Added `bootstrap.Service.RepairDefaultACLs()` as the normal live seam. It
  repairs bootstrap-core and core-object ACLs together, persists through the
  configured stores, and rolls back live maps plus already-saved bootstrap ACLs
  if the core-object save fails.
- Added signed operational route
  `/internal/maintenance/repair/default-acls`. It requires superuser grant
  authorization, active maintenance mode, and `{"yes": true}` before repair.
- Added CLI support:
  `opencook admin acls repair-defaults --online --yes [--org ORG]`.
  The existing offline command remains the default unless operators opt into
  `--online`.
- The repair response explicitly reports `verifier_cache: unchanged` because
  default ACL repair does not create, rename, expire, or delete user/client
  keys. Tests cover signed follow-up reads after repair to prove the verifier
  cache remains usable.
- Remaining direct PostgreSQL mutation commands stay offline-only until they
  have equally narrow live seams and cache/search safety tests.

### Task 10: Add Functional Docker Coverage

- Extend the functional Compose flow with a maintenance phase.
- Prove reads continue during maintenance while representative writes are
  blocked.
- Prove read-like POST routes remain available during maintenance.
- Prove migration/reindex/repair workflows report or require maintenance state
  consistently.
- Include PostgreSQL, OpenSearch, and filesystem-backed blob coverage.
- End with a clear success footer matching the existing functional-test style.

Status: complete.

Implementation notes:

- Added a first-class `maintenance` functional phase to
  `scripts/functional-compose.sh` and
  `scripts/run-functional-tests-in-container.sh`.
- Added `scripts/run-maintenance-functional-tests-in-container.sh`, which
  builds the current CLI, prepares deterministic fixture state, proves reindex,
  search repair, and online ACL repair reject inactive maintenance state,
  enables PostgreSQL-backed shared maintenance, verifies admin status/doctor and
  metrics surfaces, runs signed Chef-facing route checks, and disables
  maintenance before exit.
- Added functional Go coverage proving PostgreSQL-backed reads, OpenSearch
  partial search, depsolver POST, signed cookbook blob downloads, representative
  object writes, cookbook writes, and checksum uploads behave correctly during
  maintenance.
- Updated operational and migration functional scripts so confirmed online
  reindex/search-repair mutations run under an explicit maintenance window,
  while unsupported-surface discovery remains read-only through dry-run checks.
- The maintenance phase prints
  `==> maintenance functional tests passed successfully`, and the Compose
  wrapper still prints the aggregate functional success footer.

### Task 11: Update Operator Documentation And Runbooks

- Update `README.md`, `docs/functional-testing.md`, and
  `docs/chef-server-ctl-operational-runbooks.md`.
- Document how to enable/disable maintenance mode, how to verify it, and what
  it blocks.
- Document the difference between PostgreSQL-backed shared maintenance state and
  process-local standalone maintenance state.
- Add rollback and emergency-disable guidance.
- Keep migration/cutover guidance explicit about external source Chef write
  freezes.

Status: complete.

Implementation notes:

- Updated `README.md` so user-facing capabilities and limitations now describe
  live maintenance-mode write blocking, PostgreSQL-backed shared maintenance
  state, process-local standalone limitations, online default ACL repair, and
  the continuing need to externally freeze source Chef writes during cutover.
- Updated `docs/functional-testing.md` with the maintenance phase, supported
  phase list, targeted command, cleanup/success behavior, and the new coverage
  areas for blocked writes, read-like POSTs, signed blob downloads, and
  maintenance-gated repair/reindex workflows.
- Updated `docs/chef-server-ctl-operational-runbooks.md` with a dedicated
  maintenance-mode runbook, emergency disable guidance, maintenance-gated
  search/reindex steps, source-freeze caveats for migration/cutover, and the
  narrow online ACL default repair runbook.
- Removed obsolete wording that treated live maintenance-mode traffic blocking
  as still unsupported, while keeping omnibus-specific maintenance wrappers out
  of scope.

### Task 12: Sync Roadmap, Milestones, Compatibility Matrix, And Agent Notes

- Update `docs/chef-infra-server-rewrite-roadmap.md`.
- Update `docs/milestones.md`.
- Update `docs/compatibility-matrix-template.md`.
- Update `AGENTS.md`.
- Mark this bucket complete when implementation and functional coverage land.
- Point the next bucket at the highest-risk remaining Chef compatibility gap or
  production-scale migration validation, depending on what testing reveals.

Status: complete.

Implementation notes:

- Updated the roadmap so maintenance-mode write blocking, shared PostgreSQL
  maintenance state, status/doctor/metrics/log visibility, maintenance-gated
  OpenSearch reindex/search repair, and narrow online default ACL repair are
  documented as completed operational capabilities.
- Updated milestones and the compatibility matrix so maintenance mode is no
  longer listed as future work, while direct live upstream extraction and
  production-scale migration/cutover validation remain follow-on work.
- Updated `AGENTS.md` so future agents preserve the completed maintenance
  contract and treat broader online direct PostgreSQL repair mutations as
  follow-on work beyond the current default ACL repair path.
- Marked this bucket complete and pointed the next recommended bucket at
  production-scale migration validation and cutover readiness hardening.

## Test Plan

Focused tests:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/admin
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook
```

Full verification:

```text
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

Functional verification:

```text
scripts/functional-compose.sh maintenance
scripts/functional-compose.sh all
```

Required scenarios:

- Default maintenance-disabled behavior remains unchanged.
- PostgreSQL-backed maintenance state survives restart and is visible to every
  app instance using the same database.
- Standalone in-memory maintenance mode reports process-local limitations.
- Mutating Chef-facing writes are blocked during maintenance with stable error
  shape and no partial mutations.
- Read-only and read-like non-GET routes continue to work during maintenance.
- Sandbox/blob upload writes are blocked while signed blob downloads continue.
- Admin status/doctor/logs/metrics truthfully report active maintenance state.
- Migration, reindex, repair, and cutover commands respect maintenance gates or
  explicitly warn when external freezes remain required.
- Any online repair path introduced by this bucket invalidates or reloads every
  affected cache before reporting success.

## Assumptions And Defaults

- PostgreSQL is the shared coordination backend for production maintenance
  state.
- In-memory standalone mode remains useful for local development but cannot
  coordinate maintenance across multiple processes.
- Maintenance mode protects the OpenCook target. Operators still need to freeze
  writes on an upstream Chef Infra Server source during final source sync and
  cutover.
- Blocking mutating writes is a safety feature, not a new Chef API. We should
  avoid exposing extra maintenance metadata through Chef-facing responses.
- Online repair should remain conservative. It is better to keep a command
  offline-only than to introduce stale-cache or split-brain behavior.
- Production-scale shadow-read tooling beyond normalized artifacts remains a
  follow-on unless it becomes the highest-risk blocker during implementation.

## Non-Goals

- Recreating Chef Infra Server's omnibus supervisor or Redis-backed maintenance
  implementation byte-for-byte.
- Adding new Chef client configuration, new Chef-facing endpoints, or a new
  client protocol.
- Allowing direct PostgreSQL mutations while normal writes are flowing.
- Importing or repairing unsupported Chef object families.
- Adding licensing, license enforcement, license telemetry, or
  license-management endpoints.
