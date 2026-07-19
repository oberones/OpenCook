# OpenCook Operational Runbooks

Status: maintenance-mode aware operator runbook catalog.

OpenCook is a compatibility-first Chef Infra Server replacement, but it is not
an omnibus distribution. The `opencook` process serves Chef-compatible HTTP
traffic, while service supervision, restart policy, log retention, secret
delivery, and host/container lifecycle belong to the deployment platform.

Use `opencook admin runbook list` and `opencook admin runbook show NAME` for a
machine-readable copy of the runbook catalog.

## Service Management

OpenCook should run under an external supervisor.

### systemd

Use systemd for host or VM deployments where OpenCook is installed as a binary
or package.

Recommended pattern:

```sh
opencook admin config check --json
systemctl status opencook
systemctl restart opencook
journalctl -u opencook
```

Notes:

- Keep PostgreSQL, OpenSearch, and blob credentials in environment files,
  systemd credentials, or an external secret manager.
- Use `/readyz` for readiness checks and `/metrics` for Prometheus scraping.
- Use `opencook admin service doctor --offline --json` only during a maintenance
  window when direct PostgreSQL inspection is intended.

### Docker Compose

Use Docker Compose for local integration testing and small deployments where the
OpenCook, PostgreSQL, OpenSearch, and provider-compatible blob services share a
private network.

Recommended pattern:

```sh
docker compose ps
docker compose logs opencook
docker compose restart opencook
scripts/functional-compose.sh operational
```

Notes:

- Keep provider credentials in compose environment, secret files, or an external
  secret manager.
- Diagnostics bundles include log references, not copied Docker log contents.
- Compose service names should be stable enough for OpenCook to reach PostgreSQL
  and OpenSearch by DNS name on the shared network.

### Kubernetes

Use Kubernetes for orchestrated deployments where restart, rollout, probes, and
secret delivery are platform-managed.

Recommended pattern:

```sh
kubectl rollout status deployment/opencook
kubectl logs deployment/opencook
kubectl rollout restart deployment/opencook
```

Notes:

- Point readiness probes at `/readyz`.
- Point liveness probes at `/healthz`.
- Point metrics scrapers at `/metrics`.
- Store PostgreSQL, OpenSearch, and blob credentials in Kubernetes Secrets or an
  external secret manager.

## Maintenance Mode

Use maintenance mode to freeze OpenCook mutating Chef-facing writes while
keeping compatible reads available. This is the supported guard for online
OpenSearch reindex/repair and the first live ACL repair path.

Recommended pattern:

```sh
opencook admin maintenance status --json
opencook admin maintenance enable --mode repair --reason "operator maintenance window" --yes --json
opencook admin maintenance check --json
opencook admin --json status
opencook admin service doctor --offline --json
opencook admin maintenance disable --yes --json
```

Notes:

- PostgreSQL-backed deployments store maintenance state in PostgreSQL, so every
  OpenCook process using the same database observes the same write gate.
- Standalone no-PostgreSQL deployments use process-local maintenance state.
  Status and admin output report that limitation; a separate CLI process cannot
  coordinate a running standalone server.
- Maintenance mode blocks mutating Chef-facing writes with a stable static
  Chef-style `503` response. Read-only routes, partial-search POST, depsolver
  POST, status routes, metrics, and signed blob downloads remain available.
- `opencook admin maintenance disable --yes --json` is idempotent and is the
  emergency cleanup command if an operator workflow fails after enabling
  maintenance.
- `--expires-in` can add a safety timeout, but operators should still disable
  maintenance explicitly after checks pass so status does not show an expired
  stale window.

## Backup And Restore

Use the migration command family for logical backup and restore. Diagnostics
bundles are not backups.

Recommended pattern:

```sh
opencook admin migration backup create --output PATH --offline --yes --json
opencook admin migration backup inspect PATH --json
opencook admin migration restore preflight PATH --offline --json
opencook admin migration restore apply PATH --offline --yes --json
```

Notes:

- Backup and restore are explicit offline maintenance workflows.
- OpenCook maintenance mode guards a running OpenCook target; backup and restore
  commands still require `--offline` because they read or replace PostgreSQL and
  blob state directly.
- Restore applies to OpenCook logical backup bundles, not arbitrary raw
  PostgreSQL dumps or Chef Server internal state.

## Search And Reindex

Use PostgreSQL-backed state as the source of truth for OpenSearch repair and
reindex workflows.

Recommended pattern:

```sh
opencook admin search check --all-orgs --json
opencook admin search repair --all-orgs --dry-run --json
opencook admin maintenance enable --mode repair --reason TEXT --yes --json
opencook admin maintenance check --json
opencook admin search repair --all-orgs --yes --json
opencook admin reindex --all-orgs --complete --json
opencook admin search check --all-orgs --json
opencook admin maintenance disable --yes --json
```

Notes:

- Non-dry-run search repair and reindex commands require active maintenance
  mode before they mutate OpenSearch.
- Keep maintenance mode active until repair/reindex and the follow-up
  `search check` pass. Disable maintenance before returning traffic to normal
  write behavior.
- Unsupported search indexes remain intentionally rejected instead of silently
  fabricated.

## Migration And Cutover

Use preflight, source inventory/normalize or live source extraction,
import/sync, backup/restore, restored-target reindex, shadow comparison, and
cutover rehearsal before switching clients. Migration command JSON includes an
`operator_report` section with inventory totals, finding counts, dependency
evidence, retry guidance, and safe next steps; read that section first, then
drill into `dependencies`, `findings`, and `planned_mutations` when a gate is
not clear.

Prepared artifact pattern:

```sh
opencook admin migration preflight --all-orgs --json
opencook admin migration source inventory PATH --json
opencook admin migration source normalize PATH --output normalized-source --yes --json
opencook admin migration source import preflight normalized-source --offline --json
opencook admin migration source import apply normalized-source --offline --yes --progress source-import-progress.json --json
opencook admin migration source sync preflight normalized-source --offline --progress source-sync-progress.json --json
opencook admin migration source sync apply normalized-source --offline --yes --progress source-sync-progress.json --json
opencook admin maintenance enable --mode reindex --reason "post-sync reindex" --yes --json
opencook admin reindex --all-orgs --complete --json
opencook admin maintenance disable --yes --json
opencook admin search check --all-orgs --json > search-check.json
opencook admin migration shadow compare --source normalized-source --target-server-url URL --json > shadow-compare.json
opencook admin migration cutover rehearse --manifest PATH --source normalized-source --source-import-progress source-import-progress.json --source-sync-progress source-sync-progress.json --search-check-result search-check.json --shadow-result shadow-compare.json --source-frozen --rollback-ready --server-url URL --json
```

Direct live source pattern:

```sh
opencook admin migration source live preflight --source-postgres-dsn DSN --source-bookshelf-root PATH --org ORG --json
opencook admin migration source live extract --source-postgres-dsn DSN --source-bookshelf-root PATH --copy-blobs --org ORG --output live-source --yes --json
opencook admin migration source import preflight live-source --offline --json
opencook admin migration source import apply live-source --offline --yes --progress source-import-progress.json --json
opencook admin migration source sync apply live-source --offline --yes --progress source-sync-progress.json --json
opencook admin migration backup create --output restored-target-backup --offline --yes --json
opencook admin maintenance enable --mode reindex --reason "post-source-sync reindex" --yes --json
opencook admin reindex --all-orgs --complete --json
opencook admin maintenance disable --yes --json
opencook admin search check --all-orgs --json > search-check.json
opencook admin migration shadow compare --source live-source --target-server-url URL --json > shadow-compare.json
opencook admin migration cutover rehearse --manifest restored-target-backup/manifest.json --source live-source --source-import-progress source-import-progress.json --source-sync-progress source-sync-progress.json --search-check-result search-check.json --shadow-result shadow-compare.json --source-frozen --rollback-ready --server-url URL --json
```

Notes:

- Live source preflight and extract use read-only source PostgreSQL access and
  do not mutate source Chef or the OpenCook target. Import/sync still mutate
  the target and therefore remain `--offline` workflows.
- Freeze source Chef writes before the final source sync and keep the freeze
  through shadow-read comparison, cutover rehearsal, client cutover, and
  post-cutover smoke checks. OpenCook can report whether the operator confirmed
  the freeze with `--source-frozen`, but it cannot enforce writes still routed
  to source Chef Infra Server.
- OpenCook maintenance mode only blocks writes routed to OpenCook. It does not
  block writes still routed to the source Chef Infra Server.
- Source import and source sync apply remain offline-gated because they mutate
  target PostgreSQL/blob state directly. Run them against a stopped OpenCook
  target or under an externally frozen target window.
- Use live extraction `--copy-blobs` with a local `--source-bookshelf-root` when
  the migration operator can read checksum blob bytes. This creates the most
  self-contained normalized bundle and lets import/cutover verify copied
  checksum content.
- Use live extraction `--reference-blobs` when checksum bytes must remain in an
  external source provider. Reference-only bundles require separate provider
  reachability and checksum-content validation before cutover because OpenCook
  cannot prove blob bytes from the bundle alone.
- Treat source PostgreSQL DSNs, source blob URLs, source Chef HTTP private keys,
  signed URLs, and provider responses as secrets. OpenCook redacts known
  credential fields in JSON output, diagnostics, and runbook summaries, but
  operators should still pass secrets through environment files, secret
  managers, or shell-safe mechanisms instead of pasteable command history.
- Switch DNS/load balancers or Chef/Cinc `chef_server_url` only after blocker
  gates pass.
- Keep the source Chef Infra Server read/write path available until post-cutover
  smoke checks pass.
- For emergency rollback, point clients or load balancers back at source Chef,
  keep OpenCook target writes frozen, preserve the failed migration reports, and
  rerun source sync/rehearsal only after the blocker is understood.
- Cutover rehearsal errors are blockers; warnings are advisories that require an
  explicit operator decision.

### Production-Scale Functional Drill

Use the production-scale functional phases before release candidates or operator
cutover rehearsals. The default `small` profile is CI-friendly; `medium` and
`large` are opt-in and slower.

```sh
scripts/functional-compose.sh migration-scale-all
OPENCOOK_FUNCTIONAL_SCALE_PROFILE=medium scripts/functional-compose.sh migration-scale-all
OPENCOOK_FUNCTIONAL_SCALE_PROFILE=large scripts/functional-compose.sh migration-scale-all
DOCKER_HOST=ssh://example-host OPENCOOK_FUNCTIONAL_SCALE_PROFILE=medium scripts/functional-compose.sh migration-scale-all
```

Scale fixtures, backup bundles, progress files, and JSON reports are generated
inside the Compose-managed functional state volume. They are removed by default
with the stack; set `KEEP_STACK=1` to keep containers running or
`OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` to remove containers while preserving the
named volumes for report inspection.

## Deployment Evidence Triage

Use deployment evidence before choosing the next compatibility or migration
safety patch. The runner delegates to the existing functional Compose harness,
captures redacted logs, and writes a manifest that operators can attach to an
issue or handoff.

Recommended pattern:

```sh
scripts/deployment-evidence.sh smoke
scripts/deployment-evidence.sh migration
OPENCOOK_FUNCTIONAL_SCALE_PROFILE=small scripts/deployment-evidence.sh scale
```

Notes:

- Evidence is written under `.local/deployment-evidence/<timestamp>` unless
  `OPENCOOK_DEPLOYMENT_EVIDENCE_DIR` is set.
- Fix only deterministic harness issues or evidence-backed Chef compatibility
  regressions discovered by the run.
- Existing functional overrides still apply, including `DOCKER_HOST`,
  `POSTGRES_IMAGE`, `OPENSEARCH_IMAGE`, `KEEP_STACK`, and `REBUILD`.
- Set `KEEP_STACK=1` or `OPENCOOK_FUNCTIONAL_KEEP_ARTIFACTS=1` when
  Compose-managed migration or diagnostics artifacts must survive cleanup.

## Diagnostics

Use diagnostics for support handoff context, not state export.

Recommended pattern:

```sh
opencook admin logs paths --json
opencook admin diagnostics collect --output PATH --yes --json
```

Diagnostics bundles include:

- redacted configuration checks
- service status and doctor summaries
- log discovery references
- runbook metadata
- a manifest with bundle contents

Diagnostics bundles intentionally exclude:

- private keys
- Chef request signatures
- raw DSNs or provider URLs with credentials
- raw database dumps
- blob object contents
- copied log files

## Identity And ACL Repair

Use online default ACL repair only when OpenCook is running, PostgreSQL-backed
maintenance mode is active, and the repair needs live in-process state to stay
consistent without a restart.

Recommended pattern:

```sh
opencook admin maintenance enable --mode repair --reason "repair default ACLs" --yes --json
opencook admin --json acls repair-defaults --online --yes
opencook admin --json acls get org ORG
opencook admin maintenance disable --yes --json
```

For bootstrap membership repair, use the same explicit maintenance window and
the signed online commands:

```sh
opencook admin maintenance enable --mode repair --reason "repair membership" --yes --json
opencook admin --json orgs add-user ORG USER --online --yes [--admin]
opencook admin --json groups add-actor ORG GROUP ACTOR --online --yes --actor-type user
opencook admin --json server-admins grant USER --online --yes
opencook admin --json server-admins list
opencook admin maintenance disable --yes --json
```

Notes:

- The online default ACL repair route requires signed superuser authorization,
  active maintenance mode, and explicit `--yes`.
- Online membership repair also requires signed superuser authorization, active
  maintenance mode, and explicit `--yes`.
- The repair responses report changed ACLs or memberships and state that
  verifier key cache state is unchanged.
- Offline direct PostgreSQL repair commands remain available for stopped-server
  repair and inspection, while broad migration restore/import/sync workflows
  remain offline-gated.

## Unsupported Omnibus Workflows

The following upstream-style `chef-server-ctl` workflows are intentionally not
implemented:

- Embedded process supervision: use systemd, Docker Compose, Kubernetes,
  launchd, or another external supervisor.
- `reconfigure`: OpenCook configuration is environment-driven; run
  `opencook admin config check --json` and restart through the supervisor.
- Licensing and license telemetry: OpenCook is Apache-2.0 software and has no
  licensing subsystem or license-management endpoints.
- Redis-backed or omnibus-specific maintenance-mode wrappers: use OpenCook's
  maintenance-mode traffic blocking gate through `opencook admin maintenance
  ...` and deployment-platform supervision instead.
- Interactive `psql` wrapper: direct database access remains an
  operator/platform concern; supported unsafe mutations are exposed as explicit
  offline admin commands.
- Secret rotation helpers: provider credentials remain deployment/secret-manager
  concerns until OpenCook has a formal secret-store abstraction.
