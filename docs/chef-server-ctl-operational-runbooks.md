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

Use preflight, source inventory/normalize/import/sync, backup/restore,
restored-target reindex, shadow comparison, and cutover rehearsal before
switching clients.

Recommended pattern:

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
opencook admin migration cutover rehearse --manifest PATH --source normalized-source --source-import-progress source-import-progress.json --source-sync-progress source-sync-progress.json --search-check-result search-check.json --shadow-result shadow-compare.json --rollback-ready --server-url URL --json
```

Notes:

- Freeze source Chef writes before the final source sync and keep the freeze
  through post-cutover smoke checks.
- OpenCook maintenance mode only blocks writes routed to OpenCook. It does not
  block writes still routed to the source Chef Infra Server.
- Source import and source sync apply remain offline-gated because they mutate
  target PostgreSQL/blob state directly. Run them against a stopped OpenCook
  target or under an externally frozen target window.
- Switch DNS/load balancers or Chef/Cinc `chef_server_url` only after blocker
  gates pass.
- Keep the source Chef Infra Server read/write path available until post-cutover
  smoke checks pass.
- Cutover rehearsal errors are blockers; warnings are advisories that require an
  explicit operator decision.

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

Notes:

- The online default ACL repair route requires signed superuser authorization,
  active maintenance mode, and explicit `--yes`.
- The repair response reports whether bootstrap ACLs or core-object ACLs
  changed and states that verifier key cache state is unchanged.
- Other direct PostgreSQL membership and ACL repair commands remain
  offline-gated until they have equally narrow live cache/search safety seams.

## Unsupported Omnibus Workflows

The following upstream-style `chef-server-ctl` workflows are intentionally not
implemented:

- Embedded process supervision: use systemd, Docker Compose, Kubernetes,
  launchd, or another external supervisor.
- `reconfigure`: OpenCook configuration is environment-driven; run
  `opencook admin config check --json` and restart through the supervisor.
- Licensing and license telemetry: OpenCook is Apache-2.0 software and has no
  licensing subsystem or license-management endpoints.
- Redis-backed or omnibus-specific maintenance-mode wrappers: use
  `opencook admin maintenance ...` and deployment-platform supervision instead.
- Interactive `psql` wrapper: direct database access remains an
  operator/platform concern; supported unsafe mutations are exposed as explicit
  offline admin commands.
- Secret rotation helpers: provider credentials remain deployment/secret-manager
  concerns until OpenCook has a formal secret-store abstraction.
