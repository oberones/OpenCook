# Online Repair/Admin Mutation Parity Plan

## Summary

This bucket moves the remaining safe identity and authorization repair mutations
from offline-only PostgreSQL edits to signed, live, maintenance-gated admin
workflows.

The Chef-facing contract is intentionally unchanged. This slice must not change
Chef routes, payloads, auth precedence, search behavior, cookbook/blob behavior,
migration/cutover behavior, or API-version behavior. New live behavior is
internal/admin-only and must go through the live `bootstrap.Service` state so
authorization caches and persisted bootstrap state stay aligned.

## Status

Complete. Online repair/admin mutation parity is implemented for bootstrap
membership repairs: org membership, group membership, server-admin membership,
and default ACL repair now have signed, maintenance-gated live paths with
offline direct PostgreSQL paths preserved.

## Current Contract Inventory

- Offline org membership repair exists through `opencook admin orgs add-user`
  and `opencook admin orgs remove-user` with `--offline --yes`.
- Offline group membership repair exists through `opencook admin groups
  add-actor` and `opencook admin groups remove-actor` with `--offline --yes`.
- Offline server-admin repair exists through `opencook admin server-admins
  grant`, `revoke`, and `list` with direct PostgreSQL inspection/mutation.
- Online default ACL repair already exists at the signed internal route
  `/internal/maintenance/repair/default-acls` and requires active maintenance
  plus explicit confirmation.
- Migration backup, restore, source import, and source sync remain offline-only
  because they replace or replay broad durable state and cannot yet be made safe
  with a narrow live service seam.

## Task Breakdown

### Task 1: Create The Plan And Freeze Boundaries

- Add this plan as the bucket reference.
- Record the offline-only command contract and the existing online default ACL
  repair pattern.
- Keep the Chef-facing API contract frozen.

Completed.

### Task 2: Add Bootstrap Live Mutation Seams

- Add live `bootstrap.Service` methods for org membership, group membership,
  and server-admin membership repair.
- Reuse the existing pure bootstrap mutation helpers.
- Snapshot live state, apply the mutation under lock, persist through the
  bootstrap core store, and roll back live state if persistence fails.
- Keep verifier-key cache state unchanged because these commands do not mutate
  user/client key material.

Completed.

### Task 3: Add Online Org Membership Repair

- Add a signed internal maintenance route for org user add/remove.
- Require `yes=true`, active maintenance, admin authorization, and valid org/user
  inputs.
- Cover add, remove, `--admin`, `--force`, idempotent no-op, missing org,
  missing user, and no-mutation failures.

Completed.

### Task 4: Add Online Group Membership Repair

- Add a signed internal maintenance route for group actor add/remove.
- Preserve actor-type validation for `user`, `client`, and `group`.
- Cover user, client, and group actors plus invalid or missing inputs,
  idempotent no-op behavior, and no-mutation failures.

Completed.

### Task 5: Add Online Server-admin Listing And Repair

- Add a signed internal read-only server-admin listing route.
- Add a signed internal maintenance route for grant/revoke.
- Prove successful changes are visible immediately to authorization checks and
  survive restart/rehydration.

Completed.

### Task 6: Wire CLI Online Mode

- Keep existing `--offline` command behavior unchanged.
- Add explicit `--online --yes` support for online membership mutations.
- Route `server-admins list` through the signed online route by default while
  preserving `--offline` direct PostgreSQL inspection.

Completed.

### Task 7: Pin Safety And Consistency

- Online mutations must fail clearly when maintenance is inactive, maintenance
  state is unavailable, confirmation is missing, auth is insufficient, or
  persistence fails.
- Failed persistence must not partially update live service state.
- Successful mutations must update PostgreSQL-backed state and live authorization
  state without a restart.

Completed.

### Task 8: Extend Functional Coverage

- Add an `admin-repair` functional phase.
- Exercise maintenance enable, online org membership, online group membership,
  server-admin grant/revoke/list, restart persistence, and maintenance disable.
- Keep the phase focused on admin-only behavior and only use Chef-facing checks
  as smoke tests for unchanged signed reads.

Completed.

### Task 9: Sync Docs And Close The Bucket

- Update the roadmap, milestones, compatibility matrix, `AGENTS.md`, functional
  testing docs, runbooks, admin help, and this plan.
- Mark online repair/admin mutation parity complete for bootstrap membership
  repairs.
- Point the next bucket at the highest-risk remaining work discovered after this
  slice.

Completed.

## Test Plan

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./cmd/opencook`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`
- `scripts/functional-compose.sh admin-repair restart admin-repair`

## Assumptions And Defaults

- No Chef-facing route, payload, error-shape, auth-precedence, or API-version
  behavior changes are allowed in this bucket.
- No PostgreSQL schema migration is needed.
- No OpenSearch reindex is required because these membership changes affect
  live authorization state, not indexed document bodies.
- No verifier-cache update is required because key material is unchanged.
- Online dry-run is intentionally not added; operators can use the existing
  offline dry-run path for previews.
- Migration backup, restore, source import, and source sync stay offline-gated.
