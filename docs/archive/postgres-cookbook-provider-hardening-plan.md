# Postgres + Provider-backed Cookbook Lifecycle Hardening Plan

Status: completed on 2026-04-23

## Purpose

This document captured the mixed-path hardening slice after PostgreSQL-backed cookbook persistence went live.

The goal was to prove and tighten the cookbook/artifact lifecycle when:

- cookbook metadata is persisted in PostgreSQL
- cookbook file blobs live in a provider-backed blob store
- the public Chef-facing cookbook HTTP contract remains unchanged

## Slice Boundary

In scope:

- active PostgreSQL-backed cookbook/artifact reads, writes, and restart rehydration
- provider-backed blob downloads, checksum preconditions, and cleanup behavior on the active PostgreSQL path
- shared-checksum retention and unique-checksum cleanup against persisted cookbook state
- `blob_unavailable` degradation on the active PostgreSQL path
- truthful app/status reporting for active PostgreSQL plus provider-backed blob configurations

Out of scope:

- replacing the rest of the bootstrap in-memory state with PostgreSQL
- live query-on-read redesign or cross-process cache invalidation
- S3 network integration tests
- validator bootstrap compatibility
- broader OpenSearch/search replacement

## Compatibility Contract

For this slice we preserved:

- the existing `/cookbooks`, `/cookbook_artifacts`, `/universe`, and signed-download route shapes
- the current `bootstrap.CookbookStore` seam and blob interfaces
- the current activated PostgreSQL repository/cache model
- existing `503 {"error":"blob_unavailable",...}` degradation where that contract already existed
- the existing `/status` payload shape, while allowing backend wording and human-readable messages to become more truthful

## Definition Of Done

- active PostgreSQL-backed cookbook reads/downloads are pinned against a real activated `pg.Store`
- active PostgreSQL-backed cookbook mutations are pinned against provider-backed checksum checks and restart rehydration
- checksum cleanup and shared-checksum retention are pinned against persisted cookbook rows, not only same-process state
- provider-unavailable cookbook failures on the active PostgreSQL path preserve the current `blob_unavailable` contract and avoid partial metadata mutation
- status and activation reporting no longer describe the live cookbook/provider path as scaffold-only

## Task Summary

### Task 1: Freeze The Mixed Postgres + Blob Contract

Status:

- completed on 2026-04-23

What landed:

- a reusable activated-PostgreSQL test harness via [internal/store/pg/pgtest/driver.go](/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/pgtest/driver.go)
- active PostgreSQL router helpers via [internal/api/router_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/router_test.go)
- a concrete mixed-path contract inventory in this document

### Task 2: Pin Active Postgres + Provider-backed Read/Download Parity

Status:

- completed on 2026-04-23

What landed:

- active PostgreSQL + filesystem-backed read/download coverage in [internal/api/cookbook_pg_provider_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_pg_provider_routes_test.go)
- default-org and explicit-org coverage for:
  - cookbook collections
  - named cookbook reads
  - `_latest`
  - `_recipes`
  - universe
  - cookbook artifact reads
  - signed cookbook and artifact file downloads
- restart/rehydration coverage proving the first request on a fresh app/router sees persisted cookbook state

### Task 3: Pin Active Postgres + Provider-backed Mutation Parity

Status:

- completed on 2026-04-23

What landed:

- active PostgreSQL + provider-backed mutation coverage in [internal/api/cookbook_pg_provider_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_pg_provider_routes_test.go)
- persisted-state verification for:
  - cookbook version create/update/delete
  - cookbook artifact create/delete
  - repeated-`PUT` and missing-checksum no-mutation behavior
- restart coverage proving successful writes and deletes survive app reconstruction

### Task 4: Harden Checksum Cleanup And Retention Against Persisted State

Status:

- completed on 2026-04-23

What landed:

- persisted-state cleanup/retention coverage in [internal/api/cookbook_pg_provider_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_pg_provider_routes_test.go)
- explicit coverage for:
  - shared checksum reuse across cookbook versions
  - shared checksum reuse across cookbook artifacts
  - shared checksum reuse between cookbook versions and artifacts
  - sandbox-held checksum retention
- restart-before-delete coverage on the persisted cookbook-state paths

### Task 5: Pin Provider-unavailable Degradation On The Active Postgres Path

Status:

- completed on 2026-04-23

What landed:

- active PostgreSQL provider-failure coverage in [internal/api/cookbook_pg_provider_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_pg_provider_routes_test.go)
- pinned failure behavior for:
  - existence-check failures on create/update
  - signed download failures
  - cleanup/delete failures that are logged but do not block metadata deletion
- explicit no-partial-mutation checks for failed create/update paths

### Task 6: Tighten Operational Truthfulness And Activation Hardening

Status:

- completed on 2026-04-23

What landed:

- activation and status tests in [internal/app/app_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app_test.go)
- activation seam tightening in [internal/store/pg/store.go](/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/store.go) and [internal/app/app.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go)
- more truthful backend/status wording in:
  - [internal/app/app.go](/Users/oberon/Projects/coding/go/OpenCook/internal/app/app.go)
  - [internal/store/pg/store.go](/Users/oberon/Projects/coding/go/OpenCook/internal/store/pg/store.go)
  - [internal/blob/memory.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/memory.go)
  - [internal/blob/s3.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3.go)
  - [internal/blob/blob.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/blob.go)

### Task 7: Sync Docs And Close The Bucket

Status:

- completed on 2026-04-23

What landed:

- roadmap, milestone, compatibility, AGENTS, and root/status text now treat mixed PostgreSQL + provider-backed cookbook lifecycle hardening as complete
- the next bucket now points at the broader bootstrap/PostgreSQL replacement, with validator bootstrap compatibility still pending in Milestone 7

## Verification

The slice was verified with:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/blob`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

## Follow-on

This bucket is closed.

The next roadmap focus is no longer cookbook/provider mixed-mode hardening. It is:

1. replacing more of the remaining bootstrap in-memory layer with PostgreSQL-backed persistence
2. finishing validator bootstrap compatibility for stock Chef and Cinc bootstrap flows
3. continuing the longer-running OpenSearch-backed search/index replacement work
