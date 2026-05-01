# S3-Compatible Blob Operational Hardening Plan

Status: completed on 2026-04-22

## Purpose

This document breaks the next cookbook/blob bucket into one implementation slice with concrete, commit-sized tasks.

The goal is to harden the existing S3-compatible blob adapter so that request-time provider failures behave predictably through the current OpenCook compatibility layer without changing the user-facing HTTP contract unless we have strong upstream evidence to do so.

## Completion Summary

All seven tasks in this slice are complete.

The completed slice now pins:

- availability-status and transport classification for the S3-compatible adapter
- retry exhaustion, `Retry-After`, and cancellation behavior
- request construction for path-style, virtual-hosted, session-token, and TLS-disabled cases
- malformed-endpoint and missing-credential config/status handling
- sandbox and cookbook `503 blob_unavailable` degradation on the provider-backed paths that already use that compatibility contract

## Slice Boundary

In scope:

- S3-compatible request-time operational behavior for `GET`, `PUT`, `HEAD`, and `DELETE`
- retry and backoff behavior
- transport and status classification
- compatibility-safe `blob_unavailable` degradation through the current HTTP layer
- config/status hardening for the S3-compatible backend
- tests and docs needed to lock the behavior in

Out of scope:

- multipart upload support
- background reconciliation or repair workflows
- PostgreSQL or OpenSearch work
- general cookbook pedant work not directly tied to provider-backed blob behavior
- new blob backends

## Existing Seam

This slice is intentionally aligned to the current code layout:

- transport and request signing: [internal/blob/s3.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3.go)
- S3 adapter tests: [internal/blob/s3_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3_test.go)
- backend wiring: [internal/blob/store.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/store.go)
- blob config: [internal/config/config.go](/Users/oberon/Projects/coding/go/OpenCook/internal/config/config.go)
- sandbox HTTP degradation: [internal/api/sandbox_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/sandbox_routes.go)
- cookbook HTTP degradation: [internal/api/cookbook_routes.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes.go)

## Compatibility Contract

For this slice, we will treat the following as the compatibility contract:

- transient provider failures degrade to `blob.ErrUnavailable`
- provider auth and provider availability failures degrade to `blob.ErrUnavailable`
- routes that already convert `blob.ErrUnavailable` to `503` continue returning the existing `blob_unavailable` response shape
- request-context cancellation and deadline expiry should not be flattened into provider-unavailable behavior
- unexpected non-availability provider statuses should stay distinct from `blob.ErrUnavailable` unless we have evidence to widen them

## Definition Of Done

- S3 unit coverage explicitly pins retryable status handling, retry exhaustion, transport failure classification, `Retry-After`, and context cancellation
- sandbox and cookbook surfaces consistently convert provider-unavailable failures into the existing `503 blob_unavailable` responses
- config/status behavior is explicit for common S3 misconfiguration cases
- roadmap and milestone docs describe provider hardening as the active bucket rather than an undefined follow-up

## Task List

### Task 1: Freeze The Operational Contract In Blob Unit Tests

Goal:

- make the intended S3-compatible availability contract explicit before widening or tightening implementation behavior

Changes:

- extend [internal/blob/s3_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3_test.go) with table-driven coverage for:
  - retryable statuses: `408`, `429`, `500`, `502`, `503`, `504`
  - immediate availability failures: `401`, `403`
  - non-availability statuses that should remain ordinary operation failures

Exit condition:

- the status-to-error mapping is explicit in tests rather than implied by helper logic

### Task 2: Harden Transport Retry Classification

Goal:

- ensure transient network failures retry and degrade consistently without misclassifying permanent or caller-driven failures

Changes:

- add tests in [internal/blob/s3_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3_test.go) for:
  - retryable timeout and temporary network errors
  - non-retryable generic transport errors
  - context cancellation while issuing requests
- update [internal/blob/s3.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3.go) only where tests show gaps

Exit condition:

- retries are limited to real transient failures
- canceled or expired caller contexts return context errors instead of `blob.ErrUnavailable`

### Task 3: Pin Retry Timing Behavior

Goal:

- make retry/backoff behavior deterministic and compatibility-safe

Changes:

- add tests in [internal/blob/s3_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3_test.go) for:
  - exact retry-attempt exhaustion
  - `Retry-After` seconds form
  - `Retry-After` HTTP-date form
  - invalid `Retry-After` fallback to exponential backoff
  - cancellation during retry sleep

Exit condition:

- retry timing behavior is intentional, bounded, and fully pinned by tests

### Task 4: Tighten Config And Status Reporting

Goal:

- make common S3 misconfiguration cases easier to diagnose without changing the runtime compatibility contract

Changes:

- add focused tests in [internal/config/config_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/config/config_test.go) and/or [internal/blob/store_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/store_test.go)
- tighten config/status coverage for:
  - malformed `s3://` storage URLs
  - missing bucket
  - negative retry counts
  - non-positive request timeout
  - missing credentials status messaging

Exit condition:

- invalid config fails fast where appropriate
- incomplete config remains diagnosable through status messaging

### Task 5: Pin Sandbox HTTP Degradation Through The S3 Path

Goal:

- ensure sandbox flows remain compatibility-safe when the provider is unavailable

Changes:

- expand [internal/api/sandbox_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/sandbox_routes_test.go) to explicitly cover S3-backed:
  - upload failure to `503 blob_unavailable`
  - download failure to `503 blob_unavailable`
  - commit checksum-existence failure to `503 blob_unavailable`

Exit condition:

- sandbox routes never leak raw provider errors for S3 operational failures

### Task 6: Pin Cookbook HTTP Degradation Through The S3 Path

Goal:

- ensure cookbook and cookbook-artifact flows degrade consistently when blob existence checks or reads fail through the provider

Changes:

- expand [internal/api/cookbook_routes_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/api/cookbook_routes_test.go) for S3-backed:
  - cookbook create/update checksum-existence failures
  - cookbook artifact create/update checksum-existence failures
  - signed blob download/read paths if they already flow through the provider-backed getter

Exit condition:

- cookbook-facing provider failures consistently surface as the existing `blob_unavailable` shape where that contract already exists

### Task 7: Pin Request Construction And Finalize Docs

Goal:

- lock in the remaining request-construction edge cases and complete the slice with docs

Changes:

- add any missing tests in [internal/blob/s3_test.go](/Users/oberon/Projects/coding/go/OpenCook/internal/blob/s3_test.go) for:
  - path-style vs virtual-hosted-style request targets
  - session-token signing
  - TLS-disabled endpoint handling
  - object-key normalization edge cases
- sync:
  - [docs/chef-infra-server-rewrite-roadmap.md](/Users/oberon/Projects/coding/go/OpenCook/docs/chef-infra-server-rewrite-roadmap.md)
  - [docs/milestones.md](/Users/oberon/Projects/coding/go/OpenCook/docs/milestones.md)
  - [AGENTS.md](/Users/oberon/Projects/coding/go/OpenCook/AGENTS.md)
  - optionally [docs/compatibility-matrix-template.md](/Users/oberon/Projects/coding/go/OpenCook/docs/compatibility-matrix-template.md)

Exit condition:

- the provider-hardening slice is documented as complete
- the next bucket is ready to move to remaining cookbook pedant work or the next operational slice

## Suggested Commit Sequence

1. `blob: pin s3 availability status mapping`
2. `blob: tighten s3 transport retry behavior`
3. `blob: pin retry-after and cancellation behavior`
4. `config: harden s3 blob backend validation`
5. `test: pin sandbox s3 blob-unavailable parity`
6. `test: pin cookbook s3 blob-unavailable parity`
7. `docs: record s3 blob operational hardening`

## Verification

Minimum verification for each task:

```bash
gofmt -w internal/blob/s3.go internal/blob/s3_test.go internal/api/sandbox_routes_test.go internal/api/cookbook_routes_test.go internal/config/config.go internal/config/config_test.go
GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...
```

If a task only changes docs, note that explicitly and skip the test pass only when no code or test behavior changed.
