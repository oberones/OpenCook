# PostgreSQL Bootstrap Core Persistence Plan

Status: completed on 2026-04-24

## Purpose

This slice replaces the remaining identity and authorization root of the bootstrap compatibility layer with PostgreSQL-backed persistence, while preserving the Chef-facing route contracts already pinned by the current API tests.

The goal is a durable foundation for the rest of the PostgreSQL migration, not a redesign of the public bootstrap, authorization, or validator surfaces.

## Slice Boundary

In scope:

- users and user ACLs
- organizations and organization ACLs
- clients, client keys, and client ACLs
- user keys
- groups and group ACLs
- containers and container ACLs
- request verifier cache hydration from persisted user/client key records
- startup activation and status wording for active PostgreSQL cookbook plus bootstrap-core persistence

Out of scope:

- cookbooks, which already moved to PostgreSQL-backed metadata persistence
- nodes, roles, environments, data bags, policies, sandboxes, and search indexing
- classic validator-authenticated client registration compatibility
- new admin/repair endpoints or a broader storage abstraction redesign

## Compatibility Contract

For this slice we preserved:

- existing bootstrap service method signatures and Chef-facing route behavior
- current user, organization, client, group, container, ACL, and key response shapes
- existing normalization and validation in `bootstrap.Service`
- generated validator client and key behavior during organization creation
- default groups, containers, and ACL content
- `authn.MemoryKeyStore` as the runtime request-verifier cache
- the current activated repository/cache model rather than live query-on-read behavior
- `/status` payload keys and route shapes

## Implementation Summary

Task 1: Inventory the core bootstrap contract

- captured the in-scope core records in this document
- explicitly left core object APIs, search, sandboxes, and validator registration for follow-on slices

Task 2: Extract the bootstrap core store seam

- added `bootstrap.BootstrapCoreStore`
- added a memory-backed adapter preserving default in-memory behavior
- kept validation and normalization in `bootstrap.Service`
- added delegation and rollback coverage for normalized writes and failed persistence

Task 3: Add PostgreSQL schema and repository scaffold

- added the `0002_bootstrap_core_persistence.sql` migration
- added tables for users, user ACLs, user keys, organizations, clients, client keys, groups, group memberships, containers, and org-scoped ACL documents
- kept ACLs as JSON to preserve the current `authz.ACL` compatibility shape
- added repository migration and state round-trip coverage

Task 4: Wire user and key lifecycle persistence

- persisted user creation and user ACL/key records through the bootstrap core store
- hydrated `authn.MemoryKeyStore` from persisted key records on startup
- kept key create/update/rename/delete paths synchronized with the verifier cache
- added active PostgreSQL restart coverage proving persisted user keys authenticate signed requests after app reconstruction

Task 5: Wire organization, client, group, container, and ACL persistence

- persisted organization creation including default groups, containers, ACLs, validator client, and validator key metadata
- persisted client records, client keys, groups, containers, and org ACL documents
- rehydrated `_default` environment bootstrap state in memory for loaded organizations while leaving full environment persistence to a follow-on object slice
- added route-level restart coverage for organization bootstrap artifacts

Task 6: Activate PostgreSQL bootstrap core in the app

- extended `pg.Store` activation so configured PostgreSQL activates cookbook metadata and bootstrap core state in the same startup path
- loaded bootstrap core state into `bootstrap.Service` at app construction
- rehydrated the request verifier cache before serving requests
- updated PostgreSQL status wording to report active cookbook plus bootstrap-core persistence without changing status keys

Task 7: Pin failure and consistency behavior

- added startup activation failure coverage through the existing app activation seam
- added focused store-failure coverage proving failed bootstrap persistence rolls back service state and verifier keys
- kept default in-memory behavior unchanged when PostgreSQL is absent

Task 8: Sync docs and close the bucket

- updated the roadmap, milestones, compatibility matrix, and agent guidance
- marked this bucket complete
- pointed follow-on work at PostgreSQL-backed core object APIs and validator bootstrap compatibility

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

## Follow-on Work

Recommended next buckets:

- persist the remaining core object APIs: nodes, roles, environments, data bags, policies, and sandboxes
- finish Milestone 7 validator-authenticated client registration compatibility
- move search indexing toward PostgreSQL-to-OpenSearch operational flows after object persistence has a durable source of truth
