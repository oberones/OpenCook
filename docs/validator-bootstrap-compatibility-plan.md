# Validator Bootstrap Compatibility Plan

Status: completed on 2026-04-24

## Summary

This slice closes Milestone 7 by making the generated organization validator client and key usable for stock Chef and Cinc client bootstrap registration.

OpenCook creates `<org>-validator`, returns validator key material from organization bootstrap, persists client/key state in PostgreSQL, rehydrates the runtime verifier cache, and now accepts same-org validator-signed normal client registration through the existing Chef-facing `/clients` and `/organizations/{org}/clients` surfaces with the payloads, response shapes, authorization behavior, and failure semantics expected by `knife bootstrap`, Chef Infra Client, Cinc, and `oc-chef-pedant`.

This is a compatibility slice, not a new bootstrap API.

## Interfaces And Behavior

- No Chef-facing route, method, trailing-slash, payload, response, or error-shape changes beyond making the existing client registration contract compatible.
- Keep the current `/clients` and `/organizations/{org}/clients` route surfaces.
- Keep generated validator client names as `<org>-validator`.
- Keep validator keys as normal persisted client keys backed by the existing bootstrap core persistence model.
- Keep `authn.MemoryKeyStore` as the request verifier cache, hydrated and synchronized from persisted key records.
- Keep `bootstrap.Service` responsible for validation, normalization, default ACL creation, group membership side effects, persistence rollback, and verifier synchronization.
- Do not add a separate validation-key endpoint or a new storage abstraction.
- Do not make validators admins as an implementation shortcut.
- Do not widen default clients-container ACLs unless upstream inventory proves that is the compatibility contract.
- Do not pull OpenSearch-backed indexing, encrypted data bags, operational admin tooling, or broader object API work into this slice.
- Preserve the licensing-free stance. Do not add Chef licensing, telemetry, or license-management endpoints.

## Current Contract Inventory

Upstream sources to pin before changing behavior:

- `~/Projects/coding/ruby/chef-server/src/nginx/habitat/config/routes.lua`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/platform.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/knife.rb.erb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/lib/pedant/rspec/client_util.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/clients/complete_endpoint_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/api/keys/client_keys_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/org_creation/validate_acls_spec.rb`
- `~/Projects/coding/ruby/chef-server/oc-chef-pedant/spec/org_creation/validate_groups_spec.rb`

Known upstream signals:

- Pedant treats the validator as `#{org}-validator`.
- Pedant creates bootstrap clients by signing `POST /clients` with `org.validator`.
- Pedant's knife config template writes `validation_client_name "<org>-validator"` and points `validation_key` at the returned validator private key.
- Upstream default-org route rewriting includes `clients`, so default-org `/clients` behavior matters.
- Org creation specs expect the validator client to exist, to be a member of the `clients` group, and to have a default key.
- Upstream ACL specs keep the clients container create permission admin-scoped, so validator registration may be a special client-create authorization path rather than a broad default ACL change.
- Upstream client ACL expectations distinguish validator clients from normal clients, especially around update/delete behavior.

Current OpenCook state:

- Organization creation creates `<org>-validator` with `validator: true`, a default key, clients-group membership, client ACLs, and persisted key records when PostgreSQL is active.
- Startup rehydrates persisted validator keys into `authn.MemoryKeyStore`.
- Client creation routes already exist for default-org and org-scoped aliases.
- Client creation now includes a narrow same-org generated-validator authorization path without widening the default clients-container ACL.
- Client creation already creates client records, default key material, client ACLs, clients-group membership, persisted bootstrap core state, and verifier cache entries.
- Client search reads from the bootstrap service state, so validator-created clients are visible through the existing in-memory search surface once created.

## Frozen Validator Registration Contract

Task 1 status: completed on 2026-04-24.

The upstream contract for this bucket is:

- Default-org `/clients` remains part of the stock compatibility surface. Pedant's default-org rewrite includes `clients`, and its bootstrap helper signs `POST /clients` with the org validator.
- Explicit-org `/organizations/{org}/clients` remains the canonical multi-tenant route. The nginx route table still forwards named-org `clients` traffic to erchef.
- The validator principal is always the generated client named `<org>-validator`.
- The validator's default key is the key returned by organization creation and stored at `/organizations/{org}/clients/{org}-validator/keys/default`.
- In server API v1+ flows, Pedant creates normal bootstrap clients with `create_key: true` and expects a `chef_key` object containing `name: "default"`, `expiration_date: "infinity"`, `uri`, `public_key`, and generated `private_key`.
- In server API v1+ flows, providing `public_key` creates the default key from supplied material and returns `chef_key` without generated private key material.
- In server API v1+ flows, sending both `create_key: true` and `public_key` is a `400`.
- In server API v1+ flows, sending `create_key: false` with `public_key` succeeds.
- In server API v1+ flows, sending `private_key: true` on create is a `400`.
- In server API v1+ flows, sending neither `create_key` nor `public_key` succeeds but does not create a default key. This is broader actor-create API behavior; the stock bootstrap path still uses `create_key: true`.
- Legacy v0 behavior returns top-level `private_key` and `public_key`. OpenCook should not remove any already-pinned response fields while adding the validator-backed v1+ bootstrap path.
- Client create accepts `name` as the primary create field. OpenCook's existing `clientname` alias should remain accepted for compatibility with existing tests and returned client payloads.
- Client read payloads include `name`, `clientname`, `chef_type: "client"`, `orgname`, `json_class: "Chef::ApiClient"`, `validator`, and, for v1+ clients with a default key, `public_key`.

The authorization contract is:

- Org admins can create normal clients.
- Org admins can create validator clients.
- Normal clients cannot create normal clients.
- Normal clients cannot create validator clients.
- Validator clients can create normal clients in their own org.
- Validator clients cannot create validator clients.
- Validator clients cannot update, rename, read, or delete other clients through this bucket.
- Validator clients cannot update, rename, or delete themselves through this bucket.
- Wrong-org validator clients must fail authorization for the target org.
- Missing, deleted, expired, or renamed-away validator keys must fail through existing authentication/key lookup semantics.

The error and no-mutation contract to preserve is:

- Duplicate client create returns `409` with a Chef-shaped client-exists error.
- Missing `name` returns `400` with a Chef-shaped missing-name error.
- Invalid client names return `400` with a Chef-shaped invalid-name error.
- Bad public keys on API v1+ client create return `400`.
- Unauthorized normal-client creates and validator-client attempts to create validators return `403` with a missing-create-permission-style error.
- Invalid or unauthorized create attempts must not persist client records, key records, ACL documents, clients-group membership, verifier cache entries, or search-facing client rows.
- Malformed JSON, trailing JSON, empty body, and route-shape failures should continue using OpenCook's existing decode and route error shapes unless a focused upstream check proves the current shape is incompatible.

The ACL and group contract is:

- The generated validator must remain in the `clients` group as both an actor and a client member.
- The default clients container create ACL remains admin-scoped upstream. Validator registration should therefore be implemented as a narrow same-org validator registration rule, not by widening the clients-container ACL.
- The generated validator client ACL uses user/admin group semantics for read/update/delete/grant; validators should not receive admin group membership.
- Normal client ACL ownership has separate upstream expectations and should be pinned in Task 5. If OpenCook needs to add the newly created client to its own ACL actor/client lists, do that narrowly for normal clients without changing validator-client ACLs.

Implementation decision from Task 1:

- Add a validator-aware authorization path for `POST /clients` and `POST /organizations/{org}/clients` rather than changing default group membership or container ACLs.
- Require same-org client principal identity, a persisted client record with `validator: true`, and a non-validator target payload.
- Keep all mutation, persistence rollback, key creation, and verifier synchronization inside `bootstrap.Service`.
- Treat the v1+ `create_key: true` validator registration flow as the minimum stock bootstrap path for the first implementation pass, then add the explicit `public_key` variant and no-mutation failures in the same bucket.

## Slice Boundary

In scope:

- validator-authenticated normal client registration
- default-org and explicit-org `/clients` registration aliases
- API-version-sensitive key material response behavior for client registration
- explicit public-key input and generated-key registration flows
- validator key authentication after app restart
- newly registered client key authentication after app restart
- client ACL and clients-group side effects for validator-created clients
- no-mutation behavior for invalid or unauthorized registration attempts
- functional smoke coverage for a stock-bootstrap-shaped registration flow
- docs/status roadmap updates when the slice is complete

Out of scope:

- changing organization bootstrap key-generation semantics, except as needed to use the generated validator key
- creating validator clients through validator-authenticated registration unless upstream inventory explicitly requires it
- replacing the client key lifecycle API
- OpenSearch-backed indexing or reindex tooling
- new `chef-server-ctl` replacement commands
- nodes, roles, environments, policies, data bags, sandboxes, cookbooks, or blob behavior
- cross-process cache invalidation

## Task Breakdown

### Task 1: Freeze The Upstream Validator Registration Contract

Status:

- Completed. The frozen contract above captures the route flow, API-version-sensitive payload/response behavior, authorization matrix, error/no-mutation expectations, ACL/group implications, and the implementation decision to prefer a narrow same-org validator registration path over broad ACL changes.

- Inventory the exact route flow used by `oc-chef-pedant`, `knife bootstrap`, and Chef/Cinc client validation.
- Confirm whether stock bootstrap uses default-org `/clients`, explicit-org `/organizations/{org}/clients`, or both.
- Capture API-version-sensitive response expectations:
  - `private_key`
  - `chef_key`
  - `public_key`
  - `uri`
  - `Location`, if upstream emits it
- Capture payload variants:
  - `name`
  - `clientname`
  - `validator`
  - `admin`
  - `public_key`
  - `create_key`
- Capture exact statuses and error shapes for duplicate clients, malformed JSON, missing names, invalid names, invalid public keys, `validator: true`, and forbidden requestors.
- Decide from upstream evidence whether validator registration is a special-case authorization rule or an ACL membership effect.
- Record the final contract in this plan before implementing behavior.

### Task 2: Add A Validator Registration Test Harness

Status:

- Completed. The API test harness now has helpers that create an organization through the existing bootstrap route, capture the generated validator private key, build signed client requestors from validator or client-create key material, sign explicit-org and configured-default-org requests without confusing route path values, and restart an active PostgreSQL fixture against the same persisted state to prove validator and newly registered client keys rehydrate.

- Add route-level helpers that create an organization through the existing bootstrap route and capture the returned validator private key.
- Add request-signing helpers for validator clients using explicit private key material and organization scope.
- Add a reusable active-PostgreSQL app helper that can:
  - create an org
  - restart the app against the same persisted state
  - sign a request as the rehydrated validator
  - restart again and sign as the newly registered client
- Cover both configured default-org and explicit-org paths.
- Keep the harness local and deterministic; do not require an external Chef installation for unit or route tests.

### Task 3: Pin Successful Validator-authenticated Client Registration

Status:

- Completed. Route coverage now proves same-org validator clients can create normal clients on explicit-org and configured default-org aliases, with generated `create_key: true` key material, explicit `public_key` registration via `clientname`, immediate newly registered client authentication, default key metadata, clients-group membership, client ACL visibility, search-facing client rows, and preserved admin-user client creation. The implementation uses a narrow same-org persisted-validator create path and keeps the clients-container ACL unchanged.

- Add failing route tests first for validator-signed `POST /organizations/{org}/clients`.
- Add default-org alias tests for validator-signed `POST /clients` when a default org is configured or unambiguous.
- Cover generated key registration with `create_key: true`.
- Cover explicit `public_key` registration.
- Cover the stock payload shape that uses `name`.
- Cover the compatibility payload shape that uses `clientname`.
- Prove successful registration creates:
  - a client record with `validator: false` by default
  - default key metadata
  - verifier cache entries that authenticate immediately
  - clients-group membership
  - a client ACL document
  - search-facing client data through the existing in-memory search surface
- Preserve the current admin-user client create behavior while adding validator compatibility.

### Task 4: Implement Validator Create Authorization Without Broadening ACLs

Status:

- Completed. Client-create authorization now falls back only for the same-org generated `<org>-validator` principal with a persisted `validator: true` client record. Focused route coverage proves non-generated validator clients and normal clients cannot use the exception, validator-signed requests cannot create validator/admin clients, failed attempts do not persist client records, generated validators are not added to the `admins` group, and the clients-container create ACL remains admin-scoped.

- Add a narrow client-create authorization path for same-org validator registration if upstream inventory confirms special validator behavior.
- Require the request principal to be a client in the target org.
- Require the principal name to match the persisted validator client for that org.
- Require the persisted principal to have `validator: true`.
- Continue to reject missing, deleted, expired, wrong-org, or non-validator keys through the existing authentication and authorization flow.
- Do not add validators to the `admins` group.
- Do not change default clients-container ACLs just to make tests pass.
- Do not let validator-signed registration create another validator or an admin-equivalent client unless upstream compatibility explicitly requires that.
- Keep route handlers delegating actual mutation to `bootstrap.Service` so persistence rollback behavior remains centralized.

### Task 5: Pin Client ACL, Group, And Ownership Semantics

Status:

- Completed. Upstream `account_client_spec.rb` pins validator-created normal clients with the validator removed from the new client's ACL and the new client itself added as an ACL actor. OpenCook now applies that ownership side effect only to non-validator clients, while leaving generated validator client ACLs on the default validator shape. Route coverage now pins exact validator-created client ACLs, generated-validator ACL preservation, clients-group membership after PostgreSQL restart, newly registered client self-read behavior, normal-user and outside-org client rejection, and explicit-org plus configured-default-org client/key URL shapes.

- Compare validator-created normal client ACLs against upstream `oc-chef-pedant` expectations.
- Preserve the current default client ACL shape where compatible.
- If upstream expects the newly created client to appear in its own ACL actor/client lists, implement that narrowly for normal clients without changing validator-client ACLs.
- Confirm the clients group includes validator-created clients after registration.
- Confirm the validator client remains in the clients group after registration and restart.
- Confirm the created client can authenticate and read itself where current Chef-compatible ACLs allow that.
- Confirm normal users and outside-org clients do not gain registration powers through this slice.
- Confirm default-org and explicit-org URL shapes stay stable in collection, named-client, and key responses.

### Task 6: Pin Failure, Precedence, And No-mutation Behavior

Status:

- Completed. Route coverage now pins duplicate validator registration, invalid JSON, trailing JSON, empty payloads, missing names, invalid names, invalid public keys, `create_key: true` plus `public_key`, wrong-org validator authentication, deleted validator keys, expired validator keys, ambiguous default-org precedence, configured default-org malformed-body behavior, and PostgreSQL-backed no-mutation after failed registration. Failed attempts are checked against client reads, clients-group membership, client ACL visibility, search rows, persisted state after restart, and verifier-cache authentication. Key-name rename remains usable because the current Chef-style verifier authenticates any active key for the principal rather than the literal key name.

- Cover validator-signed duplicate client creation.
- Cover wrong-org validator keys attempting to register into another org.
- Cover deleted validator keys, expired validator keys, and renamed validator keys if the current key lifecycle allows those states.
- Cover validator-signed attempts to create `validator: true` clients.
- Cover validator-signed attempts to create admin clients if the `admin` payload field remains accepted.
- Cover invalid JSON, trailing JSON, empty payloads, missing name/clientname, invalid names, invalid public keys, and incompatible `create_key` plus `public_key` combinations.
- Cover ambiguous default-org behavior and configured default-org behavior.
- Prove failed registration does not mutate:
  - client maps
  - client key maps
  - clients-group membership
  - client ACL documents
  - persisted PostgreSQL bootstrap core state
  - verifier cache keys
  - search-facing client state
- Keep current error shapes and status-code precedence unless upstream inventory proves a different Chef contract.

### Task 7: Harden PostgreSQL Restart And Rehydration Behavior

Status:

- Completed. Active-PostgreSQL route coverage now creates an organization, reconstructs the app, registers a normal client with the rehydrated validator key, reconstructs again, and proves the registered client, default key authentication, clients-group membership, client ACL document, and search-facing client row all survive. Repeated reconstruction against the same persisted state is pinned to avoid duplicate clients-group members, and a matching in-memory guard proves the non-PostgreSQL runtime path remains compatible.

- Add active-PostgreSQL route tests where organization bootstrap happens before app reconstruction, then validator registration happens after reconstruction.
- Prove persisted validator keys authenticate registration after startup rehydration.
- Prove a validator-created client persists across restart.
- Prove the newly registered client's default key authenticates signed requests after restart.
- Prove clients-group membership and client ACL documents survive restart.
- Prove repeated app construction against the same database remains idempotent.
- Prove in-memory mode remains unchanged when PostgreSQL is not configured.
- Keep the activated repository/cache model; do not convert client reads to live SQL queries in this slice.

### Task 8: Add Functional Bootstrap Smoke Coverage

Status:

- Completed. The functional Docker flow now registers normal clients through the generated `<org>-validator` key captured from organization creation on both explicit-org and configured default-org routes, stores the new clients' private keys in the functional state volume, verifies signed follow-up requests from those clients after OpenCook restarts, checks client key metadata, group membership, ACL visibility, and search rows, and removes the clients during the delete phase. The smoke path continues to run against the compose stack's PostgreSQL-backed OpenCook service with OpenSearch present on the shared network.

- Extend the functional container test flow with a validator-registration scenario once the route-level contract is pinned.
- Use OpenCook's organization bootstrap response to obtain validator key material.
- Sign `POST /organizations/{org}/clients` as `<org>-validator`.
- Register a normal client with generated key material.
- Sign a follow-up request as the newly registered client.
- Run the same scenario against the containerized PostgreSQL-backed server.
- Keep OpenSearch available in the compose environment but do not require OpenSearch-backed indexing changes for this bucket.

### Task 9: Sync Docs And Close The Bucket

- Completed. Roadmap, milestone, compatibility matrix, AGENTS, README, plan, and root/status next-step wording now mark validator bootstrap compatibility complete and point the next bucket at OpenSearch-backed indexing and query parity.
- Update `docs/chef-infra-server-rewrite-roadmap.md`.
- Update `docs/milestones.md`.
- Update `docs/compatibility-matrix-template.md`.
- Update `AGENTS.md`.
- Update `README.md` if the quick-start or current-status text still says validator bootstrap registration is pending.
- Mark this plan complete after tests pass.
- Update root/status "next" wording if it still points at validator bootstrap as pending.
- Point the next bucket at OpenSearch-backed indexing and query parity, unless operational admin tooling becomes the more urgent follow-on.

## Likely Implementation Touchpoints

- `internal/api/client_routes.go`: add or route through a validator-aware client-create authorization check without changing response shapes.
- `internal/api/router_test.go` or a new validator-focused route test file: pin default-org and explicit-org validator registration.
- `internal/api/bootstrap_pg_persistence_routes_test.go` or a new PostgreSQL validator route test file: pin restart and rehydration behavior.
- `internal/bootstrap/service.go`: adjust client creation ACL side effects only if upstream inventory requires normal-client ACL actor/client ownership changes.
- `internal/bootstrap/service_test.go`: pin any bootstrap-service-only ACL, group, rollback, or persistence side effects.
- `internal/app/app_test.go`: add app-level active PostgreSQL startup/restart coverage if route helpers do not already prove the behavior.
- `test/functional`: add a stock-bootstrap-shaped smoke test after route coverage is stable.

## Test Plan

Focused verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/bootstrap`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/api`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/app`
- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./internal/store/pg`

Full verification:

- `GOCACHE=/tmp/opencook-go-cache /usr/local/go/bin/go test ./...`

Functional verification:

- Run the functional compose flow after adding the validator-registration smoke scenario.
- Confirm the containerized app can bootstrap an org, use the returned validator key to register a client, and then authenticate as that client against the PostgreSQL-backed server.

Required scenarios:

- validator key returned by org bootstrap can register a normal client
- validator registration works after PostgreSQL restart/rehydration
- newly registered client key authenticates immediately and after restart
- default-org and explicit-org registration aliases match existing route contracts
- explicit-public-key and generated-key registration flows match upstream response shapes
- validator-created client ACLs and clients-group membership match Chef expectations
- duplicate, invalid, wrong-org, expired-key, deleted-key, and unauthorized registration attempts do not partially mutate state
- default in-memory behavior remains unchanged when PostgreSQL is absent

## Assumptions

- The validator registration gap is primarily authorization and compatibility semantics, not key storage or key authentication.
- The validator client should remain a normal persisted client record with `validator: true`.
- The validator key should remain a normal persisted client key named `default`.
- The existing bootstrap core persistence and verifier rehydration work should be reused rather than replaced.
- OpenCook should prefer a narrow validator-registration rule over broad ACL changes unless upstream contract evidence says otherwise.
- OpenSearch indexing can remain the current in-memory compatibility path for this slice.
