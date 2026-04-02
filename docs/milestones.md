# OpenCook Milestones

## Milestone 1: Contract Inventory

Status: in progress

- initial route inventory captured in the compatibility registry and roadmap docs
- `oc-chef-pedant` coverage mapped to the first compatibility surfaces
- golden request and response fixtures still need to be captured
- undocumented behavior still needs a dedicated compatibility inventory pass

## Milestone 2: Auth Compatibility Slice

Status: in progress

- Chef header signing verification is implemented and enforced on the first real endpoints
- in-memory key lookup is implemented for signed request verification
- `/keys` list, create, update, and delete behavior is implemented for users and organization clients
- key expiration is now enforced during request verification
- API version edge cases and broader actor/resource compatibility semantics are still pending
- fixture-based canonical request coverage still needs to be expanded

## Milestone 3: Org and ACL Bootstrap

Status: in progress

- users, organizations, clients, groups, containers, and default ACLs are implemented in memory
- Bifrost-style ACL permission checks are implemented in the bootstrap layer
- org bootstrap and validator client creation flows are working
- organization membership and broader association workflows still need follow-on slices

## Milestone 4: Core Object APIs

- implement nodes, roles, environments, data bags, and clients
- persist objects in PostgreSQL
- make one complete read/write vertical slice production-like

## Milestone 5: Search Compatibility

- implement document expansion
- index and query via OpenSearch
- preserve ACL-filtered result handling
- add safe reindex tooling

## Milestone 6: Cookbook and Blob Workflows

- implement sandboxes and checksum lifecycle
- integrate S3-compatible blob storage
- support cookbook artifacts and universe endpoints

## Milestone 7: Operations and Migration

- add health, metrics, repair, backup, and reindex commands
- define migration path from existing Chef Infra Server installs
- rehearse shadow traffic and cutover workflows
