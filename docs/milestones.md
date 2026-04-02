# OpenCook Milestones

## Milestone 1: Contract Inventory

- extract route inventory from Chef Server routing rules
- map `oc-chef-pedant` coverage by compatibility surface
- capture golden request and response fixtures
- identify undocumented behavior needing bespoke tests

## Milestone 2: Auth Compatibility Slice

- implement Chef header signing verification
- implement key lookup and `/keys` contract behavior
- preserve API version edge cases
- add fixture-based tests for canonical request variants

## Milestone 3: Org and ACL Bootstrap

- implement users, organizations, clients, groups, and ACL defaults
- model Bifrost-compatible permission checks
- support org bootstrap and association workflows

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

