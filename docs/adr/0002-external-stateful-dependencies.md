# ADR 0002: External Stateful Dependencies

## Status

Accepted

## Context

Chef Infra Server historically supported standalone, tiered, and HA backends with embedded operational control planes. OpenCook is intended to preserve HA capability while modernizing operations and supporting newer PostgreSQL and OpenSearch releases.

Rebuilding a bespoke HA control plane would slow the rewrite and create new operational surface area before compatibility is proven.

## Decision

OpenCook will assume stateless API nodes backed by external stateful systems:

- PostgreSQL as the system of record
- OpenSearch for indexing and search
- S3-compatible blob storage for cookbook and checksum objects

Any in-process or local development mode will exist only as a developer convenience.

## Consequences

- production HA depends on proven external databases and search clusters
- deployment and failure domains are simpler than legacy embedded backend topologies
- OpenCook must provide strong dependency health, migration, and reindex tooling

