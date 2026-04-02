# ADR 0003: No Licensing Subsystem

## Status

Accepted

## Context

Upstream Chef Infra Server includes licensing-related behavior in both product mechanics and API surface. OpenCook is intended to be a fully free and open source system.

Carrying licensing concepts into the rewrite would add product behavior that is neither desired nor necessary for OpenCook's goals.

## Decision

OpenCook will not implement:

- license enforcement
- license telemetry
- license-management workflows
- licensing-specific API endpoints as product features

Compatibility work should ignore licensing as a target surface, even where upstream Chef Server exposes it.

## Consequences

- OpenCook is intentionally not a bit-for-bit clone of Chef Infra Server in this area
- any upstream tooling that depends on licensing-specific endpoints will need a migration note or deliberate fallback handling
- future contributors should treat licensing code as out of scope unless the project direction changes explicitly

