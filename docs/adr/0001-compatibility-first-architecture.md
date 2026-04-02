# ADR 0001: Compatibility-First Architecture

## Status

Accepted

## Context

OpenCook is intended to replace Chef Infra Server without requiring changes to Chef clients, `knife`, or surrounding automation. Upstream behavior is spread across Erlang services, OpenResty routing, Bookshelf upload flows, Bifrost authorization, and the `oc-chef-pedant` compatibility suite.

The largest risk in a rewrite is shipping a modern-looking API that is not behavior-compatible with real Chef traffic.

## Decision

OpenCook will be built with compatibility as the primary architectural constraint.

That means:

- external API and auth behavior are treated as product contracts
- `oc-chef-pedant` is treated as an acceptance suite, not optional validation
- route and response shims are allowed if they preserve client compatibility
- internal simplification is allowed only behind stable compatibility boundaries

## Consequences

- early milestones focus on contract capture and request signing before breadth
- some internal abstractions may initially look awkward because they encode legacy behavior
- implementation sequencing is driven by risk reduction, not by clean-room design preferences

