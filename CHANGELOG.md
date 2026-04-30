# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog and uses semantic versioning-style tags where practical.

## v0.1.0 (2026-04-29)

### Feat

- **admin**: add migration cutover tooling
- add startup banner on server launch
- harden opensearch provider capabilities
- pin api-version object semantics
- widen search query compatibility
- add encrypted data bag compatibility
- add operational admin reindex tooling
- **search**: add opensearch-backed search parity
- **api**: support validator bootstrap registration
- **postgres**: persist core object APIs
- persist bootstrap core state in Postgres
- **depsolver**: validate JSON before env lookup and auth
- tighten shared run-list validator to match upstream shape
- update behavior for missing run_list
- tighten feature parity around pessimistic constraints
- improve depsolver version pinning and recipe qualification
- expand runlist server side for depsolver
- return upstream style cookbook payload in depsolver response
- create initial depsolver surface
- harden S3-backend blob operations
- add s3-compatible rquest-time operations
- add blob provider seam
- tighten cookbook artifact create/update test parity
- add cookbook coverage for cookbook-artifact auth and delete exactness
- top-level cookbook write-field parity test coverage
- update cookbook route path validation
- tighten cookbook file set mutation parity in testing
- tighten cookbook mutation auth partiy
- add small bootstrap membership seam in service.go
- expand cookbook pedant contract coverage
- further tighten cookbook pedant exactness
- strengthen cookbook compatibility around multi version mutations and auth
- tighter cookbook pedant validation edge behavior
- cookbook and artifact mutations do checksums
- environment-filtered cookbook compatibility
- update cookbook routes to drive recipes endpoint
- improve cookbook version conversion behavior
- improve on cookbook api responses
- deepen cookbook compatibility layer
- add support for writable cookbooks
- add cookbook compatibility layer
- add sandbox and blob support
- deepen the policyfile compatibility
- add policyfile compatibility layer
- add live data bag index to search
- add initial data bag support
- add wider search support
- add environment and node search integration
- add compatibility surface for roles
- make environments into real objects connected to the node
- add first pass at core object API
- add PUT /keys support
- add key lifecycle management
- add support for keys endpoint
- add in-memory bootstrap service and acl authorizer
- add authenticating http request routing
- add authentication stubs

### Fix

- **admin**: require restore backup payloads
- **admin**: rollback cookbook restore metadata
- resolve issues with client bootstrap and registration
- reject unsafe legacy search providers
- check key detail in functional api-version coverage
- refresh generated client ACLs on validator flips
- reject malformed search boolean queries
- reject missing org in acl repair
- **search**: page opensearch ids with search_after
- **postgres**: preserve default environment ACL on rehydrate
- validate depsolver run_list before env lookup and auth
- address PR feedback
- address depsolver gap identified by tests
- I added the missing len(payload) == 4 assertion to the org-scoped second-graph test in internal/api/environment_depsolver_routes_test.go, so it now checks the full response shape instead of only the selected versions.
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- prevent RootMessages overwrite for duplicate cookbook entries in depsolver
- address PR feedback
- addres PR feedback
- address PR feedback
- update based on PR feedback
- address PR feedback
- address PR feedback
- updates based on PR feedback
- address PR feedback
- update docs based on PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- update test assumptions based on pr feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR feedback
- address PR comments
- address PR feedback
- address PR feedback
- remove dead helper from bootstrap roles library
- address PR feedback
- **authn**: honor explicit skew settings

### Refactor

- simplify depsolver payload prevalidation

## [v0.1.0-alpha.1] - 2026-04-02

### Added

- Initial OpenCook repository scaffold with a build-oriented Go module layout.
- Application entrypoint and runtime wiring for a compatibility-first HTTP service.
- Internal package boundaries for API routing, authn, authz, blob storage, search, PostgreSQL, config, versioning, and compatibility inventory.
- Scaffold health and contract-inventory endpoints, including route surfaces for core Chef-compatible API areas.
- Project docs for the rewrite roadmap, milestones, compatibility tracking, and architecture decisions.
- Compatibility test harness directories and starter tests for contract-first development.
- Example environment configuration and a basic `Makefile`.

### Changed

- Expanded the project README to describe the architecture, repository layout, and next implementation slices.
- Declared OpenCook as fully free and open source, with no licensing subsystem, no license enforcement, and no license telemetry.

### Notes

- This tag marks the initial scaffold milestone, not feature completeness.
- Go formatting and test execution were not run in this environment because a Go toolchain is not currently installed.
