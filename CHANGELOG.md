# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by Keep a Changelog and uses semantic versioning-style tags where practical.

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
