# Contributing To OpenCook

Thanks for helping make OpenCook better. The project is compatibility-first:
changes should preserve Chef and Cinc client behavior unless a documented
project stance intentionally says otherwise.

## Contribution License

OpenCook is licensed under the Apache License, Version 2.0.

Unless you clearly say otherwise in writing, any contribution you intentionally
submit to this repository is submitted under the Apache License, Version 2.0,
without additional terms or conditions. This follows the contribution rule in
Section 5 of the Apache License.

By contributing, you confirm that:

- You have the right to submit the contribution.
- Your contribution can be distributed under the Apache License, Version 2.0.
- Your contribution does not knowingly include third-party code or content that
  is incompatible with the Apache License, Version 2.0.
- If your contribution includes third-party code, generated files, or copied
  documentation, you identify the source and license clearly in the pull
  request.

OpenCook does not currently require a separate contributor license agreement.
Maintainers may ask for a `Signed-off-by` line or another provenance check for
larger or legally sensitive contributions.

## Trademarks

The OpenCook name is governed separately from the software license. Contributing
code or documentation does not grant trademark rights. See `TRADEMARKS.md` for
the project trademark policy.

## Compatibility Expectations

OpenCook is a Chef Infra Server compatibility project, not a new
configuration-management API. Pull requests should:

- Preserve wire compatibility with Chef and Cinc clients, `knife`, and existing
  automation.
- Add or update tests for compatibility-sensitive behavior.
- Document intentional divergences from upstream behavior.
- Avoid adding licensing, license-enforcement, license-telemetry, or
  license-management endpoints.

## Development Workflow

Before opening a pull request, run the focused tests for the area you changed.
For broad changes, run:

```bash
make verify
```

If your change affects PostgreSQL, OpenSearch, blob storage, migration tooling,
or the Docker functional stack, also run the relevant functional test flow
documented in `docs/functional-testing.md`.
