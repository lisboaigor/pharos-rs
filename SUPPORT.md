# Support Policy

## Security vulnerabilities

Report security vulnerabilities to the maintainers privately via
[GitHub Security Advisories](https://github.com/pharos-rs/pharos-rs/security/advisories/new).

Do **not** open a public issue for security vulnerabilities.
We aim to acknowledge reports within 72 hours and release a patch within 14 days.

## Bug reports and feature requests

Open a [GitHub issue](https://github.com/pharos-rs/pharos-rs/issues).
Use the existing templates where available.

## Supported versions

We follow [Semantic Versioning](https://semver.org/). Only the latest minor
release of each major version receives active bug fixes and security patches.

| Version | Support status |
| --- | --- |
| 0.x (current) | Active — receives bug fixes and security patches |
| < 0.1 | Not supported |

## Breaking changes and API stability

- `pharos-core` and `pharos-app` aim for API stability once each major version
  is published. Changes that affect public traits require an RFC (see
  `docs/rfc/README.md`).
- All public enums and structs that may grow new variants/fields are marked
  `#[non_exhaustive]`.
- Crates in the `pharos-*` family follow the same major version as `pharos-core`
  unless explicitly documented otherwise.

## Maintenance expectations

Pharos RS is maintained by volunteers. Response times are best-effort.
Pull requests are welcome and the fastest path to getting a fix landed.
