# Changes

## v3.3.7

- Add Enterprise Analytics mappings to versions
- Updated Columnar mappings to reflect correct server versions

## v3.3.6

- Retracted 3.3.5 due to incorrect commit being tagged

## v3.3.5 (Retracted)

- Cancel client context if we fail to update the cluster config after too many retries

## v3.3.4

- Fix incorrectly constructed IPv6 address in cbrest

## v3.3.3

- Fixed a case where errors would be swallowed by `ExecuteStream`

## v3.3.2

- Call `.String()` on `dispatching request` log statements

## v3.3.1

- Upgraded dependencies

## v3.3.0

- Add (8.0) Morpheus to versions

## v3.2.0

- Add a `HostnameTransform` option to allow modifying hostnames prior to
  dispatching requests

## v3.1.0 (Retracted)

- Add new cbrest client option to alter hostnames

## v3.0.2

- Retry on 403 responses

## v3.0.1

- Upgraded dependencies

## v3.0.0

- Moved to `log/slog`
- Upgraded dependencies

## v2.0.4

- Upgraded dependencies

## v2.0.3

- Upgraded dependencies

## v2.0.2

- Upgraded `tools-common/environment` to v1.0.2.
- Upgraded `tools-common/http` to v1.0.3.
- Upgraded `tools-common/sync` to v1.0.2.
- Upgraded `tools-common/types` to v1.1.2.
- Upgraded `tools-common/utils` to v2.0.2.

## v2.0.1

- Upgraded `tools-common/environment` to v1.0.1.
- Upgraded `tools-common/http` to v1.0.2.
- Upgraded `tools-common/sync` to v1.0.1.
- Upgraded `tools-common/types` to v1.1.1.
- Upgraded `tools-common/utils` to v2.0.1.

## v2.0.0

- BREAKING: Auth providers may now return an error when unable to provide
  credentials.

## v1.0.1

- Added 7.2 to versions.

## v1.0.0

No functional changes since v0.1.0, bumping all 'tools-common' sub-modules to
v1.0.0.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
