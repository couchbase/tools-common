# Changes

## v1.0.6

- A HTTP 403 error is no longer considered a temporary error in
`TemporaryFailureStatusCodes` or `IsTemporaryFailure`.

## v1.0.5

- Upgraded dependencies

## v1.0.4

- Upgraded dependencies

## v1.0.3

- Upgraded `tools-common/types` to v1.1.2.

## v1.0.2

- Upgraded `tools-common/types` to v1.1.1.

## v1.0.1

- The `http2: client connection force closed via ClientConn.Close` error is now
  considered as a temporary error in `IsTemporaryError`.

## v1.0.0

No functional changes since v0.1.0, bumping all 'tools-common' sub-modules to
v1.0.0.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
