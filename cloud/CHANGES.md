# Changes

## v6.0.0

- BREAKING: Removed partial support for object versioning and limited scope to
  the only supported public API (i.e. `DeleteDirectory`).
- BREAKING: Renamed `DeleteVersions` to `Versions` in `DeleteDirectoryOptions`.

## v5.0.3

- Fixed `IterateObjects` when using a `Delimiter` on the `TestClient`.
- Add error for accessing objects in long-term storage.

## v5.0.2

- Added a `DeleteVersions` parameter to the `DeleteDirectory` method which
  deletes all object versions if enabled.

## V5.0.1
- Upgraded package version because it was not updated as it should have been with the breaking V5.0.0 changes.

## V5.0.0
- BREAKING: The compression and uploads to CSPs within `objutil` returns SHA-256 checksum
  to provide file integrity assurance.

## v4.0.2

- Upgraded dependencies

## v4.0.1

- Fix a bug in `objcli.PrefixExists` which made it always exit with an error

## v4.0.0

- BREAKING: Migrated to `aws-sdk-go-v2`
- Fixes uses of `context.Background()` rather than user provided contexts

## v3.0.0

- BREAKING: Moved to `log/slog`
- Upgraded dependencies

## v2.0.6

- Upgraded dependencies

## v2.0.5

- Upgraded dependencies

## v2.0.4

- Upgraded `tools-common/environment` to v1.0.2.
- Upgraded `tools-common/http` to v1.0.3.
- Upgraded `tools-common/sync` to v1.0.2.
- Upgraded `tools-common/types` to v1.1.2.

## v2.0.3

- Upgraded `tools-common/environment` to v1.0.1.
- Upgraded `tools-common/http` to v1.0.2.
- Upgraded `tools-common/sync` to v1.0.1.
- Upgraded `tools-common/types` to v1.1.1.

## v2.0.2

- Added a `Close` method to the `objcli.Client` interface.

## v2.0.1

- Renames module from `cloud` to `cloud/v2`.

## v2.0.0

- BREAKING: Made `ObjectAttrs.Size` a pointer given it may be conditionally
  populated by `GetObject` (e.g. when the remote server is using chunked
  encoding).
- Improved documentation around the `ObjectAttrs.IsDir` function.
- BREAKING: The 'UploadPartCopy' function now accepts a dst/src bucket allowing
  copying between buckets.
- Added a `CopyObject` function to the `objcli.Client` interface.
- Added a `CopyObject` function to `objutil`.
- Added a `CopyObjects` function to `objutil`.
- Moved the `objcli.Client` interface to option structures.

## v1.0.0

No functional changes since v0.1.0, bumping all 'tools-common' sub-modules to
v1.0.0.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
