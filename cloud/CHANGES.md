# Changes

## v7.3.3

- Set versionID immediately after creating an object with TestClient
- Add delete markers to GCP `IterateObjects`

## v7.3.2

- Return precondition error from test client when a `PutObject` would fail due to the object already existing.

## v7.3.1

- Add error for pre-condition failure

## v7.3.0

- Add support for 'If-Match' conditional writes (i.e. CAS operations)

## v7.2.1

- Add the ability to download specific object versions using a `Downloader`.
- Add more descriptive error message to the `TestRequireKeyNotFound` test helper.
- Fix `TestClient.IterateObject` directory grouping


## v7.2.0

- Add option to multipart uploader to control the number of concurrent uploads.

## v7.1.1

- Fix `TestClient` object versioning.

## v7.1.0

- Add the ability to lock an object for a specified period of time in compliance mode through a new `Lock`
parameter. If it is not set it will be ignored. The following function options now accept the new `Lock` parameter:
  - `PutObjectOptions` - We can set the lock as we are uploading a new object
  - `SetObjectLockOptions` - Sets a lock for an existing object
  - `CreateMultipartUploadOptions` - For AWS the lock needs to be specified when creating a multipart upload.
  This has no effect on Azure and GCP.
  - `UploadPartOptions` - For GCP we can specify a lock when uploading each part. On AWS and Azure parts are
  automatically immutable.
  - `CompleteMultipartUploadOptions`- For Azure and GCP it must be set when completing the multipart upload. This has
  no effect on AWS.
  - `SyncOptions` - We can lock all newly uploaded objects with the specified lock.
  - `UploadOptions` - The lock will be passed down to the corresponding client methods.
  - `MPUploaderOptions` - We can set the lock when creating a new `MPUploader`
- Add a new `GetBucketLockingStatus` function to check whether the necessary conditions to lock an object in compliance
mode (versioning/locking to be enabled) are met for the specified bucket.
- Add a new `SetObjectLock` function to set a lock for an existing object. Can be used to extend the length of the
lock period.
- Add a new `DeleteObjectVersions` function which deletes specific object versions. Accepts a list of
key-versionID pairs.
- The `IterateObjects` function can list iterate object versions if the `Versions` parameter is set to `true`.
- Add the ability to verify that an object with the provided key does not already exist before overwriting it. This
can be done using the new `OnlyIfAbsent` parameter which is accepted through the following function options:
`PutObject`, `CompleteMultipartUpload`, `UploadPartOptions` (only for GCP), `SyncOptions`, `UploadOptions`,
`MPUploaderOptions`.
- `objval.ObjectAttrs` now contains the the following additional attributes:
  - `VersionID` - Used to identify a specific version when object versioning is enabled.
  - `LockExpiration` - The time the object lock will expire. Will be populated only when an object lock has been set.
  - `LockType` - The type of the object lock. Can be either `LockTypeCompliance` or `LockTypeUndefined`
  - `IsCurrentVersion` - Used to determine whether the specific version is the current (live) one when listing object
  versions.
  - `IsDeleteMarker` - Used to determine whether the specific object version is a delete marker. Only used for AWS.
- The lock data (expiration date and type) can be read through the following functions:
  - `GetObject` - only for AWS and Azure.
  - `GetObjectAttrs`
  - `IterateObjects` - only for Azure and GCP.
- Fix a `DeleteDirectory` bug on Azure. Previously it would fail to delete object versions due attempting
to delete the current (live) versions as if they are noncurrent.

## v7.0.0

- BREAKING: Added the ability to use `CompressObjects` using a source/dest client, allow uploads to another account/provider.

## v6.1.2

- Ensured GCP IterateObjects returns a handled error when the bucket does not exist.

## v6.1.1
- Send a CRC32 checksum when uploading objects.

## v6.1.0

- Updated `objutil` `CompressObjects` to allow for an empty prefix.
  I.e. options validation no longer checks for if `prefix == ""` as it's a valid input.

## v6.0.1

- Upgraded dependencies

## v6.0.0

- BREAKING: Removed partial support for object versioning and limited scope to
  the only supported public API (i.e. `DeleteDirectory`).
- BREAKING: Renamed `DeleteVersions` to `Versions` in `DeleteDirectoryOptions`.
- `DeleteDirectory` now correctly uses the worker pools for GCP/Azure.

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
