# Changes

## v2.0.0

- BREAKING: Made `ObjectAttrs.Size` a pointer given it may be conditionally
  populated by `GetObject` (e.g. when the remote server is using chunked
  encoding).
- Improved documentation around the `ObjectAttrs.IsDir` function.
- BREAKING: The 'UploadPartCopy' function now accepts a dst/src bucket allowing
  copying between buckets.

## v1.0.0

No functional changes since v0.1.0, bumping all 'tools-common' sub-modules to
v1.0.0.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
