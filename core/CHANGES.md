# Changes

## v2.1.1

- Fix flag matching in args logging: When a flag starts with '--' we
  should match the entire flag and not only the prefix.

## v2.1.0

- Add `UserDataValue` in `log` to tag user data in the format cbcollect's
  redaction expects

## v2.0.0

- Moved to `log/slog` (removed internal logging structures/interfaces).

## v1.0.0

No functional changes since v0.1.0, bumping all 'tools-common' sub-modules to
v1.0.0.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
