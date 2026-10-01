# Changes

## v2.1.4

- Mask in args logging:
  - Values given inline, e.g. `--passphrase=secret`
  - Values starting with `-`, e.g. `--passphrase -secret`
  - `--salt`, `--client-cert-password` and `--client-key-password`
  - Flags naming repository contents: `--collection-string`, `--bucket`,
    `--include-data`, `--exclude-data`, `--include-buckets` and
    `--exclude-buckets`
  - `--key`/`-k`, which were previously tagged
- Match long flags given with a single dash in args logging, e.g. `-salt`
- Stop short flags matching longer flags by prefix in args logging, e.g. `-p`
  and `-period`

## v2.1.3

- Add `--auth-token` to flags to mask

## v2.1.2

- Add `--km-refresh-token` to flags to mask

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
