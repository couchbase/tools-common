# Changes

## v3.1.0

- `Retryer` now allows more than 50 retries, after which a constant back-off is
  applied (depending on the retry algorithm).
- `Retryer` now adds some random jitter before backing off, reducing the
  likelihood of multiple simultaneous retries all retrying in sync.

## v3.0.2

- Upgraded dependencies.

## v3.0.1

- Expose retry delay calculation as a method.

## v3.0.0

- Removed `utils/maths` in favour of `min` and `max`.
- Retries may now be aborted early using 'AbortRetriesError'.
- Upgraded dependencies.
- Moved to `log/slog`

## v2.0.3

- Upgraded dependencies.

## v2.0.2

- Makes `RetryOptions` generic; missed when making `Retryer` generic.

## v2.0.1

- Renames module from `utils` to `utils/v2`.

## v2.0.0

- Made the `Retryer` type generic.

## v1.2.0

- Added a `Selection` function to 'crypto/random'.

## v1.1.0

- Added a 'crypto/random' package.

## v1.0.0

### Features

- BREAKING: Moved 'ratelimit' package from 'utils/ratelimit' to
  'types/ratelimit'.

## v0.1.0

Initial release. See [Is it possible to add a module to a multi-module
repository?](https://github.com/golang/go/wiki/Modules#is-it-possible-to-add-a-module-to-a-multi-module-repository.)
