# tools-common
[![Go Reference](https://pkg.go.dev/badge/github.com/couchbase/tools-common.svg)](https://pkg.go.dev/github.com/couchbase/tools-common)

Welcome to `tools-common`, this package contains various utilities used by the Go code maintained by the tools team.

# Dependencies

This package utilizes the [`go-sqlite3`](https://github.com/mattn/go-sqlite3) package which requires GCC to be
installed, and CGO to be enabled (`CGO_ENABLED=1`). See the latest
[README](https://github.com/mattn/go-sqlite3/blob/master/README.md) for the most up-to-date information.

# Testing

Running the unit testing requires a number of dependencies:
- GCC (Latest version available via package manager)
- Go (1.19+)
- Make (Latest version available via package manager)

Once the required dependencies have been installed, testing can be done using the repository `Makefile`. Running `make
test` will run all the available unit tests.

Generating a coverage report from the unit testing may be done by running `make coverage`, this report will be
automatically opened in the default browser.

When running tests, you may want to filter which tests are run; this may be at the package or test level. To run all the
tests in the `netutil` package, run `make PACKAGE=netutil test`. To run only tests which match a given regular
expression, run `make TESTS=TestContains test`.

Note that these variables may be used in conjunction and both also apply to the `coverage` target.

# Contributing

The following sections cover some basics of how to contribute to `tools-common` whilst following some of our common
practices/conventions.

## Gerrit

To contribute to this codebase you can upload patches through [Gerrit](http://review.couchbase.org). Make sure you have
configured the git hooks as described in the [Git Hooks](#git-hooks) section so that the code is linted and formatted
before uploading the patch.

Once you are ready to make your first commit note that *all* commits must be linked to an MB. This is done by making
sure that the commit title has the following format `MB-XXXXX Commit title` where `MB-XXXXX` is a valid issue in
[Jira](https://issues.couchbase.com).

## Git Hooks

Before contributing any patches, the Git hooks should be configured to ensure code is correctly linted and formatted.

The Git hooks require the following dependencies:
- gofmt (Standard code formatting tool)
- gofumpt (A more opinionated code formatting tool)
- goimports (Automatic insertion/sorting of imported modules)
- golangci-lint (Bulk linting tool)
- sponge (Binary provided by `moreutils` which "soaks" all input before writing output)
- wget (Used to download the `commit-msg` hook from Gerrit)

Once installed, the Git hooks may be setup using the following command:

```sh
git config core.hooksPath .githooks
```

If the Git hooks have been setup correctly, the Gerrit `commit-msg` hook will be downloaded automatically when creating
your first commit. However, this can also be done manually by downloading the
[commit-msg](http://review.couchbase.org/tools/hooks/commit-msg) script, marking it as executable and placing it into
the `.githooks` directory.

## Coding style

In this section we will cover notes on the exact coding style to use for this codebase. Most of the style rules are
enforced by the linters, so here we will only cover ones that are not.

### Documenting

- All exported functions should have a matching docstring.
- Any non-trivial unexported function should also have a matching docstring. Note this is left up to the developer and
  reviewer consideration.
- Docstrings must end on a full stop (`.`).
- Comments must be wrapped at 120 characters.
- Notes on interesting/unexpected behavior should have a newline before them and use the `// NOTE:` prefix.

Please note that not all the code in the repository follows these rules, however, newly added/updated code should
generally adhere to them.

# Related Projects
- [`backup`](https://github.com/couchbase/backup)
- [`cbbs`](https://github.com/couchbase/cbbs)
- [`couchbase-cli`](https://github.com/couchbase/couchbase-cli)

# License
Copyright 2021 Couchbase Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
