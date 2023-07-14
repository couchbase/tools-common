# tools-common
[![Go Reference](https://pkg.go.dev/badge/github.com/couchbase/tools-common.svg)](https://pkg.go.dev/github.com/couchbase/tools-common)

Welcome to `tools-common`, this library contains various utilities used across several projects at Couchbase.

# Dependencies
The library is broken down into independently versioned sub-modules which each define their own dependencies; see the
module level `README.md` for specifics on dependencies.

# Testing

The `tools-common` library is broken down into separate modules, the unit testing for each module is run independently.

Firstly, ensure all the dependencies are installed:

- Go (1.18+)
- Make (Latest version available via package manager)
- Module specific dependencies (defined in the modules `README.md`)

Testing may then be run using the modules `Makefile`, running `make test` will run all the available unit tests where
`make coverage` will also generate a coverage report that will be automatically opened in the default browser.

You may want to filter which tests are run; this may be at the package or test level. For example, to run the
`TestContains` function in the `util/contains` package, `make PACKAGE='util/contains' TESTS='TestCoverage'` may be used.

The `PACKAGE` and `TESTS` variables may be used independently and also apply to the `coverage` target.

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

### Formatting
We automatically apply formatting on any staged Go files before committing. This may not be what you want if you ever
have some hunks in a file staged but not others, as it will cause all of them to be committed. This behaviour can be
configured using:

```sh
git config couchbase.tools.format.behaviour BEHAVIOUR
```

Where BEHAVIOUR is one of the following:

1. error: echo what files have incorrect formatting and quit
2. fix: echo what files have incorrect formatting, fix them and quit
3. stage: echo what files have incorrect formatting, fix and stage them and quit
4. commit/no config value/invalid config value: fix the files with incorrect formatting, stage them and allow the commit
to proceed.

## Coding style

In this section we will cover notes on the exact coding style to use for this codebase. Most of the style rules are
enforced by the linters, so here we will only cover ones that are not.

## Versioning

In this section we will cover the versioning of `tools-common` sub-modules.

### Creating Tags

The sub-modules in `tools-common` are versioned independently following the [semantic versioning](https://semver.org)
scheme.

The release process should be as follows:

1. Create a commit which prepares the version by updating the `CHANGES.md` where relevant
2. Generate the commands required to tag using `./scripts/versioning/tag.py <module> <mode>`
3. Verify and run the output commands

```sh
$ ./scripts/versioning/tag.py fs major
git tag -a fs/v1.0.0
git push gerrit fs/v1.0.0 --no-verify
```

The `./scripts/versioning/tag.py` script will perform some sanity checks on the provided version.

### Dependency Order

The order in which dependencies are bumped is important to ensure all sub-modules receive the relevant bug fixes. The
order can be determine by using `./scripts/versioning/bump_order.py <module>`.

```sh
$ ./scripts/versioning/bump_order.py sync
sync, types, databases, http, environment, couchbase, cloud
```

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
