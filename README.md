# tools-common
[![Go Reference](https://pkg.go.dev/badge/github.com/couchbase/tools-common.svg)](https://pkg.go.dev/github.com/couchbase/tools-common)

Welcome to `tools-common`. This is package contains various utilities used by the Go code
maintained by the tools team.

# Dependencies

This package utilizes the [`go-sqlite3`](https://github.com/mattn/go-sqlite3) package which requires GCC to be
installed, and CGO to be enabled (`CGO_ENABLED=1`). For more information, see the latest
[README](https://github.com/mattn/go-sqlite3/blob/master/README.md) for the most up-to-date information.

## Contributing

To contribute to this code base you can upload patches through [Gerrit](http://review.couchbase.org). Make sure you have
configured the git hooks so that the code is linted and formatted before uploading the patch.

For the git hooks the following dependencies are required:

```
gofmt
gofumpt
goimports
golangci-lint
```

Once you have installed the dependencies set the git hooks path by using the command below:

```
git config core.hooksPath .githooks
```

Note that to push to [Gerrit](http://review.couchbase.org) you will also have to set up the ChangeID commit message
hook. You can do this by inserting the [commit-msg](http://review.couchbase.org/tools/hooks/commit-msg) script into
the `.githooks` directory.

Once you are ready to make your first commit note that *all* commits must be linked to an MB. This is done by making
sure that the commit title has the following format `MB-XXXXX Commit title` where `MB-XXXXX` is a valid issue in
[Jira](https://issues.couchbase.com).

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
