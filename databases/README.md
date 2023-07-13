# databases

The package `databases` contains sub-packages/utilities for improving/simplifying interactions with databases (e.g.
`sqlite3`).

# Dependencies

This package utilizes the [`go-sqlite3`](https://github.com/mattn/go-sqlite3) package which requires a C compiler to be
installed and the CC environment variable to be set in `go env`, and CGO to be enabled (`CGO_ENABLED=1`). See the latest
[README](https://github.com/mattn/go-sqlite3/blob/master/README.md) for the most up-to-date information.
