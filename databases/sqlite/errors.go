package sqlite

import "errors"

// ErrQueryReturnedNoRows is returned when the provided query returned no rows when executed.
var ErrQueryReturnedNoRows = errors.New("query returned no rows")

// ErrDBLocked is returned when attempting to query/execute a query against an SQLite database/transaction which is
// already locked by another thread/process.
var ErrDBLocked = errors.New("SQLite database file is locked by another thread/process, see the logs for " +
	"more information")
