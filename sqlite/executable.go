package sqlite

import "database/sql"

// Executable allows the execute functions defined in this package to work against all the executable types exposed by
// the 'sql' module for example, '*sql.DB' and '*sql.Tx'.
type Executable interface {
	Exec(query string, args ...any) (sql.Result, error)
}
