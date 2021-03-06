package sqlite

import (
	"database/sql"
	"errors"

	"github.com/mattn/go-sqlite3"
)

// Query encapsulates a query and its arguments and is used by all the SQLite utility functions in this package.
type Query struct {
	Query     string
	Arguments []interface{}
}

// ScanCallback is a readability wrapper around the SQL 'Scan' function.
type ScanCallback func(dest ...interface{}) error

// RowCallback is a readability callback which will be run for each row returned by an SQLite query.
type RowCallback func(scan ScanCallback) error

// ExecuteQuery executes the provided query against the SQLite database and return the number of rows affected.
func ExecuteQuery(db Executable, query Query) (int64, error) {
	res, err := db.Exec(query.Query, query.Arguments...)
	if err != nil {
		return 0, handleError(err)
	}

	return res.RowsAffected()
}

// QueryRow executes a query that is only expected to return a single row (or where we only care about the first
// returned row). It's the callers job to ensure the destination types are valid for the expected return value from the
// query.
func QueryRow(db Queryable, query Query, dest ...interface{}) error {
	err := db.QueryRow(query.Query, query.Arguments...).Scan(dest...)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return ErrQueryReturnedNoRows
	}

	return err
}

// QueryRows execute a query which is expected to return one or more results. The provided callback will be run for each
// row returned by the query.
func QueryRows(db Queryable, query Query, callback RowCallback) error {
	rows, err := db.Query(query.Query, query.Arguments...)
	if err != nil {
		return handleError(err)
	}
	defer rows.Close()

	var containedRows bool

	for rows.Next() {
		err = callback(rows.Scan)
		if err != nil {
			return err
		}

		containedRows = true
	}

	if !containedRows {
		return ErrQueryReturnedNoRows
	}

	return rows.Err()
}

// handleError adds more context to an error where necessary. If the provided error is of an unknown/unhandled type, it
// will be returned as is.
func handleError(err error) error {
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) && sqliteError.Code == sqlite3.ErrBusy {
		return ErrDBLocked
	}

	return err
}
