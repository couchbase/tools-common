package sqlite

import "database/sql"

// Queryable allows the query functions defined in this package to work against all the queryable types exposed by the
// 'sql' module for example, '*sql.DB' and '*sql.Tx'.
type Queryable interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
