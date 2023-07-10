package sqlite

import "fmt"

// Pragma pepresents the string representation of an SQLite PRAGMA which can be used to query the SQLite library for
// internal (non-table) data.
type Pragma string

const (
	// PragmaUserVersion is an integer that is available to applications to use however they want; SQLite makes no use
	// of the user_version itself.
	PragmaUserVersion Pragma = "user_version"

	// PragmaPageSize is size for the database, the value provided to this pragma must be a power of two between 512 and
	// 65536 inclusive.
	PragmaPageSize Pragma = "page_size"

	// PragmaPageCount is the total number of pages in the database file.
	PragmaPageCount Pragma = "page_count"

	// PragmaCacheSize is the suggested maximum number of database disk pages that SQLite will hold in memory at once
	// per open database file.
	PragmaCacheSize Pragma = "cache_size"
)

// GetPragma queries the provided pragma and stores the result in the provided interface; its the job of the caller to
// ensure the provided type is valid for the value returned by the pragma.
func GetPragma(db Queryable, pragma Pragma, data any) error {
	query := Query{
		Query: fmt.Sprintf("pragma %s;", pragma),
	}

	return QueryRow(db, query, data)
}

// SetPragma sets the provided pragma to the given value; its the job of the caller to ensure the provided value is of a
// valid type for the pragma.
func SetPragma(db Executable, pragma Pragma, value any) error {
	query := Query{
		Query: fmt.Sprintf("pragma %s=%v;", pragma, value),
	}

	_, err := ExecuteQuery(db, query)

	return err
}
