package sqlite

import (
	"database/sql"

	"github.com/couchbase/tools-common/sync"
)

// initBarrier ensures that a single thread performs initialization of the SQLite library.
//
// NOTE: When DCP streaming from a Couchbase Cluster, 'cbbackupmgr' may/will open vBucket files in any arbitrary order
// by any number of threads concurrently. This means multiple different threads can call 'sql.Open' concurrently, we've
// observed cases where this can cause a SIGSEGV whilst initializing the SQLite library.  We use an initialization
// barrier to ensure the first call to 'sql.Open' is performed by a single thread. See MB-41481 for more information.
var initBarrier = sync.NewInitBarrier()

// Open a new SQLite database on disk whilst ensuring that the first time this function is called the SQLite library is
// initialized by a single thread.
func Open(path string) (*sql.DB, error) {
	// Attempt to read from the barrier channel
	ok := initBarrier.Wait()

	// Open an SQLite database at the request location
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		if ok {
			initBarrier.Failed()
		}

		return nil, err
	}

	// We didn't read anything from the barrier channel, this means we're not the first thread to open an SQLite
	// database, this means we can safety return early as there is nothing extra that we need to do.
	if !ok {
		return db, nil
	}

	// We're the first thread to open an SQLite database, we should call 'Ping' to ensure that the first connection is
	// made ensuring the SQLite library is initialized.
	err = db.Ping()
	if err != nil {
		initBarrier.Failed()
		return nil, err
	}

	// Only close the barrier channel if the call to 'Ping' was successful, this allows other threads to begin using the
	// SQLite library whilst meaning failed calls to 'Ping' don't assume the SQLite library was initialized because it
	// might not have been.
	initBarrier.Success()

	return db, nil
}
