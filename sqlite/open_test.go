package sqlite

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	testDir := t.TempDir()

	db, err := Open(filepath.Join(testDir, "db.sqlite3"))
	require.Nil(t, err)

	_, err = db.Exec("pragma user_version=42;")
	require.Nil(t, err)

	res, err := db.Query("pragma user_version;")
	require.Nil(t, err)

	defer res.Close()

	require.True(t, res.Next())

	var version uint32

	require.Nil(t, res.Scan(&version))
	require.Equal(t, uint32(42), version)
}

// This is "smoke testing" a race condition opening the first SQLite database, our 'SQLiteOpen' function should handle
// this using an initialization barrier. If something goes wrong, this test will cause a segmentation violation, which
// when tested with Go 1.15+ will be picked up as a failure.
func TestOpenSmokeTestMultipleWorkers(t *testing.T) {
	testDir := t.TempDir()

	// Opens an new SQLite database and sets the user version to 1, setting the user_version ensures that the actual
	// connection will be opened.
	openAndSetUserVersion := func(t *testing.T, id int) {
		db, err := Open(filepath.Join(testDir, fmt.Sprintf("db-%d.sqlite3", id)))
		require.Nil(t, err)

		_, err = db.Exec("pragma user_version=1;")
		require.Nil(t, err)
	}

	var (
		signal = make(chan struct{})
		wg     sync.WaitGroup
	)

	// Create 'NumCPU' goroutines that are waiting on a synchronization barrier
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			<-signal
			openAndSetUserVersion(t, id)
		}(i)
	}

	// Give the runtime a chance to schedule the goroutines
	time.Sleep(50 * time.Millisecond)

	// Release all the workers at once, allowing them to create individual SQLite databases and set the user_version
	close(signal)
	wg.Wait()
}
