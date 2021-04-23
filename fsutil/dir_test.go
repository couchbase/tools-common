package fsutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirExists(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		testDir := t.TempDir()

		exists, err := DirExists(testDir)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("NotExists", func(t *testing.T) {
		testDir := t.TempDir()

		exists, err := DirExists(filepath.Join(testDir, "not-found"))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("NotADirectory", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		exists, err := DirExists(path)
		require.ErrorIs(t, err, ErrNotDir)
		require.False(t, exists)
	})
}

func TestMkdir(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		testDir := t.TempDir()

		err := Mkdir(testDir, 0, false, false)
		require.ErrorIs(t, err, os.ErrExist)
	})

	t.Run("ExistsIgnoreExists", func(t *testing.T) {
		testDir := t.TempDir()

		stats, err := os.Stat(testDir)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|0o755, stats.Mode())

		err = Mkdir(testDir, 0, false, true)
		require.NoError(t, err)

		// Mode should still be updated if it doesn't match what we expect it to be
		stats, err = os.Stat(testDir)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|DefaultDirMode, stats.Mode())
	})

	t.Run("NotExistsDefaultMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "dir")
		)

		err := Mkdir(path, 0, false, false)
		require.NoError(t, err)

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|DefaultDirMode, stats.Mode())
	})

	t.Run("NotExistsProvidedMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "dir")
		)

		err := Mkdir(path, 0o777, false, false)
		require.NoError(t, err)

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|0o777, stats.Mode())
	})

	t.Run("NotExistsDoNotCreateParents", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "a", "deeply", "nested", "directory")
		)

		err := Mkdir(path, 0, false, false)
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("NotExistsDoNotCreateParents", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "a", "deeply", "nested", "directory")
		)

		err := Mkdir(path, 0, true, false)
		require.NoError(t, err)

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|DefaultDirMode, stats.Mode())
	})
}
