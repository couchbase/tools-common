package fsutil

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemove(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		require.NoError(t, Remove(path, false))
		require.NoFileExists(t, path)
	})

	t.Run("Directory", func(t *testing.T) {
		testDir := t.TempDir()

		require.NoError(t, Remove(testDir, false))
		require.NoFileExists(t, testDir)
	})

	t.Run("FileIgnoreNotExists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Remove(path, true))
	})

	t.Run("DirectoryIgnoreNotExists", func(t *testing.T) {
		require.NoError(t, Remove(filepath.Join(t.TempDir(), "dir"), true))
	})

	t.Run("DirectoryNotEmpty", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		err := Remove(testDir, false)
		require.Error(t, err)

		require.DirExists(t, testDir)
		require.FileExists(t, path)
	})
}

func TestRemoveAll(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		err := RemoveAll(path)
		require.NoError(t, err)
		require.DirExists(t, testDir)
		require.NoFileExists(t, path)
	})

	t.Run("Directory", func(t *testing.T) {
		testDir := t.TempDir()

		err := RemoveAll(testDir)
		require.NoError(t, err)
		require.NoDirExists(t, testDir)
	})

	t.Run("DirectoryNotEmpty", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		err := RemoveAll(testDir)
		require.NoError(t, err)
		require.NoDirExists(t, testDir)
	})
}
