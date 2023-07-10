package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSetPragma(t *testing.T) {
	testDir := t.TempDir()

	db, err := Open(filepath.Join(testDir, "sqlite.db"))
	require.Nil(t, err)

	var value uint32
	err = GetPragma(db, PragmaUserVersion, &value)
	require.Nil(t, err)
	require.Equal(t, uint32(0), value)

	err = SetPragma(db, PragmaUserVersion, 42)
	require.Nil(t, err)

	err = GetPragma(db, PragmaUserVersion, &value)
	require.Nil(t, err)
	require.Equal(t, uint32(42), value)
}
