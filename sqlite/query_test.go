package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueries(t *testing.T) {
	testDir := t.TempDir()

	db, err := Open(filepath.Join(testDir, "sqlite.db"))
	require.Nil(t, err)

	query := Query{
		Query: `
		create table if not exists vbucket_42 (
			seqno integer not null primary key
		);`,
	}

	affected, err := ExecuteQuery(db, query)
	require.Nil(t, err)
	require.Empty(t, affected, int64(0))

	query.Query = "select seqno from vbucket_42 where seqno = 128;"

	var value uint64
	err = QueryRow(db, query, &value)
	require.NotNil(t, err)
	require.ErrorIs(t, err, ErrQueryReturnedNoRows)

	query.Query = "select seqno from vbucket_42 order by seqno;"

	var seqnos []uint64

	callback := func(scan ScanCallback) error {
		var seqno uint64
		err := scan(&seqno)
		seqnos = append(seqnos, seqno)

		return err
	}

	err = QueryRows(db, query, callback)
	require.NotNil(t, err)
	require.ErrorIs(t, err, ErrQueryReturnedNoRows)

	query.Query = "insert into vbucket_42 (seqno) values (?);"
	query.Arguments = []interface{}{128}

	affected, err = ExecuteQuery(db, query)
	require.Nil(t, err)
	require.Equal(t, int64(1), affected)

	query.Query = "select seqno from vbucket_42 where seqno = 128;"

	err = QueryRow(db, query, &value)
	require.Nil(t, err)
	require.Equal(t, uint64(128), value)

	query.Query = "insert into vbucket_42 (seqno) values (?);"
	query.Arguments = []interface{}{256}

	affected, err = ExecuteQuery(db, query)
	require.Nil(t, err)
	require.Equal(t, int64(1), affected)

	query.Query = "select seqno from vbucket_42 order by seqno;"

	err = QueryRows(db, query, callback)
	require.Nil(t, err)
	require.Equal(t, []uint64{128, 256}, seqnos)
}
