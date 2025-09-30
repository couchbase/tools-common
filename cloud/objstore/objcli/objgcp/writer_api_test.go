package objgcp

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
)

func TestWriterSetLock(t *testing.T) {
	writer := writer{
		w: &storage.Writer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), writer.w.Retention)

	now := time.Now()

	err := writer.SetLock(objcli.NewComplianceLock(now))

	require.NoError(t, err)

	require.Equal(t, storage.ObjectRetention{
		Mode:        "Locked",
		RetainUntil: now,
	}, *writer.w.Retention)
}

func TestWriterSetLockUndefined(t *testing.T) {
	writer := writer{
		w: &storage.Writer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), writer.w.Retention)

	err := writer.SetLock(&objcli.ObjectLock{Type: objval.LockTypeUndefined})

	require.Error(t, err)

	require.Equal(t, (*storage.ObjectRetention)(nil), writer.w.Retention)
}

func TestWriterSetLockNil(t *testing.T) {
	writer := writer{
		w: &storage.Writer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), writer.w.Retention)

	err := writer.SetLock(nil)

	require.NoError(t, err)

	require.Equal(t, (*storage.ObjectRetention)(nil), writer.w.Retention)
}
