package objgcp

import (
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
)

func TestComposerSetLock(t *testing.T) {
	composer := composer{
		c: &storage.Composer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), composer.c.Retention)

	now := time.Now()

	err := composer.SetLock(objcli.NewComplianceLock(now))

	require.NoError(t, err)

	require.Equal(t, storage.ObjectRetention{
		Mode:        "Locked",
		RetainUntil: now,
	}, *composer.c.Retention)
}

func TestComposerSetLockUndefined(t *testing.T) {
	composer := composer{
		c: &storage.Composer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), composer.c.Retention)

	err := composer.SetLock(&objcli.ObjectLock{Type: objval.LockTypeUndefined})

	require.Error(t, err)

	require.Equal(t, (*storage.ObjectRetention)(nil), composer.c.Retention)
}

func TestComposerSetLockNil(t *testing.T) {
	composer := composer{
		c: &storage.Composer{},
	}

	require.Equal(t, (*storage.ObjectRetention)(nil), composer.c.Retention)

	err := composer.SetLock(nil)

	require.NoError(t, err)

	require.Equal(t, (*storage.ObjectRetention)(nil), composer.c.Retention)
}
