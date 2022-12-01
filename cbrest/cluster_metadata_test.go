package cbrest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetClusterMetaData(t *testing.T) {
	type test struct {
		name             string
		enterprise       bool
		developerPreview bool
	}

	tests := []*test{
		{
			name:       "EE",
			enterprise: true,
		},
		{
			name: "CE",
		},
		{
			name:             "EE-DP",
			enterprise:       true,
			developerPreview: true,
		},
		{
			name:             "CE-DP",
			developerPreview: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{
				Enterprise:       test.enterprise,
				DeveloperPreview: test.developerPreview,
			})
			defer cluster.Close()

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)

			meta, err := client.getClusterMetaData()
			require.NoError(t, err)
			require.Equal(t, test.enterprise, meta.Enterprise)
			require.Equal(t, test.developerPreview, meta.DeveloperPreview)
		})
	}
}
