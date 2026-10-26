package hc

import (
	"testing"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
)

func TestApplyHostedClusterResult_EmptyFeedbackDoesNotRewriteStatus(t *testing.T) {
	cluster := &privatev1.Cluster{}
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.15.0"}

	changed, err := (&Reconciler{}).applyHostedClusterResult(cluster, true, map[string]string{
		"version":           "4.15.0",
		"availableVersions": "[]",
		"versionConditions": "[]",
	}, cluster.Generation)

	require.NoError(t, err)
	require.False(t, changed)
}
