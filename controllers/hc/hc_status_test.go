package hc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestApplyHostedClusterResultSkipsSemanticallyEqualTime(t *testing.T) {
	instant := time.Date(2026, time.October, 19, 12, 0, 0, 0, time.UTC)
	local := metav1.NewTime(instant.In(time.FixedZone("EDT", -4*60*60)))
	utc := metav1.NewTime(instant)
	cluster := &privatev1.Cluster{
		Status: privatev1.ClusterStatus{HostedClusterResult: &privatev1.HostedClusterResult{
			Version: "4.22.14",
			ObservedConditions: []metav1.Condition{{
				Type: "HostedClusterDegraded", Status: metav1.ConditionFalse,
				ObservedGeneration: 3, LastTransitionTime: local,
			}},
		}},
	}
	conditions, err := json.Marshal([]metav1.Condition{{
		Type: "Degraded", Status: metav1.ConditionFalse, LastTransitionTime: utc,
	}})
	require.NoError(t, err)

	changed, err := (&Reconciler{}).applyHostedClusterResult(cluster, true, map[string]string{
		"version":           "4.22.14",
		"availableVersions": "[]",
		"versionConditions": string(conditions),
	}, 3)

	require.NoError(t, err)
	require.False(t, changed, "equivalent instants must not cause a status write")
}
