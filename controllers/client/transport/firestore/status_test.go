package firestore

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openshift-online/kube-applier-gcp/pkg/api/kubeapplier"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func applyCond(status metav1.ConditionStatus) kubeapplier.ApplyDesire {
	return kubeapplier.ApplyDesire{
		Status: kubeapplier.ApplyDesireStatus{
			Conditions: []metav1.Condition{
				{Type: "Successful", Status: status, Reason: "NoErrors"},
			},
		},
	}
}

func TestAggregateConditions_AllSuccessful(t *testing.T) {
	desires := []kubeapplier.ApplyDesire{
		applyCond(metav1.ConditionTrue),
		applyCond(metav1.ConditionTrue),
	}
	conds := aggregateConditions(desires)
	require.Len(t, conds, 1)
	assert.Equal(t, "Applied", conds[0].Type)
	assert.Equal(t, metav1.ConditionTrue, conds[0].Status)
}

func TestAggregateConditions_OneNotSuccessful(t *testing.T) {
	desires := []kubeapplier.ApplyDesire{
		applyCond(metav1.ConditionTrue),
		applyCond(metav1.ConditionFalse),
	}
	conds := aggregateConditions(desires)
	require.Len(t, conds, 1)
	assert.Equal(t, metav1.ConditionFalse, conds[0].Status)
}

func TestAggregateConditions_Empty(t *testing.T) {
	conds := aggregateConditions(nil)
	require.Len(t, conds, 1)
	assert.Equal(t, metav1.ConditionFalse, conds[0].Status)
	assert.Equal(t, "NoApplyDesires", conds[0].Reason)
}

func TestAggregateConditions_SuccessfulUnknownIsPending(t *testing.T) {
	desires := []kubeapplier.ApplyDesire{
		applyCond(metav1.ConditionTrue),
		applyCond(metav1.ConditionUnknown),
	}
	conds := aggregateConditions(desires)
	require.Len(t, conds, 1)
	assert.Equal(t, metav1.ConditionFalse, conds[0].Status)
	assert.Equal(t, "Pending", conds[0].Reason)
}

func TestAggregateConditions_NoSuccessfulCondition(t *testing.T) {
	// Desire exists but kube-applier-gcp hasn't set status yet
	desires := []kubeapplier.ApplyDesire{
		{Status: kubeapplier.ApplyDesireStatus{Conditions: nil}},
	}
	conds := aggregateConditions(desires)
	assert.Equal(t, metav1.ConditionFalse, conds[0].Status)
	assert.Equal(t, "Pending", conds[0].Reason)
}

func makeReadDesire(ref kubeapplier.ResourceReference, kubeJSON []byte) kubeapplier.ReadDesire {
	rd := kubeapplier.ReadDesire{
		Spec: kubeapplier.ReadDesireSpec{TargetItem: ref},
	}
	if kubeJSON != nil {
		rd.Status.KubeContent = &runtime.RawExtension{Raw: kubeJSON}
	}
	return rd
}

func TestExtractResourceStatuses_HostedCluster(t *testing.T) {
	hcJSON, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True"},
				map[string]any{"type": "Degraded", "status": "False", "reason": "AsExpected"},
				map[string]any{"type": "ClusterVersionUpgradeable", "status": "True", "reason": "AsExpected"},
			},
			"controlPlaneEndpoint": map[string]any{"host": "api.example.com"},
			"version": map[string]any{
				"desired": map[string]any{"version": "4.14.1"},
				"availableUpdates": []any{
					map[string]any{"version": "4.14.1"},
					map[string]any{"version": "4.15.0"},
				},
				"history": []any{
					map[string]any{"version": "4.14.0", "state": "Completed"},
				},
			},
		},
	})
	require.NoError(t, err)

	ref := kubeapplier.ResourceReference{
		Group: "hypershift.openshift.io", Version: "v1beta1",
		Resource: "hostedclusters", Namespace: "clusters-abc", Name: "my-hc",
	}
	reads := []kubeapplier.ReadDesire{makeReadDesire(ref, hcJSON)}
	statuses, err := extractResourceStatuses(reads)
	require.NoError(t, err)

	key := "hypershift.openshift.io/v1beta1/hostedclusters/clusters-abc/my-hc"
	require.Contains(t, statuses, key)
	assert.Equal(t, "True", statuses[key]["availableCondition"])
	assert.Equal(t, "api.example.com", statuses[key]["controlPlaneEndpoint"])
	assert.Equal(t, "4.14.0", statuses[key]["version"])
	assert.Equal(t, "4.14.1", statuses[key]["desiredVersion"])
	assert.JSONEq(t, `["4.14.1","4.15.0"]`, statuses[key]["availableVersions"])
	var versionConditions []metav1.Condition
	require.NoError(t, json.Unmarshal([]byte(statuses[key]["versionConditions"]), &versionConditions))
	require.Len(t, versionConditions, 2)
	assert.Equal(t, "Degraded", versionConditions[0].Type)
	assert.Equal(t, metav1.ConditionFalse, versionConditions[0].Status)
	assert.Equal(t, "ClusterVersionUpgradeable", versionConditions[1].Type)
	assert.Equal(t, metav1.ConditionTrue, versionConditions[1].Status)
}

func TestExtractResourceStatuses_HostedCluster_PartialVersionSkipped(t *testing.T) {
	// History is newest-first: the first entry is Partial (upgrade in progress).
	// Only the Completed entry should be reported as the cluster version.
	hcJSON, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True"},
			},
			"version": map[string]any{
				"history": []any{
					map[string]any{"version": "4.15.0", "state": "Partial"},
					map[string]any{"version": "4.14.0", "state": "Completed"},
				},
			},
		},
	})
	require.NoError(t, err)

	ref := kubeapplier.ResourceReference{
		Group: "hypershift.openshift.io", Version: "v1beta1",
		Resource: "hostedclusters", Namespace: "clusters-abc", Name: "my-hc",
	}
	reads := []kubeapplier.ReadDesire{makeReadDesire(ref, hcJSON)}
	statuses, err := extractResourceStatuses(reads)
	require.NoError(t, err)

	key := "hypershift.openshift.io/v1beta1/hostedclusters/clusters-abc/my-hc"
	require.Contains(t, statuses, key)
	assert.Equal(t, "4.14.0", statuses[key]["version"], "should report the last Completed version, not the Partial one")
}

func TestExtractResourceStatuses_NodePool(t *testing.T) {
	npJSON, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": "True"},
				map[string]any{"type": "AllNodesHealthy", "status": "True"},
			},
		},
	})
	require.NoError(t, err)

	ref := kubeapplier.ResourceReference{
		Group: "hypershift.openshift.io", Version: "v1beta1",
		Resource: "nodepools", Namespace: "clusters-abc", Name: "my-np",
	}
	reads := []kubeapplier.ReadDesire{makeReadDesire(ref, npJSON)}
	statuses, err := extractResourceStatuses(reads)
	require.NoError(t, err)

	key := "hypershift.openshift.io/v1beta1/nodepools/clusters-abc/my-np"
	require.Contains(t, statuses, key)
	assert.Equal(t, "True", statuses[key]["readyCondition"])
	assert.Equal(t, "True", statuses[key]["allNodesHealthyCondition"])
}

func TestExtractResourceStatuses_NilKubeContent(t *testing.T) {
	ref := kubeapplier.ResourceReference{
		Group: "hypershift.openshift.io", Version: "v1beta1",
		Resource: "hostedclusters", Namespace: "clusters-abc", Name: "my-hc",
	}
	reads := []kubeapplier.ReadDesire{makeReadDesire(ref, nil)}
	statuses, err := extractResourceStatuses(reads)
	require.NoError(t, err)
	// Key is present but inner map is empty
	key := "hypershift.openshift.io/v1beta1/hostedclusters/clusters-abc/my-hc"
	assert.Contains(t, statuses, key)
	assert.Empty(t, statuses[key])
}

func TestExtractResourceStatuses_CorruptKubeContent_ReturnsError(t *testing.T) {
	ref := kubeapplier.ResourceReference{
		Group: "hypershift.openshift.io", Version: "v1beta1",
		Resource: "hostedclusters", Namespace: "clusters-abc", Name: "my-hc",
	}
	reads := []kubeapplier.ReadDesire{makeReadDesire(ref, []byte("not valid json"))}
	_, err := extractResourceStatuses(reads)
	require.Error(t, err)
}

func TestExtractCertFields(t *testing.T) {
	fields, err := extractCertFields([]byte(`{"status":{"conditions":[{"type":"Ready","status":"True"}]}}`))
	require.NoError(t, err)
	assert.Equal(t, "True", fields["readyCondition"])

	fields, err = extractCertFields([]byte(`{"status":{"conditions":[{"type":"Issuing","status":"True"}]}}`))
	require.NoError(t, err)
	assert.Empty(t, fields)

	_, err = extractCertFields([]byte(`{`))
	require.Error(t, err)
}

func TestExtractNPFields(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
		wantErr  bool
	}{
		{
			name: "all conditions present",
			input: `{
				"status": {
					"conditions": [
						{"type": "Ready", "status": "True"},
						{"type": "AllNodesHealthy", "status": "True"},
						{"type": "AllMachinesReady", "status": "True"},
						{"type": "UpdatingConfig", "status": "False"},
						{"type": "UpdatingVersion", "status": "False"}
					]
				}
			}`,
			expected: map[string]string{
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingConfigCondition":   "False",
				"updatingVersionCondition":  "False",
			},
		},
		{
			name: "partial conditions",
			input: `{
				"status": {
					"conditions": [
						{"type": "Ready", "status": "True"},
						{"type": "AllMachinesReady", "status": "False"}
					]
				}
			}`,
			expected: map[string]string{
				"readyCondition":            "True",
				"allMachinesReadyCondition": "False",
			},
		},
		{
			name:     "empty conditions array",
			input:    `{"status": {"conditions": []}}`,
			expected: map[string]string{},
		},
		{
			name:    "invalid JSON",
			input:   `{bad`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, err := extractNPFields([]byte(tt.input))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, fields)
		})
	}
}
