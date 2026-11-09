package firestore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/openshift-online/gecko/controllers/client/transport"
	fstransport "github.com/openshift-online/gecko/controllers/client/transport/firestore"
	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/kube-applier-gcp/pkg/api/kubeapplier"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// testMCName is the GCP project ID that identifies the MC.
	testMCName    = "test-project"
	testClusterID = "cluster-abc"
	// testGroupKey is the project-scoped groupKey used by existing integration tests.
	testGroupKey = "projects/test-ns/clusters/cluster-abc"
)

func emulatorOpts(t *testing.T) []option.ClientOption {
	t.Helper()
	host := os.Getenv("FIRESTORE_EMULATOR_HOST")
	if host == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set — skipping integration test")
	}
	return []option.ClientOption{
		option.WithEndpoint(host),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
}

func newTestClient(t *testing.T) *fstransport.Client {
	t.Helper()
	log, err := logger.NewLogger(logger.DefaultConfig())
	require.NoError(t, err)
	opts := emulatorOpts(t)
	return fstransport.New(log, opts...)
}

func hcManifest(t *testing.T, clusterID, clusterName string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "HostedCluster",
		"metadata":   map[string]any{"name": clusterName, "namespace": fmt.Sprintf("clusters-%s", clusterID)},
	})
	require.NoError(t, err)
	return raw
}

func npManifest(t *testing.T, clusterID, npName string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "NodePool",
		"metadata":   map[string]any{"name": npName, "namespace": fmt.Sprintf("clusters-%s", clusterID)},
	})
	require.NoError(t, err)
	return raw
}

func configMapManifest(t *testing.T, namespace, name string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
	})
	require.NoError(t, err)
	return raw
}

// clearCollection deletes all documents in a collection (used between tests).
func clearCollection(ctx context.Context, t *testing.T, client *firestore.Client, coll string) {
	t.Helper()
	snaps, err := client.Collection(coll).Documents(ctx).GetAll()
	require.NoError(t, err)
	for _, snap := range snaps {
		_, err := snap.Ref.Delete(ctx)
		require.NoError(t, err)
	}
}

func closeFirestoreClient(t *testing.T, client *firestore.Client) {
	t.Helper()
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
}

func uniqueIntegrationProject(prefix string) string {
	return fmt.Sprintf("gecko-%s-%s", prefix, strconv.FormatInt(time.Now().UnixNano(), 36))
}

func cleanupCollections(t *testing.T, clients []*firestore.Client, collections ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, client := range clients {
			for _, collection := range collections {
				clearCollection(ctx, t, client, collection)
			}
		}
	})
}

// applyDesireSpec builds a minimal ApplyDesireSpec for the given groupKey and resource name.
func applyDesireSpec(groupKey, name string) kubeapplier.ApplyDesireSpec {
	return kubeapplier.ApplyDesireSpec{
		ManagementCluster: testMCName,
		GroupKey:          groupKey,
		TargetItem: kubeapplier.ResourceReference{
			Group:     "hypershift.openshift.io",
			Version:   "v1beta1",
			Resource:  "hostedclusters",
			Namespace: fmt.Sprintf("clusters-%s", groupKey),
			Name:      name,
		},
	}
}

// specsApplyDesire returns an ApplyDesire suitable for writing to the specs DB.
func specsApplyDesire(groupKey, name string) kubeapplier.ApplyDesire {
	return kubeapplier.ApplyDesire{Spec: applyDesireSpec(groupKey, name)}
}

func deleteDesire(groupKey, name string, conditions []metav1.Condition) kubeapplier.DeleteDesire {
	return kubeapplier.DeleteDesire{
		Spec: kubeapplier.DeleteDesireSpec{
			ManagementCluster: testMCName,
			GroupKey:          groupKey,
			TargetItem:        applyDesireSpec(groupKey, name).TargetItem,
		},
		Status: kubeapplier.DeleteDesireStatus{Conditions: conditions},
	}
}

// statusApplyDesire returns an ApplyDesire as kube-applier-gcp writes it to the status DB.
func statusApplyDesire(groupKey, name string, conditions []metav1.Condition) kubeapplier.ApplyDesire {
	return kubeapplier.ApplyDesire{
		Spec:   applyDesireSpec(groupKey, name),
		Status: kubeapplier.ApplyDesireStatus{Conditions: conditions},
	}
}

func TestIntegration_Apply_WritesApplyAndReadDesires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)
	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")

	manifests := [][]byte{
		hcManifest(t, testClusterID, "my-hc"),
	}

	_, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)

	snaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, snaps, 1, "expected 1 ApplyDesire for the HostedCluster manifest")

	data := snaps[0].Data()
	assert.NotContains(t, data, "manifestWorkName")
	assert.NotNil(t, data["spec_kubeContent"])

	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, readSnaps, 1, "expected 1 ReadDesire for the HostedCluster manifest")
}

func TestIntegration_Apply_ManifestHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)
	project := uniqueIntegrationProject("apply")
	groupKey := testGroupKey

	specsClient, err := firestore.NewClientWithDatabase(ctx, project, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)
	cleanupCollections(t, []*firestore.Client{specsClient}, "applydesires", "readdesires")

	_, err = c.Apply(ctx, project, groupKey, [][]byte{nil})
	require.NoError(t, err)

	_, err = c.Apply(ctx, project, groupKey, [][]byte{[]byte(`{`)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse manifest")

	unknownManifest := []byte(`{"apiVersion":"custom.io/v1","kind":"Widget","metadata":{"name":"sample","namespace":"default"}}`)
	_, err = c.Apply(ctx, project, groupKey, [][]byte{unknownManifest})
	require.NoError(t, err)

	snapshots, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	var desire kubeapplier.ApplyDesire
	require.NoError(t, snapshots[0].DataTo(&desire))
	assert.Equal(t, "widgets", desire.Spec.TargetItem.Resource)
}

func TestIntegration_GetStatus_AllSuccessful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, statusClient, "applydesires")
	clearCollection(ctx, t, statusClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, statusClient, "applydesires")
	defer clearCollection(ctx, t, statusClient, "readdesires")

	const docID = "doc-1"

	writeResult, err := specsClient.Collection("applydesires").Doc(docID).Set(ctx, specsApplyDesire(testGroupKey, "my-hc"))
	require.NoError(t, err)
	applyStatus := statusApplyDesire(testGroupKey, "my-hc", []metav1.Condition{
		{Type: "Successful", Status: metav1.ConditionTrue, Reason: "NoErrors"},
	})
	applyStatus.Status.ObservedDesireUpdateTime = writeResult.UpdateTime
	_, err = statusClient.Collection("applydesires").Doc(docID).Set(ctx, applyStatus)
	require.NoError(t, err)

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	require.Len(t, status.Conditions, 1)
	assert.Equal(t, "Applied", status.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, status.Conditions[0].Status)
	assert.False(t, status.Stale)
}

func TestIntegration_GetStatus_IsolatesApplyStatusesByGroupKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, statusClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, statusClient, "applydesires")

	otherGroupKey, err := transport.ClusterGroupKey("other-ns", testClusterID)
	require.NoError(t, err)

	_, err = specsClient.Collection("applydesires").Doc("doc-1").Set(ctx, specsApplyDesire(testGroupKey, "hc-1"))
	require.NoError(t, err)
	_, err = specsClient.Collection("applydesires").Doc("doc-2").Set(ctx, specsApplyDesire(otherGroupKey, "hc-2"))
	require.NoError(t, err)
	_, err = statusClient.Collection("applydesires").Doc("doc-1").Set(ctx, statusApplyDesire(testGroupKey, "hc-1", []metav1.Condition{
		{Type: "Successful", Status: metav1.ConditionTrue, Reason: "NoErrors"},
	}))
	require.NoError(t, err)
	_, err = statusClient.Collection("applydesires").Doc("doc-2").Set(ctx, statusApplyDesire(otherGroupKey, "hc-2", []metav1.Condition{
		{Type: "Successful", Status: metav1.ConditionFalse, Reason: "Failed"},
	}))
	require.NoError(t, err)

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	require.Len(t, status.Conditions, 1)
	assert.Equal(t, "Applied", status.Conditions[0].Type)
	assert.Equal(t, metav1.ConditionTrue, status.Conditions[0].Status)
}

func TestIntegration_GetStatus_ExtractsHCKubeContent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, statusClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, statusClient, "readdesires")

	const docID = "rd-1"

	specsReadDesire := kubeapplier.ReadDesire{
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: testMCName,
			GroupKey:          testGroupKey,
			TargetItem: kubeapplier.ResourceReference{
				Group:     "hypershift.openshift.io",
				Version:   "v1beta1",
				Resource:  "hostedclusters",
				Namespace: "clusters-abc",
				Name:      "my-hc",
			},
		},
	}
	_, err = specsClient.Collection("readdesires").Doc(docID).Set(ctx, specsReadDesire)
	require.NoError(t, err)
	// status_kubeContent carries the live object at the document root.
	hcLiveObject := map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True"},
			},
			"controlPlaneEndpoint": map[string]any{"host": "api.my-hc.example.com"},
			"version": map[string]any{
				"history": []any{map[string]any{"version": "4.14.1", "state": "Completed"}},
			},
		},
	}
	_, err = statusClient.Collection("readdesires").Doc(docID).Set(ctx, map[string]any{
		"spec":               specsReadDesire.Spec,
		"status":             kubeapplier.ReadDesireStatus{},
		"status_kubeContent": hcLiveObject,
	})
	require.NoError(t, err)

	otherGroupKey, err := transport.ClusterGroupKey("other-ns", testClusterID)
	require.NoError(t, err)
	otherSpec := specsReadDesire.Spec
	otherSpec.GroupKey = otherGroupKey
	otherSpec.TargetItem.Name = "other-hc"
	_, err = specsClient.Collection("readdesires").Doc("rd-other").Set(ctx, kubeapplier.ReadDesire{Spec: otherSpec})
	require.NoError(t, err)
	_, err = statusClient.Collection("readdesires").Doc("rd-other").Set(ctx, map[string]any{
		"spec":               otherSpec,
		"status":             kubeapplier.ReadDesireStatus{},
		"status_kubeContent": hcLiveObject,
	})
	require.NoError(t, err)

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)

	key := "hypershift.openshift.io/v1beta1/hostedclusters/clusters-abc/my-hc"
	require.Contains(t, status.ResourceStatuses, key)
	assert.NotContains(t, status.ResourceStatuses, "hypershift.openshift.io/v1beta1/hostedclusters/clusters-abc/other-hc")
	assert.Equal(t, "True", status.ResourceStatuses[key]["availableCondition"])
	assert.Equal(t, "api.my-hc.example.com", status.ResourceStatuses[key]["controlPlaneEndpoint"])
	assert.Equal(t, "4.14.1", status.ResourceStatuses[key]["version"])
}

func TestIntegration_Delete_WritesDeleteDesireAndRemovesApplyRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")

	manifests := [][]byte{
		npManifest(t, testClusterID, "my-np"),
	}

	_, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)

	err = c.Delete(ctx, testMCName, testGroupKey)
	require.NoError(t, err)

	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Empty(t, applySnaps, "ApplyDesires should be deleted")

	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Empty(t, readSnaps, "ReadDesires should be deleted")

	deleteSnaps, err := specsClient.Collection("deletedesires").Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, deleteSnaps, 1, "DeleteDesire should have been written")

	var dd kubeapplier.DeleteDesire
	require.NoError(t, deleteSnaps[0].DataTo(&dd))
	assert.Empty(t, dd.Spec.ClusterID)
	assert.Empty(t, dd.Spec.NodePoolName)
	assert.Equal(t, testGroupKey, dd.Spec.GroupKey)
}

func TestIntegration_Delete_IsolatesNodePoolsWithSameNameAcrossClusters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	defer statusClient.Close()

	collections := []string{"applydesires", "readdesires", "deletedesires"}
	for _, coll := range collections {
		clearCollection(ctx, t, specsClient, coll)
		clearCollection(ctx, t, statusClient, coll)
	}
	defer func() {
		for _, coll := range collections {
			clearCollection(ctx, t, specsClient, coll)
			clearCollection(ctx, t, statusClient, coll)
		}
	}()

	const (
		nodePoolName   = "workers"
		firstClusterID = "customer-cluster-a"
		otherClusterID = "customer-cluster-b"
	)

	firstGroupKey, err := transport.NodePoolGroupKey("test-ns", firstClusterID, nodePoolName)
	require.NoError(t, err)
	otherGroupKey, err := transport.NodePoolGroupKey("test-ns", otherClusterID, nodePoolName)
	require.NoError(t, err)

	_, err = c.Apply(ctx, testMCName, firstGroupKey, [][]byte{
		npManifest(t, firstClusterID, nodePoolName),
	})
	require.NoError(t, err)
	_, err = c.Apply(ctx, testMCName, otherGroupKey, [][]byte{
		npManifest(t, otherClusterID, nodePoolName),
	})
	require.NoError(t, err)

	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", otherGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, applySnaps, 1)
	otherClusterRef := applySnaps[0].Ref

	err = c.Delete(ctx, testMCName, firstGroupKey)
	require.NoError(t, err)

	otherClusterSnap, err := otherClusterRef.Get(ctx)
	if assert.NoError(t, err, "deleting one NodePool must not delete the other cluster's ApplyDesire") {
		assert.True(t, otherClusterSnap.Exists())
	}
}

func TestIntegration_GetDeleteStatus_IsolatesByGroupKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()
	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	defer statusClient.Close()

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "deletedesires")
	clearCollection(ctx, t, statusClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, statusClient, "deletedesires")

	otherGroupKey, err := transport.ClusterGroupKey("other-ns", testClusterID)
	require.NoError(t, err)
	writeResult, err := specsClient.Collection("deletedesires").Doc("delete-1").Set(ctx, deleteDesire(testGroupKey, "hc-1", nil))
	require.NoError(t, err)
	deleteStatus := deleteDesire(testGroupKey, "hc-1", []metav1.Condition{
		{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionTrue},
	})
	deleteStatus.Status.ObservedDesireUpdateTime = writeResult.UpdateTime
	_, err = statusClient.Collection("deletedesires").Doc("delete-1").Set(ctx, deleteStatus)
	require.NoError(t, err)
	_, err = statusClient.Collection("deletedesires").Doc("delete-other").Set(ctx, deleteDesire(otherGroupKey, "hc-2", []metav1.Condition{
		{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionFalse},
	}))
	require.NoError(t, err)

	status, err := c.GetDeleteStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.True(t, status.AllSuccessful)
	assert.Zero(t, status.PendingCount)
	assert.Equal(t, 1, status.TotalCount)
}

func TestIntegration_CleanupDeleteDesires_LeavesStatusDocumentsForKubeApplier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, specsClient.Close()) })
	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, statusClient.Close()) })

	clearCollection(ctx, t, specsClient, "deletedesires")
	clearCollection(ctx, t, statusClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, statusClient, "deletedesires")

	const documentID = "delete-1"
	otherGroupKey, err := transport.ClusterGroupKey("other-ns", testClusterID)
	require.NoError(t, err)
	writeResult, err := specsClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteDesire(testGroupKey, "hc-1", nil))
	require.NoError(t, err)
	_, err = specsClient.Collection("deletedesires").Doc("delete-other").Set(ctx, deleteDesire(otherGroupKey, "hc-2", nil))
	require.NoError(t, err)
	deleteStatus := deleteDesire(testGroupKey, "hc-1", []metav1.Condition{
		{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionTrue},
	})
	deleteStatus.Status.ObservedDesireUpdateTime = writeResult.UpdateTime
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)

	require.NoError(t, c.CleanupDeleteDesires(ctx, testMCName, testGroupKey))
	// Controllers retry cleanup after a transient finalizer-update failure.
	require.NoError(t, c.CleanupDeleteDesires(ctx, testMCName, testGroupKey))

	specSnap, err := specsClient.Collection("deletedesires").Doc(documentID).Get(ctx)
	require.NotNil(t, specSnap)
	assert.False(t, specSnap.Exists())
	assert.Equal(t, codes.NotFound, status.Code(err), "Gecko should remove the spec document")
	otherSpecSnap, err := specsClient.Collection("deletedesires").Doc("delete-other").Get(ctx)
	require.NoError(t, err)
	assert.True(t, otherSpecSnap.Exists(), "cleanup must not touch another groupKey's DeleteDesire")

	statusSnap, err := statusClient.Collection("deletedesires").Doc(documentID).Get(ctx)
	require.NoError(t, err)
	assert.True(t, statusSnap.Exists(), "kube-applier-gcp owns status document cleanup")
}

func TestIntegration_GetDeleteStatus_RequiresExactStatusRevision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, specsClient.Close()) })
	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, statusClient.Close()) })

	clearCollection(ctx, t, specsClient, "deletedesires")
	clearCollection(ctx, t, statusClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, statusClient, "deletedesires")

	const documentID = "delete-1"
	_, err = specsClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteDesire(testGroupKey, "hc-1", nil))
	require.NoError(t, err)
	specSnap, err := specsClient.Collection("deletedesires").Doc(documentID).Get(ctx)
	require.NoError(t, err)

	deleteStatus := deleteDesire(testGroupKey, "hc-1", []metav1.Condition{
		{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionTrue},
	})
	deleteStatus.Status.ObservedDesireUpdateTime = specSnap.UpdateTime.Add(-time.Second)
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)

	result, err := c.GetDeleteStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.False(t, result.AllSuccessful, "a status from an earlier DeleteDesire must not complete the current cycle")
	assert.Equal(t, 1, result.PendingCount)
	assert.Equal(t, 1, result.TotalCount)

	deleteStatus.Status.ObservedDesireUpdateTime = specSnap.UpdateTime.Add(time.Second)
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)

	result, err = c.GetDeleteStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.False(t, result.AllSuccessful, "a status for a different DeleteDesire revision must not complete the current cycle")
	assert.Equal(t, 1, result.PendingCount)
	assert.Equal(t, 1, result.TotalCount)

	deleteStatus.Status.ObservedDesireUpdateTime = specSnap.UpdateTime
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)

	result, err = c.GetDeleteStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.True(t, result.AllSuccessful, "a status observing the current DeleteDesire revision must complete the current cycle")
	assert.Zero(t, result.PendingCount)
	assert.Equal(t, 1, result.TotalCount)
}

// TestIntegration_Delete_ChunksLargeBatches verifies that Delete correctly
// processes more resources than the Firestore 500-write-per-transaction limit
// by chunking them into multiple transactions.
func TestIntegration_Delete_ChunksLargeBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")

	// Seed 200 ApplyDesire + ReadDesire docs — enough to require 2 chunks
	// (maxDeleteBatchSize = 166, so 200 requires 2 transactions).
	const resourceCount = 200
	chunkedGroupKey, err := transport.ClusterGroupKey("test-ns", "cluster-chunked")
	require.NoError(t, err)

	batch := specsClient.BulkWriter(ctx)
	for i := range resourceCount {
		name := fmt.Sprintf("resource-%d", i)
		docID := fmt.Sprintf("chunked-%d", i)

		ad := specsApplyDesire(chunkedGroupKey, name)
		applyRef := specsClient.Collection("applydesires").Doc(docID)
		_, err := batch.Set(applyRef, ad)
		require.NoError(t, err)

		rd := kubeapplier.ReadDesire{Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: testMCName,
			GroupKey:          chunkedGroupKey,
			TargetItem:        ad.Spec.TargetItem,
		}}
		readRef := specsClient.Collection("readdesires").Doc(docID)
		_, err = batch.Set(readRef, rd)
		require.NoError(t, err)
	}
	batch.Flush()

	err = c.Delete(ctx, testMCName, chunkedGroupKey)
	require.NoError(t, err)

	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", chunkedGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Empty(t, applySnaps, "all ApplyDesires should be deleted")

	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", chunkedGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Empty(t, readSnaps, "all ReadDesires should be deleted")

	deleteSnaps, err := specsClient.Collection("deletedesires").Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, deleteSnaps, resourceCount, "one DeleteDesire per resource")
}

func TestIntegration_DeleteStatusAndCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)
	project := uniqueIntegrationProject("delete")
	groupKey := testGroupKey

	specsClient, err := firestore.NewClientWithDatabase(ctx, project, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)
	statusClient, err := firestore.NewClientWithDatabase(ctx, project, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	cleanupCollections(t, []*firestore.Client{specsClient, statusClient}, "applydesires", "readdesires", "deletedesires")

	status, err := c.GetDeleteStatus(ctx, project, groupKey)
	require.NoError(t, err)
	assert.True(t, status.AllSuccessful)
	assert.Zero(t, status.TotalCount)
	assert.Zero(t, status.ApplyDesiresCount)
	require.NoError(t, c.Delete(ctx, project, groupKey))

	_, err = c.Apply(ctx, project, groupKey, [][]byte{npManifest(t, testClusterID, "delete-status-np")})
	require.NoError(t, err)
	status, err = c.GetDeleteStatus(ctx, project, groupKey)
	require.NoError(t, err)
	assert.False(t, status.AllSuccessful)
	assert.Zero(t, status.TotalCount)
	assert.Equal(t, 1, status.ApplyDesiresCount)

	require.NoError(t, c.Delete(ctx, project, groupKey))
	deleteSnapshots, err := specsClient.Collection("deletedesires").
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, deleteSnapshots, 1)
	documentID := deleteSnapshots[0].Ref.ID

	status, err = c.GetDeleteStatus(ctx, project, groupKey)
	require.NoError(t, err)
	assert.False(t, status.AllSuccessful)
	assert.Equal(t, 1, status.PendingCount)
	assert.Equal(t, 1, status.TotalCount)
	assert.Zero(t, status.ApplyDesiresCount)

	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, map[string]any{
		"spec":   kubeapplier.DeleteDesireSpec{GroupKey: groupKey},
		"status": "not-a-delete-desire-status",
	})
	require.NoError(t, err)
	_, err = c.GetDeleteStatus(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode")

	var deleteStatus kubeapplier.DeleteDesire
	require.NoError(t, deleteSnapshots[0].DataTo(&deleteStatus))
	deleteStatus.Status = kubeapplier.DeleteDesireStatus{Conditions: []metav1.Condition{{
		Type:   kubeapplier.ConditionTypeSuccessful,
		Status: metav1.ConditionFalse,
		Reason: "DeleteFailed",
	}}}
	deleteStatus.Status.ObservedDesireUpdateTime = deleteSnapshots[0].UpdateTime
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)
	status, err = c.GetDeleteStatus(ctx, project, groupKey)
	require.NoError(t, err)
	assert.False(t, status.AllSuccessful)
	assert.Equal(t, 1, status.PendingCount)

	deleteStatus.Status.Conditions[0].Status = metav1.ConditionTrue
	deleteStatus.Status.Conditions[0].Reason = "NoErrors"
	_, err = statusClient.Collection("deletedesires").Doc(documentID).Set(ctx, deleteStatus)
	require.NoError(t, err)
	status, err = c.GetDeleteStatus(ctx, project, groupKey)
	require.NoError(t, err)
	assert.True(t, status.AllSuccessful)
	assert.Zero(t, status.PendingCount)
	assert.Equal(t, 1, status.TotalCount)

	require.NoError(t, c.CleanupDeleteDesires(ctx, project, groupKey))
	remainingSpecs, err := specsClient.Collection("deletedesires").Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Empty(t, remainingSpecs)
	remainingStatuses, err := statusClient.Collection("deletedesires").Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, remainingStatuses, 1, "kube-applier-gcp owns status document cleanup")
	require.NoError(t, c.CleanupDeleteDesires(ctx, project, groupKey))

	_, err = specsClient.Collection("applydesires").Doc("malformed-apply-desire").Set(ctx, map[string]any{
		"spec": map[string]any{
			"groupKey":   groupKey,
			"targetItem": "not-a-resource-reference",
		},
	})
	require.NoError(t, err)
	_, err = c.GetStatus(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode specs apply desire")
	err = c.Delete(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode apply desire")
	_, err = specsClient.Collection("applydesires").Doc("malformed-apply-desire").Delete(ctx)
	require.NoError(t, err)

	const malformedStatusID = "malformed-status"
	malformedStatusSpec := specsApplyDesire(groupKey, "malformed-status")
	malformedStatusSpec.Spec.ManagementCluster = project
	_, err = specsClient.Collection("applydesires").Doc(malformedStatusID).Set(ctx, malformedStatusSpec)
	require.NoError(t, err)
	_, err = statusClient.Collection("applydesires").Doc(malformedStatusID).Set(ctx, map[string]any{
		"spec":   kubeapplier.ApplyDesireSpec{GroupKey: groupKey},
		"status": "not-an-apply-desire-status",
	})
	require.NoError(t, err)
	_, err = c.GetStatus(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode apply desire")
	_, err = specsClient.Collection("applydesires").Doc(malformedStatusID).Delete(ctx)
	require.NoError(t, err)

	readSpec := kubeapplier.ReadDesire{Spec: kubeapplier.ReadDesireSpec{
		ManagementCluster: project,
		GroupKey:          groupKey,
		TargetItem: kubeapplier.ResourceReference{
			Group: "hypershift.openshift.io", Version: "v1beta1", Resource: "hostedclusters", Namespace: "clusters-abc", Name: "malformed-status",
		},
	}}
	_, err = specsClient.Collection("readdesires").Doc(malformedStatusID).Set(ctx, readSpec)
	require.NoError(t, err)
	_, err = statusClient.Collection("readdesires").Doc(malformedStatusID).Set(ctx, map[string]any{
		"spec":   readSpec.Spec,
		"status": "not-a-read-desire-status",
	})
	require.NoError(t, err)
	_, err = c.GetStatus(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode read desire")

	_, err = statusClient.Collection("readdesires").Doc(malformedStatusID).Set(ctx, map[string]any{
		"spec":               readSpec.Spec,
		"status":             kubeapplier.ReadDesireStatus{},
		"status_kubeContent": map[string]any{"status": "not-an-object"},
	})
	require.NoError(t, err)
	_, err = c.GetStatus(ctx, project, groupKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extract resource status")
}

// TestIntegration_Apply_StaleWhenStatusNotProcessed verifies that Apply returns
// Stale=true when the status DB has not yet been updated by kube-applier-gcp
// (i.e. ObservedDesireUpdateTime does not match the spec write timestamp).
func TestIntegration_Apply_StaleWhenStatusNotProcessed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, statusClient, "applydesires")
	clearCollection(ctx, t, statusClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, statusClient, "applydesires")
	defer clearCollection(ctx, t, statusClient, "readdesires")

	manifests := [][]byte{npManifest(t, testClusterID, "stale-np")}

	// First Apply — no status docs exist yet → stale.
	status, err := c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)
	assert.True(t, status.Stale, "should be stale when no status docs exist")

	// Simulate kube-applier-gcp processing: write status docs with a
	// mismatched ObservedDesireUpdateTime (an old timestamp).
	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, applySnaps, 1)

	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = statusClient.Collection("applydesires").Doc(applySnaps[0].Ref.ID).Set(ctx, map[string]any{
		"spec": applyDesireSpec(testGroupKey, "stale-np"),
		"status": kubeapplier.ApplyDesireStatus{
			Conditions:               []metav1.Condition{{Type: "Successful", Status: metav1.ConditionTrue, Reason: "NoErrors"}},
			ObservedDesireUpdateTime: oldTime,
		},
	})
	require.NoError(t, err)

	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, readSnaps, 1)

	_, err = statusClient.Collection("readdesires").Doc(readSnaps[0].Ref.ID).Set(ctx, map[string]any{
		"spec": kubeapplier.ReadDesireSpec{GroupKey: testGroupKey, TargetItem: applyDesireSpec(testGroupKey, "stale-np").TargetItem},
		"status": kubeapplier.ReadDesireStatus{
			ObservedDesireUpdateTime: oldTime,
		},
	})
	require.NoError(t, err)

	// Second Apply — status docs exist but with old timestamps → stale.
	status, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)
	assert.True(t, status.Stale, "should be stale when ObservedDesireUpdateTime is old")

	// Now simulate kube-applier-gcp catching up: update status docs with the
	// current spec write timestamps. We need to read the spec UpdateTime.
	applySnaps, err = specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, applySnaps, 1)

	_, err = statusClient.Collection("applydesires").Doc(applySnaps[0].Ref.ID).Set(ctx, map[string]any{
		"spec": applyDesireSpec(testGroupKey, "stale-np"),
		"status": kubeapplier.ApplyDesireStatus{
			Conditions:               []metav1.Condition{{Type: "Successful", Status: metav1.ConditionTrue, Reason: "NoErrors"}},
			ObservedDesireUpdateTime: applySnaps[0].UpdateTime,
		},
	})
	require.NoError(t, err)

	readSnaps, err = specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, readSnaps, 1)

	_, err = statusClient.Collection("readdesires").Doc(readSnaps[0].Ref.ID).Set(ctx, map[string]any{
		"spec": kubeapplier.ReadDesireSpec{GroupKey: testGroupKey, TargetItem: applyDesireSpec(testGroupKey, "stale-np").TargetItem},
		"status": kubeapplier.ReadDesireStatus{
			ObservedDesireUpdateTime: readSnaps[0].UpdateTime,
		},
	})
	require.NoError(t, err)

	// Third Apply — same manifests, status timestamps now match → not stale.
	status, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)
	assert.False(t, status.Stale, "should not be stale when ObservedDesireUpdateTime matches")
}

func TestIntegration_GetStatus_PendingWhenOneOfTwoApplyStatusesIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()
	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	defer statusClient.Close()

	collections := []string{"applydesires", "readdesires"}
	for _, collection := range collections {
		clearCollection(ctx, t, specsClient, collection)
		clearCollection(ctx, t, statusClient, collection)
	}
	defer func() {
		for _, collection := range collections {
			clearCollection(ctx, t, specsClient, collection)
			clearCollection(ctx, t, statusClient, collection)
		}
	}()

	manifests := [][]byte{
		hcManifest(t, testClusterID, "my-hc"),
		configMapManifest(t, "clusters-"+testClusterID, "cluster-config"),
	}
	_, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)

	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, applySnaps, 2)
	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, readSnaps, 2)

	var applyDesire kubeapplier.ApplyDesire
	require.NoError(t, applySnaps[0].DataTo(&applyDesire))
	applyDesire.Status = kubeapplier.ApplyDesireStatus{
		Conditions:               []metav1.Condition{{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionTrue}},
		ObservedDesireUpdateTime: applySnaps[0].UpdateTime,
	}
	_, err = statusClient.Collection("applydesires").Doc(applySnaps[0].Ref.ID).Set(ctx, applyDesire)
	require.NoError(t, err)

	for _, snap := range readSnaps {
		var readDesire kubeapplier.ReadDesire
		require.NoError(t, snap.DataTo(&readDesire))
		readDesire.Status.ObservedDesireUpdateTime = snap.UpdateTime
		_, err = statusClient.Collection("readdesires").Doc(snap.Ref.ID).Set(ctx, readDesire)
		require.NoError(t, err)
	}

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	require.Len(t, status.Conditions, 1)
	assert.Equal(t, metav1.ConditionFalse, status.Conditions[0].Status)
	assert.Equal(t, "Pending", status.Conditions[0].Reason)
	assert.True(t, status.Stale, "a missing expected Apply status must make the result stale")
}

func TestIntegration_GetStatus_StaleWhenOneOfTwoReadStatusesIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()
	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	defer statusClient.Close()

	collections := []string{"applydesires", "readdesires"}
	for _, collection := range collections {
		clearCollection(ctx, t, specsClient, collection)
		clearCollection(ctx, t, statusClient, collection)
	}
	defer func() {
		for _, collection := range collections {
			clearCollection(ctx, t, specsClient, collection)
			clearCollection(ctx, t, statusClient, collection)
		}
	}()

	manifests := [][]byte{
		hcManifest(t, testClusterID, "my-hc"),
		configMapManifest(t, "clusters-"+testClusterID, "cluster-config"),
	}
	_, err = c.Apply(ctx, testMCName, testGroupKey, manifests)
	require.NoError(t, err)

	applySnaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, applySnaps, 2)
	for _, snap := range applySnaps {
		var applyDesire kubeapplier.ApplyDesire
		require.NoError(t, snap.DataTo(&applyDesire))
		applyDesire.Status = kubeapplier.ApplyDesireStatus{
			Conditions:               []metav1.Condition{{Type: kubeapplier.ConditionTypeSuccessful, Status: metav1.ConditionTrue}},
			ObservedDesireUpdateTime: snap.UpdateTime,
		}
		_, err = statusClient.Collection("applydesires").Doc(snap.Ref.ID).Set(ctx, applyDesire)
		require.NoError(t, err)
	}

	readSnaps, err := specsClient.Collection("readdesires").
		Where("spec.groupKey", "==", testGroupKey).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, readSnaps, 2)
	var processedRead kubeapplier.ReadDesire
	require.NoError(t, readSnaps[0].DataTo(&processedRead))
	processedRead.Status.ObservedDesireUpdateTime = readSnaps[0].UpdateTime
	_, err = statusClient.Collection("readdesires").Doc(readSnaps[0].Ref.ID).Set(ctx, processedRead)
	require.NoError(t, err)

	var missingRead kubeapplier.ReadDesire
	require.NoError(t, readSnaps[1].DataTo(&missingRead))
	missingKey := transport.ResourceKey(
		missingRead.Spec.TargetItem.Group,
		missingRead.Spec.TargetItem.Version,
		missingRead.Spec.TargetItem.Resource,
		missingRead.Spec.TargetItem.Namespace,
		missingRead.Spec.TargetItem.Name,
	)

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	require.Len(t, status.Conditions, 1)
	assert.Equal(t, metav1.ConditionTrue, status.Conditions[0].Status)
	assert.True(t, status.Stale, "a missing expected Read status must make the result stale")
	require.Contains(t, status.ResourceStatuses, missingKey)
	assert.Empty(t, status.ResourceStatuses[missingKey])
}

// hcManifestNS builds a HostedCluster manifest with an explicit namespace,
// used in collision regression tests where the namespace is not derived from clusterID.
func hcManifestNS(t *testing.T, ns, name string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "HostedCluster",
		"metadata":   map[string]any{"name": name, "namespace": ns},
	})
	require.NoError(t, err)
	return raw
}

// npManifestNS builds a NodePool manifest with an explicit namespace,
// used in collision regression tests where the namespace is not derived from clusterID.
func npManifestNS(t *testing.T, ns, name string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "NodePool",
		"metadata":   map[string]any{"name": name, "namespace": ns},
	})
	require.NoError(t, err)
	return raw
}

// TestIntegration_Regression_HCCollision is a regression test for GCP-1153.
// Two HostedClusters with the same name in different namespaces must not
// interfere: deleting ns-alpha's cluster must leave ns-beta's Desires intact.
func TestIntegration_Regression_HCCollision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")

	const clusterName = "my-cluster"
	gkAlpha, err := transport.ClusterGroupKey("ns-alpha", clusterName)
	require.NoError(t, err)
	gkBeta, err := transport.ClusterGroupKey("ns-beta", clusterName)
	require.NoError(t, err)

	_, err = c.Apply(ctx, testMCName, gkAlpha, [][]byte{hcManifestNS(t, "ns-alpha", clusterName)})
	require.NoError(t, err)
	_, err = c.Apply(ctx, testMCName, gkBeta, [][]byte{hcManifestNS(t, "ns-beta", clusterName)})
	require.NoError(t, err)

	// Capture ns-beta's ApplyDesire ref before deleting ns-alpha.
	snaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", gkBeta).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, snaps, 1, "expected exactly one ApplyDesire for ns-beta")
	nsBetaRef := snaps[0].Ref

	// Delete ns-alpha's cluster — must not touch ns-beta's Desires.
	err = c.Delete(ctx, testMCName, gkAlpha)
	require.NoError(t, err)

	nsBetaSnap, err := nsBetaRef.Get(ctx)
	require.NoError(t, err, "deleting ns-alpha cluster must not delete ns-beta's ApplyDesire")
	require.True(t, nsBetaSnap.Exists(), "ns-beta ApplyDesire must still exist after ns-alpha cluster Delete")
}

// TestIntegration_Regression_NodePoolCollision is a regression test for GCP-1153.
// Two NodePools with the same name in different namespace/cluster scopes must not
// interfere: deleting ns-alpha's NodePool must leave ns-beta's Desires intact.
func TestIntegration_Regression_NodePoolCollision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	defer c.Close()
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	defer specsClient.Close()

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, specsClient, "readdesires")
	clearCollection(ctx, t, specsClient, "deletedesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "readdesires")
	defer clearCollection(ctx, t, specsClient, "deletedesires")

	const npName = "workers"
	gkAlpha, err := transport.NodePoolGroupKey("ns-alpha", "cluster-a", npName)
	require.NoError(t, err)
	gkBeta, err := transport.NodePoolGroupKey("ns-beta", "cluster-b", npName)
	require.NoError(t, err)

	_, err = c.Apply(ctx, testMCName, gkAlpha, [][]byte{npManifestNS(t, "ns-alpha", npName)})
	require.NoError(t, err)
	_, err = c.Apply(ctx, testMCName, gkBeta, [][]byte{npManifestNS(t, "ns-beta", npName)})
	require.NoError(t, err)

	// Capture ns-beta's ApplyDesire ref before deleting ns-alpha's NodePool.
	snaps, err := specsClient.Collection("applydesires").
		Where("spec.groupKey", "==", gkBeta).
		Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, snaps, 1, "expected exactly one ApplyDesire for ns-beta")
	nsBetaRef := snaps[0].Ref

	// Delete ns-alpha's NodePool — must not touch ns-beta's Desires.
	err = c.Delete(ctx, testMCName, gkAlpha)
	require.NoError(t, err)

	nsBetaSnap, err := nsBetaRef.Get(ctx)
	require.NoError(t, err, "deleting ns-alpha NodePool must not delete ns-beta's ApplyDesire")
	require.True(t, nsBetaSnap.Exists(), "ns-beta ApplyDesire must still exist after ns-alpha NodePool Delete")
}

func TestIntegration_GetStatus_UsesSpecUpdateTimeForStaleness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newTestClient(t)
	opts := emulatorOpts(t)

	specsClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "specs", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, specsClient)

	statusClient, err := firestore.NewClientWithDatabase(ctx, testMCName, "status", opts...)
	require.NoError(t, err)
	closeFirestoreClient(t, statusClient)

	clearCollection(ctx, t, specsClient, "applydesires")
	clearCollection(ctx, t, statusClient, "applydesires")
	defer clearCollection(ctx, t, specsClient, "applydesires")
	defer clearCollection(ctx, t, statusClient, "applydesires")

	const docID = "doc-getstatus"
	writeResult, err := specsClient.Collection("applydesires").Doc(docID).Set(ctx, specsApplyDesire(testGroupKey, "my-hc"))
	require.NoError(t, err)
	applyStatus := statusApplyDesire(testGroupKey, "my-hc", []metav1.Condition{
		{Type: "Successful", Status: metav1.ConditionTrue, Reason: "NoErrors"},
	})
	applyStatus.Status.ObservedDesireUpdateTime = writeResult.UpdateTime.Add(-time.Second)
	_, err = statusClient.Collection("applydesires").Doc(docID).Set(ctx, applyStatus)
	require.NoError(t, err)

	status, err := c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.True(t, status.Stale, "status observing an older spec should be stale")

	applyStatus.Status.ObservedDesireUpdateTime = writeResult.UpdateTime
	_, err = statusClient.Collection("applydesires").Doc(docID).Set(ctx, applyStatus)
	require.NoError(t, err)

	status, err = c.GetStatus(ctx, testMCName, testGroupKey)
	require.NoError(t, err)
	assert.False(t, status.Stale, "status observing the current spec should not be stale")
}
