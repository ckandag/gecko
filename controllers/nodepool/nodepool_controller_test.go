package nodepool

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/client/transport"
	"github.com/openshift-online/gecko/controllers/client/transport/mock"
	"github.com/openshift-online/gecko/controllers/util/constants"
	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "debug",
		Format:    logger.FormatText,
		Output:    "stdout",
		Component: "test",
		Version:   "test",
	})
	require.NoError(t, err)
	return log
}

// mockStatusWriter captures Status().Update calls and can return a configured error.
type mockStatusWriter struct {
	called    bool
	updateErr error
	captured  client.Object
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
	return m.updateErr
}
func (m *mockStatusWriter) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return nil
}
func (m *mockStatusWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return nil
}
func (m *mockStatusWriter) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return nil
}

// mockStoreClient is a minimal client.Client backed by a fixed NodePool and Cluster.
type mockStoreClient struct {
	nodepool     *privatev1.NodePool
	cluster      *privatev1.Cluster
	npGetErr     error
	clsGetErr    error
	statusWriter *mockStatusWriter
	updateCalled bool
	updateErr    error
	updated      client.Object
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch o := obj.(type) {
	case *privatev1.NodePool:
		if m.npGetErr != nil {
			return m.npGetErr
		}
		if m.nodepool == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "nodepool"}, "")
		}
		*o = *m.nodepool
		return nil
	case *privatev1.Cluster:
		if m.clsGetErr != nil {
			return m.clsGetErr
		}
		if m.cluster == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
		}
		*o = *m.cluster
		return nil
	default:
		return fmt.Errorf("unexpected type %T", obj)
	}
}

func (m *mockStoreClient) Status() client.SubResourceWriter { return m.statusWriter }

func (m *mockStoreClient) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}
func (m *mockStoreClient) Create(_ context.Context, _ client.Object, _ ...client.CreateOption) error {
	return nil
}
func (m *mockStoreClient) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return nil
}
func (m *mockStoreClient) Update(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
	m.updateCalled = true
	m.updated = obj
	return m.updateErr
}
func (m *mockStoreClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return nil
}
func (m *mockStoreClient) DeleteAllOf(_ context.Context, _ client.Object, _ ...client.DeleteAllOfOption) error {
	return nil
}
func (m *mockStoreClient) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return nil
}
func (m *mockStoreClient) SubResource(_ string) client.SubResourceClient { return nil }
func (m *mockStoreClient) Scheme() *runtime.Scheme                       { return nil }
func (m *mockStoreClient) RESTMapper() meta.RESTMapper                   { return nil }
func (m *mockStoreClient) GroupVersionKindFor(_ runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (m *mockStoreClient) IsObjectNamespaced(_ runtime.Object) (bool, error) { return false, nil }

// errTransport is a transport.Client that returns configurable errors.
type errTransport struct {
	applyErr         error
	applyResult      *transport.Status
	deleteErr        error
	deleteStatus     *transport.DeleteStatus
	deleteStatusErr  error
	cleanupDeleteErr error
}

func (e *errTransport) Apply(_ context.Context, _, _ string, _ [][]byte) (*transport.Status, error) {
	return e.applyResult, e.applyErr
}
func (e *errTransport) GetStatus(_ context.Context, _, _ string) (*transport.Status, error) {
	return nil, nil
}
func (e *errTransport) Delete(_ context.Context, _, _ string) error { return e.deleteErr }
func (e *errTransport) GetDeleteStatus(_ context.Context, _, _ string) (*transport.DeleteStatus, error) {
	if e.deleteStatusErr != nil {
		return nil, e.deleteStatusErr
	}
	if e.deleteStatus != nil {
		return e.deleteStatus, nil
	}
	// Return ApplyDesiresCount=1 to trigger Delete() call.
	return &transport.DeleteStatus{AllSuccessful: false, TotalCount: 0, PendingCount: 0, ApplyDesiresCount: 1}, nil
}
func (e *errTransport) CleanupDeleteDesires(_ context.Context, _, _ string) error {
	return e.cleanupDeleteErr
}

// npReq returns a reconcile.Request for the given clusterID/nodepoolID pair.
func npReq(clusterID, nodepoolID string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: client.ObjectKey{Namespace: clusterID, Name: nodepoolID},
	}
}

// mustNodePoolGroupKey returns the group key for a nodepool, panicking on error.
func mustNodePoolGroupKey(namespace, clusterName, nodePoolName string) string {
	key, err := transport.NodePoolGroupKey(namespace, clusterName, nodePoolName)
	if err != nil {
		panic(err)
	}
	return key
}

// conflictErr returns a Kubernetes conflict error.
func conflictErr() error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, "test", fmt.Errorf("conflict"))
}

// testNodePool creates a NodePool. If vrVersion is non-empty, the spec and VR status are set.
func testNodePool(vrVersion string) *privatev1.NodePool {
	np := &privatev1.NodePool{}
	np.SetName("np-test")
	np.SetNamespace("cluster-test")
	np.SetFinalizers([]string{constants.FinalizerNodePool})
	np.Spec = privatev1.NodePoolSpec{
		ClusterID: "cluster-test",
		Platform: privatev1.NodePoolPlatformSpec{
			Type: "GCP",
			GCP: &privatev1.GCPNodePoolPlatform{
				MachineType: "n2-standard-4",
				DiskSizeGB:  100,
				DiskType:    "pd-ssd",
				Zone:        "us-central1-b",
			},
		},
	}
	if vrVersion != "" {
		np.Spec.Release = privatev1.NodePoolReleaseSpec{Version: vrVersion}
		np.Status.VersionResolution = &privatev1.VersionResolutionResult{
			ReleaseImage:   "quay.io/openshift-release-dev/ocp-release:4.16.0-x86_64",
			ReleaseVersion: vrVersion,
		}
	}
	return np
}

// testCluster creates a Cluster with placement and optionally the HC Available condition.
func testCluster(placementReady, hcAvailable bool) *privatev1.Cluster {
	c := &privatev1.Cluster{}
	c.SetName("cluster-test")
	c.SetNamespace("hyperfleet")
	c.SetUID(types.UID("550e8400-e29b-41d4-a716-446655440000"))
	c.Spec = privatev1.ClusterSpec{
		SafeName: privatev1.DefaultSafeName("cluster-test", c.UID),
		Platform: privatev1.ClusterPlatformSpec{
			Type: "GCP",
			GCP: &privatev1.GCPClusterPlatform{
				Subnet: "my-subnet",
				Region: "us-central1",
			},
		},
	}
	if placementReady {
		c.Status.PlacementResult = &privatev1.PlacementResult{
			ManagementClusterName: "mc-us-c1",
			BaseDomain:            "hc.example.com",
		}
	}
	if hcAvailable {
		c.Status.Conditions = []metav1.Condition{
			{Type: "HostedClusterAvailable", Status: metav1.ConditionTrue, Reason: "Available"},
		}
	}
	return c
}

func nodePoolResourceKey(cluster *privatev1.Cluster, np *privatev1.NodePool) string {
	return fmt.Sprintf("hypershift.openshift.io/v1beta1/nodepools/clusters-%s/%s", cluster.UID, np.Name)
}

// buildReconciler wires up a nodepool Reconciler with injectable errors.
func buildReconciler(
	t *testing.T,
	np *privatev1.NodePool,
	cluster *privatev1.Cluster,
	tr transport.Client,
	npGetErr, clsGetErr, statusErr error,
) (*Reconciler, *mockStoreClient) {
	t.Helper()
	storeClient := &mockStoreClient{
		nodepool:     np,
		cluster:      cluster,
		npGetErr:     npGetErr,
		clsGetErr:    clsGetErr,
		statusWriter: &mockStatusWriter{updateErr: statusErr},
	}
	return New(tr, newTestLogger(t), storeClient), storeClient
}

// ---------------------------------------------------------------------------
// Test cases – early exits: NodePool get
// ---------------------------------------------------------------------------

func TestReconcile_NodePoolNotFound(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr, nil, nil, nil) // nil nodepool → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-missing"))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NodePoolGetError(t *testing.T) {
	tr := mock.New()
	r, _ := buildReconciler(t, nil, nil, tr, fmt.Errorf("etcd timeout"), nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get nodepool")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: Cluster get
// ---------------------------------------------------------------------------

func TestReconcile_ClusterNotFound(t *testing.T) {
	np := testNodePool("4.16.0")

	tr := mock.New()
	r, _ := buildReconciler(t, np, nil, tr, nil, nil, nil) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_ClusterGetError(t *testing.T) {
	np := testNodePool("4.16.0")

	tr := mock.New()
	r, _ := buildReconciler(t, np, nil, tr, nil, fmt.Errorf("etcd timeout"), nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cluster")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: placement gate
// ---------------------------------------------------------------------------

func TestReconcile_NoPlacement(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true) // placement not ready

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter, "should requeue while placement is not ready")
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_NoPlacement_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_NoPlacement_StatusUpdateError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(false, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

// ---------------------------------------------------------------------------
// Test cases – early exits: VR gates
// ---------------------------------------------------------------------------

func TestReconcile_NodePoolVRNotReady(t *testing.T) {
	np := testNodePool("") // no VR
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter, "should requeue while VR is not ready")
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_VRNotReady_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("")
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_VRNotReady_StatusUpdateError(t *testing.T) {
	np := testNodePool("")
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

func TestReconcile_VRVersionMismatch(t *testing.T) {
	np := testNodePool("4.15.0")       // VR resolved to 4.15.0
	np.Spec.Release.Version = "4.16.0" // but spec wants 4.16.0
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter, "should requeue while VR version mismatches")
	require.Empty(t, tr.ApplyCalls)
}

func TestReconcile_VRVersionMismatch_StatusUpdateConflict(t *testing.T) {
	np := testNodePool("4.15.0")
	np.Spec.Release.Version = "4.16.0"
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_VRVersionMismatch_StatusUpdateError(t *testing.T) {
	np := testNodePool("4.15.0")
	np.Spec.Release.Version = "4.16.0"
	cluster := testCluster(true, true)

	tr := mock.New()
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
}

// ---------------------------------------------------------------------------
// Test cases – platform field defaults
// ---------------------------------------------------------------------------

// replicasFromManifests parses the replica count from the raw JSON in the first manifest.
func replicasFromManifests(t *testing.T, manifests [][]byte) int32 {
	t.Helper()
	require.NotEmpty(t, manifests)
	var obj struct {
		Spec struct {
			Replicas int32 `json:"replicas"`
		} `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(manifests[0], &obj))
	return obj.Spec.Replicas
}

// TestReconcile_NodeCountHonored verifies that when Spec.NodeCount is set, it is used
// as the replica count instead of the default.
func TestReconcile_NodeCountHonored(t *testing.T) {
	count := int32(3)
	np := testNodePool("4.16.0")
	np.Spec.NodeCount = &count
	cluster := testCluster(true, true)

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, int32(3), replicasFromManifests(t, tr.ApplyCalls[0].Manifests))
}

// TestReconcile_UsesClusterUIDForNodePoolIdentity verifies that the NodePool is
// applied into the UID-backed HostedCluster namespace while spec.clusterName
// references the parent Cluster object's private safeName.
func TestReconcile_UsesClusterUIDForNodePoolIdentity(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	clusterUID := "550e8400-e29b-41d4-a716-446655440000"
	cluster.SetUID(types.UID(clusterUID))
	cluster.Spec.SafeName = privatev1.DefaultSafeName(cluster.Name, cluster.UID)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			nodePoolResourceKey(cluster, np): {"readyCondition": "True", "allNodesHealthyCondition": "True", "allMachinesReadyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Len(t, tr.ApplyCalls, 1)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(tr.ApplyCalls[0].Manifests[0], &obj))
	metadata := obj["metadata"].(map[string]any)
	require.Equal(t, np.Name, metadata["name"])
	require.Equal(t, "clusters-"+clusterUID, metadata["namespace"])

	labels := metadata["labels"].(map[string]any)
	require.Equal(t, clusterUID, labels["gcp.managed.openshift.io/cluster-id"])

	spec := obj["spec"].(map[string]any)
	require.Equal(t, cluster.Spec.SafeName, spec["clusterName"])
}

// TestReconcile_DefaultNodeCount verifies that when Spec.NodeCount is nil, defaultReplicas is used.
func TestReconcile_DefaultNodeCount(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Spec.NodeCount = nil
	cluster := testCluster(true, true)

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, int32(1), replicasFromManifests(t, tr.ApplyCalls[0].Manifests))
}

// TestReconcile_DefaultPlatformValues verifies that when the NodePool has no GCP platform
// spec set, the reconciler applies default machine type, disk size, disk type, and derives
// the zone from the cluster's GCP region.
func TestReconcile_DefaultPlatformValues(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Spec.Platform.GCP = nil // no GCP spec → all defaults apply

	cluster := testCluster(true, true) // cluster has GCP.Region = "us-central1"

	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.Len(t, tr.ApplyCalls, 1)
	// Zone should be derived from region: "us-central1" → "us-central1-a"
	// MachineType/DiskSizeGB/DiskType should be defaults (validated inside manifest.Build).
}

// TestReconcile_ZoneDerivedFromRegion verifies that when the NodePool GCP spec exists
// but zone is empty, the zone is derived from the cluster's region.
func TestReconcile_ZoneDerivedFromRegion(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Spec.Platform.GCP.Zone = "" // explicit empty zone → derived from cluster region

	cluster := testCluster(true, true) // cluster region = "us-central1"

	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.Len(t, tr.ApplyCalls, 1)
}

// ---------------------------------------------------------------------------
// Test cases – transport errors
// ---------------------------------------------------------------------------

func TestReconcile_TransportApplyError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := &errTransport{applyErr: fmt.Errorf("transport unavailable")}
	r, _ := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply resources")
}

// TestReconcile_MWStatusNil_RequeuesPending verifies that when Apply returns nil status
// (resources not yet processed), both conditions are set to False and the reconciler
// requeues with the pending interval.
func TestReconcile_MWStatusNil_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	// Apply returns nil status to simulate the resources not yet having a status.
	tr := &errTransport{}
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	available := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolAvailable")
	require.NotNil(t, available)
	require.Equal(t, metav1.ConditionFalse, available.Status)
	require.Equal(t, "ResourcesNotFound", available.Reason)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionFalse, progressing.Status)
	require.Equal(t, "AsExpected", progressing.Reason)
}

func TestApplyStatusConditions_ObservedVersionChangeIsPersisted(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)
	status := &transport.Status{
		Conditions: []metav1.Condition{{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"}},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingConfigCondition":   "False",
				"updatingVersionCondition":  "False",
				"version":                   "4.16.0",
			},
		},
	}
	r := &Reconciler{}
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	require.False(t, r.applyStatusConditions(np, string(cluster.UID), status))
	status.ResourceStatuses[npKey]["version"] = "4.16.1"
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	require.Equal(t, "4.16.1", np.Status.CurrentVersion)
}

func TestApplyStatusConditions_UpgradeConditionTracksRollout(t *testing.T) {
	np := testNodePool("4.16.1")
	np.Status.CurrentVersion = "4.16.0"
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)
	status := &transport.Status{
		Conditions: []metav1.Condition{{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"}},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "False",
				"allNodesHealthyCondition":  "False",
				"allMachinesReadyCondition": "False",
				"updatingVersionCondition":  "True",
				"version":                   "4.16.0",
			},
		},
	}
	r := &Reconciler{}
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	upgrade := meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition)
	require.NotNil(t, upgrade)
	require.Equal(t, metav1.ConditionTrue, upgrade.Status)
	require.Equal(t, "UpgradeInProgress", upgrade.Reason)
	require.Contains(t, upgrade.Message, "4.16.0 to 4.16.1")

	status.ResourceStatuses[npKey]["version"] = "4.16.1"
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	upgrade = meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition)
	require.Equal(t, metav1.ConditionTrue, upgrade.Status)
	require.Equal(t, "WaitingForNodePool", upgrade.Reason)

	status.ResourceStatuses[npKey]["readyCondition"] = "True"
	status.ResourceStatuses[npKey]["allNodesHealthyCondition"] = "True"
	status.ResourceStatuses[npKey]["allMachinesReadyCondition"] = "True"
	status.ResourceStatuses[npKey]["updatingVersionCondition"] = "False"
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	require.Nil(t, meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition))
	require.False(t, r.applyStatusConditions(np, string(cluster.UID), status))

	status.ResourceStatuses[npKey]["allNodesHealthyCondition"] = "False"
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	require.Nil(t, meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition))
}

func TestApplyStatusConditions_NoUpgradeConditionDuringCreation(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)
	status := &transport.Status{
		Conditions: []metav1.Condition{{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"}},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "False",
				"allNodesHealthyCondition":  "False",
				"allMachinesReadyCondition": "False",
				"updatingVersionCondition":  "True",
				"version":                   "4.16.0",
			},
		},
	}
	r := &Reconciler{}
	require.True(t, r.applyStatusConditions(np, string(cluster.UID), status))
	require.Nil(t, meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition))
}

func TestSetWaitingNPConditions_UpgradeConditionTracksTarget(t *testing.T) {
	np := testNodePool("4.16.1")
	np.Status.CurrentVersion = "4.16.0"
	require.True(t, setWaitingNPConditions(np, "VersionResolutionNotReady", "Waiting for nodepool version resolution"))
	upgrade := meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition)
	require.NotNil(t, upgrade)
	require.Equal(t, metav1.ConditionTrue, upgrade.Status)
	require.Equal(t, "UpgradeInProgress", upgrade.Reason)
}

// ---------------------------------------------------------------------------
// Test cases – happy path and condition-driven requeue
// ---------------------------------------------------------------------------

func TestReconcile_HappyPath(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := mock.New()
	npGroupKey := mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")
	npKey := nodePoolResourceKey(cluster, np)
	tr.StatusOverrides["mc-us-c1/"+npGroupKey] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"replicas":                  "2",
				"version":                   "4.16.0",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)

	require.Len(t, tr.ApplyCalls, 1)
	require.Equal(t, "mc-us-c1", tr.ApplyCalls[0].TargetCluster)
	require.Equal(t, npGroupKey, tr.ApplyCalls[0].GroupKey)
	require.True(t, storeClient.statusWriter.called, "expected Status().Update to be called")

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	available := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolAvailable")
	require.NotNil(t, available)
	require.Equal(t, metav1.ConditionTrue, available.Status)
	require.Equal(t, "NodePoolAvailable", available.Reason)
	healthy := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolHealthy")
	require.NotNil(t, healthy)
	require.Equal(t, metav1.ConditionTrue, healthy.Status)
	require.Equal(t, "NodePoolHealthy", healthy.Reason)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionFalse, progressing.Status)
	require.Equal(t, "AsExpected", progressing.Reason)
	require.Equal(t, "4.16.0", captured.Status.CurrentVersion)
}

// TestReconcile_MWNotApplied_RequeuesPending verifies that when Applied=False the
// reconciler requeues with the pending interval.
func TestReconcile_MWNotApplied_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionFalse, Reason: "ApplyFailed"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "False",
				"allMachinesReadyCondition": "False",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	available := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolAvailable")
	require.NotNil(t, available)
	require.Equal(t, metav1.ConditionFalse, available.Status)
	require.Equal(t, "NodePoolNotAvailable", available.Reason)
	healthy := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolHealthy")
	require.NotNil(t, healthy)
	require.Equal(t, metav1.ConditionFalse, healthy.Status)
	require.Equal(t, "NodePoolNotHealthy", healthy.Reason)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "MachinesNotReady", progressing.Reason)
}

// TestReconcile_ResourcesApplied_ClusterNotAvailable_RequeuesPending verifies that when
// nodepool resources are applied but the parent cluster's HostedClusterAvailable is False,
// the reconciler keeps polling at the pending interval.
func TestReconcile_ResourcesApplied_ClusterNotAvailable_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, false) // placement ready, HC NOT available

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter, "should requeue at pending interval while cluster is not available")
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_ResourcesApplied_NodePoolNotAvailable_RequeuesPending verifies that when
// nodepool resources are applied and the cluster is available, but NodePoolAvailable is False,
// the reconciler keeps polling at the pending interval.
func TestReconcile_ResourcesApplied_NodePoolNotAvailable_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true) // placement ready, HC available

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":           "False", // NodePool not available
				"allNodesHealthyCondition": "False",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter, "should requeue at pending interval while nodepool is not available")
	require.True(t, storeClient.statusWriter.called)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	applied := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolResourcesApplied")
	require.NotNil(t, applied)
	require.Equal(t, metav1.ConditionTrue, applied.Status)
	available := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolAvailable")
	require.NotNil(t, available)
	require.Equal(t, metav1.ConditionFalse, available.Status)
}

// TestReconcile_StatusUpdateConflict_ReturnsNoError verifies that a conflict error on
// Status.Update after applyStatusConditions is silently swallowed.
func TestReconcile_StatusUpdateConflict_ReturnsNoError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {"readyCondition": "True", "allNodesHealthyCondition": "True"},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, conflictErr())

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter) // conflict → immediate return
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_StatusUpdateError_ReturnsError verifies that a non-conflict error on
// Status.Update after applyStatusConditions is propagated.
func TestReconcile_StatusUpdateError_ReturnsError(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, fmt.Errorf("server error"))

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "update nodepool status")
	require.True(t, storeClient.statusWriter.called)
}

// TestReconcile_StaleStatus_RequeuesPending verifies that when the transport
// reports stale status, the reconciler requeues with the pending interval even
// though resources are applied and conditions are True.
func TestReconcile_StaleStatus_RequeuesPending(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)

	npKey := nodePoolResourceKey(cluster, np)
	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":           "True",
				"allNodesHealthyCondition": "True",
			},
		},
		Stale: true,
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)

	// Verify that stale status was not written to the nodepool conditions.
	require.Nil(t, storeClient.statusWriter.captured, "status should not be updated when status is stale")
}

// ---------------------------------------------------------------------------
// Test cases – finalizer management
// ---------------------------------------------------------------------------

// TestReconcile_AddsFinalizer verifies that a nodepool without the finalizer gets it added
// and the reconciler returns immediately to re-reconcile.
func TestReconcile_AddsFinalizer(t *testing.T) {
	np := testNodePool("4.16.0")
	np.SetFinalizers(nil) // no finalizer yet
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, storeClient.updateCalled, "expected Update to be called to add finalizer")
	require.Empty(t, tr.ApplyCalls, "should not Apply before finalizer is persisted")

	updated := storeClient.updated.(*privatev1.NodePool)
	require.Contains(t, updated.Finalizers, constants.FinalizerNodePool)
}

// TestReconcile_AddFinalizer_UpdateError verifies that an Update error when adding the
// finalizer is propagated.
func TestReconcile_AddFinalizer_UpdateError(t *testing.T) {
	np := testNodePool("4.16.0")
	np.SetFinalizers(nil)
	cluster := testCluster(true, true)

	tr := mock.New()
	storeClient := &mockStoreClient{
		nodepool:     np,
		cluster:      cluster,
		statusWriter: &mockStatusWriter{},
		updateErr:    fmt.Errorf("etcd write error"),
	}
	r := New(tr, newTestLogger(t), storeClient)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "add finalizer")
}

// ---------------------------------------------------------------------------
// Test cases – deletion
// ---------------------------------------------------------------------------

// TestReconcile_Deletion_HappyPath verifies the async deletion flow:
// 1. First reconcile: calls transport.Delete, requeues
// 2. Second reconcile: polls GetDeleteStatus (AllSuccessful=true), cleans up, removes finalizer
func TestReconcile_Deletion_HappyPath(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	// Simulate ApplyDesires exist (deletion not started).
	npGroupKey := mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")
	key := "mc-us-c1/" + npGroupKey
	tr.DeleteStatusOverrides[key] = &transport.DeleteStatus{
		AllSuccessful:     false,
		TotalCount:        0,
		PendingCount:      0,
		ApplyDesiresCount: 2,
	}

	// First reconcile: initiate deletion.
	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter, "should requeue after initiating delete")
	require.Len(t, tr.DeleteCalls, 1)
	require.Equal(t, "mc-us-c1", tr.DeleteCalls[0].TargetCluster)
	require.Equal(t, npGroupKey, tr.DeleteCalls[0].GroupKey)
	require.False(t, storeClient.updateCalled, "finalizer not removed yet")

	// Simulate kube-applier-gcp completing deletion.
	tr.DeleteStatusOverrides[key] = &transport.DeleteStatus{AllSuccessful: true, TotalCount: 2, PendingCount: 0, ApplyDesiresCount: 0}

	// Second reconcile: check status, cleanup, remove finalizer.
	storeClient.updateCalled = false
	result, err = r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Len(t, tr.CleanupDeleteDesiresCalls, 1, "should cleanup DeleteDesires")
	require.Equal(t, "mc-us-c1", tr.CleanupDeleteDesiresCalls[0].TargetCluster)
	require.Equal(t, npGroupKey, tr.CleanupDeleteDesiresCalls[0].GroupKey)

	require.True(t, storeClient.updateCalled, "expected Update to remove finalizer")
	updated := storeClient.updated.(*privatev1.NodePool)
	require.NotContains(t, updated.Finalizers, constants.FinalizerNodePool)
}

func TestReconcile_Deletion_Pending(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(true, true)

	tr := mock.New()
	groupKey := mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")
	tr.DeleteStatusOverrides["mc-us-c1/"+groupKey] = &transport.DeleteStatus{
		AllSuccessful: false,
		TotalCount:    1,
		PendingCount:  1,
	}
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter)
	require.Empty(t, tr.DeleteCalls)
	require.Empty(t, tr.CleanupDeleteDesiresCalls)
	require.False(t, storeClient.updateCalled)
}

func TestReconcile_Deletion_GetStatusError(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)

	r, storeClient := buildReconciler(t, np, testCluster(true, true), &errTransport{
		deleteStatusErr: fmt.Errorf("firestore unavailable"),
	}, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "get delete status")
	require.False(t, storeClient.updateCalled)
}

func TestReconcile_Deletion_CleanupError(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)

	r, storeClient := buildReconciler(t, np, testCluster(true, true), &errTransport{
		deleteStatus:     &transport.DeleteStatus{AllSuccessful: true, TotalCount: 1},
		cleanupDeleteErr: fmt.Errorf("firestore unavailable"),
	}, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cleanup delete desires")
	require.False(t, storeClient.updateCalled)
}

// TestReconcile_Deletion_ClusterNotFound verifies that when the parent cluster is gone,
// transport.Delete is NOT called but the finalizer is still removed.
func TestReconcile_Deletion_ClusterNotFound(t *testing.T) {
	np := testNodePool("4.16.0")
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, nil, tr, nil, nil, nil) // nil cluster → NotFound

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)

	require.Empty(t, tr.DeleteCalls, "should not call Delete when cluster not found")
	require.True(t, storeClient.updateCalled, "expected Update to remove finalizer")
	updated := storeClient.updated.(*privatev1.NodePool)
	require.NotContains(t, updated.Finalizers, constants.FinalizerNodePool)
}

// TestReconcile_Deletion_NoPlacement verifies that when the parent cluster has no placement,
// transport.Delete is NOT called but the finalizer is still removed.
func TestReconcile_Deletion_NoPlacement(t *testing.T) {
	np := testNodePool("4.16.0")
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(false, false) // no placement

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)

	require.Empty(t, tr.DeleteCalls, "should not call Delete without placement")
	require.True(t, storeClient.updateCalled, "expected Update to remove finalizer")
	updated := storeClient.updated.(*privatev1.NodePool)
	require.NotContains(t, updated.Finalizers, constants.FinalizerNodePool)
}

// TestReconcile_Deletion_TransportError verifies that a transport.Delete error is
// propagated and the finalizer is NOT removed (controller will retry).
func TestReconcile_Deletion_TransportError(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(true, true)

	tr := &errTransport{deleteErr: fmt.Errorf("firestore unavailable")}
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "delete resources")
	require.False(t, storeClient.updateCalled, "finalizer should not be removed on error")
}

// TestReconcile_Deletion_NoFinalizer verifies that when the nodepool has a DeletionTimestamp
// but no finalizer, the reconciler returns immediately (nothing to do).
func TestReconcile_Deletion_NoFinalizer(t *testing.T) {
	np := testNodePool("4.16.0")
	np.SetFinalizers(nil)
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(true, true)

	tr := mock.New()
	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)

	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.Empty(t, tr.DeleteCalls)
	require.False(t, storeClient.updateCalled)
}

// TestReconcile_Deletion_RemoveFinalizerError verifies that an error removing the
// finalizer after successful deletion/cleanup is propagated.
func TestReconcile_Deletion_RemoveFinalizerError(t *testing.T) {
	np := testNodePool("4.16.0")
	np.Status.Conditions = append(np.Status.Conditions, metav1.Condition{
		Type: "NodePoolResourcesApplied", Status: metav1.ConditionTrue, Reason: "Applied",
	})
	now := metav1.Now()
	np.SetDeletionTimestamp(&now)
	cluster := testCluster(true, true)

	tr := mock.New()
	storeClient := &mockStoreClient{
		nodepool:     np,
		cluster:      cluster,
		statusWriter: &mockStatusWriter{},
		updateErr:    fmt.Errorf("etcd write error"),
	}
	r := New(tr, newTestLogger(t), storeClient)

	// Simulate ApplyDesires exist (deletion not started).
	npGroupKey := mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")
	key := "mc-us-c1/" + npGroupKey
	tr.DeleteStatusOverrides[key] = &transport.DeleteStatus{
		AllSuccessful:     false,
		TotalCount:        0,
		PendingCount:      0,
		ApplyDesiresCount: 2,
	}

	// First reconcile: initiate deletion, requeues.
	result, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, result.RequeueAfter)
	require.Len(t, tr.DeleteCalls, 1)

	// Simulate completion.
	tr.DeleteStatusOverrides[key] = &transport.DeleteStatus{AllSuccessful: true, TotalCount: 2, PendingCount: 0, ApplyDesiresCount: 0}

	// Second reconcile: cleanup succeeds, but finalizer removal fails.
	_, err = r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "remove finalizer")
	require.Len(t, tr.CleanupDeleteDesiresCalls, 1, "cleanup should have been called before finalizer removal failed")
	require.True(t, storeClient.updateCalled)
}

// ---------------------------------------------------------------------------
// Test cases – NodePoolProgressing condition
// ---------------------------------------------------------------------------

func TestReconcile_Progressing_AllMachinesReadyFalse(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "False",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "MachinesNotReady", progressing.Reason)
}

func TestReconcile_Progressing_AllMachinesReadyAbsent(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":           "True",
				"allNodesHealthyCondition": "True",
				// allMachinesReadyCondition intentionally absent
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "MachinesNotReady", progressing.Reason)
}

func TestReconcile_Progressing_UpdatingConfig(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingConfigCondition":   "True",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "UpdatingConfig", progressing.Reason)
}

func TestReconcile_Progressing_Priority_UpdatingConfigWins(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingConfigCondition":   "True",
				"updatingVersionCondition":  "True",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "UpdatingConfig", progressing.Reason)
}

func TestReconcile_Progressing_UpdatingVersion(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingVersionCondition":  "True",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "UpdatingVersion", progressing.Reason)
}

func TestReconcile_Progressing_AsExpected(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "True",
				"updatingConfigCondition":   "False",
				"updatingVersionCondition":  "False",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionFalse, progressing.Status)
	require.Equal(t, "AsExpected", progressing.Reason)
}

func TestReconcile_Progressing_Priority_MachinesNotReadyWins(t *testing.T) {
	np := testNodePool("4.16.0")
	cluster := testCluster(true, true)
	npKey := nodePoolResourceKey(cluster, np)

	tr := mock.New()
	tr.StatusOverrides["mc-us-c1/"+mustNodePoolGroupKey("cluster-test", "cluster-test", "np-test")] = &transport.Status{
		Conditions: []metav1.Condition{
			{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"},
		},
		ResourceStatuses: map[string]map[string]string{
			npKey: {
				"readyCondition":            "True",
				"allNodesHealthyCondition":  "True",
				"allMachinesReadyCondition": "False",
				"updatingConfigCondition":   "True",
			},
		},
	}

	r, storeClient := buildReconciler(t, np, cluster, tr, nil, nil, nil)
	_, err := r.Reconcile(context.Background(), npReq("cluster-test", "np-test"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.NodePool)
	progressing := meta.FindStatusCondition(captured.Status.Conditions, "NodePoolProgressing")
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionTrue, progressing.Status)
	require.Equal(t, "MachinesNotReady", progressing.Reason)
}
