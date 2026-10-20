package nodepoolupgrade

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	testNamespace = "customer"
	testCluster   = "example"
	testPool      = "workers"
	testRequest   = "upgrade-workers"
)

func testObjects() (*privatev1.NodePoolUpgradeRequest, *privatev1.NodePool, *privatev1.Cluster, *privatev1.Version) {
	request := &privatev1.NodePoolUpgradeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: testRequest, Namespace: testNamespace, UID: types.UID("request-uid")},
		Spec: privatev1.NodePoolUpgradeRequestSpec{
			ClusterID: testCluster, NodePoolID: testPool, TargetVersion: "4.22.16",
		},
	}
	pool := &privatev1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: testPool, Namespace: testNamespace}}
	pool.Spec.ClusterID = testCluster
	pool.Spec.Release.Version = "4.22.14"
	pool.Spec.Release.ChannelGroup = "stable"
	pool.Status.CurrentVersion = "4.22.14"
	cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: testCluster, Namespace: testNamespace}}
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.22.16"}
	release := &privatev1.Version{ObjectMeta: metav1.ObjectMeta{Name: "4.22.16"}}
	release.Spec.ChannelGroups = []string{"stable"}
	return request, pool, cluster, release
}

func testReconciler(t *testing.T, objects ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, privatev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&privatev1.NodePoolUpgradeRequest{}, &privatev1.NodePool{}, &privatev1.Cluster{}).
		WithObjects(objects...).Build()
	r := NewReconciler(c)
	r.now = func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) }
	return r, c
}

func reconcileRequest(t *testing.T, r *Reconciler) reconcile.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testRequest},
	})
	require.NoError(t, err)
	return result
}

func getRequest(t *testing.T, c client.Client) *privatev1.NodePoolUpgradeRequest {
	t.Helper()
	request := &privatev1.NodePoolUpgradeRequest{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testRequest}, request))
	return request
}

func getPool(t *testing.T, c client.Client) *privatev1.NodePool {
	t.Helper()
	pool := &privatev1.NodePool{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testPool}, pool))
	return pool
}

func TestScheduledRequestWaitsBeforePatchingNodePool(t *testing.T) {
	request, pool, cluster, release := testObjects()
	start := metav1.NewTime(time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
	request.Spec.StartTime = &start
	r, c := testReconciler(t, request, pool, cluster, release)

	result := reconcileRequest(t, r)
	require.Equal(t, maxScheduleWait, result.RequeueAfter)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	require.True(t, meta.IsStatusConditionTrue(getRequest(t, c).Status.Conditions, conditionScheduled))
}

func TestUpgradeCompletesOnlyAfterObservedNodePoolVersionAndHealth(t *testing.T) {
	request, pool, cluster, release := testObjects()
	r, c := testReconciler(t, request, pool, cluster, release)

	result := reconcileRequest(t, r)
	require.Equal(t, retryInterval, result.RequeueAfter)
	require.NotNil(t, getRequest(t, c).Status.AcceptedTime)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)

	reconcileRequest(t, r)
	updatedPool := getPool(t, c)
	require.Equal(t, "4.22.16", updatedPool.Spec.Release.Version)
	require.Equal(t, string(request.UID), updatedPool.Annotations[requestAnnotation])
	require.False(t, meta.IsStatusConditionTrue(getRequest(t, c).Status.Conditions, conditionCompleted))

	updatedPool.Status.CurrentVersion = "4.22.16"
	updatedPool.Status.Conditions = []metav1.Condition{
		{Type: "NodePoolAvailable", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: metav1.Now()},
		{Type: "NodePoolHealthy", Status: metav1.ConditionFalse, Reason: "NodesUpdating", LastTransitionTime: metav1.Now()},
		{Type: "NodePoolProgressing", Status: metav1.ConditionTrue, Reason: "UpdatingVersion", LastTransitionTime: metav1.Now()},
	}
	require.NoError(t, c.Status().Update(context.Background(), updatedPool))
	reconcileRequest(t, r)
	require.False(t, meta.IsStatusConditionTrue(getRequest(t, c).Status.Conditions, conditionCompleted))

	updatedPool = getPool(t, c)
	meta.SetStatusCondition(&updatedPool.Status.Conditions, metav1.Condition{
		Type: "NodePoolHealthy", Status: metav1.ConditionTrue, Reason: "Ready", LastTransitionTime: metav1.Now(),
	})
	meta.SetStatusCondition(&updatedPool.Status.Conditions, metav1.Condition{
		Type: "NodePoolProgressing", Status: metav1.ConditionFalse, Reason: "AsExpected", LastTransitionTime: metav1.Now(),
	})
	require.NoError(t, c.Status().Update(context.Background(), updatedPool))
	reconcileRequest(t, r)
	completed := getRequest(t, c)
	require.True(t, meta.IsStatusConditionTrue(completed.Status.Conditions, conditionCompleted))
	require.NotNil(t, completed.Status.CompletedTime)
}

func TestTargetAboveControlPlaneWaits(t *testing.T) {
	request, pool, cluster, release := testObjects()
	cluster.Status.HostedClusterResult.Version = "4.22.14"
	r, c := testReconciler(t, request, pool, cluster, release)

	result := reconcileRequest(t, r)
	require.Equal(t, waitInterval, result.RequeueAfter)
	require.Nil(t, getRequest(t, c).Status.AcceptedTime)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	require.Equal(t, "WaitingForControlPlane", meta.FindStatusCondition(getRequest(t, c).Status.Conditions, conditionAccepted).Reason)
}

func TestRejectsUnsupportedMinorStep(t *testing.T) {
	request, pool, cluster, release := testObjects()
	request.Spec.TargetVersion = "4.24.1"
	cluster.Status.HostedClusterResult.Version = "4.24.1"
	r, c := testReconciler(t, request, pool, cluster, release)

	reconcileRequest(t, r)
	require.True(t, meta.IsStatusConditionFalse(getRequest(t, c).Status.Conditions, conditionAccepted))
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
}
