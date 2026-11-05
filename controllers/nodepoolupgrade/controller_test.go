package nodepoolupgrade

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	testNamespace = "customer"
	testCluster   = "example"
	testPool      = "workers"
)

var testNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func testObjects() (*privatev1.NodePool, *privatev1.Cluster, *privatev1.Version) {
	pool := &privatev1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: testPool, Namespace: testNamespace, Generation: 1},
		Spec: privatev1.NodePoolSpec{ClusterID: testCluster,
			Release: privatev1.ReleaseSpec{Version: "4.22.14", ChannelGroup: "stable"},
			Upgrade: &privatev1.NodePoolUpgradeSpec{TargetVersion: ptr.To("4.22.16")},
		},
		Status: privatev1.NodePoolStatus{CurrentVersion: "4.22.14"},
	}
	healthy(pool)
	cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: testCluster, Namespace: testNamespace}}
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.22.17"}
	release := &privatev1.Version{ObjectMeta: metav1.ObjectMeta{Name: "4.22.16"}}
	release.Spec.ChannelGroups = []string{"stable"}
	return pool, cluster, release
}

func healthy(pool *privatev1.NodePool) {
	for _, c := range []struct {
		typ    string
		status metav1.ConditionStatus
	}{
		{"NodePoolAvailable", metav1.ConditionTrue}, {"NodePoolHealthy", metav1.ConditionTrue}, {"NodePoolProgressing", metav1.ConditionFalse},
	} {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{Type: c.typ, Status: c.status, Reason: "AsExpected", ObservedGeneration: pool.Generation})
	}
}

func testReconciler(t *testing.T, objects ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, privatev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&privatev1.NodePool{}, &privatev1.Cluster{}).WithObjects(objects...).Build()
	r := NewReconciler(c)
	r.now = func() time.Time { return testNow }
	return r, c
}

func poolRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testPool}}
}

func reconcilePool(t *testing.T, r *Reconciler) reconcile.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	return result
}

func getPool(t *testing.T, c client.Client) *privatev1.NodePool {
	t.Helper()
	pool := &privatev1.NodePool{}
	require.NoError(t, c.Get(t.Context(), poolRequest().NamespacedName, pool))
	return pool
}

func TestScheduledUpgradeWaitsWithoutChangingRelease(t *testing.T) {
	pool, cluster, release := testObjects()
	start := metav1.NewTime(testNow.Add(24 * time.Hour))
	pool.Spec.Upgrade.StartTime = &start
	r, c := testReconciler(t, pool, cluster, release)
	result := reconcilePool(t, r)
	require.Equal(t, maxScheduleWait, result.RequeueAfter)
	updated := getPool(t, c)
	require.Equal(t, "4.22.14", updated.Spec.Release.Version)
	require.Nil(t, updated.Status.Upgrade.AcceptedTime)
	require.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionScheduled))
	// Scaling changes the generation, but does not change the pending intent.
	requested := updated.Status.Upgrade.RequestedTime.DeepCopy()
	updated.Spec.NodeCount = ptr.To(int32(3))
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Equal(t, requested, updated.Status.Upgrade.RequestedTime)
	require.Equal(t, int32(3), *updated.Spec.NodeCount)
	require.Equal(t, "4.22.14", updated.Spec.Release.Version)
	r.now = func() time.Time { return start.Time }
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	require.Equal(t, retryInterval, reconcilePool(t, r).RequeueAfter)
	require.NotNil(t, getPool(t, c).Status.Upgrade.AcceptedTime)
}

func TestUpgradeUsesExistingReleaseFlowAndWaitsForHealthyFeedback(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	require.Equal(t, retryInterval, reconcilePool(t, r).RequeueAfter)
	require.NotNil(t, getPool(t, c).Status.Upgrade.AcceptedTime)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	// An unrelated scale operation after acceptance delays submission without
	// rejecting the accepted target.
	updated := getPool(t, c)
	meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: "NodePoolProgressing", Status: metav1.ConditionTrue, Reason: "Scaling", ObservedGeneration: updated.Generation})
	require.NoError(t, c.Status().Update(t.Context(), updated))
	require.Equal(t, waitInterval, reconcilePool(t, r).RequeueAfter)
	updated = getPool(t, c)
	require.Nil(t, updated.Status.Upgrade.FailureReason)
	require.Equal(t, "4.22.14", updated.Spec.Release.Version)
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Equal(t, "4.22.16", updated.Spec.Release.Version)
	require.Equal(t, "4.22.14", updated.Status.CurrentVersion)
	// Restart after the release patch but before the started timestamp was persisted.
	restarted := NewReconciler(c)
	restarted.now = r.now
	reconcilePool(t, restarted)
	require.NotNil(t, getPool(t, c).Status.Upgrade.StartedTime)
	updated = getPool(t, c)
	updated.Status.CurrentVersion = "4.22.16"
	meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: "NodePoolHealthy", Status: metav1.ConditionFalse, Reason: "Updating", ObservedGeneration: updated.Generation})
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, restarted)
	require.Nil(t, getPool(t, c).Status.Upgrade.CompletedTime)
	updated = getPool(t, c)
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, restarted)
	completed := getPool(t, c)
	require.NotNil(t, completed.Status.Upgrade.CompletedTime)
	require.False(t, meta.IsStatusConditionTrue(completed.Status.Conditions, conditionProgressing))
	require.False(t, meta.IsStatusConditionTrue(completed.Status.Conditions, conditionDegraded))
	rv := completed.ResourceVersion
	reconcilePool(t, restarted)
	require.Equal(t, rv, getPool(t, c).ResourceVersion, "completed upgrades must reconcile idempotently")
}

func TestMatchingVersionWithStaleHealthDoesNotComplete(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	reconcilePool(t, r)
	updated := getPool(t, c)
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	updated.Status.CurrentVersion = "4.22.16"
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	require.Nil(t, getPool(t, c).Status.Upgrade.CompletedTime)
}

func TestDelayedAcceptedUpgradeDoesNotDowngradeAdvancedPool(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	updated.Spec.Release.Version = "4.22.17"
	require.NoError(t, c.Update(t.Context(), updated))
	updated.Status.CurrentVersion = "4.22.17"
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Equal(t, "4.22.17", updated.Spec.Release.Version)
	require.Equal(t, "TargetRejected", *updated.Status.Upgrade.FailureReason)
}

func TestTargetRevalidationAndTerminalReconciliation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*privatev1.NodePool, *privatev1.Cluster, *privatev1.Version)
	}{
		{"above control plane", func(_ *privatev1.NodePool, c *privatev1.Cluster, _ *privatev1.Version) {
			c.Status.HostedClusterResult.Version = "4.22.14"
		}},
		{"outside channel", func(_ *privatev1.NodePool, _ *privatev1.Cluster, v *privatev1.Version) {
			v.Spec.ChannelGroups = []string{"fast"}
		}},
		{"unsupported step", func(p *privatev1.NodePool, c *privatev1.Cluster, v *privatev1.Version) {
			p.Spec.Upgrade.TargetVersion = ptr.To("4.24.1")
			c.Status.HostedClusterResult.Version = "4.24.1"
			v.Name = "4.24.1"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, cluster, release := testObjects()
			tt.change(pool, cluster, release)
			r, c := testReconciler(t, pool, cluster, release)
			reconcilePool(t, r)
			updated := getPool(t, c)
			require.Equal(t, "4.22.14", updated.Spec.Release.Version)
			require.NotNil(t, updated.Status.Upgrade.FailureReason)
			rv := updated.ResourceVersion
			reconcilePool(t, r)
			require.Equal(t, rv, getPool(t, c).ResourceVersion)
		})
	}
}

func TestPendingUpgradeCancellationAndReplacement(t *testing.T) {
	pool, cluster, release := testObjects()
	start := metav1.NewTime(testNow.Add(time.Hour))
	pool.Spec.Upgrade.StartTime = &start
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	updated.Spec.Upgrade = nil
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Nil(t, updated.Status.Upgrade.IntentID)
	require.Equal(t, "4.22.14", updated.Spec.Release.Version)
	require.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionScheduled))
	updated.Spec.Upgrade = pool.Spec.Upgrade.DeepCopy()
	updated.Spec.Upgrade.StartTime = nil
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	require.NotNil(t, getPool(t, c).Status.Upgrade.AcceptedTime)
}

func TestRejectedUpgradeCanBeClearedAndResubmitted(t *testing.T) {
	pool, cluster, release := testObjects()
	release.Spec.ChannelGroups = []string{"fast"}
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	require.NotNil(t, updated.Status.Upgrade.FailureReason)
	updated.Spec.Upgrade = nil
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Nil(t, updated.Status.Upgrade.IntentID)
	require.NotNil(t, updated.Status.Upgrade.FailureReason, "retain the last result until a new request arrives")
	// The target becomes eligible while the rejected intent is absent.
	require.NoError(t, c.Get(t.Context(), types.NamespacedName{Name: release.Name}, release))
	release.Spec.ChannelGroups = []string{"stable"}
	require.NoError(t, c.Update(t.Context(), release))
	updated.Spec.Upgrade = pool.Spec.Upgrade.DeepCopy()
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Nil(t, updated.Status.Upgrade.FailureReason)
	require.NotNil(t, updated.Status.Upgrade.AcceptedTime)
}

func TestDefaultTargetIsPinnedBeforeScheduling(t *testing.T) {
	pool, cluster, release := testObjects()
	pool.Spec.Upgrade.TargetVersion = nil
	pool.Spec.Upgrade.StartTime = ptr.To(metav1.NewTime(testNow.Add(time.Hour)))
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	require.Equal(t, "4.22.17", *updated.Spec.Upgrade.TargetVersion)
	require.Equal(t, "4.22.14", updated.Spec.Release.Version)
	require.Nil(t, updated.Status.Upgrade)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	cluster.Status.HostedClusterResult.Version = "4.22.18"
	require.NoError(t, c.Status().Update(t.Context(), cluster))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Equal(t, "4.22.17", *updated.Spec.Upgrade.TargetVersion)
	require.Equal(t, "4.22.17", updated.Status.Upgrade.TargetVersion)
	require.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionScheduled))
}

func TestNoUpgradeIntentLeavesInstallationUntouched(t *testing.T) {
	pool, cluster, release := testObjects()
	pool.Spec.Upgrade = nil
	r, c := testReconciler(t, pool, cluster, release)
	before := getPool(t, c)
	reconcilePool(t, r)
	require.Equal(t, before, getPool(t, c))
}

func TestExistingDirectReleaseUpdatesAreObserved(t *testing.T) {
	pool, cluster, release := testObjects()
	pool.Spec.Upgrade = nil
	pool.Spec.Release.Version = "4.22.16"
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	require.Equal(t, "4.22.16", updated.Status.Upgrade.TargetVersion)
	require.NotNil(t, updated.Status.Upgrade.StartedTime)
	updated.Status.CurrentVersion = "4.22.16"
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	require.NotNil(t, getPool(t, c).Status.Upgrade.CompletedTime)
}

type failingClient struct {
	client.Client
	patchErr  error
	statusErr error
	getErr    error
}

func (c *failingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*privatev1.Cluster); ok && c.getErr != nil {
		return c.getErr
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *failingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.patchErr != nil {
		return c.patchErr
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
func (c *failingClient) Status() client.SubResourceWriter {
	return &failingStatusWriter{SubResourceWriter: c.Client.Status(), owner: c}
}

type failingStatusWriter struct {
	client.SubResourceWriter
	owner *failingClient
}

func (w *failingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if w.owner.statusErr != nil {
		return w.owner.statusErr
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func TestRetriesAfterConflictsAndPartialWrites(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	faulty := &failingClient{Client: c, statusErr: errors.New("temporary status failure")}
	r.client = faulty
	_, err := r.Reconcile(t.Context(), poolRequest())
	require.Error(t, err)
	require.Nil(t, getPool(t, c).Status.Upgrade)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	faulty.statusErr = nil
	reconcilePool(t, r)
	conflict := apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, testPool, errors.New("conflict"))
	faulty.patchErr = conflict
	require.Equal(t, retryInterval, reconcilePool(t, r).RequeueAfter)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	faulty.patchErr = nil
	reconcilePool(t, r)
	require.Equal(t, "4.22.16", getPool(t, c).Spec.Release.Version)
	faulty.statusErr = errors.New("temporary status failure")
	_, err = r.Reconcile(t.Context(), poolRequest())
	require.Error(t, err)
	require.Nil(t, getPool(t, c).Status.Upgrade.StartedTime)
	faulty.statusErr = nil
	reconcilePool(t, r)
	require.NotNil(t, getPool(t, c).Status.Upgrade.StartedTime)
}

func TestTransientReadFailureDoesNotRejectUpgrade(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	faulty := &failingClient{Client: c, getErr: errors.New("temporary storage failure")}
	r.client = faulty
	_, err := r.Reconcile(t.Context(), poolRequest())
	require.Error(t, err)
	require.Equal(t, "4.22.14", getPool(t, c).Spec.Release.Version)
	require.Nil(t, getPool(t, c).Status.Upgrade)
	faulty.getErr = nil
	reconcilePool(t, r)
	require.NotNil(t, getPool(t, c).Status.Upgrade.AcceptedTime)
}
