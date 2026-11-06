package nodepoolupgrade

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
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
			Release: privatev1.ReleaseSpec{Version: "4.22.16", ChannelGroup: "stable"},
		},
		Status: privatev1.NodePoolStatus{Version: "4.22.14"},
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

func TestScheduledUpgradeWaitsWithoutChangingDesiredRelease(t *testing.T) {
	pool, cluster, release := testObjects()
	start := metav1.NewTime(testNow.Add(24 * time.Hour))
	pool.Spec.Release.UpgradeStartTime = &start
	r, c := testReconciler(t, pool, cluster, release)
	require.Equal(t, maxScheduleWait, reconcilePool(t, r).RequeueAfter)
	updated := getPool(t, c)
	require.True(t, apiequality.Semantic.DeepEqual(pool.Spec, updated.Spec))
	require.Equal(t, "4.22.14", updated.Status.Version)
	require.False(t, privatev1.NodePoolReleaseApproved(updated))
	require.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionScheduled))
	transition := meta.FindStatusCondition(updated.Status.Conditions, conditionScheduled).LastTransitionTime
	updated.Spec.NodeCount = ptr.To(int32(3))
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.Equal(t, transition, meta.FindStatusCondition(updated.Status.Conditions, conditionScheduled).LastTransitionTime)
	require.Equal(t, updated.Generation, meta.FindStatusCondition(updated.Status.Conditions, conditionScheduled).ObservedGeneration)
	r.now = func() time.Time { return start.Time }
	require.Equal(t, waitInterval, reconcilePool(t, r).RequeueAfter)
	updated = getPool(t, c)
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	require.True(t, privatev1.NodePoolReleaseApproved(getPool(t, c)))
}

func TestUpgradeConditionsResumeAcrossRestartAndScaling(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	require.Equal(t, retryInterval, reconcilePool(t, r).RequeueAfter)
	updated := getPool(t, c)
	require.True(t, privatev1.NodePoolUpgradeActive(updated))
	require.True(t, apiequality.Semantic.DeepEqual(pool.Spec, updated.Spec))
	approved := meta.FindStatusCondition(updated.Status.Conditions, conditionProgressing).LastTransitionTime
	updated.Spec.NodeCount = ptr.To(int32(3))
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	// Scaling must not discard approval or permit a conflicting release write.
	require.True(t, privatev1.NodePoolReleaseApproved(updated))
	changed := updated.DeepCopy()
	changed.Spec.Release.Version = "4.22.17"
	require.ErrorContains(t, changed.ValidateUpdate(t.Context(), updated), "accepted upgrade is active")
	restarted := NewReconciler(c)
	restarted.now = r.now
	reconcilePool(t, restarted)
	updated = getPool(t, c)
	require.Equal(t, approved, meta.FindStatusCondition(updated.Status.Conditions, conditionProgressing).LastTransitionTime)
	updated.Status.Version = "4.22.16"
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, restarted)
	require.True(t, privatev1.NodePoolUpgradeActive(getPool(t, c)), "stale healthy conditions must not complete an upgrade")
	updated = getPool(t, c)
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, restarted)
	completed := getPool(t, c)
	require.False(t, privatev1.NodePoolUpgradeActive(completed))
	require.Equal(t, "UpgradeCompleted", meta.FindStatusCondition(completed.Status.Conditions, conditionProgressing).Reason)
	rv := completed.ResourceVersion
	reconcilePool(t, restarted)
	require.Equal(t, rv, getPool(t, c).ResourceVersion)
	// The next desired version must not reuse the completed upgrade's approval.
	completed.Spec.Release.Version = "4.22.17"
	completed.Generation++
	require.NoError(t, c.Update(t.Context(), completed))
	require.False(t, privatev1.NodePoolReleaseApproved(completed))
}

func TestPreSubmissionFailureUnlocksReleaseForCancellation(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: "NodePoolVersionResolved", Status: metav1.ConditionFalse, Reason: "VersionNotAvailable", ObservedGeneration: updated.Generation})
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.False(t, privatev1.NodePoolUpgradeActive(updated))
	require.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionDegraded))
	require.False(t, privatev1.NodePoolUpgradeSubmissionStarted(updated))
	rv := updated.ResourceVersion
	reconcilePool(t, r)
	require.Equal(t, rv, getPool(t, c).ResourceVersion)
	previous := updated.DeepCopy()
	updated.Spec.Release.Version = updated.Status.Version
	updated.Spec.Release.UpgradeStartTime = nil
	require.NoError(t, updated.ValidateUpdate(t.Context(), previous))
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionDegraded))
	require.Equal(t, "UpgradeCanceled", meta.FindStatusCondition(updated.Status.Conditions, conditionProgressing).Reason)
	require.True(t, privatev1.NodePoolReleaseApproved(updated))
}

func TestUncertainSubmissionKeepsReleaseLocked(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: privatev1.NodePoolUpgradeSubmitted, Status: metav1.ConditionUnknown, Reason: "Dispatching", ObservedGeneration: updated.Generation})
	meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: "NodePoolVersionResolved", Status: metav1.ConditionFalse, Reason: "VersionNotAvailable", ObservedGeneration: updated.Generation})
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.True(t, privatev1.NodePoolUpgradeActive(updated))
	reverted := updated.DeepCopy()
	reverted.Spec.Release.Version = reverted.Status.Version
	require.ErrorContains(t, reverted.ValidateUpdate(t.Context(), updated), "accepted upgrade is active")
}

func TestTargetRevalidationAndTerminalReconciliation(t *testing.T) {
	for _, change := range []string{"control plane", "channel", "step"} {
		t.Run(change, func(t *testing.T) {
			pool, cluster, release := testObjects()
			switch change {
			case "control plane":
				cluster.Status.HostedClusterResult.Version = "4.22.14"
			case "channel":
				release.Spec.ChannelGroups = []string{"fast"}
			case "step":
				pool.Spec.Release.Version = "4.24.1"
				cluster.Status.HostedClusterResult.Version = "4.24.1"
				release.Name = "4.24.1"
			}
			r, c := testReconciler(t, pool, cluster, release)
			reconcilePool(t, r)
			updated := getPool(t, c)
			require.True(t, apiequality.Semantic.DeepEqual(pool.Spec, updated.Spec))
			require.True(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionDegraded))
			require.False(t, privatev1.NodePoolReleaseApproved(updated))
			rv := updated.ResourceVersion
			reconcilePool(t, r)
			require.Equal(t, rv, getPool(t, c).ResourceVersion)
		})
	}
}

func TestPendingUpgradeCancellationAndResubmission(t *testing.T) {
	pool, cluster, release := testObjects()
	pool.Spec.Release.UpgradeStartTime = ptr.To(metav1.NewTime(testNow.Add(time.Hour)))
	r, c := testReconciler(t, pool, cluster, release)
	reconcilePool(t, r)
	updated := getPool(t, c)
	updated.Spec.Release.Version = updated.Status.Version
	updated.Spec.Release.UpgradeStartTime = nil
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	reconcilePool(t, r)
	updated = getPool(t, c)
	require.False(t, meta.IsStatusConditionTrue(updated.Status.Conditions, conditionScheduled))
	require.True(t, privatev1.NodePoolReleaseApproved(updated))
	updated.Spec.Release.Version = "4.22.16"
	updated.Generation++
	require.NoError(t, c.Update(t.Context(), updated))
	healthy(updated)
	require.NoError(t, c.Status().Update(t.Context(), updated))
	reconcilePool(t, r)
	require.True(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
}

func TestInitialInstallationAndObservedReleaseAreUntouched(t *testing.T) {
	for _, current := range []string{"", "4.22.16"} {
		t.Run(current, func(t *testing.T) {
			pool, cluster, release := testObjects()
			pool.Status.Version = current
			r, c := testReconciler(t, pool, cluster, release)
			before := getPool(t, c)
			reconcilePool(t, r)
			require.Equal(t, before, getPool(t, c))
			require.True(t, privatev1.NodePoolReleaseApproved(pool))
		})
	}
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
	require.False(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
	require.False(t, privatev1.NodePoolReleaseApproved(getPool(t, c)))
	faulty.statusErr = apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, testPool, errors.New("conflict"))
	require.Equal(t, retryInterval, reconcilePool(t, r).RequeueAfter)
	require.False(t, privatev1.NodePoolReleaseApproved(getPool(t, c)))
	faulty.statusErr = nil
	reconcilePool(t, r)
	require.True(t, privatev1.NodePoolReleaseApproved(getPool(t, c)))
}

func TestTransientReadFailureDoesNotRejectUpgrade(t *testing.T) {
	pool, cluster, release := testObjects()
	r, c := testReconciler(t, pool, cluster, release)
	faulty := &failingClient{Client: c, getErr: errors.New("temporary storage failure")}
	r.client = faulty
	_, err := r.Reconcile(t.Context(), poolRequest())
	require.Error(t, err)
	require.False(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
	require.False(t, privatev1.NodePoolReleaseApproved(getPool(t, c)))
	faulty.getErr = nil
	reconcilePool(t, r)
	require.True(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
}

func TestMissingUpgradeDependencyIsRetryable(t *testing.T) {
	for _, kind := range []string{"Cluster", "Version"} {
		t.Run(kind, func(t *testing.T) {
			pool, cluster, release := testObjects()
			objects := []client.Object{pool}
			var missing client.Object = release
			if kind == "Cluster" {
				objects = append(objects, release)
				missing = cluster
			} else {
				objects = append(objects, cluster)
			}
			r, c := testReconciler(t, objects...)
			_, err := r.Reconcile(t.Context(), poolRequest())
			require.True(t, apierrors.IsNotFound(err), "lookup misses must trigger a retry")
			rejected := getPool(t, c)
			require.False(t, meta.IsStatusConditionTrue(rejected.Status.Conditions, conditionDegraded))
			require.False(t, privatev1.NodePoolUpgradeActive(rejected))
			require.NoError(t, c.Create(t.Context(), missing))
			reconcilePool(t, r)
			require.True(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
		})
	}
}
