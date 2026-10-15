package controlplaneupgrade

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func testLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "error",
		Format:    "text",
		Output:    "stderr",
		Component: "test",
	})
	require.NoError(t, err)
	return log
}

// mockStatusWriter captures Status().Update calls.
type mockStatusWriter struct {
	called   bool
	captured client.Object
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
	return nil
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

// mockStoreClient is a minimal client.Client backed by a fixed Cluster. Update is
// captured because this controller's only spec mutation requests a version.
type mockStoreClient struct {
	cluster      *privatev1.Cluster
	channel      *privatev1.Channel
	policies     []privatev1.ControlPlaneUpgradePolicy
	statusWriter *mockStatusWriter
	updateCalled bool
	updated      client.Object
	updateErr    error
}

func (m *mockStoreClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch typed := obj.(type) {
	case *privatev1.Cluster:
		if m.cluster == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
		}
		*typed = *m.cluster.DeepCopy()
		return nil
	case *privatev1.Channel:
		if m.channel == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "channel"}, "")
		}
		*typed = *m.channel.DeepCopy()
		return nil
	default:
		return fmt.Errorf("unexpected type %T", obj)
	}
}

func (m *mockStoreClient) Status() client.SubResourceWriter {
	if m.statusWriter == nil {
		m.statusWriter = &mockStatusWriter{}
	}
	return m.statusWriter
}

func (m *mockStoreClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return fmt.Errorf("unexpected cluster patch")
}

func (m *mockStoreClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	policies, ok := list.(*privatev1.ControlPlaneUpgradePolicyList)
	if !ok {
		return fmt.Errorf("unexpected list type %T", list)
	}
	policies.Items = append([]privatev1.ControlPlaneUpgradePolicy(nil), m.policies...)
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
	m.updated = obj.DeepCopyObject().(client.Object)
	if m.updateErr != nil {
		return m.updateErr
	}
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

func clusterReq(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "hyperfleet", Name: name}}
}

func buildReconciler(t *testing.T, cluster *privatev1.Cluster) (*Reconciler, *mockStoreClient) {
	t.Helper()
	channel := &privatev1.Channel{
		ObjectMeta: metav1.ObjectMeta{Name: "stable"},
		Spec: privatev1.ChannelSpec{
			InstallDefaultVersion: "4.15.0",
			FleetMinorVersion:     "4.15",
		},
	}
	policy := privatev1.ControlPlaneUpgradePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-1", Namespace: "hyperfleet"},
		Spec:       privatev1.ControlPlaneUpgradePolicySpec{ClusterID: "cluster-1"},
	}
	storeClient := &mockStoreClient{cluster: cluster, channel: channel, policies: []privatev1.ControlPlaneUpgradePolicy{policy}}
	return NewReconciler(storeClient, testLogger(t)), storeClient
}

// readyCluster builds a Cluster with completed=desired=spec=version and
// HostedClusterAvailable=True, i.e. steady state with nothing active.
func readyCluster(name, version string, availableUpdates []string) *privatev1.Cluster {
	cluster := &privatev1.Cluster{}
	cluster.SetName(name)
	cluster.SetNamespace("hyperfleet")
	cluster.SetGeneration(1)
	cluster.SetResourceVersion("7")
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: version}
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{
		Version:          version,
		DesiredVersion:   version,
		AvailableUpdates: availableUpdates,
		ObservedConditions: []metav1.Condition{
			// healthOK requires explicit confirmation, not just the absence of a
			// degraded signal — this reflects that confirmation.
			{Type: "HostedClusterDegraded", Status: metav1.ConditionFalse, Reason: "AsExpected"},
		},
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "HostedClusterAvailable", Status: metav1.ConditionTrue, Reason: "HostedClusterAvailable",
	})
	return cluster
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestReconcile_ClusterNotFound(t *testing.T) {
	r, _ := buildReconciler(t, nil)

	result, err := r.Reconcile(context.Background(), clusterReq("missing"))

	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconcile_WaitingForHostedClusterFeedback(t *testing.T) {
	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-1")
	cluster.SetNamespace("hyperfleet")
	// No HostedClusterResult yet.

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.True(t, storeClient.statusWriter.called)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionUnknown, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, "WaitingForHostedClusterFeedback", meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Reason)
}

func TestReconcile_NoUpgradeAvailable(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.0"})

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, "NoUpgradeAvailable", meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Reason)
	require.Nil(t, captured.Status.ControlPlaneUpgrade)
}

func TestReconcile_SelectsZStreamCandidate_PatchesSpec(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1", "4.16.0"})

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)

	patched := storeClient.updated.(*privatev1.Cluster)
	require.Equal(t, "4.15.1", patched.Spec.Release.Version, "must not select the cross-minor 4.16.0 candidate")

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionDegraded).Status)

	require.NotNil(t, captured.Status.ControlPlaneUpgrade)
	require.Equal(t, "4.15.1", captured.Status.ControlPlaneUpgrade.TargetVersion)
	require.Equal(t, "automatic", captured.Status.ControlPlaneUpgrade.TargetSource)
	require.NotNil(t, captured.Status.ControlPlaneUpgrade.RequestedAt)
}

func TestReconcile_ZStreamDoesNotRequireChannel(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)

	patched := storeClient.updated.(*privatev1.Cluster)
	require.Equal(t, "4.15.1", patched.Spec.Release.Version)
}

func TestReconcile_PrefersAuthorizedYStreamCandidate(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.22.14", []string{"4.22.17", "4.23.0-ec.1"})
	cluster.Spec.Release.ChannelGroup = "candidate"
	meta.SetStatusCondition(&cluster.Status.HostedClusterResult.ObservedConditions, metav1.Condition{
		Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue, Reason: "AsExpected",
	})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel.Name = "candidate"
	storeClient.channel.Spec.FleetMinorVersion = "4.23"

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)

	patched := storeClient.updated.(*privatev1.Cluster)
	require.Equal(t, "4.23.0-ec.1", patched.Spec.Release.Version)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.NotNil(t, captured.Status.ControlPlaneUpgrade)
	require.Equal(t, "4.23.0-ec.1", captured.Status.ControlPlaneUpgrade.TargetVersion)
}

// TestReconcile_BlockedOnUnknownDegradation verifies that an absent or Unknown
// HostedClusterDegraded reading blocks target selection exactly like a confirmed
// True does — deny by default rather than proceeding without positive evidence.
func TestReconcile_BlockedOnUnknownDegradation(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	cluster.Status.HostedClusterResult.ObservedConditions = nil // no confirmation either way

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, "HostedClusterDegraded", meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Reason)
}

func TestReconcile_BlockedOnUnhealthyCluster(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: "HostedClusterAvailable", Status: metav1.ConditionFalse, Reason: "HostedClusterNotAvailable",
	})

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, "HostedClusterUnavailable", meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Reason)
}

func TestReconcile_ActiveUpgrade_ObservesProgress(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.1", nil)
	cluster.Status.HostedClusterResult.Version = "4.15.0" // completed
	cluster.Status.HostedClusterResult.DesiredVersion = "4.15.1"
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.False(t, storeClient.updateCalled, "must not select another target while one is active")

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionDegraded).Status)
}

func TestReconcile_ActiveUpgrade_DoesNotRequireChannelOrPolicy(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.1", nil)
	cluster.Status.HostedClusterResult.Version = "4.15.0"
	cluster.Status.HostedClusterResult.DesiredVersion = "4.15.1"
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel = nil
	storeClient.policies = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_NoPolicy_AllowsZStreamUpgrade(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.policies = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)
	require.Equal(t, "4.15.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	require.Equal(t, targetSourceAutomatic, storeClient.statusWriter.captured.(*privatev1.Cluster).Status.ControlPlaneUpgrade.TargetSource)
}

func TestReconcile_VersionConflictRechecksCluster(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	r, storeClient := buildReconciler(t, cluster)
	storeClient.updateErr = apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, cluster.Name, fmt.Errorf("conflict"))

	result, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.Equal(t, "7", storeClient.updated.GetResourceVersion())
	require.Nil(t, storeClient.statusWriter, "a losing update must not publish an upgrade target")

	storeClient.cluster.Spec.Release.Version = "4.15.2"
	storeClient.cluster.SetResourceVersion("8")
	storeClient.updateErr = nil
	storeClient.updateCalled = false
	_, err = r.Reconcile(context.Background(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.False(t, storeClient.updateCalled, "retry must observe the other writer's target")
}

func TestReconcile_NoPolicy_AllowsYStreamUpgrade(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.22.14", []string{"4.22.17", "4.23.0-ec.1"})
	cluster.Spec.Release.ChannelGroup = "candidate"
	meta.SetStatusCondition(&cluster.Status.HostedClusterResult.ObservedConditions, metav1.Condition{
		Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue, Reason: "AsExpected",
	})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel.Name = "candidate"
	storeClient.channel.Spec.FleetMinorVersion = "4.23"
	storeClient.policies = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)
	require.Equal(t, "4.23.0-ec.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	require.Equal(t, targetSourceAutomatic, storeClient.statusWriter.captured.(*privatev1.Cluster).Status.ControlPlaneUpgrade.TargetSource)
}

func TestReconcile_NoPolicy_StillRequiresHealthyHostedCluster(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	cluster.Status.HostedClusterResult.ObservedConditions = nil

	r, storeClient := buildReconciler(t, cluster)
	storeClient.policies = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	require.Equal(t, "HostedClusterDegraded",
		meta.FindStatusCondition(storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions, conditionAvailable).Reason)
}

func TestReconcile_ZStreamWaitsForMaintenanceWindow(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})

	r, storeClient := buildReconciler(t, cluster)
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	storeClient.policies[0].Spec.MaintenanceWindow = &privatev1.ControlPlaneMaintenanceWindow{
		Start:           metav1.NewTime(nextDay),
		DurationMinutes: 60,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
			Frequency:  "weekly",
			DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
		},
	}

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, "OutsideMaintenanceWindow",
		meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Reason)
}

func TestReconcile_InvalidMaintenanceWindowBlocksUpgrade(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	r, storeClient := buildReconciler(t, cluster)
	storeClient.policies[0].Spec.MaintenanceWindow = &privatev1.ControlPlaneMaintenanceWindow{
		Start:           metav1.Now(),
		DurationMinutes: 240,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
			Frequency: "monthly", DaysOfWeek: []string{"monday"},
		},
	}

	result, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "InvalidMaintenanceWindow", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(conditions, conditionDegraded).Status)
}

func TestReconcile_MultiplePolicies_SkipsAutomaticUpgrade(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: conditionAvailable, Status: metav1.ConditionTrue, Reason: "UpdateSelected",
	})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.policies = append(storeClient.policies, privatev1.ControlPlaneUpgradePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-2", Namespace: "hyperfleet"},
		Spec:       privatev1.ControlPlaneUpgradePolicySpec{ClusterID: "cluster-1"},
	})

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	require.NotNil(t, storeClient.statusWriter)
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(conditions, conditionAvailable).Status)
	require.Equal(t, "MultipleUpgradePolicies", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(conditions, conditionDegraded).Status)
}

func TestReconcile_ActiveUpgrade_ReportsReleaseRejection(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.1", nil)
	cluster.Status.HostedClusterResult.Version = "4.15.0" // completed
	cluster.Status.HostedClusterResult.DesiredVersion = "4.15.1"
	cluster.Status.HostedClusterResult.ObservedConditions = []metav1.Condition{
		{
			Type: "ClusterVersionReleaseAccepted", Status: metav1.ConditionFalse, Reason: "PayloadRejected", Message: "release payload rejected",
		},
	}
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionDegraded).Status)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status)
	require.Equal(t, "4.15.0", captured.Status.HostedClusterResult.Version, "completed version must be preserved on failure")

	require.NotNil(t, captured.Status.ControlPlaneUpgrade)
	require.Equal(t, "ReleaseNotAccepted", captured.Status.ControlPlaneUpgrade.FailureReason)
}

func TestReconcile_ActiveUpgrade_DegradationRemainsProgressing(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.1", nil)
	cluster.Status.HostedClusterResult.Version = "4.15.0" // completed
	cluster.Status.HostedClusterResult.DesiredVersion = "4.15.1"
	cluster.Status.HostedClusterResult.ObservedConditions = []metav1.Condition{
		{
			Type: "HostedClusterDegraded", Status: metav1.ConditionTrue, Reason: "SomeComponentDegraded", Message: "kube-apiserver degraded",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	}
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status,
		"rollout degradation must not be treated as terminal failure")
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionDegraded).Status)
}

func TestReconcile_SpecAheadOfFeedback_DoesNotReselect(t *testing.T) {
	// spec already requests 4.15.1, but HC feedback hasn't caught up yet
	// (desired still reports the old completed version).
	cluster := readyCluster("cluster-1", "4.15.0", []string{"4.15.1"})
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status)
}

func TestReconcile_RecordsCompletion(t *testing.T) {
	cluster := readyCluster("cluster-1", "4.15.1", []string{"4.15.1"}) // no newer candidate
	cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{
		TargetVersion: "4.15.1",
		TargetSource:  "automatic",
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.NotNil(t, captured.Status.ControlPlaneUpgrade.CompletedAt)
}
