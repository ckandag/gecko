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
	called    bool
	captured  client.Object
	updateErr error
	stored    **privatev1.Cluster
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj.DeepCopyObject().(client.Object)
	if m.updateErr != nil {
		return m.updateErr
	}
	if m.stored != nil {
		(*m.stored).Status = *obj.(*privatev1.Cluster).Status.DeepCopy()
	}
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
	cluster        *privatev1.Cluster
	channel        *privatev1.Channel
	statusWriter   *mockStatusWriter
	updateCalled   bool
	updated        client.Object
	updateErr      error
	persistUpdates bool
	releases       []privatev1.Version
	versionErr     error
}

func (m *mockStoreClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch typed := obj.(type) {
	case *privatev1.Cluster:
		if m.cluster == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, "")
		}
		*typed = *m.cluster.DeepCopy()
		return nil
	case *privatev1.Version:
		if m.versionErr != nil {
			return m.versionErr
		}
		for _, release := range m.releases {
			if release.Name == key.Name {
				*typed = *release.DeepCopy()
				return nil
			}
		}
		group := "stable"
		if m.cluster != nil && m.cluster.Spec.Release.ChannelGroup != "" {
			group = m.cluster.Spec.Release.ChannelGroup
		}
		*typed = privatev1.Version{Spec: privatev1.VersionSpec{ReleaseImage: "image", ChannelGroups: []string{group}}}
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
	if m.persistUpdates {
		m.statusWriter.stored = &m.cluster
	}
	return m.statusWriter
}

func (m *mockStoreClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return fmt.Errorf("unexpected cluster patch")
}

func (m *mockStoreClient) List(_ context.Context, obj client.ObjectList, _ ...client.ListOption) error {
	list, ok := obj.(*privatev1.VersionList)
	if !ok {
		return fmt.Errorf("unexpected list %T", obj)
	}
	if m.releases != nil {
		list.Items = m.releases
		return nil
	}
	if m.cluster != nil && m.cluster.Status.HostedClusterResult != nil {
		for _, version := range m.cluster.Status.HostedClusterResult.AvailableUpdates {
			list.Items = append(list.Items, privatev1.Version{ObjectMeta: metav1.ObjectMeta{Name: version}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}, ReleaseImage: "image"}})
		}
	}
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
	if m.persistUpdates {
		m.cluster = obj.(*privatev1.Cluster).DeepCopy()
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
	storeClient := &mockStoreClient{cluster: cluster, channel: channel}
	return NewReconciler(storeClient, testLogger(t)), storeClient
}

// readyCluster builds a Cluster with completed=desired=spec=version and
// HostedClusterAvailable=True, i.e. steady state with nothing active.
func readyCluster(version string, availableUpdates []string) *privatev1.Cluster {
	cluster := &privatev1.Cluster{}
	cluster.SetName("cluster-1")
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
	cluster := readyCluster("4.15.0", []string{"4.15.0"})

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
	cluster := readyCluster("4.15.0", []string{"4.15.1", "4.16.0"})

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)

	patched := storeClient.updated.(*privatev1.Cluster)
	require.Equal(t, "4.15.1", patched.Spec.Release.Version, "must not select the cross-minor 4.16.0 candidate")

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(captured.Status.Conditions, conditionAvailable).Status)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Status)
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(captured.Status.Conditions, conditionDegraded).Status)

	require.NotNil(t, captured.Status.ControlPlaneUpgrade)
	require.Equal(t, "4.15.1", captured.Status.ControlPlaneUpgrade.TargetVersion)
	require.Equal(t, "automatic", captured.Status.ControlPlaneUpgrade.TargetSource)
	require.NotNil(t, captured.Status.ControlPlaneUpgrade.RequestedTime)
}

func TestReconcile_ZStreamDoesNotRequireChannel(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)

	patched := storeClient.updated.(*privatev1.Cluster)
	require.Equal(t, "4.15.1", patched.Spec.Release.Version)
}

func TestReconcile_PrefersAuthorizedYStreamCandidate(t *testing.T) {
	cluster := readyCluster("4.22.14", []string{"4.22.17", "4.23.0-ec.1"})
	cluster.Spec.Release.ChannelGroup = "candidate"
	meta.SetStatusCondition(&cluster.Status.HostedClusterResult.ObservedConditions, metav1.Condition{
		Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue, Reason: "AsExpected",
	})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel.Name = "candidate"
	storeClient.channel.Spec.FleetMinorVersion = "4.23"

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
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
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
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
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
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
	cluster := readyCluster("4.15.1", nil)
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

func TestReconcile_ActiveUpgrade_DoesNotRequireChannel(t *testing.T) {
	cluster := readyCluster("4.15.1", nil)
	cluster.Status.HostedClusterResult.Version = "4.15.0"
	cluster.Status.HostedClusterResult.DesiredVersion = "4.15.1"
	cluster.Spec.Release.Version = "4.15.1"

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel = nil

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueObserving, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	require.True(t, storeClient.statusWriter.called)
}

func TestReconcile_NoMaintenanceWindow_AllowsZStreamUpgrade(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)
	require.Equal(t, "4.15.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	require.Equal(t, targetSourceAutomatic, storeClient.statusWriter.captured.(*privatev1.Cluster).Status.ControlPlaneUpgrade.TargetSource)
}

func TestReconcile_VersionConflictRechecksCluster(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
	r, storeClient := buildReconciler(t, cluster)
	storeClient.updateErr = apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, cluster.Name, fmt.Errorf("conflict"))

	result, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.Equal(t, "7", storeClient.updated.GetResourceVersion())
	require.False(t, privatev1.ClusterReleaseApproved(storeClient.updated.(*privatev1.Cluster)), "a losing update must not approve a release")

	storeClient.cluster.Spec.Release.Version = "4.15.2"
	storeClient.cluster.SetResourceVersion("8")
	storeClient.updateErr = nil
	storeClient.updateCalled = false
	_, err = r.Reconcile(context.Background(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.False(t, storeClient.updateCalled, "retry must observe the other writer's target")
}

func TestReconcile_NoMaintenanceWindow_AllowsYStreamUpgrade(t *testing.T) {
	cluster := readyCluster("4.22.14", []string{"4.22.17", "4.23.0-ec.1"})
	cluster.Spec.Release.ChannelGroup = "candidate"
	meta.SetStatusCondition(&cluster.Status.HostedClusterResult.ObservedConditions, metav1.Condition{
		Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue, Reason: "AsExpected",
	})

	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel.Name = "candidate"
	storeClient.channel.Spec.FleetMinorVersion = "4.23"

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeueConflict, result.RequeueAfter)
	require.True(t, storeClient.updateCalled)
	require.Equal(t, "4.23.0-ec.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	require.Equal(t, targetSourceAutomatic, storeClient.statusWriter.captured.(*privatev1.Cluster).Status.ControlPlaneUpgrade.TargetSource)
}

func TestReconcile_NoMaintenanceWindow_StillRequiresHealthyHostedCluster(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
	cluster.Status.HostedClusterResult.ObservedConditions = nil

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.Equal(t, requeuePending, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	require.Equal(t, "HostedClusterDegraded",
		meta.FindStatusCondition(storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions, conditionAvailable).Reason)
}

func TestReconcile_ZStreamWaitsForMaintenanceWindow(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))

	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, "OutsideMaintenanceWindow",
		meta.FindStatusCondition(captured.Status.Conditions, conditionProgressing).Reason)
}

func TestReconcile_YStreamExclusionBlocksUntilRemoved(t *testing.T) {
	cluster := readyCluster("4.22.20", []string{"4.23.1"})
	cluster.Status.HostedClusterResult.ObservedConditions = append(cluster.Status.HostedClusterResult.ObservedConditions,
		metav1.Condition{Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue, Reason: "AsExpected"})
	now := time.Now().UTC().Truncate(time.Second)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime: metav1.NewTime(now.Add(-time.Hour)), DurationSeconds: 14400,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency: "weekly", DaysOfWeek: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
			},
		},
		Exclusions: &[]privatev1.ControlPlaneMaintenanceExclusion{{
			Name: "freeze", StartTime: metav1.NewTime(now.Add(-time.Hour)), EndTime: metav1.NewTime(now.Add(2 * time.Hour)),
		}},
	}
	require.NoError(t, cluster.Default(t.Context()))
	require.NoError(t, cluster.ValidateCreate(t.Context()))
	r, storeClient := buildReconciler(t, cluster)
	storeClient.channel.Spec.FleetMinorVersion = "4.23"
	for range 2 {
		_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
		require.NoError(t, err)
		require.False(t, storeClient.updateCalled)
		conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
		require.Equal(t, "MaintenanceExclusionActive", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
		require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(conditions, conditionProgressing).Status)
		require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(conditions, conditionDegraded).Status)
	}
	old := cluster.DeepCopy()
	cluster.Spec.Maintenance.Exclusions = &[]privatev1.ControlPlaneMaintenanceExclusion{}
	require.NoError(t, cluster.ValidateUpdate(t.Context(), old))
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, storeClient.updateCalled, "conditions: %+v", storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions)
	require.Equal(t, "4.23.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	upgrade := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.ControlPlaneUpgrade
	require.Equal(t, "automatic", upgrade.TargetSource)
}

// TestReconcile_CriticalUpgradeBypassesMaintenanceWindow verifies that setting
// Cluster.spec.maintenance.criticalUpgrade to the exact candidate version bypasses
// the maintenance window.
func TestReconcile_CriticalUpgradeBypassesMaintenanceWindow(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		CriticalUpgrade: "4.15.1", // exact version pinned
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	require.NoError(t, err)
	require.True(t, storeClient.updateCalled)
	require.Equal(t, "4.15.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "CriticalUpgradeRequired", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
}

// TestReconcile_CriticalUpgradePreferredOverYStream verifies that when
// criticalUpgrade pins an advertised z-stream patch and a y-stream candidate is
// also available, the controller selects the pinned version and bypasses the
// maintenance window rather than being blocked while the y-stream is selected.
func TestReconcile_CriticalUpgradePreferredOverYStream(t *testing.T) {
	// 4.15.1 is the pinned critical patch; 4.15.2 is also available z-stream.
	// The test relies on buildReconciler which configures channel FleetMinorVersion
	// "4.15", so there is no y-stream candidate here — use a cross-stream setup.
	cluster := readyCluster("4.15.0", []string{"4.15.1", "4.15.2"})
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		CriticalUpgrade: "4.15.1", // pinned; 4.15.1 IS advertised
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	// criticalUpgrade "4.15.1" is advertised, so it is selected before the
	// z-stream winner (4.15.2). The override then applies and bypasses the window.
	require.NoError(t, err)
	require.True(t, storeClient.updateCalled, "pinned critical version must be selected and bypass the window")
	require.Equal(t, "4.15.1", storeClient.updated.(*privatev1.Cluster).Spec.Release.Version)
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "CriticalUpgradeRequired", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
}

// TestReconcile_CriticalUpgradeNotAdvertisedFallsBackToNormalSelection verifies
// that when criticalUpgrade names a version not in AvailableUpdates, normal
// candidate selection proceeds (z-stream or y-stream) and maintenance applies.
func TestReconcile_CriticalUpgradeNotAdvertisedFallsBackToNormalSelection(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.2"}) // 4.15.1 is NOT advertised
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		CriticalUpgrade: "4.15.1", // pinned but not advertised
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	// 4.15.1 is not advertised, so normal selection picks 4.15.2. The pin does
	// not match the candidate so no override applies and the window blocks it.
	require.NoError(t, err)
	require.False(t, storeClient.updateCalled, "non-advertised pin must not bypass maintenance")
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "OutsideMaintenanceWindow", meta.FindStatusCondition(conditions, conditionProgressing).Reason)
}

// TestReconcile_CriticalUpgradeCrossMinorPinIgnored verifies that a criticalUpgrade
// pin in a different minor version does not bypass y-stream gates or maintenance.
// The pin must be a z-stream update of the current completed version.
func TestReconcile_CriticalUpgradeCrossMinorPinIgnored(t *testing.T) {
	// cluster is at 4.15.0; pin is in a different minor (4.16.1); maintenance window is in future.
	cluster := readyCluster("4.15.0", []string{"4.15.1", "4.16.1"})
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		CriticalUpgrade: "4.16.1", // cross-minor pin — must not bypass
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	// The cross-minor pin is rejected; normal selection picks 4.15.1 (z-stream).
	// 4.15.1 != "4.16.1" so no override, maintenance window blocks it.
	require.NoError(t, err)
	require.False(t, storeClient.updateCalled, "cross-minor pin must not bypass maintenance")
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "OutsideMaintenanceWindow", meta.FindStatusCondition(conditions, conditionProgressing).Reason)
}

// TestReconcile_CrossMinorPinMatchingYStreamDoesNotBypassMaintenance is the
// exact scenario from the chai bot bug report:
//  1. Operator sets CriticalUpgrade = "4.16.0" on a cluster at 4.15.3
//  2. isValidCriticalPin rejects it (cross-minor) — target selection picks y-stream "4.16.0"
//  3. overrideCondition must NOT fire even though pin == candidate, because the
//     pin is not a valid z-stream update — maintenance window must still block it.
func TestReconcile_CrossMinorPinMatchingYStreamDoesNotBypassMaintenance(t *testing.T) {
	// Cluster at 4.15.3; 4.16.0 is the only advertised update (y-stream).
	// buildReconciler sets FleetMinorVersion="4.15" which blocks 4.16 — so we
	// override the channel to authorize 4.16 to make the y-stream actually selected.
	cluster := readyCluster("4.15.3", []string{"4.16.0"})
	nextDay := time.Now().UTC().Add(24 * time.Hour)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		CriticalUpgrade: "4.16.0", // cross-minor pin that matches the y-stream candidate
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(nextDay),
			DurationSeconds: 3600,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{strings.ToLower(nextDay.Weekday().String())},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)
	// Authorize 4.16 so the y-stream candidate is actually selected.
	storeClient.channel.Spec.FleetMinorVersion = "4.16"

	_, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	// The y-stream candidate 4.16.0 IS selected (pin == candidate), but the pin
	// is not a valid z-stream update of 4.15.3, so overrideCondition must NOT fire.
	// The upgrade is blocked (by health gate or maintenance) and no update is requested.
	require.NoError(t, err)
	require.False(t, storeClient.updateCalled, "cross-minor pin matching y-stream must not bypass maintenance")
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	progressing := meta.FindStatusCondition(conditions, conditionProgressing)
	require.NotNil(t, progressing)
	require.Equal(t, metav1.ConditionFalse, progressing.Status, "upgrade must be blocked, not progressing")
}

func TestReconcile_InvalidMaintenanceWindowBlocksUpgrade(t *testing.T) {
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{
		Window: &privatev1.ControlPlaneMaintenanceWindow{
			StartTime:       metav1.Now(),
			DurationSeconds: 14400,
			Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
				Frequency: "monthly", DaysOfWeek: []string{"monday"},
			},
		},
	}

	r, storeClient := buildReconciler(t, cluster)

	result, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))

	require.NoError(t, err)
	require.Equal(t, requeueStable, result.RequeueAfter)
	require.False(t, storeClient.updateCalled)
	conditions := storeClient.statusWriter.captured.(*privatev1.Cluster).Status.Conditions
	require.Equal(t, "InvalidMaintenanceWindow", meta.FindStatusCondition(conditions, conditionAvailable).Reason)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(conditions, conditionDegraded).Status)
}

func TestReconcile_ActiveUpgrade_ReportsReleaseRejection(t *testing.T) {
	cluster := readyCluster("4.15.1", nil)
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
	cluster := readyCluster("4.15.1", nil)
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
	cluster := readyCluster("4.15.0", []string{"4.15.1"})
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
	cluster := readyCluster("4.15.1", []string{"4.15.1"}) // no newer candidate
	cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{
		TargetVersion: "4.15.1",
		TargetSource:  "automatic",
	}

	r, storeClient := buildReconciler(t, cluster)

	_, err := r.Reconcile(context.Background(), clusterReq("cluster-1"))
	require.NoError(t, err)

	captured := storeClient.statusWriter.captured.(*privatev1.Cluster)
	require.NotNil(t, captured.Status.ControlPlaneUpgrade.CompletedTime)
}

func TestWriteStatusSkipsSemanticallyEqualTime(t *testing.T) {
	instant := time.Date(2026, time.October, 19, 12, 0, 0, 0, time.UTC)
	local := metav1.NewTime(instant.In(time.FixedZone("EDT", -4*60*60)))
	utc := metav1.NewTime(instant)
	cluster := &privatev1.Cluster{
		Status: privatev1.ClusterStatus{ControlPlaneUpgrade: &privatev1.ControlPlaneUpgradeResult{
			TargetVersion: "4.23.0", RequestedTime: &local,
		}},
	}
	result := cluster.Status.ControlPlaneUpgrade.DeepCopy()
	result.RequestedTime = &utc
	store := &mockStoreClient{}
	r := NewReconciler(store, testLogger(t))

	_, err := r.writeStatus(context.Background(), cluster, result, requeueStable)

	require.NoError(t, err)
	require.Nil(t, store.statusWriter, "equivalent instants must not cause a status write")
}

func TestReconcile_InlineMinorNormalizesBeforeCapacityApproval(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.23.8", "4.23.9", "4.23.10"})
	cluster.Spec.Release.Version = "4.23"
	cluster.Status.HostedClusterResult.ObservedConditions = append(cluster.Status.HostedClusterResult.ObservedConditions,
		metav1.Condition{Type: "ClusterVersionUpgradeable", Status: metav1.ConditionTrue})
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	store.releases = []privatev1.Version{
		{ObjectMeta: metav1.ObjectMeta{Name: "4.23.8"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "4.23.9"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "4.23.10"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"fast"}}},
	}
	allowed := false
	gate := func(context.Context, *privatev1.Cluster, string) (bool, error) { return allowed, nil }
	r.WithAdmissionGate(gate)
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.23.9", store.cluster.Spec.Release.Version)
	require.Equal(t, "4.22.19", store.cluster.Status.HostedClusterResult.Version)
	require.False(t, privatev1.ClusterReleaseApproved(store.cluster))
	// A restarted controller sees the exact normalized intent, even if a newer
	// supported patch appears while all progressive-rollout slots are occupied.
	store.cluster.Status.HostedClusterResult.AvailableUpdates = append(store.cluster.Status.HostedClusterResult.AvailableUpdates, "4.23.11")
	r = NewReconciler(store, testLogger(t)).WithAdmissionGate(gate)
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "WaitingForUpgradeCapacity", meta.FindStatusCondition(store.cluster.Status.Conditions, conditionProgressing).Reason)
	require.Equal(t, "4.23.9", store.cluster.Spec.Release.Version)
	require.Equal(t, "4.22.19", privatev1.ClusterInstallationVersion(store.cluster))
	allowed = true
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterReleaseApproved(store.cluster))
	require.Equal(t, "customer", store.cluster.Status.ControlPlaneUpgrade.TargetSource)
	require.Equal(t, "4.23.9", privatev1.ClusterInstallationVersion(store.cluster))
}

func TestReconcile_InlineApprovalWriteFailureDoesNotAuthorizeRollout(t *testing.T) {
	for _, failure := range []error{fmt.Errorf("status unavailable"), apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, "cluster-1", fmt.Errorf("conflict"))} {
		t.Run(failure.Error(), func(t *testing.T) {
			cluster := readyCluster("4.22.19", []string{"4.22.20"})
			cluster.Spec.Release.Version = "4.22.20"
			r, store := buildReconciler(t, cluster)
			store.persistUpdates = true
			store.statusWriter = &mockStatusWriter{updateErr: failure}
			result, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
			if apierrors.IsConflict(failure) {
				require.NoError(t, err)
				require.Equal(t, requeueConflict, result.RequeueAfter)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.False(t, privatev1.ClusterReleaseApproved(store.cluster))
			require.Equal(t, "4.22.19", privatev1.ClusterInstallationVersion(store.cluster))
			require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
		})
	}
}

func TestReconcile_InlineManualWaitsForHealthAndBypassesMaintenance(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	cluster.Spec.Release.Version = "4.22.20"
	exclusions := []privatev1.ControlPlaneMaintenanceExclusion{{Name: "freeze", StartTime: metav1.NewTime(time.Now().Add(-time.Hour)), EndTime: metav1.NewTime(time.Now().Add(time.Hour))}}
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{Exclusions: &exclusions}
	cluster.Status.HostedClusterResult.ObservedConditions = nil
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.False(t, privatev1.ClusterReleaseApproved(store.cluster))
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
	store.cluster.Status.HostedClusterResult.ObservedConditions = []metav1.Condition{{Type: "HostedClusterDegraded", Status: metav1.ConditionFalse}}
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterReleaseApproved(store.cluster))
}

func TestReconcile_InlineApprovedTargetSurvivesRestartAndCompletion(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	cluster.Spec.Release.Version = "4.22.20"
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterUpgradeActive(store.cluster))
	// Eligibility may disappear once dispatched. Persisted approval is sufficient
	// to continue observing the same target; do not reject or reselect it.
	store.cluster.Status.HostedClusterResult.AvailableUpdates = []string{"4.22.21"}
	meta.SetStatusCondition(&store.cluster.Status.Conditions, metav1.Condition{Type: privatev1.ControlPlaneUpgradeSubmitted, Status: metav1.ConditionUnknown})
	r = NewReconciler(store, testLogger(t))
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterUpgradeActive(store.cluster))
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
	store.cluster.Status.HostedClusterResult.Version = "4.22.20"
	store.cluster.Status.HostedClusterResult.DesiredVersion = "4.22.20"
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.False(t, privatev1.ClusterUpgradeActive(store.cluster))
	require.NotNil(t, store.cluster.Status.ControlPlaneUpgrade.CompletedTime)
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
}

func TestReconcile_InlineFailureLockDependsOnDispatchBoundary(t *testing.T) {
	for _, submitted := range []metav1.ConditionStatus{metav1.ConditionFalse, metav1.ConditionUnknown, metav1.ConditionTrue} {
		t.Run(string(submitted), func(t *testing.T) {
			cluster := readyCluster("4.22.19", []string{"4.22.20"})
			cluster.Spec.Release.Version = "4.22.20"
			cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{TargetVersion: "4.22.20", TargetSource: "customer"}
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: privatev1.ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue})
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: privatev1.ControlPlaneUpgradeSubmitted, Status: submitted})
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "VersionResolved", Status: metav1.ConditionFalse, ObservedGeneration: cluster.Generation})
			r, store := buildReconciler(t, cluster)
			store.persistUpdates = true
			_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
			require.NoError(t, err)
			require.Equal(t, submitted != metav1.ConditionFalse, privatev1.ClusterUpgradeActive(store.cluster))
		})
	}
}

func TestMaintenanceCompletedUpgradeWaitsForNextOpening(t *testing.T) {
	cluster := readyCluster("4.23.9", []string{"4.23.10"})
	completed := metav1.NewTime(time.Date(2026, time.November, 9, 10, 30, 0, 0, time.UTC))
	cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{TargetVersion: "4.23.9", CompletedTime: &completed}
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{Window: &privatev1.ControlPlaneMaintenanceWindow{
		StartTime: metav1.NewTime(time.Date(2026, time.November, 1, 10, 0, 0, 0, time.UTC)), DurationSeconds: 14400,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{Frequency: "weekly", DaysOfWeek: []string{"monday", "tuesday"}},
	}}
	permitted, reason, _, next := maintenancePermitted(cluster, completed.Add(time.Hour))
	require.False(t, permitted)
	require.Equal(t, "WaitingForNextMaintenanceWindow", reason)
	require.Equal(t, time.Date(2026, time.November, 10, 10, 0, 0, 0, time.UTC), *next)
	permitted, _, _, _ = maintenancePermitted(cluster, next.Add(time.Minute))
	require.True(t, permitted)
}

func TestReconcile_InitialMinorNormalizesFromSelectedChannel(t *testing.T) {
	cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-1", Namespace: "hyperfleet"}, Spec: privatev1.ClusterSpec{Release: privatev1.ReleaseSpec{Version: "4.23", ChannelGroup: "stable"}}}
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	store.releases = []privatev1.Version{
		{ObjectMeta: metav1.ObjectMeta{Name: "4.23.9"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "4.23.10"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"fast"}}},
	}
	require.False(t, privatev1.ClusterReleaseApproved(cluster), "an unresolved minor must never reach initial installation")
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.23.9", store.cluster.Spec.Release.Version)
	require.True(t, privatev1.ClusterReleaseApproved(store.cluster), "normal initial installation is preserved")
}

func TestReconcile_AutomaticCapacityWaitRetainsMaintenanceRules(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	start := time.Date(2026, time.November, 9, 10, 0, 0, 0, time.UTC)
	cluster.Spec.Maintenance = &privatev1.ClusterMaintenanceSpec{Window: &privatev1.ControlPlaneMaintenanceWindow{
		StartTime: metav1.NewTime(start), DurationSeconds: 14400,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{Frequency: "weekly", DaysOfWeek: []string{"monday", "tuesday"}},
	}}
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	r.now = func() time.Time { return start.Add(time.Hour) }
	allowed := false
	gate := func(context.Context, *privatev1.Cluster, string) (bool, error) { return allowed, nil }
	r.WithAdmissionGate(gate)
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
	require.Equal(t, "automatic", store.cluster.Status.ControlPlaneUpgrade.TargetSource)
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "WaitingForUpgradeCapacity", meta.FindStatusCondition(store.cluster.Status.Conditions, conditionProgressing).Reason)
	r = NewReconciler(store, testLogger(t)).WithAdmissionGate(gate)
	r.now = func() time.Time { return start.Add(23 * time.Hour) }
	allowed = true
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.False(t, privatev1.ClusterReleaseApproved(store.cluster))
	require.Equal(t, "OutsideMaintenanceWindow", meta.FindStatusCondition(store.cluster.Status.Conditions, conditionProgressing).Reason)
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version)
}

func TestReconcile_AutomaticIntentStatusFailureDoesNotChangeSpec(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	store.statusWriter = &mockStatusWriter{updateErr: fmt.Errorf("status unavailable")}
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.Error(t, err)
	require.False(t, store.updateCalled)
	require.Equal(t, "4.22.19", store.cluster.Spec.Release.Version)
}

func TestReconcile_AdvertisedTargetWaitsForCatalogRecovery(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	cluster.Spec.Release.Version = "4.22.20"
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	store.versionErr = apierrors.NewNotFound(schema.GroupResource{Resource: "versions"}, "4.22.20")
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "WaitingForVersionCatalog", meta.FindStatusCondition(store.cluster.Status.Conditions, conditionProgressing).Reason)
	require.False(t, privatev1.ClusterReleaseApproved(store.cluster))
	store.versionErr = nil
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterReleaseApproved(store.cluster))
}

func TestReconcile_AutomaticSelectionUsesSelectedChannel(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20", "4.22.21"})
	r, store := buildReconciler(t, cluster)
	store.releases = []privatev1.Version{
		{ObjectMeta: metav1.ObjectMeta{Name: "4.22.20"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "4.22.21"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"fast"}}},
	}
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.22.20", store.updated.(*privatev1.Cluster).Spec.Release.Version)
}

func TestReconcile_ExistingRolloutDoesNotConsumeDifferentPendingIntent(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.23.9"})
	cluster.Spec.Release.Version = "4.24"
	cluster.Status.HostedClusterResult.DesiredVersion = "4.23.9"
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.24", store.cluster.Spec.Release.Version)
	require.Equal(t, "4.23.9", privatev1.ClusterInstallationVersion(store.cluster))
	require.False(t, meta.IsStatusConditionTrue(store.cluster.Status.Conditions, privatev1.ControlPlaneUpgradeAccepted), "observing another rollout must not approve different intent")
	store.cluster.Status.HostedClusterResult.Version = "4.23.9"
	store.cluster.Status.HostedClusterResult.AvailableUpdates = []string{"4.24.2"}
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name)) // Persist completion.
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name)) // Normalize pending intent.
	require.NoError(t, err)
	require.Equal(t, "4.24.2", store.cluster.Spec.Release.Version)
}

func TestReconcile_ObservedTargetKeepsLockUntilHealthy(t *testing.T) {
	cluster := readyCluster("4.23.9", nil)
	cluster.Status.HostedClusterResult.ObservedConditions = nil
	cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{TargetVersion: "4.23.9", TargetSource: "customer"}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: privatev1.ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue, ObservedGeneration: cluster.Generation})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "VersionResolved", Status: metav1.ConditionFalse, ObservedGeneration: cluster.Generation})
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, privatev1.ClusterUpgradeActive(store.cluster), "an observed target proves dispatch even if its acknowledgement was lost")
	require.Nil(t, store.cluster.Status.ControlPlaneUpgrade.CompletedTime)
}

func TestReconcile_ChannelRepairRetriesTargetSelection(t *testing.T) {
	cluster := readyCluster("4.22.19", []string{"4.22.20"})
	r, store := buildReconciler(t, cluster)
	store.persistUpdates = true
	store.channel.Spec.FleetMinorVersion = "invalid"
	_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.True(t, meta.IsStatusConditionTrue(store.cluster.Status.Conditions, conditionDegraded))
	store.channel.Spec.FleetMinorVersion = "4.22"
	_, err = r.Reconcile(t.Context(), clusterReq(cluster.Name))
	require.NoError(t, err)
	require.Equal(t, "4.22.20", store.cluster.Spec.Release.Version, "channel repair must recover without a customer spec edit")
}
