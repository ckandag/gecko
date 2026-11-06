package nodepoolupgrade

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/client/transport"
	"github.com/openshift-online/gecko/controllers/client/transport/mock"
	installer "github.com/openshift-online/gecko/controllers/nodepool"
	"github.com/openshift-online/gecko/controllers/nodepoolvrresolution"
	"github.com/openshift-online/gecko/controllers/util/constants"
	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type uncertainTransport struct{ *mock.Client }

func (tr *uncertainTransport) Apply(ctx context.Context, target, group string, manifests [][]byte) (*transport.Status, error) {
	_, _ = tr.Client.Apply(ctx, target, group, manifests)
	return nil, errors.New("transport outcome is unknown")
}

func pipelineObjects() (*privatev1.NodePool, *privatev1.Cluster, *privatev1.Version) {
	pool, cluster, release := testObjects()
	pool.Finalizers = []string{constants.FinalizerNodePool}
	pool.Spec.Platform = privatev1.NodePoolPlatformSpec{Type: "GCP", GCP: &privatev1.GCPNodePoolPlatform{MachineType: "n2-standard-4", Zone: "us-central1-a", Subnet: "workers"}}
	pool.Status.VersionResolution = &privatev1.VersionResolutionResult{ReleaseVersion: "4.22.14", ChannelGroup: "stable", ReleaseImage: "old-image"}
	cluster.UID = types.UID("test-cluster-uid")
	cluster.Spec.SafeName = cluster.Name
	cluster.Spec.Platform = privatev1.ClusterPlatformSpec{Type: "GCP", GCP: &privatev1.GCPClusterPlatform{ProjectID: "test-project", Region: "us-central1", Subnet: "workers"}}
	cluster.Status.PlacementResult = &privatev1.PlacementResult{ManagementClusterName: "test-mc"}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "HostedClusterAvailable", Status: metav1.ConditionTrue, Reason: "AsExpected"})
	release.Spec.ReleaseImage = "target-image"
	return pool, cluster, release
}

func pipelineControllers(t *testing.T, c client.Client) (*nodepoolvrresolution.Reconciler, *installer.Reconciler, *mock.Client) {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{Level: "error", Format: logger.FormatText, Component: "test"})
	require.NoError(t, err)
	tr := mock.New()
	return nodepoolvrresolution.NewReconciler(log, c), installer.New(tr, log, c), tr
}

func appliedImage(t *testing.T, tr *mock.Client) string {
	t.Helper()
	require.NotEmpty(t, tr.ApplyCalls)
	var object map[string]any
	require.NoError(t, json.Unmarshal(tr.ApplyCalls[len(tr.ApplyCalls)-1].Manifests[0], &object))
	return object["spec"].(map[string]any)["release"].(map[string]any)["image"].(string)
}

func TestScheduledReleaseThroughCoordinatorResolutionAndInstallation(t *testing.T) {
	pool, cluster, release := pipelineObjects()
	start := metav1.NewTime(testNow.Add(time.Hour))
	pool.Spec.Release.UpgradeStartTime = &start
	r, c := testReconciler(t, pool, cluster, release)
	vr, install, tr := pipelineControllers(t, c)
	group, err := transport.NodePoolGroupKey(pool.Namespace, pool.Spec.ClusterID, pool.Name)
	require.NoError(t, err)
	resourceKey := transport.ResourceKey(constants.HyperShiftGroup, constants.HyperShiftVersion, "nodepools", "clusters-"+string(cluster.UID), pool.Name)
	feedback := &transport.Status{
		Conditions: []metav1.Condition{{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"}},
		ResourceStatuses: map[string]map[string]string{resourceKey: {
			"version": "4.22.14", "readyCondition": "True", "allNodesHealthyCondition": "True", "allMachinesReadyCondition": "True",
		}},
	}
	tr.StatusOverrides["test-mc/"+group] = feedback

	reconcilePool(t, r)
	_, err = vr.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Equal(t, "old-image", appliedImage(t, tr), "scheduled upgrades must keep reconciling the observed release")
	require.Equal(t, "4.22.16", getPool(t, c).Spec.Release.Version)
	require.Equal(t, "4.22.14", getPool(t, c).Status.Version)

	r.now = func() time.Time { return start.Time }
	reconcilePool(t, r)
	accepted := getPool(t, c)
	require.True(t, privatev1.NodePoolUpgradeActive(accepted))
	conflicting := accepted.DeepCopy()
	conflicting.Spec.Release.Version = "4.22.17"
	require.ErrorContains(t, conflicting.ValidateUpdate(t.Context(), accepted), "accepted upgrade is active")
	require.NoError(t, accepted.DeepCopy().ValidateUpdate(t.Context(), accepted), "identical retries must be accepted")
	_, err = vr.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Equal(t, "target-image", getPool(t, c).Status.VersionResolution.ReleaseImage)

	before := len(tr.ApplyCalls)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Len(t, tr.ApplyCalls, before, "persist the dispatch boundary before writing the target")
	require.Equal(t, metav1.ConditionUnknown, meta.FindStatusCondition(getPool(t, c).Status.Conditions, privatev1.NodePoolUpgradeSubmitted).Status)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Equal(t, "target-image", appliedImage(t, tr))
	require.True(t, meta.IsStatusConditionTrue(getPool(t, c).Status.Conditions, privatev1.NodePoolUpgradeSubmitted))

	feedback.ResourceStatuses[resourceKey]["version"] = "4.22.16"
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	reconcilePool(t, r)
	completed := getPool(t, c)
	require.Equal(t, "4.22.16", completed.Status.Version)
	require.False(t, privatev1.NodePoolUpgradeActive(completed))
	require.Equal(t, "UpgradeCompleted", meta.FindStatusCondition(completed.Status.Conditions, conditionProgressing).Reason)
	rv := completed.ResourceVersion
	reconcilePool(t, r)
	require.Equal(t, rv, getPool(t, c).ResourceVersion)
}

func TestCatalogFailureBeforeSubmissionCanBeCanceled(t *testing.T) {
	pool, cluster, release := pipelineObjects()
	r, c := testReconciler(t, pool, cluster, release)
	vr, install, tr := pipelineControllers(t, c)
	reconcilePool(t, r)
	require.True(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
	// Catalog membership changes after synchronous admission/coordinator approval.
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(release), release))
	release.Spec.ChannelGroups = []string{"fast"}
	require.NoError(t, c.Update(t.Context(), release))
	_, err := vr.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	reconcilePool(t, r)
	failed := getPool(t, c)
	require.True(t, meta.IsStatusConditionTrue(failed.Status.Conditions, conditionDegraded))
	require.False(t, privatev1.NodePoolUpgradeActive(failed))
	require.False(t, privatev1.NodePoolUpgradeSubmissionStarted(failed))
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Equal(t, "old-image", appliedImage(t, tr), "the failed target must never reach transport")
	failed = getPool(t, c)
	reverted := failed.DeepCopy()
	reverted.Spec.Release.Version = reverted.Status.Version
	reverted.Spec.Release.UpgradeStartTime = nil
	require.NoError(t, reverted.ValidateUpdate(t.Context(), failed))
	reverted.Generation++
	require.NoError(t, c.Update(t.Context(), reverted))
	reconcilePool(t, r)
	canceled := getPool(t, c)
	require.Equal(t, "4.22.14", canceled.Spec.Release.Version)
	require.False(t, meta.IsStatusConditionTrue(canceled.Status.Conditions, conditionDegraded))
}

func TestDispatchErrorDoesNotUnlockAnUncertainUpgrade(t *testing.T) {
	pool, cluster, release := pipelineObjects()
	r, c := testReconciler(t, pool, cluster, release)
	vr, _, tr := pipelineControllers(t, c)
	log, err := logger.NewLogger(logger.Config{Level: "error", Format: logger.FormatText, Component: "test"})
	require.NoError(t, err)
	install := installer.New(&uncertainTransport{Client: tr}, log, c)
	reconcilePool(t, r)
	_, err = vr.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Empty(t, tr.ApplyCalls)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.ErrorContains(t, err, "transport outcome is unknown")
	require.Len(t, tr.ApplyCalls, 1)
	reconcilePool(t, r)
	uncertain := getPool(t, c)
	require.True(t, privatev1.NodePoolUpgradeActive(uncertain))
	require.Equal(t, metav1.ConditionUnknown, meta.FindStatusCondition(uncertain.Status.Conditions, privatev1.NodePoolUpgradeSubmitted).Status)
	changed := uncertain.DeepCopy()
	changed.Spec.Release.Version = changed.Status.Version
	require.ErrorContains(t, changed.ValidateUpdate(t.Context(), uncertain), "accepted upgrade is active")
}

func TestInitialVersionIsReportedThroughNormalInstallationFeedback(t *testing.T) {
	pool, cluster, release := pipelineObjects()
	pool.Status = privatev1.NodePoolStatus{}
	r, c := testReconciler(t, pool, cluster, release)
	vr, install, tr := pipelineControllers(t, c)
	group, err := transport.NodePoolGroupKey(pool.Namespace, pool.Spec.ClusterID, pool.Name)
	require.NoError(t, err)
	key := transport.ResourceKey(constants.HyperShiftGroup, constants.HyperShiftVersion, "nodepools", "clusters-"+string(cluster.UID), pool.Name)
	feedback := &transport.Status{
		Conditions:       []metav1.Condition{{Type: "Applied", Status: metav1.ConditionTrue, Reason: "AppliedSuccessfully"}},
		ResourceStatuses: map[string]map[string]string{key: {"readyCondition": "True", "allNodesHealthyCondition": "True", "allMachinesReadyCondition": "True"}},
	}
	tr.StatusOverrides["test-mc/"+group] = feedback
	reconcilePool(t, r)
	_, err = vr.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Equal(t, "target-image", appliedImage(t, tr))
	submitted := getPool(t, c)
	require.Empty(t, submitted.Status.Version)
	changed := submitted.DeepCopy()
	changed.Spec.Release.Version = "4.22.17"
	require.ErrorContains(t, changed.ValidateUpdate(t.Context(), submitted), "observed NodePool version")
	feedback.ResourceStatuses[key]["version"] = "4.22.16"
	feedback.Stale = true
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	require.Empty(t, getPool(t, c).Status.Version, "stale feedback must not establish a version")
	feedback.Stale = false
	_, err = install.Reconcile(t.Context(), poolRequest())
	require.NoError(t, err)
	observed := getPool(t, c)
	require.Equal(t, "4.22.16", observed.Status.Version)
	reconcilePool(t, r)
	require.False(t, privatev1.NodePoolUpgradeActive(getPool(t, c)))
	changed = observed.DeepCopy()
	changed.Spec.Release.Version = "4.22.17"
	require.NoError(t, changed.ValidateUpdate(t.Context(), observed))
}
