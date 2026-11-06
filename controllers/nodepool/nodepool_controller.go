// Package nodepool implements the nodepool controller reconciler.
package nodepool

import (
	"context"
	"fmt"
	"time"

	"github.com/openshift-online/gecko/controllers/client/transport"
	"github.com/openshift-online/gecko/controllers/nodepool/manifest"
	"github.com/openshift-online/gecko/controllers/util/constants"
	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	adapterName    = "nodepool-controller"
	requeuePending = 15 * time.Second
	requeueStable  = 5 * time.Minute
)

// Reconciler implements the nodepool controller reconciliation loop.
type Reconciler struct {
	transport transport.Client
	log       logger.Logger
	client    client.Client
}

// New creates a new nodepool Reconciler.
func New(transport transport.Client, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		transport: transport,
		log:       log,
		client:    c,
	}
}

// Reconcile runs the nodepool controller loop for one nodepool event.
// req.Namespace = project namespace, req.Name = nodepoolID.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nodepoolID := req.Name
	log := r.log.With("nodepoolID", nodepoolID)

	// Read nodepool from cache.
	var np privatev1.NodePool
	if err := r.client.Get(ctx, req.NamespacedName, &np); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "nodepool %s not found, skipping", nodepoolID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get nodepool: %w", err)
	}

	// Read parent cluster from cache using the cluster ID from spec.
	clusterID := np.Spec.ClusterID
	log = log.With("clusterID", clusterID)
	var cluster privatev1.Cluster
	clusterFound := true
	clusterKey := types.NamespacedName{Namespace: req.Namespace, Name: clusterID}
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get cluster: %w", err)
		}
		clusterFound = false
	}

	// Handle deletion.
	if !np.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &np, &cluster, clusterFound, log)
	}

	// If cluster not found, nothing to reconcile.
	if !clusterFound {
		log.Infof(ctx, "cluster %s not found for nodepool %s, skipping", clusterID, nodepoolID)
		return reconcile.Result{}, nil
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&np, constants.FinalizerNodePool) {
		controllerutil.AddFinalizer(&np, constants.FinalizerNodePool)
		if err := r.client.Update(ctx, &np); err != nil {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: add finalizer: %w", err)
		}
		return reconcile.Result{}, nil // re-reconcile with finalizer in place
	}

	// Gate: cluster placement must be ready.
	managementCluster := placedManagementCluster(&cluster)
	if managementCluster == "" {
		if setWaitingNPConditions(&np, "PlacementNotReady", "Waiting for cluster placement to select a management cluster") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		log.Infof(ctx, "placement not ready for nodepool %s, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	// Read existing transport feedback before resolving or applying a new target
	// for pools created before status.version was introduced.
	if privatev1.NodePoolVersionObservationRequired(&np) {
		return r.backfillObservedVersion(ctx, &np, &cluster, managementCluster)
	}

	// Gate: nodepool VR must be ready.
	if np.Status.VersionResolution == nil || np.Status.VersionResolution.ReleaseVersion == "" {
		if setWaitingNPConditions(&np, "VersionResolutionNotReady", "Waiting for nodepool version resolution") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		log.Infof(ctx, "nodepool VR not ready for nodepool %s, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	// While a desired upgrade waits for approval, continue reconciling the
	// observed release so scaling and fresh health feedback are not blocked.
	version := installationVersion(&np)
	// Gate: VR must match the release currently permitted for installation.
	if np.Status.VersionResolution.ReleaseVersion != version {
		msg := fmt.Sprintf("VR version %q does not match spec version %q",
			np.Status.VersionResolution.ReleaseVersion, version)
		if setWaitingNPConditions(&np, "VersionMismatch", msg) {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		log.Infof(ctx, "nodepool VR version %q does not match spec version %q, requeueing after %s",
			np.Status.VersionResolution.ReleaseVersion, version, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	// Extract nodepool GCP platform fields.
	var machineType, zone string
	var diskSizeGB int64
	var diskType string
	if gcp := np.Spec.Platform.GCP; gcp != nil {
		machineType = gcp.MachineType
		diskSizeGB = gcp.DiskSizeGB
		diskType = gcp.DiskType
		zone = gcp.Zone
	}
	if machineType == "" {
		machineType = manifest.DefaultMachineType
	}
	if diskSizeGB == 0 {
		diskSizeGB = manifest.DefaultDiskSizeGB
	}
	if diskType == "" {
		diskType = manifest.DefaultDiskType
	}

	// Extract cluster GCP platform fields.
	var gcpRegion, gcpSubnet string
	if gcp := cluster.Spec.Platform.GCP; gcp != nil {
		gcpRegion = gcp.Region
		gcpSubnet = gcp.Subnet
	}
	if zone == "" && gcpRegion != "" {
		zone = gcpRegion + "-a"
	}

	replicas := defaultReplicas
	if np.Spec.NodeCount != nil {
		replicas = *np.Spec.NodeCount
	}
	safeName := cluster.Spec.SafeName
	if safeName == "" {
		safeName = privatev1.DefaultSafeName(cluster.Name, cluster.UID)
	}

	manifests, err := manifest.Build(manifest.Input{
		NodePoolID:         nodepoolID,
		NodePoolName:       np.Name,
		NodePoolGeneration: np.Generation,
		ClusterID:          string(cluster.UID),
		ClusterName:        safeName,
		Replicas:           replicas,
		MachineType:        machineType,
		GCPRegion:          gcpRegion,
		Zone:               zone,
		GCPSubnet:          gcpSubnet,
		DiskSizeGB:         diskSizeGB,
		DiskType:           diskType,
		ReleaseImage:       np.Status.VersionResolution.ReleaseImage,
	})
	if err != nil {
		return reconcile.Result{}, r.preparationFailure(ctx, &np, err)
	}

	groupKey, err := transport.NodePoolGroupKey(req.Namespace, np.Spec.ClusterID, req.Name)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("build group key: %w", err)
	}

	mwStatus, err := r.applyResources(ctx, &np, managementCluster, groupKey, manifests)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: apply resources: %w", err)
	}

	// If status is stale, skip condition updates and requeue quickly.
	if mwStatus != nil && mwStatus.Stale {
		log.Infof(ctx, "nodepool reconciler: nodepool %s status is stale, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	// Write nodepool status conditions — only update if something changed.
	if r.applyStatusConditions(&np, string(cluster.UID), mwStatus) {
		if err := r.client.Status().Update(ctx, &np); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
		}
	}

	if !meta.IsStatusConditionTrue(np.Status.Conditions, "NodePoolResourcesApplied") {
		log.Infof(ctx, "nodepool reconciler: nodepool %s resources not yet applied, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	if !meta.IsStatusConditionTrue(cluster.Status.Conditions, "HostedClusterAvailable") {
		log.Infof(ctx, "nodepool reconciler: cluster %s not yet available, requeueing nodepool %s after %s", clusterID, nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	if !meta.IsStatusConditionTrue(np.Status.Conditions, "NodePoolAvailable") {
		log.Infof(ctx, "nodepool reconciler: nodepool %s not yet available, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}

	log.Infof(ctx, "nodepool reconciler: nodepool %s reconciled, requeueing after %s", nodepoolID, requeueStable)
	return reconcile.Result{RequeueAfter: requeueStable}, nil
}

// handleDeletion cleans up management-cluster resources and removes the finalizer.
// Deletion flow:
// 1. Call transport.Delete to enqueue DeleteDesires (async)
// 2. Requeue, wait for kube-applier-gcp to process (poll GetDeleteStatus)
// 3. Once all DeleteDesires report Successful=True, cleanup DeleteDesires
// 4. Remove finalizer
func (r *Reconciler) handleDeletion(ctx context.Context, np *privatev1.NodePool, cluster *privatev1.Cluster, clusterFound bool, log logger.Logger) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(np, constants.FinalizerNodePool) {
		return reconcile.Result{}, nil
	}

	// Only call transport.Delete if resources were applied to an MC.
	if clusterFound &&
		meta.FindStatusCondition(np.Status.Conditions, "NodePoolResourcesApplied") != nil &&
		cluster.Status.PlacementResult != nil && cluster.Status.PlacementResult.ManagementClusterName != "" {
		mcName := cluster.Status.PlacementResult.ManagementClusterName

		groupKey, err := transport.NodePoolGroupKey(np.Namespace, np.Spec.ClusterID, np.Name)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: build group key: %w", err)
		}

		// Check if deletion already in progress by querying delete status first.
		deleteStatus, err := r.transport.GetDeleteStatus(ctx, mcName, groupKey)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: get delete status: %w", err)
		}

		if deleteStatus.TotalCount == 0 {
			// No DeleteDesires exist.
			if deleteStatus.ApplyDesiresCount > 0 {
				// ApplyDesires still present → deletion never started, call Delete().
				log.Infof(ctx, "nodepool reconciler: deleting resources for nodepool %s from %s", np.Name, mcName)
				if err := r.transport.Delete(ctx, mcName, groupKey); err != nil {
					return reconcile.Result{}, fmt.Errorf("nodepool reconciler: delete resources: %w", err)
				}
				log.Infof(ctx, "nodepool reconciler: delete initiated for nodepool %s, requeueing to poll status", np.Name)
				return reconcile.Result{RequeueAfter: requeuePending}, nil
			}
			// TotalCount=0 and ApplyDesiresCount=0 → deletion already complete (no-op), proceed to finalizer.
		}

		if !deleteStatus.AllSuccessful {
			// Deletion in progress — wait for completion.
			log.Infof(ctx, "nodepool reconciler: deletion in progress for nodepool %s (%d/%d pending), requeueing",
				np.Name, deleteStatus.PendingCount, deleteStatus.TotalCount)
			return reconcile.Result{RequeueAfter: requeuePending}, nil
		}

		// All DeleteDesires successful — cleanup before removing finalizer.
		log.Infof(ctx, "nodepool reconciler: deletion complete for nodepool %s, cleaning up %d DeleteDesires",
			np.Name, deleteStatus.TotalCount)
		if err := r.transport.CleanupDeleteDesires(ctx, mcName, groupKey); err != nil {
			return reconcile.Result{}, fmt.Errorf("nodepool reconciler: cleanup delete desires: %w", err)
		}
	} else {
		log.Infof(ctx, "nodepool reconciler: parent cluster not available for nodepool %s, skipping transport cleanup", np.Name)
	}

	controllerutil.RemoveFinalizer(np, constants.FinalizerNodePool)
	if err := r.client.Update(ctx, np); err != nil {
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: remove finalizer: %w", err)
	}

	log.Infof(ctx, "nodepool reconciler: finalizer removed for nodepool %s", np.Name)
	return reconcile.Result{}, nil
}

// placedManagementCluster returns the selected management cluster, if any.
func placedManagementCluster(cluster *privatev1.Cluster) string {
	if cluster.Status.PlacementResult == nil {
		return ""
	}
	return cluster.Status.PlacementResult.ManagementClusterName
}

// backfillObservedVersion only reads transport feedback: no target manifests are
// dispatched until an existing pool has an observed version for upgrade checks.
func (r *Reconciler) backfillObservedVersion(ctx context.Context, np *privatev1.NodePool, cluster *privatev1.Cluster, managementCluster string) (reconcile.Result, error) {
	groupKey, err := transport.NodePoolGroupKey(np.Namespace, np.Spec.ClusterID, np.Name)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("build group key for version observation: %w", err)
	}
	status, err := r.transport.GetStatus(ctx, managementCluster, groupKey)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("read NodePool version feedback: %w", err)
	}
	if status != nil && status.Stale {
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}
	if r.applyStatusConditions(np, string(cluster.UID), status) {
		if err := r.client.Status().Update(ctx, np); err != nil && !apierrors.IsConflict(err) {
			return reconcile.Result{}, fmt.Errorf("persist observed NodePool version: %w", err)
		}
	}
	return reconcile.Result{RequeueAfter: requeuePending}, nil
}

// preparationFailure reports a confirmed failure before dispatch. The upgrade
// coordinator can then release its lock without implying a HyperShift rollback.
func (r *Reconciler) preparationFailure(ctx context.Context, np *privatev1.NodePool, cause error) error {
	if privatev1.NodePoolUpgradeActive(np) && !privatev1.NodePoolUpgradeSubmissionStarted(np) {
		before := np.DeepCopy()
		meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type: "NodePoolResourcesApplied", Status: metav1.ConditionFalse, Reason: "ManifestBuildFailed",
			Message: "The desired NodePool resources could not be prepared before dispatch", ObservedGeneration: np.Generation,
		})
		if err := r.client.Status().Patch(ctx, np, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("record NodePool preparation failure: %w", err)
		}
	}
	return fmt.Errorf("nodepool reconciler: build manifests: %w", cause)
}

// applyResources persists the dispatch boundary before transport can write the
// target. An unknown result remains locked across errors or restarts; recovery
// after dispatch is intentionally outside the pre-submission cancellation flow.
func (r *Reconciler) applyResources(ctx context.Context, np *privatev1.NodePool, managementCluster, groupKey string, manifests [][]byte) (*transport.Status, error) {
	upgrading := privatev1.NodePoolUpgradeActive(np) && np.Spec.Release.Version != np.Status.Version
	if !upgrading {
		return r.transport.Apply(ctx, managementCluster, groupKey, manifests)
	}
	submitted := meta.FindStatusCondition(np.Status.Conditions, privatev1.NodePoolUpgradeSubmitted)
	if submitted == nil || submitted.Status == metav1.ConditionFalse {
		before := np.DeepCopy()
		meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type: privatev1.NodePoolUpgradeSubmitted, Status: metav1.ConditionUnknown,
			Reason: "Dispatching", Message: "The desired release is being dispatched to the management cluster",
			ObservedGeneration: np.Generation,
		})
		if err := r.client.Status().Patch(ctx, np, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil && !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("persist upgrade dispatch boundary: %w", err)
		}
		return &transport.Status{Stale: true}, nil
	}
	status, err := r.transport.Apply(ctx, managementCluster, groupKey, manifests)
	if err != nil {
		return nil, err
	}
	if submitted.Status != metav1.ConditionTrue {
		before := np.DeepCopy()
		meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type: privatev1.NodePoolUpgradeSubmitted, Status: metav1.ConditionTrue,
			Reason: "Dispatched", Message: "The desired release was dispatched to the management cluster",
			ObservedGeneration: np.Generation,
		})
		if err := r.client.Status().Patch(ctx, np, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return &transport.Status{Stale: true}, nil
			}
			return nil, fmt.Errorf("persist upgrade dispatch acknowledgement: %w", err)
		}
	}
	return status, nil
}

// installationVersion keeps unapproved release intent out of applied manifests.
func installationVersion(np *privatev1.NodePool) string {
	if !privatev1.NodePoolReleaseApproved(np) {
		return np.Status.Version
	}
	return np.Spec.Release.Version
}

// setWaitingNPConditions sets NodePoolResourcesApplied and NodePoolAvailable to Unknown.
// Returns true if either condition changed.
func setWaitingNPConditions(np *privatev1.NodePool, reason, message string) bool {
	gen := np.Generation
	a := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolResourcesApplied",
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
	b := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolAvailable",
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
	c := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolProgressing",
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
	return a || b || c
}

// applyStatusConditions derives conditions from the resource status and writes them to the nodepool.
// Returns true if any condition changed.
func (r *Reconciler) applyStatusConditions(np *privatev1.NodePool, clusterID string, mwStatus *transport.Status) bool {
	gen := np.Generation

	if mwStatus == nil {
		a := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolResourcesApplied",
			Status:             metav1.ConditionFalse,
			Reason:             "ResourcesNotFound",
			ObservedGeneration: gen,
		})
		b := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolAvailable",
			Status:             metav1.ConditionFalse,
			Reason:             "ResourcesNotFound",
			ObservedGeneration: gen,
		})
		c := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
			Type:               "NodePoolProgressing",
			Status:             metav1.ConditionFalse,
			Reason:             "AsExpected",
			ObservedGeneration: gen,
		})
		return a || b || c
	}

	// Extract conditions from top-level applied conditions.
	appliedStatus := metav1.ConditionStatus("False")
	appliedReason := "Unknown"
	for _, c := range mwStatus.Conditions {
		if c.Type == "Applied" {
			appliedStatus = c.Status
			appliedReason = c.Reason
			break
		}
	}

	// Extract resource status by NodePool resource identity key.
	npKey := transport.ResourceKey(constants.HyperShiftGroup, constants.HyperShiftVersion, "nodepools",
		fmt.Sprintf("clusters-%s", clusterID), np.Name)
	// rs is nil if npKey not in map; nil-safe below.
	rs := mwStatus.ResourceStatuses[npKey]
	versionChanged := false
	if rs != nil && rs["version"] != "" && np.Status.Version != rs["version"] {
		np.Status.Version = rs["version"]
		versionChanged = true
	}
	availableStatus := "False"
	allNodesHealthy := "False"
	// Empty string for progressing conditions: != "True" captures absent fields.
	allMachinesReady := ""
	updatingConfig := ""
	updatingVersion := ""
	if rs != nil {
		if v, ok := rs["readyCondition"]; ok {
			availableStatus = v
		}
		if v, ok := rs["allNodesHealthyCondition"]; ok {
			allNodesHealthy = v
		}
		if v, ok := rs["allMachinesReadyCondition"]; ok {
			allMachinesReady = v
		}
		if v, ok := rs["updatingConfigCondition"]; ok {
			updatingConfig = v
		}
		if v, ok := rs["updatingVersionCondition"]; ok {
			updatingVersion = v
		}
	}

	healthStatus := metav1.ConditionFalse
	healthReason := "NodePoolNotHealthy"
	if allNodesHealthy == "True" {
		healthStatus = metav1.ConditionTrue
		healthReason = "NodePoolHealthy"
	}

	availableReason := "NodePoolNotAvailable"
	if availableStatus == "True" {
		availableReason = "NodePoolAvailable"
	}

	a := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolResourcesApplied",
		Status:             appliedStatus,
		Reason:             appliedReason,
		ObservedGeneration: gen,
	})
	b := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolAvailable",
		Status:             metav1.ConditionStatus(availableStatus),
		Reason:             availableReason,
		ObservedGeneration: gen,
	})
	c := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolHealthy",
		Status:             healthStatus,
		Reason:             healthReason,
		ObservedGeneration: gen,
	})

	progressingStatus := metav1.ConditionFalse
	progressingReason := "AsExpected"
	switch {
	case allMachinesReady != "True":
		progressingStatus = metav1.ConditionTrue
		progressingReason = "MachinesNotReady"
	case updatingConfig == "True":
		progressingStatus = metav1.ConditionTrue
		progressingReason = "UpdatingConfig"
	case updatingVersion == "True":
		progressingStatus = metav1.ConditionTrue
		progressingReason = "UpdatingVersion"
	}

	d := meta.SetStatusCondition(&np.Status.Conditions, metav1.Condition{
		Type:               "NodePoolProgressing",
		Status:             progressingStatus,
		Reason:             progressingReason,
		ObservedGeneration: gen,
	})
	return a || b || c || d || versionChanged
}

// defaultReplicas is the hardcoded default for this POC.
const defaultReplicas = int32(1)
