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
	adapterName                         = "nodepool-controller"
	nodePoolUpgradeProgressingCondition = "NodePoolUpgradeProgressing"
	requeuePending                      = 15 * time.Second
	requeueStable                       = 5 * time.Minute
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
	if cluster.Status.PlacementResult == nil || cluster.Status.PlacementResult.ManagementClusterName == "" {
		if setWaitingNPConditions(&np, "PlacementNotReady", "Waiting for cluster placement to select a management cluster") {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		log.Infof(ctx, "placement not ready for nodepool %s, requeueing after %s", nodepoolID, requeuePending)
		return reconcile.Result{RequeueAfter: requeuePending}, nil
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

	// Gate: VR version must match spec version.
	if np.Status.VersionResolution.ReleaseVersion != np.Spec.Release.Version {
		msg := fmt.Sprintf("VR version %q does not match spec version %q",
			np.Status.VersionResolution.ReleaseVersion, np.Spec.Release.Version)
		if setWaitingNPConditions(&np, "VersionMismatch", msg) {
			if err := r.client.Status().Update(ctx, &np); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("nodepool reconciler: update nodepool status: %w", err)
			}
		}
		log.Infof(ctx, "nodepool VR version %q does not match spec version %q, requeueing after %s",
			np.Status.VersionResolution.ReleaseVersion, np.Spec.Release.Version, requeuePending)
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
		return reconcile.Result{}, fmt.Errorf("nodepool reconciler: build manifests: %w", err)
	}

	groupKey, err := transport.NodePoolGroupKey(req.Namespace, np.Spec.ClusterID, req.Name)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("build group key: %w", err)
	}

	managementCluster := cluster.Status.PlacementResult.ManagementClusterName

	mwStatus, err := r.transport.Apply(ctx, managementCluster, groupKey, manifests)
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
	d := setNodePoolUpgradeProgressing(np, false)
	return a || b || c || d
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
		d := setNodePoolUpgradeProgressing(np, false)
		return a || b || c || d
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
	if rs != nil && rs["version"] != "" && np.Status.CurrentVersion != rs["version"] {
		np.Status.CurrentVersion = rs["version"]
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
	e := setNodePoolUpgradeProgressing(np,
		availableStatus == "True" && healthStatus == metav1.ConditionTrue && progressingStatus == metav1.ConditionFalse)
	return a || b || c || d || e || versionChanged
}

// setNodePoolUpgradeProgressing tracks an active version change separately from
// general NodePool health. It is absent during creation and after completion.
func setNodePoolUpgradeProgressing(np *privatev1.NodePool, rolloutComplete bool) bool {
	current := np.Status.CurrentVersion
	if current == "" {
		return false
	}

	previous := meta.FindStatusCondition(np.Status.Conditions, nodePoolUpgradeProgressingCondition)
	condition := metav1.Condition{
		Type:               nodePoolUpgradeProgressingCondition,
		ObservedGeneration: np.Generation,
	}
	switch {
	case current != np.Spec.Release.Version:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "UpgradeInProgress"
		condition.Message = fmt.Sprintf("Upgrading NodePool from %s to %s", current, np.Spec.Release.Version)
	case previous == nil || previous.Status != metav1.ConditionTrue:
		return false
	case !rolloutComplete:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "WaitingForNodePool"
		condition.Message = fmt.Sprintf("Waiting for NodePool rollout to %s to complete", np.Spec.Release.Version)
	default:
		return meta.RemoveStatusCondition(&np.Status.Conditions, nodePoolUpgradeProgressingCondition)
	}
	return meta.SetStatusCondition(&np.Status.Conditions, condition)
}

// defaultReplicas is the hardcoded default for this POC.
const defaultReplicas = int32(1)
