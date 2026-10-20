// Package nodepoolupgrade reconciles one-time, customer-requested NodePool upgrades.
package nodepoolupgrade

import (
	"context"
	"fmt"
	"time"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilversion "k8s.io/apimachinery/pkg/util/version"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// The marker and NodePool release version are patched together. A restarted
	// controller can therefore identify the request that submitted the target.
	requestAnnotation   = "private.gcp.managed.openshift.io/nodepool-upgrade-request"
	waitInterval        = 30 * time.Second
	retryInterval       = time.Second
	maxScheduleWait     = time.Hour
	defaultChannelGroup = "candidate"
)

const (
	conditionScheduled   = "Scheduled"
	conditionAccepted    = "Accepted"
	conditionProgressing = "Progressing"
	conditionCompleted   = "Completed"
	conditionFailed      = "Failed"
)

// Reconciler owns scheduling, validation, and status for NodePoolUpgradeRequest.
// The existing NodePool controllers own release resolution and rollout.
type Reconciler struct {
	client client.Client
	now    func() time.Time
}

// NewReconciler creates a NodePool upgrade request reconciler.
func NewReconciler(c client.Client) *Reconciler {
	return &Reconciler{client: c, now: time.Now}
}

// Reconcile evaluates a persisted request. Pending requests are listed again
// after controller restarts; RequeueAfter is only a timer, not the source of truth.
func (r *Reconciler) Reconcile(ctx context.Context, key reconcile.Request) (reconcile.Result, error) {
	var request privatev1.NodePoolUpgradeRequest
	if err := r.client.Get(ctx, key.NamespacedName, &request); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("get NodePool upgrade request: %w", err)
	}
	if !request.DeletionTimestamp.IsZero() || isTerminal(&request) {
		return reconcile.Result{}, nil
	}

	now := r.now().UTC()
	if request.Status.AcceptedTime == nil && request.Spec.StartTime != nil && now.Before(request.Spec.StartTime.Time) {
		changed := setCondition(&request, conditionScheduled, metav1.ConditionTrue,
			"WaitingForStartTime", fmt.Sprintf("Upgrade may start at %s", request.Spec.StartTime.UTC().Format(time.RFC3339)))
		if err := r.writeStatus(ctx, &request, changed); err != nil {
			return reconcile.Result{}, err
		}
		remaining := request.Spec.StartTime.Sub(now)
		if remaining > maxScheduleWait {
			remaining = maxScheduleWait
		}
		return reconcile.Result{RequeueAfter: remaining}, nil
	}

	var pool privatev1.NodePool
	poolKey := types.NamespacedName{Namespace: request.Namespace, Name: request.Spec.NodePoolID}
	if err := r.client.Get(ctx, poolKey, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			return r.reject(ctx, &request, "NodePoolNotFound", "Referenced NodePool does not exist")
		}
		return reconcile.Result{}, fmt.Errorf("get NodePool: %w", err)
	}
	if pool.Spec.ClusterID != request.Spec.ClusterID {
		return r.reject(ctx, &request, "ClusterMismatch", "NodePool does not belong to the referenced Cluster")
	}

	var cluster privatev1.Cluster
	clusterKey := types.NamespacedName{Namespace: request.Namespace, Name: request.Spec.ClusterID}
	if err := r.client.Get(ctx, clusterKey, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.reject(ctx, &request, "ClusterNotFound", "Referenced Cluster does not exist")
		}
		return reconcile.Result{}, fmt.Errorf("get Cluster: %w", err)
	}

	if request.Status.AcceptedTime == nil {
		return r.acceptOrWait(ctx, &request, &pool, &cluster, now)
	}
	return r.reconcileAccepted(ctx, &request, &pool, now)
}

func (r *Reconciler) acceptOrWait(ctx context.Context, request *privatev1.NodePoolUpgradeRequest,
	pool *privatev1.NodePool, cluster *privatev1.Cluster, now time.Time) (reconcile.Result, error) {
	if pool.Status.CurrentVersion == "" || cluster.Status.HostedClusterResult == nil ||
		cluster.Status.HostedClusterResult.Version == "" {
		return r.wait(ctx, request, "WaitingForVersionFeedback",
			"Waiting for observed NodePool and control-plane versions")
	}
	if pool.Spec.Release.Version != pool.Status.CurrentVersion {
		return r.wait(ctx, request, "NodePoolUpgradeInProgress",
			"Waiting for the existing NodePool upgrade to finish")
	}

	current, err := utilversion.ParseSemantic(pool.Status.CurrentVersion)
	if err != nil {
		return r.wait(ctx, request, "InvalidObservedVersion", "Waiting for valid NodePool version feedback")
	}
	target, err := utilversion.ParseSemantic(request.Spec.TargetVersion)
	if err != nil {
		return r.reject(ctx, request, "InvalidTargetVersion", "Target must be a semantic OpenShift version")
	}
	controlPlane, err := utilversion.ParseSemantic(cluster.Status.HostedClusterResult.Version)
	if err != nil {
		return r.wait(ctx, request, "InvalidControlPlaneVersion", "Waiting for valid control-plane version feedback")
	}
	if !current.LessThan(target) {
		return r.reject(ctx, request, "TargetNotNewer", "Target must be newer than the completed NodePool version")
	}
	if controlPlane.LessThan(target) {
		return r.wait(ctx, request, "WaitingForControlPlane",
			"Target is newer than the completed control-plane version")
	}
	channelGroup := pool.Spec.Release.ChannelGroup
	if channelGroup == "" {
		channelGroup = defaultChannelGroup
	}
	if !validMinorStep(current, target, channelGroup) {
		return r.reject(ctx, request, "UnsupportedVersionStep",
			"Target must be a z-stream or next-minor upgrade, except an EUS-to-EUS step")
	}

	var release privatev1.Version
	if err := r.client.Get(ctx, types.NamespacedName{Name: request.Spec.TargetVersion}, &release); err != nil {
		if apierrors.IsNotFound(err) {
			return r.wait(ctx, request, "VersionNotAvailable", "Waiting for the target in the platform version catalog")
		}
		return reconcile.Result{}, fmt.Errorf("get target Version: %w", err)
	}
	if !contains(release.Spec.ChannelGroups, channelGroup) {
		return r.reject(ctx, request, "VersionOutsideChannel",
			"Target version is not available in the NodePool channel group")
	}

	accepted := metav1.NewTime(now)
	request.Status.AcceptedTime = &accepted
	changed := setCondition(request, conditionScheduled, metav1.ConditionFalse, "StartTimeReached", "Upgrade is eligible to start")
	changed = setCondition(request, conditionAccepted, metav1.ConditionTrue, "TargetAccepted", "Target passed the NodePool upgrade checks") || changed
	if err := r.writeStatus(ctx, request, changed || request.Status.AcceptedTime != nil); err != nil {
		return reconcile.Result{}, err
	}
	// Persist acceptance before changing the NodePool. Admission disallows
	// deleting an accepted request, and restart recovery can finish the patch.
	return reconcile.Result{RequeueAfter: retryInterval}, nil
}

func (r *Reconciler) reconcileAccepted(ctx context.Context, request *privatev1.NodePoolUpgradeRequest,
	pool *privatev1.NodePool, now time.Time) (reconcile.Result, error) {
	marker := pool.Annotations[requestAnnotation]
	uid := string(request.UID)
	if marker != "" && marker != uid && pool.Spec.Release.Version != pool.Status.CurrentVersion {
		return r.fail(ctx, request, "ConcurrentUpgrade", "Another NodePool upgrade owns the active target")
	}
	if pool.Spec.Release.Version != request.Spec.TargetVersion || marker != uid {
		if pool.Spec.Release.Version != pool.Status.CurrentVersion {
			return r.fail(ctx, request, "TargetChanged", "NodePool target changed after this request was accepted")
		}
		before := pool.DeepCopy()
		pool.Spec.Release.Version = request.Spec.TargetVersion
		if pool.Annotations == nil {
			pool.Annotations = map[string]string{}
		}
		pool.Annotations[requestAnnotation] = uid
		if err := r.client.Patch(ctx, pool, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{RequeueAfter: retryInterval}, nil
			}
			return reconcile.Result{}, fmt.Errorf("request NodePool upgrade: %w", err)
		}
	}

	changed := false
	if request.Status.StartedTime == nil {
		started := metav1.NewTime(now)
		request.Status.StartedTime = &started
		changed = true
	}
	if pool.Status.CurrentVersion == request.Spec.TargetVersion &&
		meta.IsStatusConditionTrue(pool.Status.Conditions, "NodePoolAvailable") &&
		meta.IsStatusConditionTrue(pool.Status.Conditions, "NodePoolHealthy") &&
		meta.IsStatusConditionFalse(pool.Status.Conditions, "NodePoolProgressing") {
		completed := metav1.NewTime(now)
		request.Status.CompletedTime = &completed
		changed = setCondition(request, conditionProgressing, metav1.ConditionFalse, "UpgradeCompleted", "NodePool rollout completed") || changed
		changed = setCondition(request, conditionCompleted, metav1.ConditionTrue, "UpgradeCompleted", "NodePool reached the requested version") || changed
		if err := r.writeStatus(ctx, request, changed); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	changed = setCondition(request, conditionProgressing, metav1.ConditionTrue, "WaitingForNodePool",
		"Waiting for HyperShift to report the requested NodePool version") || changed
	if err := r.writeStatus(ctx, request, changed); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: waitInterval}, nil
}

func (r *Reconciler) wait(ctx context.Context, request *privatev1.NodePoolUpgradeRequest,
	reason, message string) (reconcile.Result, error) {
	changed := setCondition(request, conditionAccepted, metav1.ConditionUnknown, reason, message)
	if err := r.writeStatus(ctx, request, changed); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: waitInterval}, nil
}

func (r *Reconciler) reject(ctx context.Context, request *privatev1.NodePoolUpgradeRequest,
	reason, message string) (reconcile.Result, error) {
	changed := setCondition(request, conditionAccepted, metav1.ConditionFalse, reason, message)
	if err := r.writeStatus(ctx, request, changed); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) fail(ctx context.Context, request *privatev1.NodePoolUpgradeRequest,
	reason, message string) (reconcile.Result, error) {
	changed := setCondition(request, conditionProgressing, metav1.ConditionFalse, reason, message)
	changed = setCondition(request, conditionFailed, metav1.ConditionTrue, reason, message) || changed
	if err := r.writeStatus(ctx, request, changed); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) writeStatus(ctx context.Context, request *privatev1.NodePoolUpgradeRequest, changed bool) error {
	if !changed {
		return nil
	}
	if err := r.client.Status().Update(ctx, request); err != nil {
		return fmt.Errorf("update NodePool upgrade request status: %w", err)
	}
	return nil
}

func setCondition(request *privatev1.NodePoolUpgradeRequest, typ string,
	status metav1.ConditionStatus, reason, message string) bool {
	return meta.SetStatusCondition(&request.Status.Conditions, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message,
		ObservedGeneration: request.Generation,
	})
}

func isTerminal(request *privatev1.NodePoolUpgradeRequest) bool {
	return meta.IsStatusConditionTrue(request.Status.Conditions, conditionCompleted) ||
		meta.IsStatusConditionTrue(request.Status.Conditions, conditionFailed) ||
		meta.IsStatusConditionFalse(request.Status.Conditions, conditionAccepted)
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func validMinorStep(current, target *utilversion.Version, channelGroup string) bool {
	if current.Major() != target.Major() {
		return false
	}
	if target.Minor() == current.Minor() || target.Minor() == current.Minor()+1 {
		return true
	}
	return channelGroup == "eus" && current.Minor()%2 == 0 && target.Minor() == current.Minor()+2
}
