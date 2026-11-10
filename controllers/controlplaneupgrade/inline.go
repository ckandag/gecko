package controlplaneupgrade

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/openshift-online/gecko/controllers/util/catalog"
	channelutil "github.com/openshift-online/gecko/controllers/util/channel"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func releaseGroup(c *privatev1.Cluster) string {
	if c.Spec.Release.ChannelGroup == "" {
		return channelutil.DefaultChannelGroup
	}
	return c.Spec.Release.ChannelGroup
}

// normalizeMinor pins the newest eligible concrete target before approval. Once
// written to spec it cannot drift merely because a newer catalog entry appears.
func (r *Reconciler) normalizeMinor(ctx context.Context, c *privatev1.Cluster) (reconcile.Result, error) {
	var releases privatev1.VersionList
	if err := r.apiClient.List(ctx, &releases); err != nil {
		return reconcile.Result{}, fmt.Errorf("list release catalog: %w", err)
	}
	var selected string
	var newest *utilversion.Version
	for _, release := range releases.Items {
		if !privatev1.ReleaseMatches(c.Spec.Release.Version, release.Name) || !slices.Contains(release.Spec.ChannelGroups, releaseGroup(c)) {
			continue
		}
		if hc := c.Status.HostedClusterResult; hc != nil && hc.Version != "" {
			if release.Name != hc.Version && !isAdvertised(release.Name, hc.AvailableUpdates) {
				continue
			}
			if err := privatev1.ValidateClusterUpgradeTarget(c, release.Name); release.Name != hc.Version && err != nil {
				continue
			}
		}
		v, err := utilversion.ParseSemantic(release.Name)
		if err == nil && (newest == nil || newest.LessThan(v)) {
			selected, newest = release.Name, v
		}
	}
	if selected == "" {
		return r.block(ctx, c, "TargetNotAvailable", "No eligible release in the requested minor and channel", false)
	}
	c.Spec.Release.Version = selected
	if err := r.apiClient.Update(ctx, c); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{RequeueAfter: requeueConflict}, nil
		}
		return reconcile.Result{}, fmt.Errorf("normalize desired release: %w", err)
	}
	return reconcile.Result{RequeueAfter: requeueConflict}, nil
}

// finishAutomaticPreparation copies only the durably approved automatic target
// into spec. A second status write binds installation approval to the normalized
// generation. A crash or conflict between either write keeps installers gated.
func (r *Reconciler) finishAutomaticPreparation(ctx context.Context, c *privatev1.Cluster) (reconcile.Result, error) {
	result := c.Status.ControlPlaneUpgrade
	if c.Spec.Release.Version != result.TargetVersion {
		c.Spec.Release.Version = result.TargetVersion
		if err := r.apiClient.Update(ctx, c); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{RequeueAfter: requeueConflict}, nil
			}
			return reconcile.Result{}, fmt.Errorf("normalize approved automatic release: %w", err)
		}
	}
	return r.writeStatus(ctx, c, result, requeueObserving,
		newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionTrue, "UpgradeAccepted", "The automatic release is approved for rollout"),
		newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeInProgress", "The automatic release is approved for rollout"),
		newCondition(conditionProgressing, metav1.ConditionTrue, "UpgradeAccepted", "Waiting for the HostedCluster to apply the approved release"),
		newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
}

func (r *Reconciler) approveUpgrade(ctx context.Context, c *privatev1.Cluster, target, source string) (reconcile.Result, error) {
	if err := privatev1.ValidateClusterUpgradeTarget(c, target); err != nil {
		return r.block(ctx, c, "TargetNotAvailable", err.Error(), true)
	}
	if _, err := catalog.Resolve(ctx, r.apiClient, target, releaseGroup(c)); err != nil {
		var unavailable *catalog.UnavailableError
		if !errors.As(err, &unavailable) {
			return reconcile.Result{}, err
		}
		if unavailable.Reason == "VersionNotFoundInCatalog" {
			return r.block(ctx, c, "WaitingForVersionCatalog", "The advertised target is waiting for catalog synchronization", false)
		}
		return r.block(ctx, c, unavailable.Reason, unavailable.Message, true)
	}
	current, _ := utilversion.ParseSemantic(c.Status.HostedClusterResult.Version)
	selected, _ := utilversion.ParseSemantic(target)
	isYStream := current.Major() != selected.Major() || current.Minor() != selected.Minor()
	if ok, reason, message := healthOK(c.Status.Conditions, c.Status.HostedClusterResult.ObservedConditions, isYStream); !ok {
		return r.block(ctx, c, reason, message, false)
	}
	if source == targetSourceAutomatic {
		if override, _, _ := overrideCondition(c, target, c.Status.HostedClusterResult.Version); !override {
			if permitted, reason, message, _ := maintenancePermitted(c, r.now()); !permitted {
				return r.block(ctx, c, reason, message, false)
			}
		}
	}
	if r.admission != nil {
		allowed, err := r.admission(ctx, c, target)
		if err != nil {
			return reconcile.Result{}, err
		}
		if !allowed {
			return r.block(ctx, c, "WaitingForUpgradeCapacity", "The desired upgrade is waiting for rollout capacity", false)
		}
	}
	result := recordActiveTarget(c.Status.ControlPlaneUpgrade, target)
	if result.TargetSource != source || result.CompletedTime != nil {
		now := metav1.NewTime(r.now())
		result.RequestedTime = &now
		result.CompletedTime = nil
	}
	result.TargetSource = source
	availableMessage := "The desired release is approved for rollout"
	reason, availableReason, availableStatus, retry := "UpgradeAccepted", "UpgradeInProgress", metav1.ConditionFalse, requeueObserving
	if source == targetSourceAutomatic {
		reason, availableReason, availableStatus, retry = privatev1.ControlPlaneUpgradePreparingReason, "UpdateSelected", metav1.ConditionTrue, requeueConflict
		if override, overrideReason, overrideMessage := overrideCondition(c, target, c.Status.HostedClusterResult.Version); override {
			availableReason, availableMessage = overrideReason, overrideMessage
		}
	}
	// Persist approval and its exact source/target in one optimistic status write.
	return r.writeStatus(ctx, c, result, retry,
		newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionTrue, reason, "The desired release is approved for rollout"),
		newCondition(conditionAvailable, availableStatus, availableReason, availableMessage),
		newCondition(conditionProgressing, metav1.ConditionTrue, reason, "Waiting for the HostedCluster to apply the approved release"),
		newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
}

func (r *Reconciler) block(ctx context.Context, c *privatev1.Cluster, reason, message string, degraded bool) (reconcile.Result, error) {
	degradedStatus, availableStatus := metav1.ConditionFalse, metav1.ConditionTrue
	if degraded {
		degradedStatus = metav1.ConditionTrue
	}
	if degraded || reason == "TargetNotAvailable" || reason == "WaitingForVersionCatalog" {
		availableStatus = metav1.ConditionFalse
	}
	return r.writeStatus(ctx, c, c.Status.ControlPlaneUpgrade, requeuePending,
		newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionFalse, reason, message),
		newCondition(conditionAvailable, availableStatus, reason, message),
		newCondition(conditionProgressing, metav1.ConditionFalse, reason, message),
		newCondition(conditionDegraded, degradedStatus, reason, message))
}

func feedbackTarget(completed, desired string) (bool, string) {
	if desired == "" || desired == completed {
		return false, ""
	}
	current, a := utilversion.ParseSemantic(completed)
	target, b := utilversion.ParseSemantic(desired)
	return a != nil || b != nil || current.LessThan(target), desired
}

func (r *Reconciler) observeUpgrade(ctx context.Context, c *privatev1.Cluster) (reconcile.Result, error) {
	result := c.Status.ControlPlaneUpgrade.DeepCopy()
	if result == nil || result.TargetVersion != c.Spec.Release.Version {
		return reconcile.Result{}, fmt.Errorf("approved release does not match persisted upgrade target")
	}
	hc := c.Status.HostedClusterResult
	if hc.Version != result.TargetVersion && !privatev1.ClusterUpgradeSubmissionStarted(c) {
		for _, typ := range []string{"VersionResolved", "ResourcesApplied"} {
			condition := meta.FindStatusCondition(c.Status.Conditions, typ)
			if condition != nil && condition.Status == metav1.ConditionFalse && condition.ObservedGeneration == c.Generation &&
				(typ == "VersionResolved" || condition.Reason == "ManifestBuildFailed") {
				if result.TargetSource == targetSourceAutomatic {
					return r.cancelAutomaticPreparation(ctx, c)
				}
				return r.block(ctx, c, "PreparationFailed", "The release could not be prepared before dispatch; release settings can be corrected", true)
			}
		}
	}
	healthy, _, _ := healthOK(c.Status.Conditions, hc.ObservedConditions, false)
	if hc.Version == result.TargetVersion && healthy && !meta.IsStatusConditionTrue(hc.ObservedConditions, "ClusterVersionProgressing") {
		result = recordCompletion(result, hc.Version)
		completed := metav1.NewTime(r.now())
		result.CompletedTime = &completed
		return r.writeStatus(ctx, c, result, requeuePending,
			newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionFalse, "UpgradeCompleted", "The control plane reached the approved version and is healthy"),
			newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeCompleted", "The control plane reached the approved version"),
			newCondition(conditionProgressing, metav1.ConditionFalse, "UpgradeCompleted", "The control plane reached the approved version and is healthy"),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
	}
	if reason, message, failed := upgradeFailureReason(hc.ObservedConditions); failed {
		result.FailureReason, result.FailureMessage = reason, message
		return r.writeStatus(ctx, c, result, requeueStable,
			newCondition(conditionProgressing, metav1.ConditionFalse, reason, message),
			newCondition(conditionDegraded, metav1.ConditionTrue, reason, message))
	}
	return r.writeStatus(ctx, c, result, requeueObserving,
		newCondition(conditionProgressing, metav1.ConditionTrue, "HostedClusterProgressing", "Waiting for the approved version and a healthy control plane"))
}

// observeExisting adopts a rollout already present in HyperShift feedback. It
// records the lock; HostedCluster feedback is evidence of existing dispatch.
func (r *Reconciler) observeExisting(ctx context.Context, c *privatev1.Cluster, target string) (reconcile.Result, error) {
	result := recordActiveTarget(c.Status.ControlPlaneUpgrade, target)
	progressing, degraded := metav1.ConditionTrue, metav1.ConditionFalse
	reason, message, retry := "HostedClusterProgressing", "Waiting for the active control-plane upgrade", requeueObserving
	if failure, detail, failed := upgradeFailureReason(c.Status.HostedClusterResult.ObservedConditions); failed {
		reason, message, progressing, degraded, retry = failure, detail, metav1.ConditionFalse, metav1.ConditionTrue, requeueStable
		result.FailureReason, result.FailureMessage = failure, detail
	}
	accepted, approvalReason := metav1.ConditionTrue, "UpgradeAccepted"
	if c.Spec.Release.Version != target {
		accepted, approvalReason = metav1.ConditionFalse, "WaitingForCurrentUpgrade"
	}

	return r.writeStatus(ctx, c, result, retry,
		newCondition(privatev1.ControlPlaneUpgradeAccepted, accepted, approvalReason, "An existing control-plane rollout is active"),
		newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeInProgress", message),
		newCondition(conditionProgressing, progressing, reason, message),
		newCondition(conditionDegraded, degraded, reason, message))
}

// eligibleUpdates intersects HyperShift upgrade edges with the selected channel
// in the mirrored catalog. A catalog listing alone never authorizes an edge.
func (r *Reconciler) eligibleUpdates(ctx context.Context, c *privatev1.Cluster) ([]string, error) {
	var eligible []string
	for _, candidate := range c.Status.HostedClusterResult.AvailableUpdates {
		if _, err := catalog.Resolve(ctx, r.apiClient, candidate, releaseGroup(c)); err != nil {
			var unavailable *catalog.UnavailableError
			if errors.As(err, &unavailable) {
				continue
			}
			return nil, err
		}
		eligible = append(eligible, candidate)
	}
	return eligible, nil
}

// cancelAutomaticPreparation keeps ownership until the unsubmitted automatic
// target is removed from spec. Otherwise an unrelated spec repair could make a
// later retry look like a new customer request and bypass maintenance.
func (r *Reconciler) cancelAutomaticPreparation(ctx context.Context, c *privatev1.Cluster) (reconcile.Result, error) {
	result := c.Status.ControlPlaneUpgrade.DeepCopy()
	result.FailureReason = "PreparationFailed"
	result.FailureMessage = "The automatic release could not be prepared before dispatch"
	return r.writeStatus(ctx, c, result, requeueConflict,
		newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionTrue, privatev1.ControlPlaneUpgradeCancelingReason, "Restoring the observed release before unlocking automatic intent"),
		newCondition(conditionProgressing, metav1.ConditionFalse, "PreparationFailed", result.FailureMessage),
		newCondition(conditionDegraded, metav1.ConditionTrue, "PreparationFailed", result.FailureMessage))
}

func (r *Reconciler) finishAutomaticCancellation(ctx context.Context, c *privatev1.Cluster) (reconcile.Result, error) {
	hc := c.Status.HostedClusterResult
	if privatev1.ClusterUpgradeSubmissionStarted(c) || hc.Version == c.Status.ControlPlaneUpgrade.TargetVersion {
		// Dispatch evidence wins over a stale preparation failure. Restore the
		// original approved target rather than attempting any rollback.
		return r.writeStatus(ctx, c, c.Status.ControlPlaneUpgrade, requeueConflict,
			newCondition(privatev1.ControlPlaneUpgradeAccepted, metav1.ConditionTrue, privatev1.ControlPlaneUpgradePreparingReason, "Dispatch evidence requires continuing the approved automatic release"))
	}
	if c.Spec.Release.Version != hc.Version {
		c.Spec.Release.Version = hc.Version
		if err := r.apiClient.Update(ctx, c); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{RequeueAfter: requeueConflict}, nil
			}
			return reconcile.Result{}, fmt.Errorf("restore unsubmitted automatic release: %w", err)
		}
	}
	return r.block(ctx, c, "PreparationFailed", "The unsubmitted automatic upgrade failed; release settings can be corrected", true)
}
