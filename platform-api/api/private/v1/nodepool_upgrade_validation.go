package v1

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

func (p *NodePool) ValidateCreate(_ context.Context) error {
	if p.Spec.Release.UpgradeStartTime != nil {
		return fmt.Errorf("spec.release.upgradeStartTime may only be set after the NodePool has been installed")
	}
	return nil
}

// ValidateUpdate checks release intent changes without revalidating unrelated
// scaling or controller status writes. The persisted status supplies the lock.
func (p *NodePool) ValidateUpdate(ctx context.Context, oldObj runtime.Object) error {
	old, ok := oldObj.(*NodePool)
	if !ok {
		return fmt.Errorf("expected old object to be *NodePool, got %T", oldObj)
	}
	if apiequality.Semantic.DeepEqual(p.Spec.Release, old.Spec.Release) {
		return nil
	}
	if NodePoolUpgradeActive(old) {
		return fmt.Errorf("spec.release cannot be changed while an accepted upgrade is active")
	}
	if p.Spec.Release.UpgradeStartTime != nil && p.Spec.Release.UpgradeStartTime.IsZero() {
		return fmt.Errorf("spec.release.upgradeStartTime must be a valid timestamp")
	}
	if old.Status.Version == "" {
		if NodePoolVersionObservationRequired(old) {
			return fmt.Errorf("the observed NodePool version must be populated before changing spec.release")
		}
		if p.Spec.Release.UpgradeStartTime != nil {
			return fmt.Errorf("spec.release.upgradeStartTime may only be set after the NodePool has been installed")
		}
		return nil // Preserve initial installation and existing configuration updates.
	}
	if p.Spec.Release.Version == old.Status.Version {
		if p.Spec.Release.UpgradeStartTime != nil && !apiequality.Semantic.DeepEqual(p.Spec.Release.UpgradeStartTime, old.Spec.Release.UpgradeStartTime) {
			return fmt.Errorf("spec.release.upgradeStartTime requires a version upgrade")
		}
		return nil // Returning to the observed version cancels an unaccepted request.
	}
	if _, err := utilversion.ParseSemantic(p.Spec.Release.Version); err != nil {
		return fmt.Errorf("spec.release.version must be a semantic version")
	}
	if validate := getValidatorDeps().ValidateNodePoolUpgrade; validate != nil {
		candidate := p.DeepCopy()
		candidate.Status = old.Status
		return validate(ctx, candidate, p.Spec.Release.Version)
	}
	return nil
}

func (p *NodePool) ValidateDelete(_ context.Context) error { return nil }

// These conditions describe coordinator approval and dispatch without a separate
// upgrade request or controller bookkeeping object in the NodePool API.
const (
	NodePoolUpgradeScheduled   = "NodePoolUpgradeScheduled"
	NodePoolUpgradeProgressing = "NodePoolUpgradeProgressing"
	NodePoolUpgradeDegraded    = "NodePoolUpgradeDegraded"
	NodePoolUpgradeSubmitted   = "NodePoolUpgradeSubmitted"
)

// NodePoolReleaseApproved permits initial installation and maintenance of the
// observed release. A new release requires persisted coordinator approval.
func NodePoolReleaseApproved(pool *NodePool) bool {
	return (pool.Status.Version == "" && !NodePoolVersionObservationRequired(pool)) ||
		pool.Spec.Release.Version == pool.Status.Version || NodePoolUpgradeActive(pool)
}

// NodePoolVersionObservationRequired identifies a previously submitted pool that
// needs its observed version backfilled. Healthy feedback or successful resource
// submission distinguishes it from initial installation before the first apply.
func NodePoolVersionObservationRequired(pool *NodePool) bool {
	return pool.Status.Version == "" &&
		(meta.FindStatusCondition(pool.Status.Conditions, "NodePoolHealthy") != nil ||
			meta.IsStatusConditionTrue(pool.Status.Conditions, "NodePoolResourcesApplied"))
}

// NodePoolUpgradeActive is also the admission lock on release settings. It must
// survive unrelated generation changes (for example scaling). Admission forbids
// release changes while this condition is true, and approval status writes use
// resource-version preconditions, so an old approval cannot authorize a changed
// release. Health/completion observations still require the current generation.
func NodePoolUpgradeActive(pool *NodePool) bool {
	return meta.IsStatusConditionTrue(pool.Status.Conditions, NodePoolUpgradeProgressing)
}

// NodePoolUpgradeSubmissionStarted treats Unknown conservatively: the process
// may have stopped after dispatch but before it recorded acknowledgement.
func NodePoolUpgradeSubmissionStarted(pool *NodePool) bool {
	condition := meta.FindStatusCondition(pool.Status.Conditions, NodePoolUpgradeSubmitted)
	return condition != nil && condition.Status != metav1.ConditionFalse
}

// ValidateNodePoolUpgradeTarget validates an upgrade using only mirrored platform data.
func ValidateNodePoolUpgradeTarget(pool *NodePool, cluster *Cluster, release *Version, targetVersion string) error {
	if !pool.DeletionTimestamp.IsZero() || !cluster.DeletionTimestamp.IsZero() {
		return fmt.Errorf("the NodePool or Cluster is being deleted")
	}
	if pool.Status.Version == "" || cluster.Status.HostedClusterResult == nil || cluster.Status.HostedClusterResult.Version == "" {
		return fmt.Errorf("nodepool and control-plane versions must be observed before requesting an upgrade")
	}
	current, err := utilversion.ParseSemantic(pool.Status.Version)
	if err != nil {
		return fmt.Errorf("the observed NodePool version is invalid")
	}
	target, err := utilversion.ParseSemantic(targetVersion)
	if err != nil {
		return fmt.Errorf("the target must be a semantic version")
	}
	controlPlane, err := utilversion.ParseSemantic(cluster.Status.HostedClusterResult.Version)
	if err != nil {
		return fmt.Errorf("the observed control-plane version is invalid")
	}
	if !current.LessThan(target) {
		return fmt.Errorf("the target must be newer than the observed NodePool version")
	}
	if controlPlane.LessThan(target) {
		return fmt.Errorf("the target must not be newer than the observed control-plane version")
	}
	if target.Major() != controlPlane.Major() || controlPlane.Minor()-target.Minor() > 3 {
		return fmt.Errorf("the target must be within three minor versions of the control plane")
	}
	if !ValidNodePoolUpgradeStep(current, target, pool.Spec.Release.ChannelGroup) {
		return fmt.Errorf("the target must be a z-stream or next-minor upgrade, except an EUS-to-EUS step")
	}
	for _, group := range release.Spec.ChannelGroups {
		if group == pool.Spec.Release.ChannelGroup {
			return nil
		}
	}
	return fmt.Errorf("the target version is not available in the NodePool channel group")
}

// ValidNodePoolUpgradeStep permits sequential minor upgrades and EUS-to-EUS steps.
func ValidNodePoolUpgradeStep(current, target *utilversion.Version, channelGroup string) bool {
	if current.Major() != target.Major() {
		return false
	}
	return target.Minor() == current.Minor() || target.Minor() == current.Minor()+1 ||
		(channelGroup == "eus" && current.Minor()%2 == 0 && target.Minor() == current.Minor()+2)
}
