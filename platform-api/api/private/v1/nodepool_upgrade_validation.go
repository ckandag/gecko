package v1

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// Default pins an omitted upgrade target to the control-plane version at submission.
func (p *NodePool) Default(ctx context.Context) error {
	if p.Spec.Upgrade == nil || p.Spec.Upgrade.TargetVersion != nil {
		return nil
	}
	if defaults := getValidatorDeps().DefaultNodePoolUpgrade; defaults != nil {
		return defaults(ctx, p)
	}
	return nil
}

func (p *NodePool) ValidateCreate(ctx context.Context) error {
	if p.Spec.Upgrade == nil {
		return nil
	}
	return fmt.Errorf("spec.upgrade may only be set after the NodePool has been installed")
}

// ValidateUpdate checks only changed upgrade settings and release transitions.
// Scaling and controller status updates do not repeat storage-backed admission.
func (p *NodePool) ValidateUpdate(ctx context.Context, oldObj runtime.Object) error {
	old, ok := oldObj.(*NodePool)
	if !ok {
		return fmt.Errorf("expected old object to be *NodePool, got %T", oldObj)
	}
	upgradeChanged := !apiequality.Semantic.DeepEqual(p.Spec.Upgrade, old.Spec.Upgrade)
	releaseChanged := p.Spec.Release.Version != old.Spec.Release.Version
	if NodePoolUpgradeActive(old.Status.Upgrade) {
		if upgradeChanged {
			return fmt.Errorf("spec.upgrade cannot be changed or canceled while an accepted upgrade is active")
		}
		if releaseChanged && p.Spec.Release.Version != old.Status.Upgrade.TargetVersion {
			return fmt.Errorf("spec.release.version must match the accepted upgrade target")
		}
	}
	if !upgradeChanged && !releaseChanged {
		return nil
	}
	if p.Spec.Upgrade != nil {
		if p.Spec.Upgrade.TargetVersion == nil {
			return fmt.Errorf("spec.upgrade.targetVersion must be set or defaulted from the observed control-plane version")
		}
		if _, err := utilversion.ParseSemantic(*p.Spec.Upgrade.TargetVersion); err != nil {
			return fmt.Errorf("spec.upgrade.targetVersion must be a semantic version")
		}
		if p.Spec.Upgrade.StartTime != nil && p.Spec.Upgrade.StartTime.IsZero() {
			return fmt.Errorf("spec.upgrade.startTime must be a valid timestamp")
		}
		if releaseChanged && !NodePoolUpgradeActive(old.Status.Upgrade) {
			return fmt.Errorf("spec.release.version cannot be changed while an upgrade is pending; change or cancel spec.upgrade first")
		}
	}
	// Removal cancels a pending intent; it must not require a healthy cluster.
	if !releaseChanged && p.Spec.Upgrade == nil {
		return nil
	}
	if old.Status.CurrentVersion == "" {
		if upgradeChanged && p.Spec.Upgrade != nil {
			return fmt.Errorf("the NodePool version must be observed before requesting an upgrade")
		}
		return nil // Preserve initial installation and existing configuration updates.
	}
	if old.Spec.Release.Version != old.Status.CurrentVersion && releaseChanged {
		return fmt.Errorf("a NodePool release change is already in progress")
	}
	target := p.Spec.Release.Version
	if upgradeChanged && p.Spec.Upgrade != nil {
		target = *p.Spec.Upgrade.TargetVersion
	}
	if validate := getValidatorDeps().ValidateNodePoolUpgrade; validate != nil {
		candidate := p.DeepCopy()
		candidate.Status = old.Status
		candidate.Spec.Release.Version = old.Spec.Release.Version
		return validate(ctx, candidate, target)
	}
	return nil
}

func (p *NodePool) ValidateDelete(_ context.Context) error { return nil }

// NodePoolUpgradeActive reports acceptance that has not completed or failed.
func NodePoolUpgradeActive(result *NodePoolUpgradeResult) bool {
	return result != nil && result.AcceptedTime != nil && result.CompletedTime == nil && result.FailureReason == nil
}

// ValidateNodePoolUpgradeTarget validates an upgrade using only mirrored platform data.
func ValidateNodePoolUpgradeTarget(pool *NodePool, cluster *Cluster, release *Version, targetVersion string) error {
	if !pool.DeletionTimestamp.IsZero() || !cluster.DeletionTimestamp.IsZero() {
		return fmt.Errorf("the NodePool or Cluster is being deleted")
	}
	if pool.Status.CurrentVersion == "" || cluster.Status.HostedClusterResult == nil || cluster.Status.HostedClusterResult.Version == "" {
		return fmt.Errorf("nodepool and control-plane versions must be observed before requesting an upgrade")
	}
	if pool.Spec.Release.Version != pool.Status.CurrentVersion || meta.IsStatusConditionTrue(pool.Status.Conditions, "NodePoolProgressing") {
		return fmt.Errorf("the NodePool already has a release change in progress")
	}
	current, err := utilversion.ParseSemantic(pool.Status.CurrentVersion)
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
