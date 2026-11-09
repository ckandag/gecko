package v1

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

const (
	ControlPlaneUpgradeAccepted  = "ControlPlaneUpgradeAccepted"
	ControlPlaneUpgradeSubmitted = "ControlPlaneUpgradeSubmitted"
)

var minorReleasePattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// IsMinorRelease identifies a customer-selected major.minor stream.
func IsMinorRelease(version string) bool { return minorReleasePattern.MatchString(version) }

// ReleaseMatches reports whether a concrete version satisfies the requested
// stream or exact version. A stream does not authorize arbitrary upgrade edges.
func ReleaseMatches(requested, concrete string) bool {
	if !IsMinorRelease(requested) {
		return requested == concrete
	}
	version, err := utilversion.ParseSemantic(concrete)
	return err == nil && requested == fmt.Sprintf("%d.%d", version.Major(), version.Minor())
}

// ClusterUpgradeActive is the admission lock. Approval survives unrelated
// generation changes because admission forbids changing release settings while
// accepted. HostedCluster feedback also protects upgrades started before this
// approval protocol was deployed.
func ClusterUpgradeActive(cluster *Cluster) bool {
	if meta.IsStatusConditionTrue(cluster.Status.Conditions, ControlPlaneUpgradeAccepted) {
		return true
	}
	hc := cluster.Status.HostedClusterResult
	if hc == nil || hc.Version == "" || hc.DesiredVersion == "" {
		return false
	}
	current, currentErr := utilversion.ParseSemantic(hc.Version)
	desired, desiredErr := utilversion.ParseSemantic(hc.DesiredVersion)
	return currentErr != nil || desiredErr != nil || current.LessThan(desired)
}

// ClusterReleaseApproved permits initial installation, continued reconciliation
// of the observed release, or the exact target approved by the upgrade controller.
func ClusterReleaseApproved(cluster *Cluster) bool {
	if IsMinorRelease(cluster.Spec.Release.Version) {
		return false
	}
	hc := cluster.Status.HostedClusterResult
	if hc == nil || hc.Version == "" {
		return true
	}
	if cluster.Spec.Release.Version == hc.Version {
		return true
	}
	result := cluster.Status.ControlPlaneUpgrade
	return meta.IsStatusConditionTrue(cluster.Status.Conditions, ControlPlaneUpgradeAccepted) &&
		result != nil && result.TargetVersion == cluster.Spec.Release.Version
}

// ClusterInstallationVersion keeps pending customer intent out of manifests.
func ClusterInstallationVersion(cluster *Cluster) string {
	// Preserve an already dispatched target even if older customer intent differs.
	hc := cluster.Status.HostedClusterResult
	if hc != nil {
		current, a := utilversion.ParseSemantic(hc.Version)
		desired, b := utilversion.ParseSemantic(hc.DesiredVersion)
		if a == nil && b == nil && current.LessThan(desired) {
			return hc.DesiredVersion
		}
	}
	if ClusterReleaseApproved(cluster) {
		return cluster.Spec.Release.Version
	}
	if hc != nil {
		return hc.Version
	}

	return ""
}

// ClusterUpgradeSubmissionStarted treats an uncertain dispatch conservatively.
func ClusterUpgradeSubmissionStarted(cluster *Cluster) bool {
	// Feedback is authoritative for an already dispatched legacy rollout.
	if hc := cluster.Status.HostedClusterResult; hc != nil {
		current, a := utilversion.ParseSemantic(hc.Version)
		desired, b := utilversion.ParseSemantic(hc.DesiredVersion)
		if a == nil && b == nil && current.LessThan(desired) {
			return true
		}
	}
	submitted := meta.FindStatusCondition(cluster.Status.Conditions, ControlPlaneUpgradeSubmitted)
	accepted := meta.FindStatusCondition(cluster.Status.Conditions, ControlPlaneUpgradeAccepted)
	// Approval and dispatch refer to the same immutable release configuration.
	// Unrelated generation changes do not invalidate a persisted boundary.
	return submitted != nil && accepted != nil && submitted.ObservedGeneration >= accepted.ObservedGeneration && submitted.Status != "False"
}

func (c *Cluster) validateReleaseUpdate(old *Cluster) error {
	if equality.Semantic.DeepEqual(c.Spec.Release, old.Spec.Release) {
		return nil
	}
	if ClusterUpgradeActive(old) {
		return fmt.Errorf("spec.release cannot be changed while a control-plane upgrade is active")
	}
	hc := old.Status.HostedClusterResult
	if hc == nil || hc.Version == "" {
		if meta.IsStatusConditionTrue(old.Status.Conditions, "ResourcesApplied") {
			return fmt.Errorf("wait for the observed control-plane version before changing spec.release")
		}
		return nil
	}
	if c.Spec.Release.Version == hc.Version {
		return nil
	} // Cancel pending intent.
	if c.Spec.Release.Version == old.Spec.Release.Version {
		return nil
	}
	return ValidateClusterUpgradeTarget(old, c.Spec.Release.Version)
}

// ValidateClusterUpgradeTarget uses stored HostedCluster update edges only.
// Catalog/channel eligibility is checked again before normalization and approval.
func ValidateClusterUpgradeTarget(cluster *Cluster, requested string) error {
	hc := cluster.Status.HostedClusterResult
	if hc == nil || hc.Version == "" {
		return fmt.Errorf("the control-plane version must be observed before requesting an upgrade")
	}
	if IsMinorRelease(requested) && ReleaseMatches(requested, hc.Version) {
		return nil
	}
	current, err := utilversion.ParseSemantic(hc.Version)
	if err != nil {
		return fmt.Errorf("the observed control-plane version is invalid")
	}
	for _, candidate := range hc.AvailableUpdates {
		if !ReleaseMatches(requested, candidate) {
			continue
		}
		target, err := utilversion.ParseSemantic(candidate)
		if err == nil && current.LessThan(target) {
			return nil
		}
	}
	if IsMinorRelease(requested) {
		return fmt.Errorf("the requested minor has no advertised forward control-plane upgrade")
	}
	if _, err := utilversion.ParseSemantic(requested); err != nil || strings.HasPrefix(requested, "v") {
		return fmt.Errorf("spec.release.version must be major.minor or a complete semantic version without a leading v")
	}
	return fmt.Errorf("the requested version is not an advertised forward control-plane upgrade")
}
