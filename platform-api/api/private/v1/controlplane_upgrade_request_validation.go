package v1

import (
	"context"
	"fmt"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// ValidateCreate rejects manual upgrade requests before they are persisted when
// the referenced Cluster is not ready, is already upgrading, or does not
// advertise the requested target.
func (r *ControlPlaneUpgradeRequest) ValidateCreate(ctx context.Context) error {
	if r.Spec.ClusterID == "" || r.Spec.TargetVersion == "" {
		return fmt.Errorf("spec.clusterID and spec.targetVersion are required")
	}

	parent, ok := types.ParentObjectFromContext(ctx)
	if !ok {
		// The aggregated private API does not provide the public router's parent
		// snapshot. Its callers are trusted; the controller repeats these checks
		// before acting. Public creates always validate the stored parent.
		return nil
	}
	var parentMap map[string]interface{}
	if stored, ok := parent.(*unstructured.Unstructured); ok {
		parentMap = stored.Object
	} else {
		var err error
		parentMap, err = runtime.DefaultUnstructuredConverter.ToUnstructured(parent)
		if err != nil {
			return fmt.Errorf("convert referenced Cluster: %w", err)
		}
	}
	var cluster Cluster
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(parentMap, &cluster); err != nil {
		return fmt.Errorf("decode referenced Cluster: %w", err)
	}
	if cluster.Name != r.Spec.ClusterID || cluster.Namespace != r.Namespace {
		return fmt.Errorf("referenced Cluster %q does not match request namespace and clusterID", r.Spec.ClusterID)
	}
	feedback := cluster.Status.HostedClusterResult
	if feedback == nil || feedback.Version == "" {
		return fmt.Errorf("Cluster %q has no observed HostedCluster version yet", r.Spec.ClusterID)
	}
	if cluster.Spec.Release.Version != feedback.Version ||
		(feedback.DesiredVersion != "" && feedback.DesiredVersion != feedback.Version) {
		return fmt.Errorf("Cluster %q already has a control-plane upgrade in progress", r.Spec.ClusterID)
	}
	current, err := utilversion.ParseSemantic(feedback.Version)
	if err != nil {
		return fmt.Errorf("parse observed control-plane version %q: %w", feedback.Version, err)
	}
	target, err := utilversion.ParseSemantic(r.Spec.TargetVersion)
	if err != nil || !current.LessThan(target) {
		return fmt.Errorf("version %q must be newer than observed control-plane version %q", r.Spec.TargetVersion, feedback.Version)
	}
	for _, available := range feedback.AvailableUpdates {
		if available == r.Spec.TargetVersion {
			return nil
		}
	}
	return fmt.Errorf("version %q is not an available control-plane upgrade for Cluster %q", r.Spec.TargetVersion, r.Spec.ClusterID)
}

// ValidateUpdate keeps a one-time request's target and parent immutable.
func (r *ControlPlaneUpgradeRequest) ValidateUpdate(_ context.Context, oldObj runtime.Object) error {
	oldRequest, ok := oldObj.(*ControlPlaneUpgradeRequest)
	if !ok {
		return fmt.Errorf("expected old object to be *ControlPlaneUpgradeRequest, got %T", oldObj)
	}
	if r.Spec != oldRequest.Spec {
		return fmt.Errorf("control-plane upgrade request spec is immutable")
	}
	return nil
}

func (r *ControlPlaneUpgradeRequest) ValidateDelete(_ context.Context) error { return nil }
