package v1

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// ValidateCreate checks the request shape. Version-path, Cluster relationship,
// and NodePool readiness checks run at execution time so scheduled requests
// are evaluated against current state.
func (r *NodePoolUpgradeRequest) ValidateCreate(_ context.Context) error {
	if r.Spec.ClusterID == "" || r.Spec.NodePoolID == "" || r.Spec.TargetVersion == "" {
		return fmt.Errorf("spec.clusterID, spec.nodePoolID, and spec.targetVersion are required")
	}
	if _, err := utilversion.ParseSemantic(r.Spec.TargetVersion); err != nil {
		return fmt.Errorf("spec.targetVersion %q is not a semantic version: %w", r.Spec.TargetVersion, err)
	}
	if r.Spec.StartTime != nil && r.Spec.StartTime.Time.IsZero() {
		return fmt.Errorf("spec.startTime must be a valid timestamp")
	}

	return nil
}

// ValidateUpdate preserves the one-time request contract.
func (r *NodePoolUpgradeRequest) ValidateUpdate(_ context.Context, oldObj runtime.Object) error {
	oldRequest, ok := oldObj.(*NodePoolUpgradeRequest)
	if !ok {
		return fmt.Errorf("expected old object to be *NodePoolUpgradeRequest, got %T", oldObj)
	}
	if !reflect.DeepEqual(r.Spec, oldRequest.Spec) {
		return fmt.Errorf("nodepool upgrade request spec is immutable")
	}
	return nil
}

// ValidateDelete permits cancellation before the controller starts an upgrade.
func (r *NodePoolUpgradeRequest) ValidateDelete(_ context.Context) error {
	if r.Status.AcceptedTime != nil {
		return fmt.Errorf("an accepted NodePool upgrade request cannot be deleted")
	}
	return nil
}
