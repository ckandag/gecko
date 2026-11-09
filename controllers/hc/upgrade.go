package hc

import (
	"context"
	"fmt"

	"github.com/openshift-online/gecko/controllers/client/transport"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *Reconciler) preparationFailure(ctx context.Context, c *privatev1.Cluster, cause error) error {
	if meta.IsStatusConditionTrue(c.Status.Conditions, privatev1.ControlPlaneUpgradeAccepted) && !privatev1.ClusterUpgradeSubmissionStarted(c) {
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
			Type: "ResourcesApplied", Status: metav1.ConditionFalse, Reason: "ManifestBuildFailed",
			Message: "The desired resources could not be prepared before dispatch", ObservedGeneration: c.Generation,
		})
		if err := r.client.Status().Update(ctx, c); err != nil {
			return fmt.Errorf("record preparation failure: %w", err)
		}
	}
	return fmt.Errorf("build manifests: %w", cause)
}

// applyResources records the dispatch boundary before any external write. An
// uncertain dispatch keeps the approval lock across errors and restarts.
func (r *Reconciler) applyResources(ctx context.Context, c *privatev1.Cluster, managementCluster, groupKey string, manifests [][]byte) (*transport.Status, error) {
	upgrading := meta.IsStatusConditionTrue(c.Status.Conditions, privatev1.ControlPlaneUpgradeAccepted) &&
		c.Status.HostedClusterResult != nil && c.Spec.Release.Version != c.Status.HostedClusterResult.Version
	if !upgrading {
		return r.transport.Apply(ctx, managementCluster, groupKey, manifests)
	}
	submitted := meta.FindStatusCondition(c.Status.Conditions, privatev1.ControlPlaneUpgradeSubmitted)
	accepted := meta.FindStatusCondition(c.Status.Conditions, privatev1.ControlPlaneUpgradeAccepted)
	if submitted == nil || submitted.Status == metav1.ConditionFalse || submitted.ObservedGeneration < accepted.ObservedGeneration {
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
			Type: privatev1.ControlPlaneUpgradeSubmitted, Status: metav1.ConditionUnknown,
			Reason: "Dispatching", Message: "The approved release is being dispatched", ObservedGeneration: c.Generation,
		})
		if err := r.client.Status().Update(ctx, c); err != nil && !apierrors.IsConflict(err) {
			return nil, fmt.Errorf("record dispatch boundary: %w", err)
		}
		return &transport.Status{Stale: true}, nil
	}
	status, err := r.transport.Apply(ctx, managementCluster, groupKey, manifests)
	if err != nil {
		return nil, err
	}
	if submitted.Status != metav1.ConditionTrue {
		meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
			Type: privatev1.ControlPlaneUpgradeSubmitted, Status: metav1.ConditionTrue,
			Reason: "Dispatched", Message: "The approved release was dispatched", ObservedGeneration: c.Generation,
		})
		if err := r.client.Status().Update(ctx, c); err != nil {
			if apierrors.IsConflict(err) {
				return &transport.Status{Stale: true}, nil
			}
			return nil, fmt.Errorf("record dispatch acknowledgement: %w", err)
		}
	}
	return status, nil
}
