package controlplaneupgrade

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/openshift-online/gecko/controllers/controlplaneupgrade"
	channelutil "github.com/openshift-online/gecko/controllers/util/channel"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewCommand returns the control-plane-upgrade subcommand.
//
// This controller watches Cluster directly, Channel (for fleetMinorVersion
// authorization), and ControlPlaneUpgradePolicy (for maintenance windows/exclusions).
func NewCommand(rf *setup.RootFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "control-plane-upgrade",
		Short: "Run the control-plane-upgrade controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := rf.NewLogger("control-plane-upgrade-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			scheme := setup.NewScheme()
			mgr, err := rf.NewManager(scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			// Index ControlPlaneUpgradePolicy by spec.clusterID for efficient lookups
			if err := mgr.GetFieldIndexer().IndexField(ctx, &privatev1.ControlPlaneUpgradePolicy{},
				controlplaneupgrade.PolicyClusterIDField, policyClusterID); err != nil {
				return fmt.Errorf("index upgrade policies by cluster ID: %w", err)
			}
			if err := mgr.GetFieldIndexer().IndexField(ctx, &privatev1.Cluster{},
				channelutil.ClusterChannelGroupField, channelutil.ClusterChannelGroup); err != nil {
				return fmt.Errorf("index clusters by channel group: %w", err)
			}

			rec := controlplaneupgrade.NewReconciler(mgr.GetClient(), log)

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1.Cluster{}).
				Watches(&privatev1.Channel{}, handler.EnqueueRequestsFromMapFunc(channelutil.ToClusters(mgr.GetClient(), log))).
				Watches(&privatev1.ControlPlaneUpgradePolicy{}, handler.EnqueueRequestsFromMapFunc(policyToCluster)).
				WithOptions(rf.ControllerOpts()).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	return cmd
}

// policyClusterID extracts the cluster ID field from a ControlPlaneUpgradePolicy for indexing
func policyClusterID(obj client.Object) []string {
	policy, ok := obj.(*privatev1.ControlPlaneUpgradePolicy)
	if !ok || policy.Spec.ClusterID == "" {
		return nil
	}
	return []string{policy.Spec.ClusterID}
}

// policyToCluster maps a ControlPlaneUpgradePolicy change to its owning Cluster
func policyToCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	policy, ok := obj.(*privatev1.ControlPlaneUpgradePolicy)
	if !ok || policy.Spec.ClusterID == "" {
		return nil
	}
	return []reconcile.Request{
		{NamespacedName: client.ObjectKey{
			Namespace: policy.Namespace,
			Name:      policy.Spec.ClusterID,
		}},
	}
}
