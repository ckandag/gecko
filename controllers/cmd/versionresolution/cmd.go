package versionresolution

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"
	"github.com/openshift-online/gecko/controllers/versionresolution"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewCommand returns the version-resolution subcommand.
func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var cincinnatiURL, arch string

	cmd := &cobra.Command{
		Use:   "version-resolution",
		Short: "Run the version-resolution controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := rf.NewLogger("version-resolution-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			scheme := setup.NewScheme()
			mgr, err := rf.NewManager(scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}

			cinClient := versionresolution.NewCincinnatiClient(cincinnatiURL, arch)
			rec := versionresolution.NewReconciler(cinClient, log, mgr.GetClient())

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1.Cluster{}).
				Watches(&privatev1.Channel{}, handler.EnqueueRequestsFromMapFunc(channelToClusters(mgr.GetClient(), log))).
				WithOptions(rf.ControllerOpts()).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&cincinnatiURL, "cincinnati-url", "https://api.openshift.com/api/upgrades_info/v1/graph", "Cincinnati API URL")
	cmd.Flags().StringVar(&arch, "arch", "amd64", "CPU architecture for Cincinnati query")

	return cmd
}

// channelToClusters maps a Channel change to the Clusters using that channel group.
func channelToClusters(apiClient client.Client, log logger.Logger) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		channel, ok := obj.(*privatev1.Channel)
		if !ok {
			return nil
		}

		var clusters privatev1.ClusterList
		if err := apiClient.List(ctx, &clusters); err != nil {
			log.Errorf(ctx, "list Clusters for Channel %s: %v", channel.Name, err)
			return nil
		}

		var requests []reconcile.Request
		for _, cluster := range clusters.Items {
			channelGroup := cluster.Spec.Release.ChannelGroup
			if channelGroup == "" {
				channelGroup = versionresolution.DefaultChannelGroup
			}
			if channelGroup == channel.Name {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{
					Namespace: cluster.Namespace,
					Name:      cluster.Name,
				}})
			}
		}
		return requests
	}
}
