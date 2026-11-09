package hc

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	fstransport "github.com/openshift-online/gecko/controllers/client/transport/firestore"
	hc "github.com/openshift-online/gecko/controllers/hc"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewCommand returns the hc subcommand.
func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var (
		customerLabelsFile  string
		pullSecretStoreName string
		pullSecretGCPKey    string
	)

	cmd := &cobra.Command{
		Use:   "hc",
		Short: "Run the hosted-cluster (hc) controller",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			log, err := rf.NewLogger("hc-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}

			var customerLabels map[string]string
			if customerLabelsFile != "" {
				customerLabels, err = hc.LoadCustomerLabels(customerLabelsFile)
				if err != nil {
					return fmt.Errorf("load customer labels: %w", err)
				}
			}

			t := fstransport.New(log)
			defer t.Close()

			scheme := setup.NewScheme()
			mgr, err := rf.NewManager(scheme, log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}
			if err := mgr.GetFieldIndexer().IndexField(ctx, &privatev1.NodePool{}, hc.NodePoolClusterIDField, nodePoolClusterID); err != nil {
				return fmt.Errorf("index nodepools by cluster ID: %w", err)
			}

			rec := hc.New(t, log, mgr.GetClient(), customerLabels, pullSecretStoreName, pullSecretGCPKey)

			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1.Cluster{}).
				Watches(&privatev1.NodePool{}, handler.EnqueueRequestsFromMapFunc(nodePoolToCluster),
					builder.WithPredicates(nodePoolDeletePredicate())).
				WithOptions(rf.ControllerOpts()).
				Complete(rec); err != nil {
				return fmt.Errorf("setup controller: %w", err)
			}

			return mgr.Start(ctx)
		},
	}

	cmd.Flags().StringVar(&customerLabelsFile, "customer-labels-file", "", "Path to JSON file containing customer-facing GCP resource labels (omit to disable)")
	cmd.Flags().StringVar(&pullSecretStoreName, "pull-secret-store-name", "gcp-secret-manager", "External Secrets ClusterSecretStore name for HostedCluster pull secret")
	cmd.Flags().StringVar(&pullSecretGCPKey, "pull-secret-gcp-key", "default-openshift-pull-secret", "GCP Secret Manager key used for HostedCluster pull secret")

	return cmd
}

func nodePoolClusterID(obj ctrlclient.Object) []string {
	nodePool, ok := obj.(*privatev1.NodePool)
	if !ok || nodePool.Spec.ClusterID == "" {
		return nil
	}
	return []string{nodePool.Spec.ClusterID}
}

func nodePoolToCluster(_ context.Context, obj ctrlclient.Object) []reconcile.Request {
	nodePool, ok := obj.(*privatev1.NodePool)
	if !ok || nodePool.Spec.ClusterID == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: ctrlclient.ObjectKey{
		Namespace: nodePool.Namespace,
		Name:      nodePool.Spec.ClusterID,
	}}}
}

func nodePoolDeletePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		UpdateFunc:  func(event.UpdateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
	}
}
