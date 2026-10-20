package nodepoolupgrade

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/openshift-online/gecko/controllers/nodepoolupgrade"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	ctrl "sigs.k8s.io/controller-runtime"
)

// NewCommand returns the one-time NodePool upgrade request controller command.
func NewCommand(rf *setup.RootFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "nodepool-upgrade",
		Short: "Run the NodePool upgrade request controller",
		RunE: func(cmd *cobra.Command, _ []string) error {
			log, err := rf.NewLogger("nodepool-upgrade-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}
			mgr, err := rf.NewManager(setup.NewScheme(), log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}
			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1.NodePoolUpgradeRequest{}).
				WithOptions(rf.ControllerOpts()).
				Complete(nodepoolupgrade.NewReconciler(mgr.GetClient())); err != nil {
				return fmt.Errorf("setup NodePool upgrade controller: %w", err)
			}
			return mgr.Start(cmd.Context())
		},
	}
}
