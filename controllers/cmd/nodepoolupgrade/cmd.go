package nodepoolupgrade

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/openshift-online/gecko/controllers/nodepoolupgrade"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// NewCommand returns the NodePool upgrade controller command.
func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var healthProbeBindAddress string
	command := &cobra.Command{
		Use:   "nodepool-upgrade",
		Short: "Run the NodePool upgrade controller",
		RunE: func(cmd *cobra.Command, _ []string) error {
			log, err := rf.NewLogger("nodepool-upgrade-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}
			managerFlags := *rf
			managerFlags.HealthProbeBindAddress = healthProbeBindAddress
			mgr, err := managerFlags.NewManager(setup.NewScheme(), log)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}
			if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
				return fmt.Errorf("register health check: %w", err)
			}
			if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
				return fmt.Errorf("register readiness check: %w", err)
			}
			if err := ctrl.NewControllerManagedBy(mgr).
				For(&privatev1.NodePool{}).
				WithOptions(rf.ControllerOpts()).
				Complete(nodepoolupgrade.NewReconciler(mgr.GetClient())); err != nil {
				return fmt.Errorf("setup NodePool upgrade controller: %w", err)
			}
			return mgr.Start(cmd.Context())
		},
	}
	command.Flags().StringVar(&healthProbeBindAddress, "health-probe-bind-address", ":8081", "Address for liveness and readiness probes")
	return command
}
