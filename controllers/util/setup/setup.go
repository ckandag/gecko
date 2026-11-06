package setup

import (
	"fmt"

	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// RootFlags holds persistent flags shared across all subcommands.
type RootFlags struct {
	LogLevel  string
	LogFormat string
	OrlopURL  string
	Workers   int
}

// NewLogger creates a logger from root flags.
func (rf *RootFlags) NewLogger(component string) (logger.Logger, error) {
	return logger.NewLogger(logger.Config{
		Level:     rf.LogLevel,
		Format:    rf.LogFormat,
		Output:    "stdout",
		Component: component,
	})
}

// NewScheme creates a runtime.Scheme with platform-api types registered.
func NewScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := privatev1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("failed to register platform-api types: %v", err))
	}
	return scheme
}

// NewManager creates a controller-runtime Manager.
// When OrlopURL is set it connects directly to the orlop API server (legacy / local dev).
// When empty it uses in-cluster config, talking to the kube-apiserver (API aggregation path).
func (rf *RootFlags) NewManager(scheme *runtime.Scheme, log logger.Logger) (ctrl.Manager, error) {
	ctrl.SetLogger(logger.ToLogr(log))

	var cfg *rest.Config
	if rf.OrlopURL != "" {
		cfg = &rest.Config{Host: rf.OrlopURL}
	} else {
		var err error
		cfg, err = ctrl.GetConfig()
		if err != nil {
			return nil, fmt.Errorf("get in-cluster config: %w", err)
		}
	}

	return ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
	})
}

// ControllerOpts returns per-controller options derived from root flags.
func (rf *RootFlags) ControllerOpts() controller.Options {
	return controller.Options{MaxConcurrentReconciles: rf.Workers}
}
