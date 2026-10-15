package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	cmdcontrolplaneupgrade "github.com/openshift-online/gecko/controllers/cmd/controlplaneupgrade"
	cmdhc "github.com/openshift-online/gecko/controllers/cmd/hc"
	cmdnodepool "github.com/openshift-online/gecko/controllers/cmd/nodepool"
	cmdnodepoolvrresolution "github.com/openshift-online/gecko/controllers/cmd/nodepoolvrresolution"
	cmdplacement "github.com/openshift-online/gecko/controllers/cmd/placement"
	cmdversionresolution "github.com/openshift-online/gecko/controllers/cmd/versionresolution"
	cmdversionsync "github.com/openshift-online/gecko/controllers/cmd/versionsync"
	"github.com/openshift-online/gecko/controllers/util/setup"
)

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func main() {
	rf := &setup.RootFlags{}

	root := &cobra.Command{
		Use:   "gecko-controllers",
		Short: "Gecko controllers",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if v := envOr("LOG_LEVEL", ""); v != "" && !cmd.Flags().Changed("log-level") {
				rf.LogLevel = v
			}
			if v := envOr("LOG_FORMAT", ""); v != "" && !cmd.Flags().Changed("log-format") {
				rf.LogFormat = v
			}
			if v := envOr("ORLOP_URL", ""); v != "" && !cmd.Flags().Changed("orlop-url") {
				rf.OrlopURL = v
			}
		},
	}

	root.PersistentFlags().StringVar(&rf.LogLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	root.PersistentFlags().StringVar(&rf.LogFormat, "log-format", "json", "Log format (json, text)")
	root.PersistentFlags().StringVar(&rf.OrlopURL, "orlop-url", "", "Orlop API server URL; when empty uses in-cluster kubeconfig [$ORLOP_URL]")
	root.PersistentFlags().IntVar(&rf.Workers, "workers", 10, "Concurrent reconcile goroutines")

	root.AddCommand(
		cmdversionsync.NewCommand(rf),
		cmdversionresolution.NewCommand(rf),
		cmdnodepoolvrresolution.NewCommand(rf),
		cmdplacement.NewCommand(rf),
		cmdhc.NewCommand(rf),
		cmdnodepool.NewCommand(rf),
		cmdcontrolplaneupgrade.NewCommand(rf),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
