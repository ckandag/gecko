package replication

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/spf13/cobra"

	"github.com/openshift-online/gecko/controllers/replication"
	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"

	"k8s.io/client-go/dynamic"

	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type dataSink struct{ publisher *pubsub.Publisher }

func (s dataSink) Publish(ctx context.Context, data []byte) error {
	_, err := s.publisher.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return err
}

type controlSource struct{ subscriber *pubsub.Subscriber }

func (s controlSource) Receive(ctx context.Context, handle func([]byte)) error {
	return s.subscriber.Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		handle(message.Data)
		message.Ack()
	})
}

func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var region, leaderRegion, topicProject, dataTopic, controlSubscription, resourceTypes string
	var generation uint64
	var resyncInterval time.Duration
	var captureBudget int

	cmd := &cobra.Command{
		Use:   "replication",
		Short: "Run the resource replication controller",
		RunE: func(cmd *cobra.Command, _ []string) error {
			for flag, variable := range map[string]*string{
				"region": &region, "leader-region": &leaderRegion, "pubsub-topic-project": &topicProject,
				"pubsub-data-topic": &dataTopic, "pubsub-control-subscription": &controlSubscription,
				"replicate": &resourceTypes,
			} {
				if !cmd.Flags().Changed(flag) {
					if value := os.Getenv(envName(flag)); value != "" {
						*variable = value
					}
				}
			}
			if !cmd.Flags().Changed("leader-generation") && os.Getenv("REPL_LEADER_GENERATION") != "" {
				value, err := strconv.ParseUint(os.Getenv("REPL_LEADER_GENERATION"), 10, 64)
				if err != nil {
					return fmt.Errorf("REPL_LEADER_GENERATION: %w", err)
				}
				generation = value
			}
			if !cmd.Flags().Changed("resync-interval") && os.Getenv("REPL_RESYNC_INTERVAL") != "" {
				value, err := time.ParseDuration(os.Getenv("REPL_RESYNC_INTERVAL"))
				if err != nil {
					return fmt.Errorf("REPL_RESYNC_INTERVAL: %w", err)
				}
				resyncInterval = value
			}
			if region == "" || leaderRegion == "" || generation == 0 {
				return fmt.Errorf("--region, --leader-region, and a positive --leader-generation are required")
			}
			if region != leaderRegion {
				return fmt.Errorf("replication publisher runs only in the leader region: --region %q differs from --leader-region %q", region, leaderRegion)
			}
			if topicProject == "" {
				return fmt.Errorf("--pubsub-topic-project is required")
			}
			if dataTopic == "" {
				return fmt.Errorf("--pubsub-data-topic is required")
			}
			if controlSubscription == "" {
				return fmt.Errorf("--pubsub-control-subscription is required")
			}
			if resyncInterval <= 0 {
				return fmt.Errorf("--resync-interval must be positive")
			}
			resources, err := replication.ParseResources(resourceTypes)
			if err != nil {
				return fmt.Errorf("--replicate: %w", err)
			}

			baseLog, err := rf.NewLogger("replication-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}
			mgr, err := rf.NewManager(setup.NewScheme(), baseLog)
			if err != nil {
				return fmt.Errorf("create manager: %w", err)
			}
			api, err := dynamic.NewForConfig(mgr.GetConfig())
			if err != nil {
				return fmt.Errorf("create API client: %w", err)
			}
			log := logger.ToLogr(baseLog)
			metrics := replication.NewMetrics(ctrlmetrics.Registry)
			cfg := replication.Config{
				Region: region, LeaderRegion: leaderRegion, Generation: generation,
				Resources: resources, ResyncInterval: resyncInterval, MaxCaptureBytes: captureBudget,
			}
			if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
				client, err := pubsub.NewClient(ctx, topicProject)
				if err != nil {
					return fmt.Errorf("create Pub/Sub client: %w", err)
				}
				defer client.Close()
				dataPublisher := client.Publisher(dataTopic)
				dataPublisher.PublishSettings.CountThreshold = 1
				defer dataPublisher.Stop()
				controlSubscriber := client.Subscriber(controlSubscription)
				controlSubscriber.ReceiveSettings.MaxOutstandingMessages = 1
				publisher, err := replication.NewPublisher(cfg, api, dataSink{dataPublisher}, controlSource{controlSubscriber}, metrics, log)
				if err != nil {
					return err
				}
				return publisher.Run(ctx)
			})); err != nil {
				return fmt.Errorf("add replication publisher: %w", err)
			}
			return mgr.Start(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&region, "region", "", "This region [$REPL_REGION]")
	cmd.Flags().StringVar(&leaderRegion, "leader-region", "", "Configured leader region [$REPL_LEADER_REGION]")
	cmd.Flags().Uint64Var(&generation, "leader-generation", 0, "Leadership generation [$REPL_LEADER_GENERATION]")
	cmd.Flags().StringVar(&topicProject, "pubsub-topic-project", "gecko-local", "Project containing shared Pub/Sub topics [$PUBSUB_TOPIC_PROJECT]")
	cmd.Flags().StringVar(&dataTopic, "pubsub-data-topic", "resource-replication", "Data topic in the shared project [$REPL_PUBSUB_DATA_TOPIC]")
	cmd.Flags().StringVar(&controlSubscription, "pubsub-control-subscription", "", "Full regional control subscription name (projects/.../subscriptions/...) [$REPL_PUBSUB_CONTROL_SUBSCRIPTION]")
	cmd.Flags().StringVar(&resourceTypes, "replicate", "", "Comma-separated replicated group/resources [$REPL_RESOURCE_TYPES]")
	cmd.Flags().DurationVar(&resyncInterval, "resync-interval", 30*time.Minute, "Full snapshot interval [$REPL_RESYNC_INTERVAL]")
	cmd.Flags().IntVar(&captureBudget, "capture-max-bytes", 32<<20, "Maximum in-memory bytes for one resource snapshot; keep well below the container memory limit")
	return cmd
}

func envName(flag string) string {
	switch flag {
	case "region":
		return "REPL_REGION"
	case "leader-region":
		return "REPL_LEADER_REGION"
	case "pubsub-topic-project":
		return "PUBSUB_TOPIC_PROJECT"
	case "pubsub-data-topic":
		return "REPL_PUBSUB_DATA_TOPIC"
	case "pubsub-control-subscription":
		return "REPL_PUBSUB_CONTROL_SUBSCRIPTION"
	case "replicate":
		return "REPL_RESOURCE_TYPES"
	default:
		return ""
	}
}
