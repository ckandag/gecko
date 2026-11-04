package replication

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/openshift-online/gecko/controllers/replication"
	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"

	"k8s.io/client-go/dynamic"

	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type topicSink struct{ publisher *pubsub.Publisher }

func (s topicSink) Publish(ctx context.Context, data []byte) error {
	_, err := s.publisher.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return err
}

type subscriptionSource struct{ subscriber *pubsub.Subscriber }

func (s subscriptionSource) Receive(ctx context.Context, handle func(context.Context, []byte) error) error {
	return s.subscriber.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		if err := handle(ctx, message.Data); err != nil {
			message.Nack()
			return
		}
		message.Ack()
	})
}

type replicationRunner struct {
	client              *pubsub.Client
	api                 dynamic.Interface
	config              replication.Config
	metrics             *replication.Metrics
	log                 logr.Logger
	dataTopic           string
	dataSubscription    string
	controlTopic        string
	controlSubscription string
}

func (r *replicationRunner) newDataSource() subscriptionSource {
	subscriber := r.client.Subscriber(r.dataSubscription)
	subscriber.ReceiveSettings.MaxOutstandingMessages = 1
	subscriber.ReceiveSettings.NumGoroutines = 1
	return subscriptionSource{subscriber: subscriber}
}

func (r *replicationRunner) runLeader(ctx context.Context) error {
	receiver, err := replication.NewReceiver(r.config, r.newDataSource(), nil, nil, r.metrics, r.log)
	if err != nil {
		return err
	}
	dataPublisher := r.client.Publisher(r.dataTopic)
	dataPublisher.PublishSettings.CountThreshold = 1
	defer dataPublisher.Stop()
	controlSubscriber := r.client.Subscriber(r.controlSubscription)
	controlSubscriber.ReceiveSettings.MaxOutstandingMessages = 1
	publisher, err := replication.NewPublisher(r.config, r.api, topicSink{dataPublisher}, subscriptionSource{controlSubscriber}, r.metrics, r.log)
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return receiver.Run(groupCtx) })
	group.Go(func() error { return publisher.Run(groupCtx) })
	return group.Wait()
}

func (r *replicationRunner) runFollower(ctx context.Context) error {
	mirror, err := replication.NewAPIMirror(r.api, r.config)
	if err != nil {
		return err
	}
	controlPublisher := r.client.Publisher(r.controlTopic)
	controlPublisher.PublishSettings.CountThreshold = 1
	defer controlPublisher.Stop()
	receiver, err := replication.NewReceiver(r.config, r.newDataSource(), topicSink{controlPublisher}, mirror, r.metrics, r.log)
	if err != nil {
		return err
	}
	return receiver.Run(ctx)
}

func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var region, leaderRegion, topicProject, dataTopic, dataSubscription, controlTopic, controlSubscription, resourceTypes string
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
				"pubsub-data-subscription": &dataSubscription, "pubsub-control-topic": &controlTopic,
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
			if err := validatePubSubFlags(region, leaderRegion, topicProject, dataTopic, dataSubscription, controlTopic, controlSubscription); err != nil {
				return err
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
				runner := &replicationRunner{
					client: client, api: api, config: cfg, metrics: metrics, log: log,
					dataTopic: dataTopic, dataSubscription: dataSubscription,
					controlTopic: controlTopic, controlSubscription: controlSubscription,
				}
				if region == leaderRegion {
					return runner.runLeader(ctx)
				}
				return runner.runFollower(ctx)
			})); err != nil {
				return fmt.Errorf("add replication controller: %w", err)
			}
			return mgr.Start(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&region, "region", "", "This region [$REPL_REGION]")
	cmd.Flags().StringVar(&leaderRegion, "leader-region", "", "Configured leader region [$REPL_LEADER_REGION]")
	cmd.Flags().Uint64Var(&generation, "leader-generation", 0, "Leadership generation [$REPL_LEADER_GENERATION]")
	cmd.Flags().StringVar(&topicProject, "pubsub-topic-project", "gecko-local", "Project containing shared Pub/Sub topics [$PUBSUB_TOPIC_PROJECT]")
	cmd.Flags().StringVar(&dataTopic, "pubsub-data-topic", "resource-replication", "Data topic in the shared project [$REPL_PUBSUB_DATA_TOPIC]")
	cmd.Flags().StringVar(&dataSubscription, "pubsub-data-subscription", "", "Full regional data subscription name (projects/.../subscriptions/...) [$REPL_PUBSUB_DATA_SUBSCRIPTION]")
	cmd.Flags().StringVar(&controlTopic, "pubsub-control-topic", "resource-replication-control", "Control topic in the shared project [$REPL_PUBSUB_CONTROL_TOPIC]")
	cmd.Flags().StringVar(&controlSubscription, "pubsub-control-subscription", "", "Full regional control subscription name (projects/.../subscriptions/...) [$REPL_PUBSUB_CONTROL_SUBSCRIPTION]")
	cmd.Flags().StringVar(&resourceTypes, "replicate", "", "Comma-separated replicated group/resources [$REPL_RESOURCE_TYPES]")
	cmd.Flags().DurationVar(&resyncInterval, "resync-interval", 30*time.Minute, "Full snapshot interval [$REPL_RESYNC_INTERVAL]")
	cmd.Flags().IntVar(&captureBudget, "capture-max-bytes", 32<<20, "Maximum in-memory bytes for one resource snapshot; keep well below the container memory limit")
	return cmd
}

func validatePubSubFlags(region, leaderRegion, topicProject, dataTopic, dataSubscription, controlTopic, controlSubscription string) error {
	if topicProject == "" {
		return fmt.Errorf("--pubsub-topic-project is required")
	}
	if dataTopic == "" {
		return fmt.Errorf("--pubsub-data-topic is required")
	}
	if dataSubscription == "" {
		return fmt.Errorf("--pubsub-data-subscription is required")
	}
	if region == leaderRegion && controlSubscription == "" {
		return fmt.Errorf("--pubsub-control-subscription is required in the leader region")
	}
	if region != leaderRegion && controlTopic == "" {
		return fmt.Errorf("--pubsub-control-topic is required in a follower region")
	}
	return nil
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
	case "pubsub-data-subscription":
		return "REPL_PUBSUB_DATA_SUBSCRIPTION"
	case "pubsub-control-topic":
		return "REPL_PUBSUB_CONTROL_TOPIC"
	case "pubsub-control-subscription":
		return "REPL_PUBSUB_CONTROL_SUBSCRIPTION"
	case "replicate":
		return "REPL_RESOURCE_TYPES"
	default:
		return ""
	}
}
