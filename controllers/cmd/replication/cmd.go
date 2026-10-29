package replication

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/openshift-online/gecko/controllers/replication"
	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"
)

type dataSink struct{ publisher *pubsub.Publisher }

func (s dataSink) Publish(ctx context.Context, data []byte) error {
	_, err := s.publisher.Publish(ctx, &pubsub.Message{Data: data}).Get(ctx)
	return err
}

type dataSource struct{ subscriber *pubsub.Subscriber }

func (s dataSource) Receive(ctx context.Context, handle func(context.Context, []byte) error) error {
	return s.subscriber.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
		if err := handle(ctx, message.Data); err != nil {
			// Even API admission errors can be temporary when a RoleBinding
			// arrives before its Role. Keep the event for retry; the data
			// subscription backs off redelivery of this individual message.
			message.Nack()
			return
		}
		message.Ack()
	})
}

type controlSource struct{ subscriber *pubsub.Subscriber }

func (s controlSource) Receive(ctx context.Context, handle func([]byte)) error {
	return s.subscriber.Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		handle(message.Data)
		message.Ack()
	})
}

func NewCommand(rf *setup.RootFlags) *cobra.Command {
	var region, leaderRegion, project, topic, subscription, controlTopic, controlSubscription, resourceTypes, metricsAddress string
	var generation uint64
	var resyncInterval time.Duration
	var captureBudget int

	cmd := &cobra.Command{
		Use:   "replication",
		Short: "Run the regional resource replication controller",
		RunE: func(cmd *cobra.Command, _ []string) error {
			for flag, variable := range map[string]*string{
				"region": &region, "leader-region": &leaderRegion, "pubsub-project": &project,
				"pubsub-topic": &topic, "pubsub-subscription": &subscription,
				"pubsub-control-topic": &controlTopic, "pubsub-control-subscription": &controlSubscription,
				"replicate": &resourceTypes, "metrics-bind-address": &metricsAddress,
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
			resources, err := replication.ParseResources(resourceTypes)
			if err != nil {
				return fmt.Errorf("--replicate: %w", err)
			}
			if region == "" || leaderRegion == "" || generation == 0 {
				return fmt.Errorf("--region, --leader-region, and a positive --leader-generation are required")
			}
			if project == "" {
				return fmt.Errorf("--pubsub-project is required")
			}
			if subscription == "" {
				return fmt.Errorf("--pubsub-subscription is required")
			}
			if region == leaderRegion && controlSubscription == "" {
				return fmt.Errorf("--pubsub-control-subscription is required in the leader region")
			}
			if region != leaderRegion && controlTopic == "" {
				return fmt.Errorf("--pubsub-control-topic is required in a follower region")
			}
			if resyncInterval <= 0 {
				return fmt.Errorf("--resync-interval must be positive")
			}

			baseLog, err := rf.NewLogger("replication-controller")
			if err != nil {
				return fmt.Errorf("create logger: %w", err)
			}
			log := logger.ToLogr(baseLog)
			registry := prometheus.NewRegistry()
			metrics := replication.NewMetrics(registry)

			server := &http.Server{Addr: metricsAddress, Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
			g, ctx := errgroup.WithContext(cmd.Context())
			g.Go(func() error {
				err := server.ListenAndServe()
				if errors.Is(err, http.ErrServerClosed) {
					return nil
				}
				return fmt.Errorf("metrics server: %w", err)
			})
			g.Go(func() error {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return server.Shutdown(shutdownCtx)
			})
			g.Go(func() error {
				if region == leaderRegion && topic == "" {
					return fmt.Errorf("--pubsub-topic is required in the leader region")
				}
				var restConfig *rest.Config
				if rf.OrlopURL != "" {
					restConfig = &rest.Config{Host: rf.OrlopURL}
				} else {
					var err error
					restConfig, err = ctrl.GetConfig()
					if err != nil {
						return fmt.Errorf("get API config: %w", err)
					}
				}
				api, err := dynamic.NewForConfig(restConfig)
				if err != nil {
					return fmt.Errorf("create API client: %w", err)
				}
				client, err := pubsub.NewClient(ctx, project)
				if err != nil {
					return fmt.Errorf("create Pub/Sub client: %w", err)
				}
				defer client.Close()
				dataSubscriber := client.Subscriber(subscription)
				dataSubscriber.ReceiveSettings.MaxOutstandingMessages = 1
				dataSubscriber.ReceiveSettings.NumGoroutines = 1
				cfg := replication.Config{
					Region: region, LeaderRegion: leaderRegion, Generation: generation,
					Resources: resources, ResyncInterval: resyncInterval, MaxCaptureBytes: captureBudget,
				}
				var mirror replication.Mirror
				var controlSink replication.ControlSink
				if region != leaderRegion {
					mirror, err = replication.NewAPIMirror(api, cfg)
					if err != nil {
						return err
					}
					controlPublisher := client.Publisher(controlTopic)
					controlPublisher.PublishSettings.CountThreshold = 1
					defer controlPublisher.Stop()
					controlSink = dataSink{controlPublisher}
				}
				receiver, err := replication.NewReceiver(cfg, dataSource{dataSubscriber}, controlSink, mirror, metrics, log)
				if err != nil {
					return err
				}
				workers, workerCtx := errgroup.WithContext(ctx)
				workers.Go(func() error { return receiver.Run(workerCtx) })
				if region == leaderRegion {
					dataPublisher := client.Publisher(topic)
					dataPublisher.PublishSettings.CountThreshold = 1
					defer dataPublisher.Stop()
					controlSubscriber := client.Subscriber(controlSubscription)
					controlSubscriber.ReceiveSettings.MaxOutstandingMessages = 1
					publisher, err := replication.NewPublisher(cfg, api, dataSink{dataPublisher}, controlSource{controlSubscriber}, metrics, log)
					if err != nil {
						return err
					}
					workers.Go(func() error { return publisher.Run(workerCtx) })
				}
				return workers.Wait()
			})
			return g.Wait()
		},
	}
	cmd.Flags().StringVar(&region, "region", "", "This region [$REPL_REGION]")
	cmd.Flags().StringVar(&leaderRegion, "leader-region", "", "Configured leader region [$REPL_LEADER_REGION]")
	cmd.Flags().Uint64Var(&generation, "leader-generation", 0, "Leadership generation [$REPL_LEADER_GENERATION]")
	cmd.Flags().StringVar(&project, "pubsub-project", "gecko-local", "Pub/Sub project [$PUBSUB_PROJECT]")
	cmd.Flags().StringVar(&topic, "pubsub-topic", "resource-replication", "Data topic [$REPL_PUBSUB_TOPIC]")
	cmd.Flags().StringVar(&subscription, "pubsub-subscription", "", "Regional data subscription [$REPL_PUBSUB_SUBSCRIPTION]")
	cmd.Flags().StringVar(&controlTopic, "pubsub-control-topic", "resource-replication-control", "Control topic [$REPL_PUBSUB_CONTROL_TOPIC]")
	cmd.Flags().StringVar(&controlSubscription, "pubsub-control-subscription", "", "Leader control subscription [$REPL_PUBSUB_CONTROL_SUBSCRIPTION]")
	cmd.Flags().StringVar(&resourceTypes, "replicate", "", "Comma-separated replicated group/resources [$REPL_RESOURCE_TYPES]")
	cmd.Flags().DurationVar(&resyncInterval, "resync-interval", 30*time.Minute, "Full snapshot interval [$REPL_RESYNC_INTERVAL]")
	cmd.Flags().IntVar(&captureBudget, "capture-max-bytes", 32<<20, "Maximum in-memory bytes for one resource snapshot; keep well below the container memory limit")
	cmd.Flags().StringVar(&metricsAddress, "metrics-bind-address", ":8080", "Prometheus HTTP bind address [$REPL_METRICS_BIND_ADDRESS]")
	return cmd
}

func envName(flag string) string {
	switch flag {
	case "region":
		return "REPL_REGION"
	case "leader-region":
		return "REPL_LEADER_REGION"
	case "pubsub-project":
		return "PUBSUB_PROJECT"
	case "pubsub-topic":
		return "REPL_PUBSUB_TOPIC"
	case "pubsub-subscription":
		return "REPL_PUBSUB_SUBSCRIPTION"
	case "pubsub-control-topic":
		return "REPL_PUBSUB_CONTROL_TOPIC"
	case "pubsub-control-subscription":
		return "REPL_PUBSUB_CONTROL_SUBSCRIPTION"
	case "replicate":
		return "REPL_RESOURCE_TYPES"
	case "metrics-bind-address":
		return "REPL_METRICS_BIND_ADDRESS"
	default:
		return ""
	}
}
