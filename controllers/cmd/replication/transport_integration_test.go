package replication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	publisher "github.com/openshift-online/gecko/controllers/replication"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

// This test exercises the real Pub/Sub client adapters against an in-process
// emulator. The kind deployment test remains a separate environment check.
func TestPublisherPubSubRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	emulator := pstest.NewServer()
	defer emulator.Close()
	conn, err := grpc.NewClient(emulator.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client, err := pubsub.NewClient(ctx, "test-project", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, topic := range []string{"data", "control"} {
		if _, err := client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: "projects/test-project/topics/" + topic}); err != nil {
			t.Fatal(err)
		}
	}
	for _, subscription := range []struct{ name, topic string }{{"test-data", "data"}, {"leader-control", "control"}} {
		if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
			Name:  "projects/test-project/subscriptions/" + subscription.name,
			Topic: "projects/test-project/topics/" + subscription.topic,
		}); err != nil {
			t.Fatal(err)
		}
	}

	resource, err := publisher.ParseResources("roles.gcp.managed.openshift.io")
	if err != nil {
		t.Fatal(err)
	}
	api := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{resource[0].GVR: "RoleList"})
	role := func(version string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "gcp.managed.openshift.io/v1", "kind": "Role",
			"metadata": map[string]interface{}{"namespace": "tenant-a", "name": "viewer", "uid": "uid-1", "resourceVersion": version},
			"spec":     map[string]interface{}{"permissions": []interface{}{"cluster.get"}},
		}}
	}
	api.PrependReactor("list", "roles", func(_ ktesting.Action) (bool, runtime.Object, error) {
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*role("4")}}
		list.SetResourceVersion("5")
		return true, list, nil
	})
	stream := watch.NewRaceFreeFake()
	api.PrependWatchReactor("roles", func(_ ktesting.Action) (bool, watch.Interface, error) { return true, stream, nil })
	dataPublisher := client.Publisher("data")
	dataPublisher.PublishSettings.CountThreshold = 1
	defer dataPublisher.Stop()
	controlPublisher := client.Publisher("control")
	controlPublisher.PublishSettings.CountThreshold = 1
	defer controlPublisher.Stop()
	controller, err := publisher.NewPublisher(publisher.Config{
		Region: "us-east1", LeaderRegion: "us-east1", Generation: 3,
		Resources: resource, ResyncInterval: time.Hour,
	}, api, topicSink{dataPublisher}, subscriptionSource{client.Subscriber("leader-control")}, publisher.NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	controllerResult := make(chan error, 1)
	go func() { controllerResult <- controller.Run(ctx) }()
	events := make(chan publisher.Event, 10)
	subscriberResult := make(chan error, 1)
	go func() {
		subscriberResult <- client.Subscriber("test-data").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
			var event publisher.Event
			if json.Unmarshal(message.Data, &event) == nil {
				events <- event
			}
			message.Ack()
		})
	}()
	next := func() publisher.Event {
		t.Helper()
		select {
		case event := <-events:
			return event
		case <-ctx.Done():
			t.Fatal("timed out waiting for Pub/Sub event")
			return publisher.Event{}
		}
	}
	// Pub/Sub does not guarantee delivery order without an ordering key.
	nextSnapshot := func() (publisher.Event, publisher.Event) {
		t.Helper()
		first, second := next(), next()
		if first.EventType == publisher.Inventory {
			first, second = second, first
		}
		return first, second
	}
	initialObject, initialInventory := nextSnapshot()
	if initialObject.EventType != publisher.CreateOrUpdate || initialInventory.EventType != publisher.Inventory || initialObject.SourceResourceVersion != "4" || initialObject.SnapshotResourceVersion != "5" || initialObject.SyncID != initialInventory.SyncID {
		t.Fatalf("unexpected initial events: %+v / %+v", initialObject, initialInventory)
	}
	request, _ := json.Marshal(publisher.Event{EventType: publisher.ResyncRequest, ResourceKind: resource[0].ID, Generation: 3})
	if _, err := controlPublisher.Publish(ctx, &pubsub.Message{Data: request}).Get(ctx); err != nil {
		t.Fatal(err)
	}
	resyncObject, resyncInventory := nextSnapshot()
	if resyncObject.SyncID == initialObject.SyncID || resyncInventory.SyncID != resyncObject.SyncID || resyncObject.EventType != publisher.CreateOrUpdate || resyncInventory.EventType != publisher.Inventory {
		t.Fatalf("resync request did not produce a new capture: %+v / %+v", resyncObject, resyncInventory)
	}
	stream.Delete(role("6"))
	deletion := next()
	if deletion.EventType != publisher.Delete || deletion.SourceResourceVersion != "6" || deletion.Name != "viewer" {
		t.Fatalf("incorrect deletion: %+v", deletion)
	}
	cancel()
	if err := <-controllerResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-subscriberResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestFollowerPubSubRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	emulator := pstest.NewServer()
	defer emulator.Close()
	conn, err := grpc.NewClient(emulator.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client, err := pubsub.NewClient(ctx, "test-project", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, topic := range []string{"data", "control"} {
		if _, err := client.TopicAdminClient.CreateTopic(ctx, &pb.Topic{Name: "projects/test-project/topics/" + topic}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pb.Subscription{
		Name: "projects/test-project/subscriptions/follower-data", Topic: "projects/test-project/topics/data",
	}); err != nil {
		t.Fatal(err)
	}
	resources, err := publisher.ParseResources("roles.gcp.managed.openshift.io")
	if err != nil {
		t.Fatal(err)
	}
	api := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		resources[0].GVR: "RoleList", {Version: "v1", Resource: "namespaces"}: "NamespaceList",
	})
	config := publisher.Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3, Resources: resources, ResyncInterval: time.Hour}
	mirror, err := publisher.NewAPIMirror(api, config)
	if err != nil {
		t.Fatal(err)
	}
	controlPublisher := client.Publisher("control")
	controlPublisher.PublishSettings.CountThreshold = 1
	defer controlPublisher.Stop()
	dataSubscriber := client.Subscriber("follower-data")
	dataSubscriber.ReceiveSettings.MaxOutstandingMessages = 1
	dataSubscriber.ReceiveSettings.NumGoroutines = 1
	receiver, err := publisher.NewReceiver(config, subscriptionSource{dataSubscriber}, topicSink{controlPublisher}, mirror, publisher.NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	receiverResult := make(chan error, 1)
	go func() { receiverResult <- receiver.Run(ctx) }()
	role := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "gcp.managed.openshift.io/v1", "kind": "Role",
		"metadata": map[string]interface{}{"namespace": "tenant-a", "name": "viewer", "uid": "source-uid", "resourceVersion": "4"},
		"spec":     map[string]interface{}{"permissions": []interface{}{"cluster.get"}},
	}}
	object, err := json.Marshal(role.Object)
	if err != nil {
		t.Fatal(err)
	}
	event, err := json.Marshal(publisher.Event{
		EventType: publisher.CreateOrUpdate, ResourceKind: resources[0].ID,
		OriginRegion: "us-central1", Generation: 3, Namespace: "tenant-a", Name: "viewer",
		SourceResourceVersion: "4", Object: object, UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	dataPublisher := client.Publisher("data")
	dataPublisher.PublishSettings.CountThreshold = 1
	defer dataPublisher.Stop()
	if _, err := dataPublisher.Publish(ctx, &pubsub.Message{Data: event}).Get(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		local, err := api.Resource(resources[0].GVR).Namespace("tenant-a").Get(ctx, "viewer", metav1.GetOptions{})
		if err == nil {
			if local.GetAnnotations()["replication.gcp.managed.openshift.io/replicated-from"] != "us-central1" {
				t.Fatalf("local mirror lacks leader provenance: %v", local.GetAnnotations())
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("follower did not apply Pub/Sub event: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	if err := <-receiverResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
