package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

type recordingSink struct {
	mu       sync.Mutex
	messages [][]byte
	failOnce bool
	updates  chan struct{}
}

func (s *recordingSink) Publish(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, bytes.Clone(data))
	if s.failOnce {
		s.failOnce = false
		return status.Error(codes.Unavailable, "temporary outage")
	}
	if s.updates != nil {
		select {
		case s.updates <- struct{}{}:
		default:
		}
	}
	return nil
}

func (s *recordingSink) events(t *testing.T) []Event {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []Event
	for _, data := range s.messages {
		var event Event
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatal(err)
		}
		result = append(result, event)
	}
	return result
}

func testRole(name, version string) unstructured.Unstructured {
	return unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "gcp.managed.openshift.io/v1", "kind": "Role",
		"metadata": map[string]interface{}{"namespace": "tenant-a", "name": name, "uid": "uid-" + name, "resourceVersion": version},
		"spec":     map[string]interface{}{"permissions": []interface{}{"cluster.get"}},
	}}
}

func testSource(t *testing.T, list func() (*unstructured.UnstructuredList, error), stream watch.Interface) *fake.FakeDynamicClient {
	t.Helper()
	resource := supportedResources["roles.gcp.managed.openshift.io"]
	api := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{resource: "RoleList"})
	api.PrependReactor("list", "roles", func(_ ktesting.Action) (bool, runtime.Object, error) {
		result, err := list()
		if result == nil {
			result = &unstructured.UnstructuredList{}
		}
		return true, result, err
	})
	if stream != nil {
		api.PrependWatchReactor("roles", func(_ ktesting.Action) (bool, watch.Interface, error) {
			return true, stream, nil
		})
	}
	return api
}

func testPublisher(t *testing.T, source *fake.FakeDynamicClient, sink *recordingSink, control ControlSource, region, leader string) *Publisher {
	t.Helper()
	p, err := NewPublisher(Config{Region: region, LeaderRegion: leader, Generation: 3,
		Resources:      []Resource{{ID: "roles.gcp.managed.openshift.io", GVR: supportedResources["roles.gcp.managed.openshift.io"]}},
		ResyncInterval: time.Hour}, source, sink, control, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

type quietControl struct{}

func (quietControl) Receive(ctx context.Context, _ func([]byte)) error { <-ctx.Done(); return nil }

type deniedControl struct{}

func (deniedControl) Receive(context.Context, func([]byte)) error {
	return status.Error(codes.PermissionDenied, "no subscription access")
}

func TestDeniedControlSubscriptionFailsVisibly(t *testing.T) {
	p := &Publisher{control: deniedControl{}, log: logr.Discard()}
	if err := p.consumeControl(context.Background()); err == nil {
		t.Fatal("expected a permission error from the control subscription")
	}
}

func TestSnapshotPublishesSourceVersionsAndInventory(t *testing.T) {
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testRole("admin", "4")}}
		list.SetResourceVersion("5")
		return list, nil
	}, nil)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	boundary, err := p.snapshot(context.Background(), p.config.Resources[0])
	if err != nil {
		t.Fatal(err)
	}
	if boundary != "5" {
		t.Fatalf("snapshot boundary = %q", boundary)
	}
	events := sink.events(t)
	if len(events) != 2 {
		t.Fatalf("expected object and inventory, got %d events", len(events))
	}
	if events[0].EventType != CreateOrUpdate || events[0].SourceResourceVersion != "4" || events[0].SnapshotResourceVersion != "5" || events[0].SyncID == "" {
		t.Fatalf("wrong object event: %+v", events[0])
	}
	if events[1].EventType != Inventory || events[1].SourceResourceVersion != "" || events[1].SnapshotResourceVersion != "5" || events[1].SyncID != events[0].SyncID || len(events[1].Inventory) != 1 || events[1].Inventory[0].SourceResourceVersion != "4" {
		t.Fatalf("wrong inventory event: %+v", events[1])
	}
}

func TestSnapshotBudgetIncludesEncodedEventsAndInventory(t *testing.T) {
	objects := []unstructured.Unstructured{testRole("first", "1"), testRole("second", "2")}
	for i := range objects {
		objects[i].Object["data"] = strings.Repeat("x", 600_000)
	}
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		list := &unstructured.UnstructuredList{Items: objects}
		list.SetResourceVersion("2")
		return list, nil
	}, nil)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	p.config.MaxCaptureBytes = 2 << 20
	resource := p.config.Resources[0]
	if _, err := p.snapshot(context.Background(), resource); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, message := range sink.messages {
		total += len(message)
	}
	if total <= MaxEventBytes+100 {
		t.Fatalf("snapshot too small to exercise configured capture limit: %d bytes", total)
	}

	// The raw-object estimate fits, but the messages the follower must buffer
	// exceed this budget. Reject the capture before publishing any messages.
	p.config.MaxCaptureBytes = total - 100
	if _, _, err := p.capture(context.Background(), resource); err != nil {
		t.Fatalf("raw capture should fit the budget: %v", err)
	}
	sink.messages = nil
	_, err := p.snapshot(context.Background(), resource)
	var budget *captureBudgetError
	if !errors.As(err, &budget) {
		t.Fatalf("expected encoded snapshot budget error, got %v", err)
	}
	if len(sink.messages) != 0 {
		t.Fatal("published a snapshot that the follower cannot buffer")
	}

	p.config.MaxCaptureBytes = total + 100
	if _, err := p.snapshot(context.Background(), resource); err != nil {
		t.Fatalf("snapshot within the encoded budget should publish: %v", err)
	}
	if len(sink.messages) != 3 {
		t.Fatalf("expected two objects and inventory, got %d messages", len(sink.messages))
	}
}

func TestEmptySnapshotPublishesInventory(t *testing.T) {
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		list := &unstructured.UnstructuredList{}
		list.SetResourceVersion("0")
		return list, nil
	}, nil)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	if _, err := p.snapshot(context.Background(), p.config.Resources[0]); err != nil {
		t.Fatal(err)
	}
	events := sink.events(t)
	if len(events) != 1 || events[0].EventType != Inventory || events[0].SnapshotResourceVersion != "0" || events[0].PageCount != 1 || len(events[0].Inventory) != 0 {
		t.Fatalf("wrong empty inventory: %+v", events)
	}
}

func TestWatchDeleteUsesAPIVersion(t *testing.T) {
	stream := watch.NewRaceFreeFake()
	api := testSource(t, func() (*unstructured.UnstructuredList, error) { t.Fatal("unexpected LIST"); return nil, nil }, stream)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	result := make(chan error, 1)
	go func() {
		version, _, err := p.watchFrom(context.Background(), p.config.Resources[0], "5")
		if err == nil && version != "7" {
			err = status.Errorf(codes.Internal, "last handled version = %s", version)
		}
		result <- err
	}()
	object := testRole("admin", "7")
	stream.Delete(&object)
	stream.Stop()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	events := sink.events(t)
	if len(events) != 1 || events[0].EventType != Delete || events[0].SourceResourceVersion != "7" || len(events[0].Object) != 0 {
		t.Fatalf("wrong DELETE event: %+v", events)
	}
}

func TestExpiredWatchRequestsFreshList(t *testing.T) {
	api := testSource(t, func() (*unstructured.UnstructuredList, error) { t.Fatal("unexpected LIST"); return nil, nil }, nil)
	api.PrependWatchReactor("roles", func(_ ktesting.Action) (bool, watch.Interface, error) {
		return true, nil, apierrors.NewResourceExpired("watch history expired")
	})
	p := testPublisher(t, api, &recordingSink{}, quietControl{}, "us-east1", "us-east1")
	version, relist, err := p.watchFrom(context.Background(), p.config.Resources[0], "5")
	if err != nil || !relist || version != "5" {
		t.Fatalf("watch expiration returned version=%q relist=%v err=%v", version, relist, err)
	}
}

func TestZeroBookmarkIsValidForEmptyCollection(t *testing.T) {
	stream := watch.NewRaceFreeFake()
	api := testSource(t, func() (*unstructured.UnstructuredList, error) { t.Fatal("unexpected LIST"); return nil, nil }, stream)
	p := testPublisher(t, api, &recordingSink{}, quietControl{}, "us-east1", "us-east1")
	result := make(chan error, 1)
	go func() {
		version, relist, err := p.watchFrom(context.Background(), p.config.Resources[0], "0")
		if err == nil && (version != "0" || relist) {
			err = fmt.Errorf("bookmark returned version=%q relist=%v", version, relist)
		}
		result <- err
	}()
	bookmark := &unstructured.Unstructured{}
	bookmark.SetResourceVersion("0")
	stream.Action(watch.Bookmark, bookmark)
	stream.Stop()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRoleBindingSnapshotAndMutations(t *testing.T) {
	resource := Resource{ID: "rolebindings.gcp.managed.openshift.io", GVR: supportedResources["rolebindings.gcp.managed.openshift.io"]}
	api := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{resource.GVR: "RoleBindingList"})
	makeBinding := func(version string) unstructured.Unstructured {
		return unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "gcp.managed.openshift.io/v1", "kind": "RoleBinding",
			"metadata": map[string]interface{}{"namespace": "tenant-a", "name": "admin", "uid": "binding-uid", "resourceVersion": version},
			"spec":     map[string]interface{}{"subject": "user@example.com", "roleRef": map[string]interface{}{"kind": "PlatformRole", "name": "admin", "apiGroup": "gcp.managed.openshift.io"}},
		}}
	}
	api.PrependReactor("list", "rolebindings", func(_ ktesting.Action) (bool, runtime.Object, error) {
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{makeBinding("5")}}
		list.SetResourceVersion("6")
		return true, list, nil
	})
	stream := watch.NewRaceFreeFake()
	api.PrependWatchReactor("rolebindings", func(_ ktesting.Action) (bool, watch.Interface, error) { return true, stream, nil })
	sink := &recordingSink{}
	p, err := NewPublisher(Config{Region: "us-east1", LeaderRegion: "us-east1", Generation: 3,
		Resources: []Resource{resource}, ResyncInterval: time.Hour}, api, sink, quietControl{}, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.snapshot(context.Background(), resource); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		version, _, err := p.watchFrom(context.Background(), resource, "6")
		if err == nil && version != "9" {
			err = fmt.Errorf("last handled version = %s", version)
		}
		result <- err
	}()
	added, modified, deleted := makeBinding("7"), makeBinding("8"), makeBinding("9")
	stream.Add(&added)
	stream.Modify(&modified)
	stream.Delete(&deleted)
	stream.Stop()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	events := sink.events(t)
	if len(events) != 5 {
		t.Fatalf("expected snapshot, inventory, and 3 mutations, got %d", len(events))
	}
	for i, expected := range []struct{ kind, version string }{{CreateOrUpdate, "7"}, {CreateOrUpdate, "8"}, {Delete, "9"}} {
		event := events[i+2]
		if event.EventType != expected.kind || event.SourceResourceVersion != expected.version || event.ResourceKind != resource.ID {
			t.Fatalf("mutation %d = %+v", i, event)
		}
	}
}

func TestExpiredContinuationStartsNewCapture(t *testing.T) {
	var calls int
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		calls++
		if calls == 2 {
			return nil, apierrors.NewResourceExpired("continuation expired")
		}
		list := &unstructured.UnstructuredList{}
		if calls == 1 {
			list.Items = []unstructured.Unstructured{testRole("old", "4")}
			list.SetResourceVersion("5")
			list.SetContinue("next-page")
		} else {
			list.Items = []unstructured.Unstructured{testRole("current", "7")}
			list.SetResourceVersion("7")
		}
		return list, nil
	}, nil)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	boundary, err := p.retrySnapshot(ctx, p.config.Resources[0])
	if err != nil {
		t.Fatal(err)
	}
	if boundary != "7" || calls != 3 {
		t.Fatalf("boundary %q after %d LIST calls", boundary, calls)
	}
	events := sink.events(t)
	if len(events) != 2 || events[0].Name != "current" || events[1].Inventory[0].Name != "current" {
		t.Fatalf("published objects from an expired capture: %+v", events)
	}
}

type channelControl struct{ messages chan []byte }

func (s channelControl) Receive(ctx context.Context, handle func([]byte)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-s.messages:
			handle(data)
		}
	}
}

func TestResyncRequestPublishesFreshSnapshot(t *testing.T) {
	var mu sync.Mutex
	listCalls := 0
	stream := watch.NewRaceFreeFake()
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		mu.Lock()
		listCalls++
		version := listCalls
		mu.Unlock()
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testRole("admin", "1")}}
		list.SetResourceVersion(fmt.Sprint(version))
		return list, nil
	}, stream)
	sink := &recordingSink{updates: make(chan struct{}, 4)}
	control := channelControl{messages: make(chan []byte, 1)}
	p := testPublisher(t, api, sink, control, "us-east1", "us-east1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- p.Run(ctx) }()
	deadline := time.After(4 * time.Second)
	for len(sink.events(t)) < 2 {
		select {
		case <-sink.updates:
		case <-deadline:
			t.Fatal("initial snapshot was not published")
		}
	}
	data, err := json.Marshal(Event{EventType: ResyncRequest, ResourceKind: "roles.gcp.managed.openshift.io", Generation: 3})
	if err != nil {
		t.Fatal(err)
	}
	control.messages <- data
	for len(sink.events(t)) < 4 {
		select {
		case <-sink.updates:
		case <-deadline:
			t.Fatal("resync request did not publish a fresh snapshot")
		}
	}
	cancel()
	if err := <-result; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	events := sink.events(t)
	if events[0].SyncID == events[2].SyncID || events[2].SnapshotResourceVersion != "2" {
		t.Fatalf("resync did not create a fresh capture: %+v", events)
	}
}

func TestPermanentResourceErrorDegradesWithoutCrashing(t *testing.T) {
	var mu sync.Mutex
	listCalls := 0
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		mu.Lock()
		listCalls++
		mu.Unlock()
		object := testRole("admin", "1")
		object.Object["data"] = strings.Repeat("x", MaxEventBytes) // Permanently oversized.
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{object}}
		list.SetResourceVersion("1")
		return list, nil
	}, nil)
	p, err := NewPublisher(Config{Region: "us-east1", LeaderRegion: "us-east1", Generation: 3,
		Resources:      []Resource{{ID: "roles.gcp.managed.openshift.io", GVR: supportedResources["roles.gcp.managed.openshift.io"]}},
		ResyncInterval: 20 * time.Millisecond}, api, &recordingSink{}, quietControl{}, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.runResource(ctx, p.config.Resources[0]) }()

	// A permanent per-resource failure must degrade and retry, not return. Seeing
	// repeated LIST attempts proves the resource keeps running after failing.
	calls := func() int { mu.Lock(); defer mu.Unlock(); return listCalls }
	deadline := time.After(3 * time.Second)
	for calls() < 2 {
		select {
		case <-deadline:
			t.Fatal("permanent resource error was not retried; the resource loop stopped")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("permanent per-resource error should not stop the resource loop or crash, got %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRequestedResyncsAreRateLimited(t *testing.T) {
	var mu sync.Mutex
	lists := 0
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		mu.Lock()
		lists++
		mu.Unlock()
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{testRole("admin", "1")}}
		list.SetResourceVersion("1")
		return list, nil
	}, nil)
	p, err := NewPublisher(Config{Region: "us-east1", LeaderRegion: "us-east1", Generation: 3,
		Resources:      []Resource{{ID: "roles.gcp.managed.openshift.io", GVR: supportedResources["roles.gcp.managed.openshift.io"]}},
		ResyncInterval: time.Hour, MinResyncGap: 200 * time.Millisecond}, api, &recordingSink{}, quietControl{}, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	resource := p.config.Resources[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.periodicSnapshots(ctx, resource, p.locks[resource.ID]) }()

	count := func() int { mu.Lock(); defer mu.Unlock(); return lists }
	request := func() { p.requests[resource.ID] <- struct{}{} }

	// The first request snapshots immediately.
	request()
	waitFor(t, func() bool { return count() == 1 }, time.Second, "first requested snapshot")

	// A second request within the gap is coalesced, not run immediately.
	request()
	time.Sleep(50 * time.Millisecond)
	if c := count(); c != 1 {
		t.Fatalf("second request within the gap was not rate-limited: %d snapshots", c)
	}

	// It runs exactly once more after the gap elapses.
	waitFor(t, func() bool { return count() == 2 }, time.Second, "coalesced snapshot after the gap")
	time.Sleep(100 * time.Millisecond)
	if c := count(); c != 2 {
		t.Fatalf("expected exactly 2 snapshots after coalescing, got %d", c)
	}
}

func TestOversizedResourceStopsBeforeInventory(t *testing.T) {
	api := testSource(t, func() (*unstructured.UnstructuredList, error) {
		object := testRole("admin", "1")
		object.Object["data"] = strings.Repeat("x", MaxEventBytes)
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{object}}
		list.SetResourceVersion("1")
		return list, nil
	}, nil)
	sink := &recordingSink{}
	p := testPublisher(t, api, sink, quietControl{}, "us-east1", "us-east1")
	_, err := p.snapshot(context.Background(), p.config.Resources[0])
	var oversized *oversizedEventError
	if !errors.As(err, &oversized) {
		t.Fatalf("expected oversized resource error, got %v", err)
	}
	if len(sink.events(t)) != 0 {
		t.Fatal("published incomplete oversized snapshot")
	}
}

func TestRetryPreservesCapturedBytes(t *testing.T) {
	sink := &recordingSink{failOnce: true}
	p := &Publisher{config: Config{Region: "us-east1", LeaderRegion: "us-east1"}, sink: sink, metrics: NewMetrics(prometheus.NewRegistry()), log: logr.Discard()}
	payload := []byte(`{"sourceResourceVersion":"42"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.publishData(ctx, "roles.gcp.managed.openshift.io", Delete, payload); err != nil {
		t.Fatal(err)
	}
	if len(sink.messages) != 2 || !bytes.Equal(sink.messages[0], sink.messages[1]) {
		t.Fatalf("publish retry changed the captured payload: %q", sink.messages)
	}
}

func TestFollowerCannotPublishData(t *testing.T) {
	sink := &recordingSink{}
	p := &Publisher{config: Config{Region: "eu-west1", LeaderRegion: "us-east1"}, sink: sink, metrics: NewMetrics(prometheus.NewRegistry()), log: logr.Discard()}
	if err := p.publishData(context.Background(), "roles.gcp.managed.openshift.io", Delete, []byte(`{}`)); err == nil {
		t.Fatal("follower data publish succeeded")
	}
	if len(sink.messages) != 0 {
		t.Fatal("follower reached the data topic")
	}
}

func TestInventoryPagesUseEncodedSize(t *testing.T) {
	p := &Publisher{config: Config{Region: "us-east1", LeaderRegion: "us-east1", Generation: 3}, metrics: NewMetrics(prometheus.NewRegistry())}
	resource := Resource{ID: "roles.gcp.managed.openshift.io"}
	refs := []ObjectRef{
		{Name: "first", SourceResourceVersion: "1", SourceUID: strings.Repeat("a", 600000)},
		{Name: "second", SourceResourceVersion: "2", SourceUID: strings.Repeat("b", 600000)},
	}
	pages, err := p.inventoryPages(resource, "2", "capture", time.Now(), refs)
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 {
		t.Fatalf("expected 2 byte-bounded pages, got %d", len(pages))
	}
	for index, page := range pages {
		if len(page) > MaxEventBytes {
			t.Fatalf("page %d has %d bytes", index, len(page))
		}
		var event Event
		if err := json.Unmarshal(page, &event); err != nil {
			t.Fatal(err)
		}
		if event.PageIndex != uint32(index) || event.PageCount != 2 || len(event.Inventory) != 1 {
			t.Fatalf("wrong page metadata: %+v", event)
		}
	}
}
