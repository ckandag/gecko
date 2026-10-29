package replication

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

const roleResourceID = "roles.gcp.managed.openshift.io"

func testMirrorAPI() *fake.FakeDynamicClient {
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		supportedResources[roleResourceID]:                          "RoleList",
		supportedResources["rolebindings.gcp.managed.openshift.io"]: "RoleBindingList",
		namespaceResource: "NamespaceList",
	})
}

func testMirror(t *testing.T, api *fake.FakeDynamicClient) *APIMirror {
	t.Helper()
	mirror, err := NewAPIMirror(api, Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3, ResyncInterval: time.Hour, MaxCaptureBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return mirror
}

func roleEvent(t *testing.T, name, version string) Event {
	t.Helper()
	object := testRole(name, version)
	data, err := json.Marshal(object.Object)
	if err != nil {
		t.Fatal(err)
	}
	return Event{
		EventType: CreateOrUpdate, ResourceKind: roleResourceID,
		OriginRegion: "us-central1", Generation: 3, Namespace: "tenant-a",
		Name: name, SourceResourceVersion: version, Object: data, UpdatedAt: time.Now().UTC(),
	}
}

func TestAPIMirrorAppliesMutationAndKeepsLocalMetadata(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	create := roleEvent(t, "admin", "1")
	if applied, err := mirror.ApplyIncremental(ctx, create); err != nil || !applied {
		t.Fatalf("create applied=%v, err=%v", applied, err)
	}
	if _, err := api.Resource(namespaceResource).Get(ctx, "tenant-a", metav1.GetOptions{}); err != nil {
		t.Fatalf("target namespace was not created: %v", err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	local, err := roles.Get(ctx, "admin", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(local.GetUID()) == "uid-admin" || local.GetResourceVersion() == "1" {
		t.Fatalf("source server metadata copied to mirror: uid=%q rv=%q", local.GetUID(), local.GetResourceVersion())
	}
	if local.GetAnnotations()[replicatedFromAnnotation] != "us-central1" || local.GetAnnotations()[sourceVersionAnnotation] != "1" {
		t.Fatalf("missing mirror provenance: %v", local.GetAnnotations())
	}
	update := roleEvent(t, "admin", "2")
	if applied, err := mirror.ApplyIncremental(ctx, update); err != nil || !applied {
		t.Fatalf("update applied=%v, err=%v", applied, err)
	}
	deleteEvent := Event{EventType: Delete, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3, Namespace: "tenant-a", Name: "admin", SourceResourceVersion: "3", UpdatedAt: time.Now().UTC()}
	if applied, err := mirror.ApplyIncremental(ctx, deleteEvent); err != nil || !applied {
		t.Fatalf("delete applied=%v, err=%v", applied, err)
	}
	if _, err := roles.Get(ctx, "admin", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Role still exists after delete: %v", err)
	}
	if applied, err := mirror.ApplyIncremental(ctx, update); err != nil || applied {
		t.Fatalf("same-process stale upsert applied=%v, err=%v", applied, err)
	}
}

func TestAPIMirrorEnsuresNamespaceOnceForMultipleObjects(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	for _, name := range []string{"first", "second"} {
		if applied, err := mirror.ApplyIncremental(ctx, roleEvent(t, name, "1")); err != nil || !applied {
			t.Fatalf("apply %s: applied=%v err=%v", name, applied, err)
		}
	}
	namespaceGets := 0
	for _, action := range api.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "namespaces" {
			namespaceGets++
		}
	}
	if namespaceGets != 1 {
		t.Fatalf("expected one namespace GET, got %d", namespaceGets)
	}
}

func TestAPIMirrorAcceptsEqualVersionDeleteAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	role := roleEvent(t, "admin", "41")
	role.SyncID = "capture-before-delete"
	role.SnapshotResourceVersion = "50"
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "50", SyncID: role.SyncID, PageCount: 1, UpdatedAt: time.Now().UTC(),
		Inventory: []ObjectRef{{Namespace: "tenant-a", Name: "admin", SourceResourceVersion: "41", SourceUID: "uid-admin"}}}
	if _, err := mirror.StageSnapshot(ctx, role); err != nil {
		t.Fatal(err)
	}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || !result.Completed {
		t.Fatalf("snapshot result=%+v, err=%v", result, err)
	}
	deleteEvent := Event{EventType: Delete, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		Namespace: "tenant-a", Name: "admin", SourceResourceVersion: "41", UpdatedAt: time.Now().UTC()}
	if applied, err := mirror.ApplyIncremental(ctx, deleteEvent); err != nil || !applied {
		t.Fatalf("equal-version delete applied=%v, err=%v", applied, err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	if _, err := roles.Get(ctx, "admin", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("equal-version delete left Role behind: %v", err)
	}
	if applied, err := mirror.ApplyIncremental(ctx, deleteEvent); err != nil || applied {
		t.Fatalf("duplicate delete applied=%v, err=%v", applied, err)
	}
	if applied, err := mirror.ApplyIncremental(ctx, roleEvent(t, "admin", "51")); err != nil || !applied {
		t.Fatalf("newer upsert applied=%v, err=%v", applied, err)
	}
	if applied, err := mirror.ApplyIncremental(ctx, deleteEvent); err != nil || applied {
		t.Fatalf("older delete removed newer Role: applied=%v, err=%v", applied, err)
	}
	if _, err := roles.Get(ctx, "admin", metav1.GetOptions{}); err != nil {
		t.Fatalf("older delete removed newer Role: %v", err)
	}
}

func TestAPIMirrorRepairsDriftWithCompleteInventory(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	role := roleEvent(t, "viewer", "4")
	role.SyncID = "capture-1"
	role.SnapshotResourceVersion = "5"
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "5", SyncID: "capture-1", PageCount: 1, PageIndex: 0, UpdatedAt: time.Now().UTC(),
		Inventory: []ObjectRef{{Namespace: "tenant-a", Name: "viewer", SourceResourceVersion: "4", SourceUID: "uid-viewer"}}}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || result.Completed {
		t.Fatalf("incomplete capture result=%+v, err=%v", result, err)
	}
	if result, err := mirror.StageSnapshot(ctx, role); err != nil || !result.Completed || result.Applied != 1 {
		t.Fatalf("completed capture result=%+v, err=%v", result, err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	if err := roles.Delete(ctx, "viewer", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	role.SyncID = "capture-2"
	page.SyncID = "capture-2"
	if _, err := mirror.StageSnapshot(ctx, role); err != nil {
		t.Fatal(err)
	}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || !result.Completed || result.Applied != 1 {
		t.Fatalf("same-boundary repair result=%+v, err=%v", result, err)
	}
	if _, err := roles.Get(ctx, "viewer", metav1.GetOptions{}); err != nil {
		t.Fatalf("drifted Role was not restored: %v", err)
	}
}

func TestAPIMirrorNeverPrunesFromIncompleteInventory(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	if _, err := mirror.ApplyIncremental(ctx, roleEvent(t, "old", "1")); err != nil {
		t.Fatal(err)
	}
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "2", SyncID: "capture", PageCount: 2, PageIndex: 0, UpdatedAt: time.Now().UTC()}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || result.Completed || result.Pruned != 0 {
		t.Fatalf("incomplete inventory result=%+v, err=%v", result, err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	if _, err := roles.Get(ctx, "old", metav1.GetOptions{}); err != nil {
		t.Fatalf("incomplete inventory pruned Role: %v", err)
	}
	page.PageIndex = 1
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || !result.Completed || result.Pruned != 1 {
		t.Fatalf("complete empty inventory result=%+v, err=%v", result, err)
	}
	if _, err := roles.Get(ctx, "old", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("complete inventory did not prune Role: %v", err)
	}
	if len(mirror.seen) != 0 {
		t.Fatalf("completed inventory retained %d obsolete tombstones", len(mirror.seen))
	}
	if applied, err := mirror.ApplyIncremental(ctx, roleEvent(t, "old", "1")); err != nil || applied {
		t.Fatalf("old event restored a pruned Role: applied=%v err=%v", applied, err)
	}
	oldDelete := Event{EventType: Delete, ResourceKind: roleResourceID, Namespace: "tenant-a", Name: "old", SourceResourceVersion: "1"}
	if applied, err := mirror.ApplyIncremental(ctx, oldDelete); err != nil || applied || len(mirror.seen) != 0 {
		t.Fatalf("old delete recreated a tombstone: applied=%v err=%v seen=%d", applied, err, len(mirror.seen))
	}
}

func TestAPIMirrorPrunesAcrossLocalListPages(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	resource := supportedResources[roleResourceID]
	roles := api.Resource(resource).Namespace("tenant-a")
	objects := []unstructured.Unstructured{testRole("first", "1"), testRole("second", "2")}
	for i := range objects {
		if _, err := roles.Create(ctx, &objects[i], metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	firstDeleted := false
	api.PrependReactor("delete", "roles", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.DeleteAction).GetName() == "first" {
			firstDeleted = true
		}
		return false, nil, nil
	})
	listCalls := 0
	api.PrependReactor("list", "roles", func(_ ktesting.Action) (bool, runtime.Object, error) {
		listCalls++
		if listCalls > 2 {
			t.Fatal("unexpected extra local LIST")
		}
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{objects[listCalls-1]}}
		list.SetResourceVersion("2")
		if listCalls == 1 {
			list.SetContinue("next-page")
		} else if firstDeleted {
			// A local delete before reading this page would advance Spanner's
			// collection version and invalidate a consistent paginated LIST.
			list.SetResourceVersion("3")
		}
		return true, list, nil
	})
	mirror := testMirror(t, api)
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "2", SyncID: "empty-capture", PageCount: 1, UpdatedAt: time.Now().UTC()}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || !result.Completed || result.Pruned != 2 {
		t.Fatalf("paginated prune result=%+v err=%v", result, err)
	}
	if listCalls != 2 {
		t.Fatalf("expected two local LIST pages, got %d", listCalls)
	}
	for _, name := range []string{"first", "second"} {
		if _, err := roles.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("Role %s survived prune: %v", name, err)
		}
	}
}

func TestAPIMirrorRoutesRoleBindingToItsResource(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	binding := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gcp.managed.openshift.io/v1", "kind": "RoleBinding",
		"metadata": map[string]any{"namespace": "tenant-a", "name": "viewer", "uid": "source-binding", "resourceVersion": "7"},
		"spec":     map[string]any{"subject": "user@example.com", "roleRef": map[string]any{"kind": "Role", "name": "viewer"}},
	}}
	data, err := json.Marshal(binding.Object)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{EventType: CreateOrUpdate, ResourceKind: "rolebindings.gcp.managed.openshift.io", OriginRegion: "us-central1", Generation: 3,
		Namespace: "tenant-a", Name: "viewer", SourceResourceVersion: "7", Object: data, UpdatedAt: time.Now().UTC()}
	if applied, err := mirror.ApplyIncremental(ctx, event); err != nil || !applied {
		t.Fatalf("binding applied=%v, err=%v", applied, err)
	}
	if _, err := api.Resource(supportedResources[event.ResourceKind]).Namespace("tenant-a").Get(ctx, "viewer", metav1.GetOptions{}); err != nil {
		t.Fatalf("RoleBinding absent from local API: %v", err)
	}
}

func TestAPIMirrorDoesNotAcknowledgePendingDeletion(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	if _, err := mirror.ApplyIncremental(ctx, roleEvent(t, "admin", "1")); err != nil {
		t.Fatal(err)
	}
	// Simulate an API delete accepted while a finalizer keeps the object.
	api.PrependReactor("delete", "roles", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, nil
	})
	deleteEvent := Event{EventType: Delete, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		Namespace: "tenant-a", Name: "admin", SourceResourceVersion: "2", UpdatedAt: time.Now().UTC()}
	if _, err := mirror.ApplyIncremental(ctx, deleteEvent); err == nil {
		t.Fatal("expected pending deletion to remain retryable")
	}
	if _, err := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a").Get(ctx, "admin", metav1.GetOptions{}); err != nil {
		t.Fatalf("pending object unexpectedly disappeared: %v", err)
	}
}

func TestAPIMirrorAcceptsNewLeaderVersionsAndPrunesOldLeaderState(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	oldMirror := testMirror(t, api)
	old := roleEvent(t, "stale", "100")
	if _, err := oldMirror.ApplyIncremental(ctx, old); err != nil {
		t.Fatal(err)
	}
	newMirror, err := NewAPIMirror(api, Config{Region: "us-west1", LeaderRegion: "us-east1", Generation: 4, ResyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	newRole := roleEvent(t, "current", "5")
	newRole.OriginRegion = "us-east1"
	newRole.Generation = 4
	newRole.SyncID = "new-leader"
	newRole.SnapshotResourceVersion = "6"
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-east1", Generation: 4,
		SnapshotResourceVersion: "6", SyncID: "new-leader", PageCount: 1, UpdatedAt: time.Now().UTC(),
		Inventory: []ObjectRef{{Namespace: "tenant-a", Name: "current", SourceResourceVersion: "5", SourceUID: "uid-current"}}}
	if _, err := newMirror.StageSnapshot(ctx, newRole); err != nil {
		t.Fatal(err)
	}
	if result, err := newMirror.StageSnapshot(ctx, page); err != nil || !result.Completed || result.Pruned != 1 {
		t.Fatalf("new leader capture result=%+v, err=%v", result, err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	if _, err := roles.Get(ctx, "stale", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("old-leader Role still exists: %v", err)
	}
	current, err := roles.Get(ctx, "current", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current.GetAnnotations()[sourceGenerationAnnotation] != "4" || current.GetAnnotations()[sourceVersionAnnotation] != "5" {
		t.Fatalf("new leader provenance missing: %v", current.GetAnnotations())
	}
}

func TestAPIMirrorResyncRemovesStalePostDeleteDelivery(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	first := testMirror(t, api)
	if _, err := first.ApplyIncremental(ctx, roleEvent(t, "deleted", "41")); err != nil {
		t.Fatal(err)
	}
	deleted := Event{EventType: Delete, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		Namespace: "tenant-a", Name: "deleted", SourceResourceVersion: "42", UpdatedAt: time.Now().UTC()}
	if _, err := first.ApplyIncremental(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	// Simulate a process restart and a delayed older delivery: the in-memory
	// delete version is gone, so this first-stage mirror may recreate the Role.
	restarted := testMirror(t, api)
	if _, err := restarted.ApplyIncremental(ctx, roleEvent(t, "deleted", "41")); err != nil {
		t.Fatal(err)
	}
	roles := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a")
	if _, err := roles.Get(ctx, "deleted", metav1.GetOptions{}); err != nil {
		t.Fatalf("expected temporary stale Role after restart: %v", err)
	}
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "43", SyncID: "after-delete", PageCount: 1, UpdatedAt: time.Now().UTC()}
	if result, err := restarted.StageSnapshot(ctx, page); err != nil || !result.Completed || result.Pruned != 1 {
		t.Fatalf("resync result=%+v, err=%v", result, err)
	}
	if _, err := roles.Get(ctx, "deleted", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("resync did not remove stale Role: %v", err)
	}
}

func TestAPIMirrorReplacesSameKeyAfterLeaderChange(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	oldMirror := testMirror(t, api)
	if _, err := oldMirror.ApplyIncremental(ctx, roleEvent(t, "shared", "100")); err != nil {
		t.Fatal(err)
	}
	newMirror, err := NewAPIMirror(api, Config{Region: "us-west1", LeaderRegion: "us-east1", Generation: 4, ResyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	newRole := roleEvent(t, "shared", "5")
	newRole.OriginRegion = "us-east1"
	newRole.Generation = 4
	if applied, err := newMirror.ApplyIncremental(ctx, newRole); err != nil || !applied {
		t.Fatalf("new leader update applied=%v, err=%v", applied, err)
	}
	local, err := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a").Get(ctx, "shared", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if local.GetAnnotations()[sourceVersionAnnotation] != "5" || local.GetAnnotations()[sourceGenerationAnnotation] != "4" || local.GetAnnotations()[replicatedFromAnnotation] != "us-east1" {
		t.Fatalf("old leader version blocked new leader: %v", local.GetAnnotations())
	}
}

func TestAPIMirrorQuarantinesInvalidCaptureID(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	if _, err := mirror.ApplyIncremental(ctx, roleEvent(t, "old", "1")); err != nil {
		t.Fatal(err)
	}
	payload := roleEvent(t, "current", "2")
	payload.SyncID = "bad-capture"
	payload.SnapshotResourceVersion = "3"
	if _, err := mirror.StageSnapshot(ctx, payload); err != nil {
		t.Fatal(err)
	}
	conflicting := payload
	var changed map[string]any
	if err := json.Unmarshal(payload.Object, &changed); err != nil {
		t.Fatal(err)
	}
	changed["spec"] = map[string]any{"permissions": []any{"cluster.list"}}
	conflicting.Object, _ = json.Marshal(changed)
	if result, err := mirror.StageSnapshot(ctx, conflicting); err != nil || !result.RequestResync {
		t.Fatalf("conflicting payload result=%+v, err=%v", result, err)
	}
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3,
		SnapshotResourceVersion: "3", SyncID: "bad-capture", PageCount: 1, UpdatedAt: time.Now().UTC(),
		Inventory: []ObjectRef{{Namespace: "tenant-a", Name: "current", SourceResourceVersion: "2", SourceUID: "uid-current"}}}
	if result, err := mirror.StageSnapshot(ctx, page); err != nil || result.Completed {
		t.Fatalf("invalid SyncID completed: result=%+v, err=%v", result, err)
	}
	if _, err := api.Resource(supportedResources[roleResourceID]).Namespace("tenant-a").Get(ctx, "old", metav1.GetOptions{}); err != nil {
		t.Fatalf("invalid capture pruned local Role: %v", err)
	}
}

func TestAPIMirrorExpiresOnlyConfirmedInvalidCaptures(t *testing.T) {
	mirror := testMirror(t, testMirrorAPI())
	old := time.Now().Add(-invalidCaptureRetention - time.Hour)
	confirmed := captureKey{kind: roleResourceID, syncID: "confirmed"}
	pending := captureKey{kind: roleResourceID, syncID: "pending"}
	mirror.invalid[confirmed] = invalidCapture{requested: true, createdAt: old}
	mirror.invalid[pending] = invalidCapture{createdAt: old}
	page := Event{EventType: Inventory, ResourceKind: roleResourceID, SyncID: "new-capture",
		SnapshotResourceVersion: "1", PageCount: 2, UpdatedAt: time.Now().UTC()}
	if _, err := mirror.StageSnapshot(context.Background(), page); err != nil {
		t.Fatal(err)
	}
	if _, exists := mirror.invalid[confirmed]; exists {
		t.Fatal("confirmed invalid capture remained beyond data retention")
	}
	if _, exists := mirror.invalid[pending]; !exists {
		t.Fatal("unpublished resync request was forgotten")
	}
}

type failingControlSink struct {
	attempts int
	failure  error
}

func (s *failingControlSink) Publish(context.Context, []byte) error {
	s.attempts++
	if s.attempts == 1 {
		return s.failure
	}
	return nil
}

func TestAbandonedCaptureRetriesFailedResyncPublish(t *testing.T) {
	ctx := context.Background()
	api := testMirrorAPI()
	mirror := testMirror(t, api)
	sink := &failingControlSink{failure: status.Error(codes.PermissionDenied, "control topic unavailable")}
	receiver, err := NewReceiver(Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3,
		Resources: []Resource{{ID: roleResourceID, GVR: supportedResources[roleResourceID]}}},
		unusedDataSource{}, sink, mirror, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	payload := roleEvent(t, "current", "2")
	payload.SyncID = "bad-capture"
	payload.SnapshotResourceVersion = "3"
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Handle(ctx, data); err != nil {
		t.Fatal(err)
	}
	var changed map[string]any
	if err := json.Unmarshal(payload.Object, &changed); err != nil {
		t.Fatal(err)
	}
	changed["spec"] = map[string]any{"permissions": []any{"cluster.list"}}
	payload.Object, err = json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	conflicting, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Handle(ctx, conflicting); err == nil || sink.attempts != 1 {
		t.Fatalf("expected failed control publish and one attempt, got err=%v attempts=%d", err, sink.attempts)
	}
	if err := receiver.Handle(ctx, conflicting); err != nil || sink.attempts != 2 {
		t.Fatalf("redelivery did not retry control publish: err=%v attempts=%d", err, sink.attempts)
	}
	if err := receiver.Handle(ctx, conflicting); err != nil || sink.attempts != 2 {
		t.Fatalf("confirmed request was republished: err=%v attempts=%d", err, sink.attempts)
	}
}

func TestBootstrapResyncRetriesTransientPublishError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sink := &failingControlSink{failure: status.Error(codes.Unavailable, "temporary outage")}
	receiver, err := NewReceiver(Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3,
		Resources: []Resource{{ID: roleResourceID, GVR: supportedResources[roleResourceID]}}},
		cancelingDataSource{cancel: cancel}, sink, &fakeMirror{}, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Run(ctx); err != nil || sink.attempts != 2 {
		t.Fatalf("bootstrap did not retry transient error: err=%v attempts=%d", err, sink.attempts)
	}
}

type fakeMirror struct{ applied int }

func (m *fakeMirror) ApplyIncremental(context.Context, Event) (bool, error) {
	m.applied++
	return true, nil
}
func (m *fakeMirror) StageSnapshot(context.Context, Event) (StageResult, error) {
	return StageResult{}, nil
}
func (m *fakeMirror) ConfirmResyncRequested(string, string) {}

type unusedDataSource struct{}

func (unusedDataSource) Receive(context.Context, func(context.Context, []byte) error) error {
	return nil
}

type cancelingDataSource struct{ cancel context.CancelFunc }

func (s cancelingDataSource) Receive(ctx context.Context, _ func(context.Context, []byte) error) error {
	s.cancel()
	<-ctx.Done()
	return nil
}

func TestFollowerRequestsBootstrapSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &recordingSink{}
	resource := Resource{ID: roleResourceID, GVR: supportedResources[roleResourceID]}
	receiver, err := NewReceiver(Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3, Resources: []Resource{resource}}, cancelingDataSource{cancel}, sink, &fakeMirror{}, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.Run(ctx); err != nil {
		t.Fatal(err)
	}
	events := sink.events(t)
	if len(events) != 1 || events[0].EventType != ResyncRequest || events[0].Generation != 3 || events[0].OriginRegion != "us-west1" {
		t.Fatalf("unexpected bootstrap requests: %+v", events)
	}
}

func TestReceiverRejectsInvalidOrOldMessagesBeforeApply(t *testing.T) {
	mirror := &fakeMirror{}
	resource := Resource{ID: roleResourceID, GVR: supportedResources[roleResourceID]}
	receiver, err := NewReceiver(Config{Region: "us-west1", LeaderRegion: "us-central1", Generation: 3, Resources: []Resource{resource}}, unusedDataSource{}, &recordingSink{}, mirror, NewMetrics(prometheus.NewRegistry()), logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	valid := roleEvent(t, "admin", "1")
	for _, event := range []Event{
		{EventType: ResyncRequest, ResourceKind: roleResourceID, OriginRegion: "us-central1", Generation: 3, UpdatedAt: time.Now().UTC()},
		func() Event { e := valid; e.OriginRegion = "us-east1"; return e }(),
		func() Event { e := valid; e.Generation = 2; return e }(),
		func() Event { e := valid; e.SourceResourceVersion = "bad"; return e }(),
	} {
		data, _ := json.Marshal(event)
		if err := receiver.Handle(context.Background(), data); err != nil {
			t.Fatal(err)
		}
	}
	if mirror.applied != 0 {
		t.Fatalf("invalid message reached mirror %d times", mirror.applied)
	}
	data, _ := json.Marshal(valid)
	if err := receiver.Handle(context.Background(), data); err != nil || mirror.applied != 1 {
		t.Fatalf("valid message err=%v, applied=%d", err, mirror.applied)
	}
}
