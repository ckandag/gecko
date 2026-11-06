package v1_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-logr/logr"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/handlers"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
	publicv1 "github.com/openshift-online/gecko/platform-api/api/public/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func TestNodePoolUpgradePublicAPI(t *testing.T) {
	privateScheme, publicScheme := runtime.NewScheme(), runtime.NewScheme()
	if err := privatev1.AddToScheme(privateScheme); err != nil {
		t.Fatal(err)
	}
	if err := publicv1.AddToScheme(publicScheme); err != nil {
		t.Fatal(err)
	}
	gvk := privatev1.GroupVersion.WithKind("NodePool")
	store := memory.NewMemoryStore("nodepools", privateScheme, gvk)
	pool := &privatev1.NodePool{
		TypeMeta:   metav1.TypeMeta{APIVersion: gvk.GroupVersion().String(), Kind: "NodePool"},
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "customer", Generation: 1},
		Spec: privatev1.NodePoolSpec{
			ClusterID: "example", Release: privatev1.ReleaseSpec{Version: "4.22.14", ChannelGroup: "stable"},
			Platform: privatev1.NodePoolPlatformSpec{Type: "GCP", GCP: &privatev1.GCPNodePoolPlatform{MachineType: "n2-standard-4", Zone: "us-central1-a", Subnet: "workers"}},
		},
		Status: privatev1.NodePoolStatus{Version: "4.22.14", VersionResolution: &privatev1.VersionResolutionResult{ReleaseImage: "private-image", ReleaseVersion: "4.22.14"}},
	}
	if err := store.Create(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	converter := conversion.NewConverter(publicScheme, privateScheme, "")
	handler := handlers.NewConvertingResourceHandler(store, maintenanceClusterSchemaProcessor(t, publicv1.NodePoolSchemaYAML), converter, gvk, "nodepools", publicScheme, privateScheme, nil, logr.Discard())
	router := chi.NewRouter()
	base := "/apis/gcp.managed.openshift.io/v1/namespaces/{namespace}/nodepools/{name}"
	router.Get(base, handler.Get)
	router.Put(base, handler.Update)
	router.Patch(base, handler.Patch)
	path := "/apis/gcp.managed.openshift.io/v1/namespaces/customer/nodepools/workers"
	request := func(method string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		if method == http.MethodPatch {
			req.Header.Set("Content-Type", "application/merge-patch+json")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	read := func() *privatev1.NodePool {
		t.Helper()
		obj, err := store.Get(t.Context(), "customer", "workers")
		if err != nil {
			t.Fatal(err)
		}
		return obj.(*privatev1.NodePool)
	}
	assertOK := func(response *httptest.ResponseRecorder) {
		t.Helper()
		if response.Code != http.StatusOK {
			t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
		}
	}
	// The desired version changes immediately; observed and private state do not.
	assertOK(request(http.MethodPatch, map[string]any{
		"spec":   map[string]any{"release": map[string]any{"version": "4.22.16", "upgradeStartTime": "2026-11-07T12:00:00Z"}},
		"status": map[string]any{"version": "99.0.0"},
	}))
	updated := read()
	if updated.Spec.Release.Version != "4.22.16" || updated.Status.Version != "4.22.14" || updated.Spec.Release.UpgradeStartTime == nil {
		t.Fatal("desired and observed versions or schedule were not preserved")
	}
	// Ordinary PUT omission preserves the pending schedule and private status.
	publicObj, err := converter.PrivateToPublic(updated)
	if err != nil {
		t.Fatal(err)
	}
	publicPool := publicObj.(*publicv1.NodePool)
	publicPool.Spec.Release.UpgradeStartTime = nil
	publicPool.Spec.NodeCount = ptr.To(int32(3))
	assertOK(request(http.MethodPut, publicPool))
	updated = read()
	if updated.Spec.Release.UpgradeStartTime == nil || updated.Status.VersionResolution.ReleaseImage != "private-image" {
		t.Fatal("unrelated update lost schedule or private status")
	}
	// Explicit null removes the schedule, making the desired upgrade immediate.
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method+" clear schedule", func(t *testing.T) {
			assertOK(request(http.MethodPatch, map[string]any{"spec": map[string]any{"release": map[string]any{"upgradeStartTime": "2026-11-07T12:00:00Z"}}}))
			var body map[string]any
			if method == http.MethodPut {
				get := request(http.MethodGet, nil)
				assertOK(get)
				if err := json.Unmarshal(get.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				body["spec"].(map[string]any)["release"].(map[string]any)["upgradeStartTime"] = nil
			} else {
				body = map[string]any{"spec": map[string]any{"release": map[string]any{"upgradeStartTime": nil}}}
			}
			assertOK(request(method, body))
			if read().Spec.Release.UpgradeStartTime != nil {
				t.Fatal("schedule was not removed")
			}
		})
	}
	assertOK(request(http.MethodPatch, map[string]any{"spec": map[string]any{"release": map[string]any{"version": "4.22.14", "upgradeStartTime": nil}}}))
	if read().Spec.Release.Version != "4.22.14" {
		t.Fatal("pending request was not canceled")
	}
	assertOK(request(http.MethodPatch, map[string]any{"spec": map[string]any{"release": map[string]any{"version": "4.22.16"}}}))
	updated = read()
	now := metav1.Now()
	updated.Status.Conditions = []metav1.Condition{{Type: "NodePoolUpgradeProgressing", Status: metav1.ConditionTrue, Reason: "UpgradeAccepted", LastTransitionTime: now, ObservedGeneration: updated.Generation}}
	if err := store.Update(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	// Customer status input cannot bypass the persisted acceptance lock.
	response := request(http.MethodPatch, map[string]any{"spec": map[string]any{"release": map[string]any{"version": "4.22.14"}}, "status": map[string]any{"conditions": []any{}}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("accepted cancellation returned %d: %s", response.Code, response.Body.String())
	}
	response = request(http.MethodGet, nil)
	assertOK(response)
	var publicResponse publicv1.NodePool
	if err := json.Unmarshal(response.Body.Bytes(), &publicResponse); err != nil {
		t.Fatal(err)
	}
	if publicResponse.Status.Version != "4.22.14" || publicResponse.Spec.Release.Version != "4.22.16" {
		t.Fatal("public API did not distinguish the observed version from the desired version")
	}
	if bytes.Contains(response.Body.Bytes(), []byte("\"currentVersion\"")) {
		t.Fatal("public API exposed the old status field name")
	}
	if !bytes.Contains(response.Body.Bytes(), []byte("NodePoolUpgradeProgressing")) {
		t.Fatal("public response did not expose upgrade conditions")
	}
	if bytes.Contains(response.Body.Bytes(), []byte("intentID")) || bytes.Contains(response.Body.Bytes(), []byte("private-image")) || bytes.Contains(response.Body.Bytes(), []byte("\"upgrade\"")) {
		t.Fatal("public response exposed private controller state")
	}
}

func TestUpgradeRequestResourceRemovedFromDiscovery(t *testing.T) {
	for _, resources := range [][]string{privateResourceKinds(), publicResourceKinds()} {
		for _, kind := range resources {
			if kind == "NodePoolUpgradeRequest" {
				t.Fatal("removed request resource is still registered")
			}
		}
	}
}
func privateResourceKinds() []string {
	var result []string
	for _, info := range privatev1.GetResourceInfos() {
		result = append(result, info.GVK.Kind)
	}
	return result
}
func publicResourceKinds() []string {
	var result []string
	for _, info := range publicv1.GetResourceInfos() {
		result = append(result, info.GVK.Kind)
	}
	return result
}
