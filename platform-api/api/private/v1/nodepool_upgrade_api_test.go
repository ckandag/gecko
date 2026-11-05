package v1_test

import (
	"bytes"
	"context"
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
		Status: privatev1.NodePoolStatus{CurrentVersion: "4.22.14", VersionResolution: &privatev1.VersionResolutionResult{ReleaseImage: "private-image", ReleaseVersion: "4.22.14"}},
	}
	if err := store.Create(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	privatev1.SetValidatorDeps(privatev1.ValidatorDeps{
		DefaultNodePoolUpgrade: func(_ context.Context, p *privatev1.NodePool) error {
			p.Spec.Upgrade.TargetVersion = ptr.To("4.22.16")
			return nil
		},
	})
	t.Cleanup(func() { privatev1.SetValidatorDeps(privatev1.ValidatorDeps{}) })
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
	// The empty upgrade object chooses the default exactly once. Customer status input is stripped.
	assertOK(request(http.MethodPatch, map[string]any{
		"spec":   map[string]any{"upgrade": map[string]any{}},
		"status": map[string]any{"currentVersion": "99.0.0"},
	}))
	updated := read()
	if updated.Spec.Upgrade == nil || ptr.Deref(updated.Spec.Upgrade.TargetVersion, "") != "4.22.16" {
		t.Fatal("upgrade target was not defaulted")
	}
	if updated.Spec.Release.Version != "4.22.14" || updated.Status.CurrentVersion != "4.22.14" {
		t.Fatal("request prematurely changed release or observed version")
	}
	// An ordinary public PUT omitting upgrade must preserve the pending intent and private status.
	publicObj, err := converter.PrivateToPublic(updated)
	if err != nil {
		t.Fatal(err)
	}
	publicPool := publicObj.(*publicv1.NodePool)
	publicPool.Spec.Upgrade = nil
	publicPool.Spec.NodeCount = ptr.To(int32(3))
	assertOK(request(http.MethodPut, publicPool))
	updated = read()
	if updated.Spec.Upgrade == nil || updated.Status.VersionResolution.ReleaseImage != "private-image" {
		t.Fatal("unrelated update lost pending intent or private status")
	}
	// Both explicit PUT null and merge-patch removal must cancel pending upgrades.
	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		t.Run(method+" cancellation", func(t *testing.T) {
			assertOK(request(http.MethodPatch, map[string]any{"spec": map[string]any{"upgrade": map[string]any{"targetVersion": "4.22.16"}}}))
			var body map[string]any
			if method == http.MethodPut {
				get := request(http.MethodGet, nil)
				assertOK(get)
				if err := json.Unmarshal(get.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				body["spec"].(map[string]any)["upgrade"] = nil
			} else {
				body = map[string]any{"spec": map[string]any{"upgrade": nil}}
			}
			assertOK(request(method, body))
			if read().Spec.Upgrade != nil {
				t.Fatal("pending upgrade was not canceled")
			}
		})
	}
	// Admission uses stored acceptance; a customer cannot clear status to bypass the lock.
	assertOK(request(http.MethodPatch, map[string]any{"spec": map[string]any{"upgrade": map[string]any{"targetVersion": "4.22.16"}}}))
	updated = read()
	now := metav1.Now()
	updated.Status.Upgrade = &privatev1.NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now, IntentID: ptr.To("private-intent")}
	if err := store.Update(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	response := request(http.MethodPatch, map[string]any{"spec": map[string]any{"upgrade": nil}, "status": map[string]any{"upgrade": nil}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("accepted cancellation returned %d: %s", response.Code, response.Body.String())
	}
	response = request(http.MethodGet, nil)
	assertOK(response)
	if bytes.Contains(response.Body.Bytes(), []byte("intentID")) || bytes.Contains(response.Body.Bytes(), []byte("private-image")) {
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
