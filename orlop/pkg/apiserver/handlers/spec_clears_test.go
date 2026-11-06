package handlers

import (
	"bytes"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/go-logr/logr"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	pkgschema "github.com/openshift-online/gecko/orlop/pkg/apiserver/schema"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"

	corev1 "k8s.io/api/core/v1"
	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func TestPublicSpecClearsPreserveOtherFields(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	h := &ConvertingResourceHandler{privateScheme: scheme, gvk: corev1.SchemeGroupVersion.WithKind("Pod")}
	tests := []struct {
		name          string
		current       map[string]interface{}
		previous      map[string]interface{}
		clearAffinity bool
		removeMapKey  bool
	}{
		{
			name: "PUT explicit null", current: map[string]interface{}{"affinity": nil},
			clearAffinity: true,
		},
		{name: "PUT omission preserves existing fields", current: map[string]interface{}{}},
		{
			name: "PATCH removed object", current: map[string]interface{}{},
			previous: map[string]interface{}{"affinity": map[string]interface{}{}}, clearAffinity: true,
		},
		{
			name:    "PATCH removed map entry",
			current: map[string]interface{}{"nodeSelector": map[string]interface{}{"keep": "value"}},
			previous: map[string]interface{}{"nodeSelector": map[string]interface{}{
				"keep": "value", "remove": "value",
			}},
			removeMapKey: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The original contains fields unavailable in the public representation.
			// An integer beyond float64's exact range also must survive the round trip.
			const preciseInteger = int64(1<<53 + 1)
			original := &corev1.Pod{Spec: corev1.PodSpec{
				Affinity:              &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}},
				NodeSelector:          map[string]string{"keep": "value", "remove": "value", "private": "hidden"},
				ActiveDeadlineSeconds: ptr.To(preciseInteger),
			}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			result, err := h.applyPublicSpecClears(original, map[string]interface{}{"spec": tt.current}, map[string]interface{}{"spec": tt.previous})
			if err != nil {
				t.Fatal(err)
			}
			updated := result.(*corev1.Pod)
			if (updated.Spec.Affinity == nil) != tt.clearAffinity {
				t.Fatal("unexpected affinity removal")
			}
			_, hasRemovedKey := updated.Spec.NodeSelector["remove"]
			if hasRemovedKey == tt.removeMapKey {
				t.Fatal("unexpected map entry removal")
			}
			if updated.Spec.NodeSelector["private"] != "hidden" || updated.Status.Phase != corev1.PodRunning || *updated.Spec.ActiveDeadlineSeconds != preciseInteger {
				t.Fatal("clearing a public field changed private data or status")
			}
			if original.Spec.Affinity == nil || original.Spec.NodeSelector["remove"] != "value" {
				t.Fatal("clearing fields mutated the input object")
			}
		})
	}
}

// Only these Pod fields are public; ActiveDeadlineSeconds and status remain private.
type specClearPublicPod struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              specClearPublicPodSpec `json:"spec"`
}
type specClearPublicPodSpec struct {
	Affinity     *corev1.Affinity  `json:"affinity,omitempty"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

func (p *specClearPublicPod) DeepCopyObject() runtime.Object {
	if p == nil {
		return nil
	}
	out := *p
	p.DeepCopyInto(&out.ObjectMeta)
	out.Spec.Affinity = p.Spec.Affinity.DeepCopy()
	out.Spec.NodeSelector = maps.Clone(p.Spec.NodeSelector)
	return &out
}
func newSpecClearProcessor(t *testing.T) *pkgschema.Processor {
	t.Helper()
	props := &apiext.JSONSchemaProps{Type: "object", Properties: map[string]apiext.JSONSchemaProps{
		"apiVersion": {Type: "string"}, "kind": {Type: "string"},
		"metadata": {Type: "object", XPreserveUnknownFields: ptr.To(true)},
		"spec": {Type: "object", Properties: map[string]apiext.JSONSchemaProps{
			"affinity":     {Type: "object", Nullable: true, XPreserveUnknownFields: ptr.To(true)},
			"nodeSelector": {Type: "object", AdditionalProperties: &apiext.JSONSchemaPropsOrBool{Allows: true, Schema: &apiext.JSONSchemaProps{Type: "string"}}},
		}},
	}}
	structural, err := structuralschema.NewStructural(props)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := pkgschema.NewProcessor(structural, props)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}
func TestConvertingResourceHandlerSpecClears(t *testing.T) {
	tests := []struct {
		name, method, body                             string
		clearAffinity, clearNodeAffinity, removeMapKey bool
	}{
		{name: "PATCH removes optional object", method: http.MethodPatch, body: `{"spec":{"affinity":null}}`, clearAffinity: true},
		{name: "PATCH removes nested object", method: http.MethodPatch, body: `{"spec":{"affinity":{"nodeAffinity":null}}}`, clearNodeAffinity: true},
		{name: "PATCH removes map key", method: http.MethodPatch, body: `{"spec":{"nodeSelector":{"remove":null}}}`, removeMapKey: true},
		{name: "PATCH omission preserves object", method: http.MethodPatch, body: `{"spec":{"nodeSelector":{"keep":"value"}}}`},
		{name: "PUT omission preserves object", method: http.MethodPut, body: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod","namespace":"default"},"spec":{"nodeSelector":{"keep":"value","remove":"value"}}}`},
		{name: "PUT explicit nullable object is cleared", method: http.MethodPut, body: `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod","namespace":"default"},"spec":{"affinity":null,"nodeSelector":{"keep":"value","remove":"value"}}}`, clearAffinity: true},
		{name: "private field and status cannot be cleared", method: http.MethodPatch, body: `{"spec":{"activeDeadlineSeconds":null},"status":{"phase":"Failed"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			privateScheme, publicScheme := runtime.NewScheme(), runtime.NewScheme()
			if err := corev1.AddToScheme(privateScheme); err != nil {
				t.Fatal(err)
			}
			gvk := corev1.SchemeGroupVersion.WithKind("Pod")
			publicScheme.AddKnownTypeWithName(gvk, &specClearPublicPod{})
			store := memory.NewMemoryStore("pods", privateScheme, gvk)
			const preciseInteger = int64(1<<53 + 1)
			original := &corev1.Pod{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default", Generation: 1},
				Spec:       corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}, NodeSelector: map[string]string{"keep": "value", "remove": "value"}, ActiveDeadlineSeconds: ptr.To(preciseInteger)},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			}
			if err := store.Create(t.Context(), original); err != nil {
				t.Fatal(err)
			}
			converter := conversion.NewConverter(publicScheme, privateScheme, "")
			h := NewConvertingResourceHandler(store, newSpecClearProcessor(t), converter, gvk, "pods", publicScheme, privateScheme, nil, logr.Discard())
			router := chi.NewRouter()
			route := "/apis/test/v1/namespaces/{namespace}/pods/{name}"
			router.Patch(route, h.Patch)
			router.Put(route, h.Update)
			send := func() {
				t.Helper()
				req := httptest.NewRequest(tt.method, "/apis/test/v1/namespaces/default/pods/test-pod", bytes.NewBufferString(tt.body))
				req.Header.Set("Content-Type", "application/json")
				if tt.method == http.MethodPatch {
					req.Header.Set("Content-Type", "application/merge-patch+json")
				}
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				if response.Code != http.StatusOK {
					t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
				}
			}
			send()
			obj, err := store.Get(t.Context(), "default", "test-pod")
			if err != nil {
				t.Fatal(err)
			}
			updated := obj.(*corev1.Pod)
			if (updated.Spec.Affinity == nil) != tt.clearAffinity {
				t.Fatal("optional object deletion was not preserved")
			}
			if updated.Spec.Affinity != nil && (updated.Spec.Affinity.NodeAffinity == nil) != tt.clearNodeAffinity {
				t.Fatal("nested object deletion was not preserved")
			}
			_, hasRemoved := updated.Spec.NodeSelector["remove"]
			if hasRemoved == tt.removeMapKey {
				t.Fatal("map key deletion was not preserved")
			}
			if updated.Spec.NodeSelector["keep"] != "value" || updated.Spec.ActiveDeadlineSeconds == nil || *updated.Spec.ActiveDeadlineSeconds != preciseInteger || updated.Status.Phase != corev1.PodRunning {
				t.Fatal("request changed unrelated or private data")
			}
			generation := updated.Generation
			send()
			obj, err = store.Get(t.Context(), "default", "test-pod")
			if err != nil {
				t.Fatal(err)
			}
			if obj.GetGeneration() != generation {
				t.Fatal("repeated request was not idempotent")
			}

		})
	}
}
