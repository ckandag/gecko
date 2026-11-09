package v1_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-logr/logr"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/aggregated"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/handlers"
	pkgschema "github.com/openshift-online/gecko/orlop/pkg/apiserver/schema"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
	publicv1 "github.com/openshift-online/gecko/platform-api/api/public/v1"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	"sigs.k8s.io/yaml"
)

func TestClusterMaintenancePublicAPIValidation(t *testing.T) {
	privateScheme, publicScheme := runtime.NewScheme(), runtime.NewScheme()
	if err := privatev1.AddToScheme(privateScheme); err != nil {
		t.Fatal(err)
	}
	if err := publicv1.AddToScheme(publicScheme); err != nil {
		t.Fatal(err)
	}
	gvk := privatev1.GroupVersion.WithKind("Cluster")
	store := memory.NewMemoryStore("clusters", privateScheme, gvk)
	converter := conversion.NewConverter(publicScheme, privateScheme, "")
	handler := handlers.NewConvertingResourceHandler(store, maintenanceClusterSchemaProcessor(t, publicv1.ClusterSchemaYAML),
		converter, gvk, "clusters", publicScheme, privateScheme, nil, logr.Discard())
	base := "/apis/gcp.managed.openshift.io/v1/namespaces/{namespace}/clusters"
	router := chi.NewRouter()
	router.Post(base, handler.Create)
	router.Put(base+"/{name}", handler.Update)
	router.Patch(base+"/{name}", handler.Patch)
	path := "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters"
	request := func(method, url string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		if method == http.MethodPatch {
			req.Header.Set("Content-Type", "application/merge-patch+json")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	cluster := clusterForMaintenanceAPITest()
	start := cluster.Spec.Maintenance.Window.StartTime.Time
	invalid := cluster.DeepCopy()
	invalid.Spec.Maintenance.Window.DurationSeconds = 60
	assertRejected := func(response *httptest.ResponseRecorder) {
		t.Helper()
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "at least 48 hours") {
			t.Fatalf("expected descriptive HTTP 400, got %d: %s", response.Code, response.Body.String())
		}
	}
	assertRejected(request(http.MethodPost, path, invalid))
	invalidFutureStart := cluster.DeepCopy()
	invalidFutureStart.Spec.Maintenance.Exclusions = nil
	invalidFutureStart.Spec.Maintenance.Window.StartTime = metav1.NewTime(time.Date(9999, time.January, 1, 0, 0, 0, 0, time.UTC))
	invalidFutureStart.Spec.Maintenance.Window.DurationSeconds = 86400
	assertRejected(request(http.MethodPost, path, invalidFutureStart))
	invalidWithoutWindow := cluster.DeepCopy()
	invalidWithoutWindow.Spec.Maintenance.Window = nil
	freezeEnd := start.Add(130 * 24 * time.Hour)
	invalidWithoutWindow.Spec.Maintenance.Exclusions = &[]publicv1.ControlPlaneMaintenanceExclusion{
		{Name: "freeze", StartTime: metav1.NewTime(start.Add(100 * 24 * time.Hour)), EndTime: metav1.NewTime(freezeEnd)},
		{Name: "four-hours", StartTime: metav1.NewTime(freezeEnd), EndTime: metav1.NewTime(freezeEnd.Add(4 * time.Hour))},
		{Name: "five-hours", StartTime: metav1.NewTime(freezeEnd.Add(4 * time.Hour)), EndTime: metav1.NewTime(freezeEnd.Add(9 * time.Hour))},
	}
	assertRejected(request(http.MethodPost, path, invalidWithoutWindow))
	if _, err := store.Get(t.Context(), "customer", "test-cluster"); !apierrors.IsNotFound(err) {
		t.Fatalf("invalid create persisted a cluster: %v", err)
	}
	response := request(http.MethodPost, path, &cluster)
	if response.Code != http.StatusCreated {
		t.Fatalf("valid create returned %d: %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &cluster); err != nil {
		t.Fatal(err)
	}
	invalidFutureStart.ResourceVersion = cluster.ResourceVersion
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalidFutureStart))
	assertRejected(request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"window": map[string]any{"startTime": "9999-01-01T00:00:00Z", "durationSeconds": 86400}}},
	}))
	// A public PUT that omits maintenance must preserve the existing schedule.
	omitted := cluster.DeepCopy()
	omitted.Spec.Maintenance = nil
	response = request(http.MethodPut, path+"/test-cluster", omitted)
	if response.Code != http.StatusOK {
		t.Fatalf("omitting maintenance returned %d: %s", response.Code, response.Body.String())
	}
	cluster = publicv1.Cluster{}
	if err := json.Unmarshal(response.Body.Bytes(), &cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Maintenance == nil || cluster.Spec.Maintenance.Window == nil || cluster.Spec.Maintenance.Exclusions == nil || len(*cluster.Spec.Maintenance.Exclusions) != 1 {
		t.Fatal("omitting maintenance did not preserve the window and exclusions")
	}
	// Omitted exclusions are preserved before validating a replacement window.
	invalid = cluster.DeepCopy()
	invalid.Spec.Maintenance.Exclusions = nil
	invalid.Spec.Maintenance.Window.DurationSeconds = 7200
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalid))
	invalid = cluster.DeepCopy()
	invalid.Spec.Maintenance.Window.DurationSeconds = 60
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalid))
	invalid = cluster.DeepCopy()
	(*invalid.Spec.Maintenance.Exclusions)[0].EndTime.Time = (*invalid.Spec.Maintenance.Exclusions)[0].EndTime.Add(time.Second)
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalid))
	assertRejected(request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"window": map[string]any{"durationSeconds": 60}}},
	}))
	assertRejected(request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"exclusions": invalid.Spec.Maintenance.Exclusions}},
	}))
	assertStoredMaintenanceClusterUnchanged(t, store, &cluster)
	response = request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"window": nil}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("removing optional window returned %d: %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Spec.Maintenance.Window != nil {
		t.Fatal("removing the optional window did not clear it")
	}
	invalid = cluster.DeepCopy()
	invalid.Spec.Maintenance.Window = nil
	invalid.Spec.Maintenance.Exclusions = invalidWithoutWindow.Spec.Maintenance.Exclusions
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalid))
	assertRejected(request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"exclusions": invalid.Spec.Maintenance.Exclusions}},
	}))
	// Seed a formerly accepted schedule, bypassing admission as if it had been
	// persisted before the rolling-availability validation was introduced.
	legacy := assertStoredMaintenanceClusterUnchanged(t, store, &cluster)
	legacy.Spec.Maintenance.Window = &privatev1.ControlPlaneMaintenanceWindow{
		StartTime: metav1.NewTime(start), DurationSeconds: 14400,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{Frequency: "weekly", DaysOfWeek: []string{"saturday"}},
	}
	if err := store.Update(t.Context(), legacy); err != nil {
		t.Fatal(err)
	}
	publicLegacy, err := converter.PrivateToPublic(legacy)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*publicv1.Cluster)
	}{
		{name: "unchanged timing", mutate: func(*publicv1.Cluster) {}},
		{name: "omitted maintenance", mutate: func(c *publicv1.Cluster) { c.Spec.Maintenance = nil }},
		{name: "omitted exclusions", mutate: func(c *publicv1.Cluster) { c.Spec.Maintenance.Exclusions = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updated := publicLegacy.(*publicv1.Cluster).DeepCopy()
			tc.mutate(updated)
			response := request(http.MethodPut, path+"/test-cluster", updated)
			if response.Code != http.StatusOK {
				t.Fatalf("legacy PUT returned %d: %s", response.Code, response.Body.String())
			}
			var next publicv1.Cluster
			if err := json.Unmarshal(response.Body.Bytes(), &next); err != nil {
				t.Fatal(err)
			}
			if !apiequality.Semantic.DeepEqual(next.Spec.Maintenance, publicLegacy.(*publicv1.Cluster).Spec.Maintenance) {
				t.Fatal("legacy PUT changed the effective maintenance settings")
			}
			publicLegacy = &next
		})
	}
	response = request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"metadata": map[string]any{"labels": map[string]string{"review": "legacy"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("unrelated legacy PATCH returned %d: %s", response.Code, response.Body.String())
	}
	var afterPatch publicv1.Cluster
	if err := json.Unmarshal(response.Body.Bytes(), &afterPatch); err != nil {
		t.Fatal(err)
	}
	invalid = afterPatch.DeepCopy()
	invalid.Spec.Maintenance.Window.DurationSeconds = 18000
	assertRejected(request(http.MethodPut, path+"/test-cluster", invalid))
	assertRejected(request(http.MethodPatch, path+"/test-cluster", map[string]any{
		"spec": map[string]any{"maintenance": map[string]any{"window": map[string]any{"durationSeconds": 18000}}},
	}))
	assertStoredMaintenanceClusterUnchanged(t, store, &afterPatch)
}

func TestClusterMaintenancePublicPatchPreservesPrivateUpgradePin(t *testing.T) {
	for _, tc := range []struct {
		name        string
		critical    string
		privateOnly bool
	}{
		{name: "public settings and private pin", critical: "4.22.21"},
		{name: "private pin only", critical: "4.22.21", privateOnly: true},
		{name: "no private pin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			privateScheme, publicScheme := runtime.NewScheme(), runtime.NewScheme()
			if err := privatev1.AddToScheme(privateScheme); err != nil {
				t.Fatal(err)
			}
			if err := publicv1.AddToScheme(publicScheme); err != nil {
				t.Fatal(err)
			}
			converter := conversion.NewConverter(publicScheme, privateScheme, "")
			publicCluster := clusterForMaintenanceAPITest()
			converted, err := converter.PublicToPrivate(&publicCluster, nil)
			if err != nil {
				t.Fatal(err)
			}
			original := converted.(*privatev1.Cluster)
			original.Spec.Maintenance.CriticalUpgrade = tc.critical
			if tc.privateOnly {
				original.Spec.Maintenance.Window = nil
				original.Spec.Maintenance.Exclusions = nil
			}
			if err := original.Default(t.Context()); err != nil {
				t.Fatal(err)
			}
			gvk := privatev1.GroupVersion.WithKind("Cluster")
			store := memory.NewMemoryStore("clusters", privateScheme, gvk)
			if err := store.Create(t.Context(), original); err != nil {
				t.Fatal(err)
			}
			handler := handlers.NewConvertingResourceHandler(store, maintenanceClusterSchemaProcessor(t, publicv1.ClusterSchemaYAML),
				converter, gvk, "clusters", publicScheme, privateScheme, nil, logr.Discard())
			router := chi.NewRouter()
			router.Patch("/apis/gcp.managed.openshift.io/v1/namespaces/{namespace}/clusters/{name}", handler.Patch)
			var generation int64
			for attempt := range 2 {
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPatch,
					"/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/test-cluster",
					strings.NewReader(`{"spec":{"maintenance":null}}`))
				req.Header.Set("Content-Type", "application/merge-patch+json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				if response.Code != http.StatusOK {
					t.Fatalf("clearing maintenance returned %d: %s", response.Code, response.Body.String())
				}
				persisted, err := store.Get(t.Context(), original.Namespace, original.Name)
				if err != nil {
					t.Fatal(err)
				}
				updated := persisted.(*privatev1.Cluster)
				if tc.critical == "" {
					if updated.Spec.Maintenance != nil {
						t.Fatal("maintenance without private state was not removed")
					}
				} else if updated.Spec.Maintenance == nil || updated.Spec.Maintenance.CriticalUpgrade != tc.critical {
					t.Fatal("clearing public maintenance removed the private upgrade pin")
				}
				if m := updated.Spec.Maintenance; m != nil && (m.Window != nil || m.Exclusions != nil) {
					t.Fatal("clearing maintenance preserved public settings")
				}
				if updated.Spec.Release != original.Spec.Release || updated.Spec.SafeName != original.Spec.SafeName {
					t.Fatal("clearing maintenance changed unrelated spec fields")
				}
				if strings.Contains(response.Body.String(), "criticalUpgrade") {
					t.Fatal("private upgrade pin leaked into the public response")
				}
				if attempt > 0 && updated.Generation != generation {
					t.Fatal("repeated maintenance clearing was not idempotent")
				}
				generation = updated.Generation
			}
		})
	}
}

func assertStoredMaintenanceClusterUnchanged(t *testing.T, store *memory.MemoryStore, expected *publicv1.Cluster) *privatev1.Cluster {
	t.Helper()
	persisted, err := store.Get(t.Context(), expected.Namespace, expected.Name)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.GetResourceVersion() != expected.ResourceVersion {
		t.Fatal("rejected update changed the stored cluster")
	}
	return persisted.(*privatev1.Cluster)
}

func TestClusterMaintenanceLegacyAggregatedStatusUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := privatev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	publicCluster := clusterForMaintenanceAPITest()
	converted, err := newClusterConverter(t).PublicToPrivate(&publicCluster, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := converted.(*privatev1.Cluster)
	legacy.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"saturday"}
	ctx := genericapirequest.WithNamespace(t.Context(), legacy.Namespace)
	if err := legacy.Default(ctx); err != nil {
		t.Fatal(err)
	}
	gvk := privatev1.GroupVersion.WithKind("Cluster")
	store := memory.NewMemoryStore("clusters", scheme, gvk)
	// Simulate an existing object accepted before the new availability limit.
	if err := store.Create(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	strategy := aggregated.NewResourceStrategy(scheme, maintenanceClusterSchemaProcessor(t, privatev1.ClusterSchemaYAML), true, gvk, logr.Discard())
	storage := aggregated.NewResourceStorage(store, strategy, gvk, "clusters", "cluster", scheme, logr.Discard(), nil)
	statusStorage := aggregated.NewStatusStorage(storage)
	updater := rest.DefaultUpdatedObjectInfo(nil, func(_ context.Context, _, old runtime.Object) (runtime.Object, error) {
		updated := old.(*privatev1.Cluster).DeepCopy()
		updated.Spec.Release.Version = "9.9.9"
		updated.Status.Conditions = []metav1.Condition{{
			Type: "HostedClusterAvailable", Status: metav1.ConditionTrue, Reason: "AsExpected", LastTransitionTime: metav1.Now(),
		}}
		return updated, nil
	})
	if _, _, err := statusStorage.Update(ctx, legacy.Name, updater, nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("legacy maintenance blocked aggregated status update: %v", err)
	}
	persisted, err := store.Get(ctx, legacy.Namespace, legacy.Name)
	if err != nil {
		t.Fatal(err)
	}
	updated := persisted.(*privatev1.Cluster)
	if !apiequality.Semantic.DeepEqual(updated.Spec, legacy.Spec) {
		t.Fatal("status update changed the stored spec")
	}
	if len(updated.Status.Conditions) != 1 || updated.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("status conditions were not persisted: %v", updated.Status.Conditions)
	}
}

func TestClusterMaintenanceExclusionCountSchemas(t *testing.T) {
	for _, tc := range []struct{ name, schema string }{
		{"private", privatev1.ClusterSchemaYAML},
		{"public", publicv1.ClusterSchemaYAML},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processor := maintenanceClusterSchemaProcessor(t, tc.schema)
			for _, count := range []int{3, 4} {
				cluster := clusterForMaintenanceAPITest()
				exclusions := make([]publicv1.ControlPlaneMaintenanceExclusion, count)
				for i := range count {
					exclusions[i] = publicv1.ControlPlaneMaintenanceExclusion{
						Name: string(rune('a' + i)), StartTime: metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)),
						EndTime: metav1.NewTime(time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)),
					}
				}
				cluster.Spec.Maintenance.Exclusions = &exclusions
				data, err := json.Marshal(cluster)
				if err != nil {
					t.Fatal(err)
				}
				var object map[string]any
				if err := json.Unmarshal(data, &object); err != nil {
					t.Fatal(err)
				}
				errs := processor.Process(t.Context(), object, nil)
				if (len(errs) > 0) != (count > 3) {
					t.Fatalf("count %d: schema errors = %v", count, errs)
				}
			}
		})
	}
}

func clusterForMaintenanceAPITest() publicv1.Cluster {
	start := time.Date(2026, time.January, 5, 2, 0, 0, 0, time.UTC)
	return publicv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: publicv1.GroupVersion.String(), Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "customer"},
		Spec: publicv1.ClusterSpec{
			Platform: publicv1.ClusterPlatformSpec{
				Type: "GCP",
				GCP: &publicv1.GCPClusterPlatform{
					ProjectID: "customer-project", Region: "us-central1", Network: "test-network", Subnet: "test-subnet",
					WorkloadIdentity: publicv1.WorkloadIdentitySpec{
						PoolID: "test-pool", ProjectNumber: "123456789012", ProviderID: "test-provider",
						ServiceAccountsRef: &publicv1.ServiceAccountsRef{
							NodePoolEmail: "nodepool@example.iam.gserviceaccount.com", ControlPlaneEmail: "controlplane@example.iam.gserviceaccount.com",
							CloudControllerEmail: "cloudcontroller@example.iam.gserviceaccount.com", ImageRegistryEmail: "registry@example.iam.gserviceaccount.com",
							NetworkEmail: "network@example.iam.gserviceaccount.com", StorageEmail: "storage@example.iam.gserviceaccount.com",
						},
					},
				},
			},
			Release: publicv1.ReleaseSpec{Version: "4.22.20", ChannelGroup: "stable"},
			Maintenance: &publicv1.ClusterMaintenanceSpec{
				Window: &publicv1.ControlPlaneMaintenanceWindow{
					StartTime: metav1.NewTime(start), DurationSeconds: 14400,
					Recurrence: publicv1.ControlPlaneMaintenanceRecurrence{
						Frequency: "weekly", DaysOfWeek: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
					},
				},
				Exclusions: &[]publicv1.ControlPlaneMaintenanceExclusion{{
					Name: "freeze", StartTime: metav1.NewTime(start.Add(100 * 24 * time.Hour)), EndTime: metav1.NewTime(start.Add(120 * 24 * time.Hour)),
				}},
			},
		},
	}
}

func maintenanceClusterSchemaProcessor(t *testing.T, schemaYAML string) *pkgschema.Processor {
	t.Helper()
	var propsV1 apiextv1.JSONSchemaProps
	if err := yaml.Unmarshal([]byte(schemaYAML), &propsV1); err != nil {
		t.Fatal(err)
	}
	var props apiext.JSONSchemaProps
	if err := apiextv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&propsV1, &props, nil); err != nil {
		t.Fatal(err)
	}
	structural, err := structuralschema.NewStructural(&props)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := pkgschema.NewProcessor(structural, &props)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}
