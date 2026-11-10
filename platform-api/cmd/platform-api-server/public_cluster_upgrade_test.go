package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func TestPublicAPIClusterUpgradeValidatesStoredRelease(t *testing.T) {
	privatePort, publicPort := freePublicAPITestPort(t), freePublicAPITestPort(t)
	privateScheme := getPrivateScheme()
	clusterGVK := privatev1.GroupVersion.WithKind("Cluster")
	clusterStore := memory.NewMemoryStore(apiserver.GroupKindResourceType(clusterGVK.GroupKind()), privateScheme, clusterGVK)
	cluster := testCluster("customer-project", "example-cluster", "us-central1")
	cluster.Spec.Release.Version = "4.22.19"
	cluster.Spec.Release.ChannelGroup = "stable"
	cluster.Spec.Platform.GCP = &privatev1.GCPClusterPlatform{
		ProjectID: "customer-project", Region: "us-central1", Network: "network", Subnet: "subnet",
		WorkloadIdentity: privatev1.WorkloadIdentitySpec{PoolID: "pool", ProjectNumber: "123456789", ProviderID: "provider",
			ServiceAccountsRef: &privatev1.ServiceAccountsRef{
				NodePoolEmail: "nodepool@example.iam.gserviceaccount.com", ControlPlaneEmail: "control@example.iam.gserviceaccount.com",
				CloudControllerEmail: "cloud@example.iam.gserviceaccount.com", StorageEmail: "storage@example.iam.gserviceaccount.com",
				ImageRegistryEmail: "registry@example.iam.gserviceaccount.com", NetworkEmail: "network@example.iam.gserviceaccount.com",
			},
		},
	}

	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{
		Version: "4.22.19", DesiredVersion: "4.22.19", AvailableUpdates: []string{"4.23.9"},
	}
	if err := clusterStore.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	server, err := apiserver.New(apiserver.Options{
		Address: "127.0.0.1",
		Private: apiserver.PrivateAPIOptions{
			Port: privatePort, Scheme: privateScheme, Resources: getPrivateResources(), DisableAuth: true,
		},
		Public: apiserver.PublicAPIOptions{
			Enable: true, Address: "127.0.0.1", Port: publicPort, Scheme: getPublicScheme(), Resources: getPublicResources(),
		},
		StorageFactory: func(resourceType string, scheme *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
			switch gvk.Kind {
			case "Cluster":
				return clusterStore, nil
			}
			return memory.NewMemoryStore(resourceType, scheme, gvk), nil
		},
		Logger: logr.Discard(),
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan struct{})
	var runErr error
	go func() {
		runErr = server.Run()
		close(runDone)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown API server: %v", err)
		}
		select {
		case <-runDone:
			if runErr != nil {
				t.Errorf("API server stopped with error: %v", runErr)
			}
		case <-ctx.Done():
			t.Errorf("API server did not stop: %v", ctx.Err())
		}
	})
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", publicPort)
	client := &http.Client{Timeout: 3 * time.Second}
	waitForPublicAPITestServer(t, client, baseURL+"/apis", runDone, &runErr)
	for _, test := range []struct {
		name, version string
		status        int
	}{
		{name: "unadvertised minor", version: "4.24", status: http.StatusBadRequest},
		{name: "supported minor", version: "4.23", status: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]interface{}{"spec": map[string]interface{}{"release": map[string]string{"version": test.version}}})
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPatch, baseURL+"/apis/gcp.managed.openshift.io/v1/namespaces/customer-project/clusters/example-cluster", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/merge-patch+json")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != test.status {
				t.Fatalf("HTTP status=%d, want %d: %s", response.StatusCode, test.status, data)
			}
			if test.status == http.StatusBadRequest && !strings.Contains(string(data), "advertised") {
				t.Fatalf("expected target validation error: %s", data)
			}
			stored, err := clusterStore.Get(t.Context(), cluster.Namespace, cluster.Name)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(stored)
			if err != nil {
				t.Fatal(err)
			}
			var actual privatev1.Cluster
			if err := json.Unmarshal(raw, &actual); err != nil {
				t.Fatal(err)
			}
			expected := "4.22.19"
			if test.status == http.StatusOK {
				expected = test.version
			}
			if actual.Spec.Release.Version != expected {
				t.Fatalf("persisted desired version=%s, want %s", actual.Spec.Release.Version, expected)
			}
			if actual.Status.HostedClusterResult.Version != "4.22.19" {
				t.Fatal("request advanced observed version")
			}
			if test.status == http.StatusOK && privatev1.ClusterReleaseApproved(&actual) {
				t.Fatal("public PATCH bypassed upgrade approval")
			}
		})
	}
}
