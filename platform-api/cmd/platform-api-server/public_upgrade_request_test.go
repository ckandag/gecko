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
	publicv1 "github.com/openshift-online/gecko/platform-api/api/public/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func TestPublicAPIUpgradeRequestRejectsUnadvertisedTarget(t *testing.T) {
	privatePort, publicPort := freePublicAPITestPort(t), freePublicAPITestPort(t)
	privateScheme := getPrivateScheme()
	clusterGVK := privatev1.GroupVersion.WithKind("Cluster")
	requestGVK := privatev1.GroupVersion.WithKind("ControlPlaneUpgradeRequest")
	clusterStore := memory.NewMemoryStore(apiserver.GroupKindResourceType(clusterGVK.GroupKind()), privateScheme, clusterGVK)
	requestStore := memory.NewMemoryStore(apiserver.GroupKindResourceType(requestGVK.GroupKind()), privateScheme, requestGVK)
	cluster := testCluster("customer-project", "example-cluster", "us-central1")
	cluster.Spec.Release.Version = "4.15.0"
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{
		Version: "4.15.0", DesiredVersion: "4.15.0", AvailableUpdates: []string{"4.15.1"},
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
			case "ControlPlaneUpgradeRequest":
				return requestStore, nil
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
	for i, path := range []string{
		"/apis/gcp.managed.openshift.io/v1/namespaces/customer-project/controlplaneupgraderequests",
		"/apis/gcp.managed.openshift.io/v1/namespaces/customer-project/clusters/example-cluster/controlplaneupgraderequests/",
	} {
		t.Run(path, func(t *testing.T) {
			name := fmt.Sprintf("unadvertised-target-%d", i)
			body, err := json.Marshal(&publicv1.ControlPlaneUpgradeRequest{
				TypeMeta:   metav1.TypeMeta{APIVersion: publicv1.GroupVersion.String(), Kind: "ControlPlaneUpgradeRequest"},
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec:       publicv1.ControlPlaneUpgradeRequestSpec{ClusterID: cluster.Name, TargetVersion: "4.15.2"},
			})
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(data), "not an available control-plane upgrade") {
				t.Fatalf("expected target validation to return HTTP 400, got %d: %s", response.StatusCode, data)
			}
			if _, err := requestStore.Get(t.Context(), cluster.Namespace, name); !apierrors.IsNotFound(err) {
				t.Fatalf("rejected request was persisted: %v", err)
			}
		})
	}
}
