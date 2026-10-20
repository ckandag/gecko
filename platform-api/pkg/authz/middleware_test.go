package authz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/handlers"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
	"github.com/openshift-online/gecko/platform-api/pkg/authn"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func TestMiddlewareAllowsAuthorizationExemptCatalogGets(t *testing.T) {
	resources := []types.ResourceInfo{
		{
			GVK:                      runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Version"},
			Plural:                   "versions",
			Verbs:                    []string{"get", "list"},
			AuthorizationExemptVerbs: []string{"get", "list"},
		},
		{
			GVK:                      runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Channel"},
			Plural:                   "channels",
			Verbs:                    []string{"get", "list"},
			AuthorizationExemptVerbs: []string{"get", "list"},
		},
	}
	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "list versions", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/versions", status: http.StatusNoContent},
		{name: "get version", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/versions/4.22.1", status: http.StatusNoContent},
		{name: "list channels", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/channels", status: http.StatusNoContent},
		{name: "get channel", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/channels/stable", status: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(context.Background(), tt.method, tt.path, nil)
			response := httptest.NewRecorder()
			Middleware(nil, logr.Discard(), resources)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(response, request)
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
		})
	}
}

func TestMiddlewareDeniesNonExemptClusterScopedRequests(t *testing.T) {
	resources := []types.ResourceInfo{{
		GVK:                      runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Version"},
		Plural:                   "versions",
		Verbs:                    []string{"get", "list"},
		AuthorizationExemptVerbs: []string{"get", "list"},
	}}
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "write", method: http.MethodPost, path: "/apis/gcp.managed.openshift.io/v1/versions"},
		{name: "watch", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/versions?watch=true"},
		{name: "other cluster resource", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/platformroles"},
		{name: "nested item path", method: http.MethodGet, path: "/apis/gcp.managed.openshift.io/v1/versions/4.22.1/status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := authn.WithUser(context.Background(), "alice@example.com")
			request := httptest.NewRequestWithContext(ctx, tt.method, tt.path, nil)
			response := httptest.NewRecorder()
			Middleware(nil, logr.Discard(), resources)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Fatal("denied request reached the next handler")
			})).ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
			}
		})
	}
}

func TestMiddlewareDoesNotBypassAuthorizationForClusterOrNodePool(t *testing.T) {
	resources := []types.ResourceInfo{
		{
			GVK:                      runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Version"},
			Plural:                   "versions",
			AuthorizationExemptVerbs: []string{"get", "list"},
		},
		{
			GVK:    runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Cluster"},
			Plural: "clusters",
		},
		{
			GVK:        runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "NodePool"},
			Plural:     "nodepools",
			Namespaced: true,
		},
	}
	for _, requestPath := range []string{
		"/apis/gcp.managed.openshift.io/v1/clusters",
		"/apis/gcp.managed.openshift.io/v1/clusters/example",
		"/apis/gcp.managed.openshift.io/v1/namespaces/project-a/nodepools",
		"/apis/gcp.managed.openshift.io/v1/namespaces/project-a/nodepools/example",
	} {
		t.Run(requestPath, func(t *testing.T) {
			request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, requestPath, nil)
			response := httptest.NewRecorder()
			Middleware(nil, logr.Discard(), resources)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("authorization-exempt request reached the next handler")
			})).ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
			}
		})
	}
}

func TestMiddlewareFailsClosedWithoutAuthorizer(t *testing.T) {
	resources := []types.ResourceInfo{{
		GVK:        runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "NodePool"},
		Plural:     "nodepools",
		Namespaced: true,
	}}
	ctx := authn.WithUser(context.Background(), "alice@example.com")
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/apis/gcp.managed.openshift.io/v1/namespaces/project-a/nodepools", nil)
	response := httptest.NewRecorder()
	Middleware(nil, logr.Discard(), resources)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("request reached the next handler without an authorizer")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestMiddlewareNamespacedListFiltersObjectState(t *testing.T) {
	stores, authorizer := newTestAuthorizer(t)
	condition := `context.spec.region == "us-east1"`
	seedClusterListBinding(t, stores, "project-a", &condition)
	if err := authorizer.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	resources := []types.ResourceInfo{{
		GVK:        runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Cluster"},
		Plural:     "clusters",
		Namespaced: true,
	}}
	ctx := authn.WithUser(context.Background(), "alice@example.com")
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/apis/gcp.managed.openshift.io/v1/namespaces/project-a/clusters", nil)
	response := httptest.NewRecorder()
	Middleware(authorizer, logr.Discard(), resources)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filter, ok := handlers.ItemFilterFromContext(r.Context())
		if !ok {
			t.Fatal("item filter was not added to the request context")
		}
		allowed, err := filter(r.Context(), authorizationTestObject("east", "us-east1"))
		if err != nil || !allowed {
			t.Fatalf("allowed object filter result = %t, %v; want allowed", allowed, err)
		}
		allowed, err = filter(r.Context(), authorizationTestObject("west", "us-west1"))
		if err != nil || allowed {
			t.Fatalf("denied object filter result = %t, %v; want denied", allowed, err)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestMiddlewareNamespacedListUsesPermissionBeforeConditions(t *testing.T) {
	stores, authorizer := newTestAuthorizer(t)
	condition := "false"
	seedClusterListBinding(t, stores, "project-a", &condition)
	if err := authorizer.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	resources := []types.ResourceInfo{{
		GVK:        runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Cluster"},
		Plural:     "clusters",
		Namespaced: true,
	}}
	ctx := authn.WithUser(context.Background(), "alice@example.com")
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/apis/gcp.managed.openshift.io/v1/namespaces/project-a/clusters", nil)
	response := httptest.NewRecorder()
	Middleware(authorizer, logr.Discard(), resources)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filter, ok := handlers.ItemFilterFromContext(r.Context())
		if !ok {
			t.Fatal("item filter was not added to the request context")
		}
		allowed, err := filter(r.Context(), authorizationTestObject("cluster", "us-east1"))
		if err != nil || allowed {
			t.Fatalf("item filter result = %t, %v; want denied", allowed, err)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestMiddlewareDefersObjectAuthorizationToHandler(t *testing.T) {
	stores, authorizer := newTestAuthorizer(t)
	condition := `context.spec.region == "us-east1"`
	seedClusterGetBinding(t, stores, "project-a", &condition)
	if err := authorizer.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}

	resources := []types.ResourceInfo{{
		GVK:        runtimeschema.GroupVersionKind{Group: "gcp.managed.openshift.io", Version: "v1", Kind: "Cluster"},
		Plural:     "clusters",
		Namespaced: true,
	}}
	ctx := authn.WithUser(context.Background(), "alice@example.com")
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/apis/gcp.managed.openshift.io/v1/namespaces/project-a/clusters/east", nil)
	response := httptest.NewRecorder()
	Middleware(authorizer, logr.Discard(), resources)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		objectAuthorizer, ok := handlers.ObjectAuthorizerFromContext(r.Context())
		if !ok {
			t.Fatal("object authorizer was not added to the request context")
		}
		allowed, err := objectAuthorizer(r.Context(), authorizationTestObject("east", "us-east1"))
		if err != nil || !allowed {
			t.Fatalf("allowed object authorization = %t, %v; want allowed", allowed, err)
		}
		allowed, err = objectAuthorizer(r.Context(), authorizationTestObject("east", "us-west1"))
		if err != nil || allowed {
			t.Fatalf("denied object authorization = %t, %v; want denied", allowed, err)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func seedClusterListBinding(t *testing.T, stores Stores, namespace string, condition *string) {
	t.Helper()
	seedClusterBinding(t, stores, namespace, []string{"cluster.list"}, condition)
}

func seedClusterGetBinding(t *testing.T, stores Stores, namespace string, condition *string) {
	t.Helper()
	seedClusterBinding(t, stores, namespace, []string{"cluster.get"}, condition)
}

func seedClusterBinding(t *testing.T, stores Stores, namespace string, permissions []string, condition *string) {
	t.Helper()
	role := &privatev1.PlatformRole{Spec: privatev1.PlatformRoleSpec{Permissions: permissions}}
	role.Name = "cluster-viewer"
	role.SetGroupVersionKind(privatev1.GroupVersion.WithKind("PlatformRole"))
	if err := stores.PlatformRoles.Create(context.Background(), role); err != nil {
		t.Fatal(err)
	}
	binding := &privatev1.RoleBinding{
		Spec: privatev1.RoleBindingSpec{
			Subject:   "alice@example.com",
			RoleRef:   privatev1.RoleRef{Kind: "PlatformRole", Name: "cluster-viewer", APIGroup: privatev1.GroupVersion.Group},
			Condition: condition,
		},
	}
	binding.Name = "alice-viewer"
	binding.Namespace = namespace
	binding.SetGroupVersionKind(privatev1.GroupVersion.WithKind("RoleBinding"))
	if err := stores.RoleBindings.Create(context.Background(), binding); err != nil {
		t.Fatal(err)
	}
}

func authorizationTestObject(name, region string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "gcp.managed.openshift.io/v1",
		"kind":       "Cluster",
		"metadata": map[string]interface{}{
			"namespace": "project-a",
			"name":      name,
		},
		"spec": map[string]interface{}{
			"region": region,
		},
	}}
}

func TestParseClusterChildRequestPath(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		plural string
		named  bool
	}{
		{name: "cluster item", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example", plural: "clusters", named: true},
		{name: "nodepool collection", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/nodepools", plural: "nodepools"},
		{name: "upgrade policy collection", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/controlplaneupgradepolicies", plural: "controlplaneupgradepolicies"},
		{name: "upgrade request collection", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/controlplaneupgraderequests", plural: "controlplaneupgraderequests"},
		{name: "upgrade request item", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/controlplaneupgraderequests/request-1", plural: "controlplaneupgraderequests", named: true},
		{name: "nodepool upgrade request collection", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/nodepoolupgraderequests", plural: "nodepoolupgraderequests"},
		{name: "nodepool upgrade request item", path: "/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/nodepoolupgraderequests/request-1", plural: "nodepoolupgraderequests", named: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRequestPath(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if got.namespace != "customer" || got.plural != tt.plural || got.named != tt.named {
				t.Fatalf("parsed request = %+v, want namespace customer, plural %q, named %t", got, tt.plural, tt.named)
			}
		})
	}
}

func TestNodePoolUpgradeRequestActions(t *testing.T) {
	tests := []struct {
		method string
		named  bool
		want   Action
	}{
		{method: http.MethodPost, want: CreateNodePoolUpgradeRequest},
		{method: http.MethodGet, want: ListNodePoolUpgradeRequests},
		{method: http.MethodGet, named: true, want: GetNodePoolUpgradeRequest},
		{method: http.MethodDelete, named: true, want: DeleteNodePoolUpgradeRequest},
	}
	for _, tt := range tests {
		got, err := actionForRequest(tt.method, "nodepoolupgraderequests", tt.named)
		if err != nil {
			t.Fatalf("%s named=%t: %v", tt.method, tt.named, err)
		}
		if got != tt.want {
			t.Errorf("%s named=%t: got %s, want %s", tt.method, tt.named, got, tt.want)
		}
	}
	if _, err := actionForRequest(http.MethodPatch, "nodepoolupgraderequests", true); err == nil {
		t.Fatal("NodePool upgrade requests must not be editable through the public API")
	}
}

func TestUnknownClusterChildRequestIsDenied(t *testing.T) {
	parsed, err := parseRequestPath("/apis/gcp.managed.openshift.io/v1/namespaces/customer/clusters/example/unknown")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actionForRequest(http.MethodGet, parsed.plural, parsed.named); err == nil {
		t.Fatal("expected authorization to reject an unknown child resource")
	}
}
