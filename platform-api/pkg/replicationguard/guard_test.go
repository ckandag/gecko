package replicationguard

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/constants"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"
	publicv1 "github.com/openshift-online/gecko/platform-api/api/public/v1"
	"github.com/openshift-online/gecko/platform-api/pkg/authn"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	apiPrefix           = "/apis/gcp.managed.openshift.io/v1"
	replicatedResources = "roles.gcp.managed.openshift.io,rolebindings.gcp.managed.openshift.io"
)

func testConfig() Config {
	return Config{
		Enabled: true, Region: "us-east4", LeaderRegion: "us-central1",
		Replicate: replicatedResources, Resources: publicv1.GetResourceInfos(),
	}
}

func TestFollowerWriteGuard(t *testing.T) {
	guard, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{name: "create Role", method: http.MethodPost, path: apiPrefix + "/namespaces/customer/roles", wantStatus: http.StatusConflict},
		{name: "update Role", method: http.MethodPut, path: apiPrefix + "/namespaces/customer/roles/viewer", wantStatus: http.StatusConflict},
		{name: "patch Role", method: http.MethodPatch, path: apiPrefix + "/namespaces/customer/roles/viewer", wantStatus: http.StatusConflict},
		{name: "delete Role", method: http.MethodDelete, path: apiPrefix + "/namespaces/customer/roles/viewer", wantStatus: http.StatusConflict},
		{name: "create RoleBinding", method: http.MethodPost, path: apiPrefix + "/namespaces/customer/rolebindings", wantStatus: http.StatusConflict},
		{name: "update RoleBinding", method: http.MethodPut, path: apiPrefix + "/namespaces/customer/rolebindings/viewer", wantStatus: http.StatusConflict},
		{name: "patch RoleBinding", method: http.MethodPatch, path: apiPrefix + "/namespaces/customer/rolebindings/viewer", wantStatus: http.StatusConflict},
		{name: "delete RoleBinding", method: http.MethodDelete, path: apiPrefix + "/namespaces/customer/rolebindings/viewer", wantStatus: http.StatusConflict},
		{name: "read Role", method: http.MethodGet, path: apiPrefix + "/namespaces/customer/roles/viewer", wantStatus: http.StatusNoContent},
		{name: "list RoleBindings", method: http.MethodGet, path: apiPrefix + "/namespaces/customer/rolebindings", wantStatus: http.StatusNoContent},
		{name: "create Cluster", method: http.MethodPost, path: apiPrefix + "/namespaces/customer/clusters", wantStatus: http.StatusNoContent},
		{name: "PlatformRole is not replicated", method: http.MethodPost, path: apiPrefix + "/platformroles", wantStatus: http.StatusNoContent},
		{name: "other API group", method: http.MethodPost, path: "/apis/other.example/v1/namespaces/customer/roles", wantStatus: http.StatusNoContent},
		{name: "other API version", method: http.MethodPost, path: "/apis/gcp.managed.openshift.io/v2/namespaces/customer/roles", wantStatus: http.StatusNoContent},
		{name: "POST on a named resource", method: http.MethodPost, path: apiPrefix + "/namespaces/customer/roles/viewer", wantStatus: http.StatusNoContent},
		{name: "PUT on a collection", method: http.MethodPut, path: apiPrefix + "/namespaces/customer/roles", wantStatus: http.StatusNoContent},
		{name: "PATCH on a collection", method: http.MethodPatch, path: apiPrefix + "/namespaces/customer/roles", wantStatus: http.StatusNoContent},
		{name: "DELETE on a collection", method: http.MethodDelete, path: apiPrefix + "/namespaces/customer/roles", wantStatus: http.StatusNoContent},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextCalls := 0
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				nextCalls++
				w.WriteHeader(http.StatusNoContent)
			})
			request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil)
			response := httptest.NewRecorder()
			guard(next).ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantStatus == http.StatusConflict {
				if nextCalls != 0 {
					t.Fatalf("resource handler called %d times for a rejected write", nextCalls)
				}
				if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
					t.Fatalf("content type = %q, want application/json", contentType)
				}
				if body := response.Body.String(); body != "{\"error\":\"region is read-only\",\"retryable\":false}" {
					t.Fatalf("read-only response = %q", body)
				}
			} else if nextCalls != 1 {
				t.Fatalf("resource handler called %d times, want one", nextCalls)
			}
		})
	}
}

func TestLeaderAndDisabledGuardAllowWrites(t *testing.T) {
	leader := testConfig()
	leader.Region = leader.LeaderRegion
	disabled := testConfig()
	disabled.Enabled = false
	for _, config := range []Config{leader, disabled} {
		guard, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, apiPrefix+"/namespaces/customer/roles", nil)
		response := httptest.NewRecorder()
		guard(next).ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("config %+v: status = %d, want %d", config, response.Code, http.StatusNoContent)
		}
	}
}

func TestEnabledGuardRequiresBothRegions(t *testing.T) {
	base := testConfig()
	configs := []Config{base, base, base, base}
	configs[0].Region = ""
	configs[0].LeaderRegion = ""
	configs[1].LeaderRegion = ""
	configs[2].Region = ""
	configs[3].Region = " us-east4"
	for _, config := range configs {
		if _, err := New(config); err == nil {
			t.Fatalf("New(%+v) succeeded with incomplete region configuration", config)
		}
	}
}

func TestAuthenticationRunsBeforeFollowerGuard(t *testing.T) {
	guard, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := authn.Middleware(authn.Config{})(guard(next))
	requestPath := apiPrefix + "/namespaces/customer/roles"

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, requestPath, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing identity status = %d, want %d", response.Code, http.StatusUnauthorized)
	}

	request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, requestPath, nil)
	claims := base64.RawURLEncoding.EncodeToString([]byte("{\"email\":\"user@example.com\",\"email_verified\":true}"))
	request.Header.Set(constants.HeaderEndpointAPIUserInfo, claims)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "read-only") {
		t.Fatalf("authenticated follower write status = %d, body = %q", response.Code, response.Body.String())
	}
}

func TestRejectedWriteCounterIsExported(t *testing.T) {
	config := testConfig()
	config.Region = "metric-follower"
	config.LeaderRegion = "leader"
	guard, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	before := rejectedWriteCount(t)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("rejected write reached the resource handler")
	})
	response := httptest.NewRecorder()
	guard(next).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodPost, apiPrefix+"/namespaces/customer/roles", nil))
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}

	if got := rejectedWriteCount(t); got != before+1 {
		t.Fatalf("rejection count = %v, want %v", got, before+1)
	}
}

func rejectedWriteCount(t *testing.T) float64 {
	t.Helper()
	families, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "replication_readonly_write_rejections_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["region"] == "metric-follower" && labels["resource_kind"] == "Role" && labels["verb"] == "create" {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestReplicatedResourcesAreConfigured(t *testing.T) {
	config := testConfig()
	config.Replicate = "roles.gcp.managed.openshift.io"
	guard, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	for _, test := range []struct {
		path string
		want int
	}{
		{apiPrefix + "/namespaces/customer/roles", http.StatusConflict},
		{apiPrefix + "/namespaces/customer/rolebindings", http.StatusNoContent},
	} {
		response := httptest.NewRecorder()
		guard(next).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodPost, test.path, nil))
		if response.Code != test.want {
			t.Errorf("POST %s: status = %d, want %d", test.path, response.Code, test.want)
		}
	}
}

func TestConfiguredPublicResourceUsesRegisteredRoute(t *testing.T) {
	config := testConfig()
	config.Replicate = "widgets.example.io,globalwidgets.example.io"
	config.Resources = []types.ResourceInfo{
		{GVK: schema.GroupVersionKind{Group: "example.io", Version: "v1", Kind: "Widget"}, Plural: "widgets", Namespaced: true},
		{GVK: schema.GroupVersionKind{Group: "example.io", Version: "v1", Kind: "GlobalWidget"}, Plural: "globalwidgets"},
	}
	guard, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	for _, path := range []string{
		"/apis/example.io/v1/namespaces/customer/widgets",
		"/apis/example.io/v1/globalwidgets",
	} {
		response := httptest.NewRecorder()
		guard(next).ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil))
		if response.Code != http.StatusConflict {
			t.Errorf("POST %s: status = %d, want %d", path, response.Code, http.StatusConflict)
		}
	}
}

func TestEnabledGuardRejectsUnknownOrEmptyResourceList(t *testing.T) {
	for _, list := range []string{"", "unknown.gcp.managed.openshift.io"} {
		config := testConfig()
		config.Replicate = list
		if _, err := New(config); err == nil {
			t.Errorf("New with resource list %q succeeded", list)
		}
	}
}
