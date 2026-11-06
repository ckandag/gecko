package aggregated

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	testv1 "github.com/openshift-online/gecko/orlop/apis/private/test/v1"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apiserver/pkg/server/healthz"
	openapiproto "k8s.io/kube-openapi/pkg/util/proto"
)

type integrationTestEnv struct {
	server *AggregatedServer
	cancel context.CancelFunc
	port   int
	client *http.Client
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func setupIntegrationTest(t *testing.T) *integrationTestEnv {
	return setupIntegrationTestWithConfig(t, nil)
}

func setupIntegrationTestWithConfig(t *testing.T, mutate func(*Config)) *integrationTestEnv {
	t.Helper()

	scheme := newTestScheme(t)
	portInt := freePort(t)

	resources := []types.ResourceInfo{testv1.ObjectResourceInfo}

	cfg := Config{
		Scheme:    scheme,
		Resources: resources,
		StorageFactory: func(resourceType string, scheme *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
			return memory.NewMemoryStore(resourceType, scheme, gvk), nil
		},
		BindPort:    portInt,
		DisableAuth: true,
		Logger:      logr.Discard(),
	}
	if mutate != nil {
		mutate(&cfg)
	}

	completed, err := cfg.Complete()
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	server, err := New(completed)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := server.GenericAPIServer.PrepareRun().RunWithContext(ctx); err != nil {
			t.Logf("server exited: %v", err)
		}
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Timeout: 5 * time.Second,
	}

	// Wait for server to become ready.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(fmt.Sprintf("https://localhost:%d/healthz", portInt))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Cleanup(cancel)

	return &integrationTestEnv{
		server: server,
		cancel: cancel,
		port:   portInt,
		client: client,
	}
}

func (e *integrationTestEnv) url(path string) string {
	return fmt.Sprintf("https://localhost:%d%s", e.port, path)
}

func TestIntegration_HealthEndpoints(t *testing.T) {
	env := setupIntegrationTest(t)

	for _, endpoint := range []string{"/healthz", "/livez", "/readyz"} {
		t.Run(endpoint, func(t *testing.T) {
			resp, err := env.client.Get(env.url(endpoint))
			if err != nil {
				t.Fatalf("GET %s failed: %v", endpoint, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("expected 200 for %s, got %d: %s", endpoint, resp.StatusCode, string(body))
			}
		})
	}
}

func TestIntegration_OpenAPIV3(t *testing.T) {
	env := setupIntegrationTest(t)

	resp, err := env.client.Get(env.url("/openapi/v3"))
	if err != nil {
		t.Fatalf("GET /openapi/v3 failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 for /openapi/v3, got %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode OpenAPI response: %v", err)
	}

	// Verify our API group appears in paths.
	paths, ok := result["paths"].(map[string]interface{})
	if !ok {
		t.Fatal("expected 'paths' in OpenAPI response")
	}

	expectedGroupPath := "apis/test.orlop.gcp.managed.openshift.io/v1"
	if _, found := paths[expectedGroupPath]; !found {
		t.Errorf("expected path %q in OpenAPI v3, got paths: %v", expectedGroupPath, keysOf(paths))
	}
}

func TestIntegration_OpenAPIV2SupportsStrategicPatchOfMetadata(t *testing.T) {
	env := setupIntegrationTest(t)

	resp, err := env.client.Get(env.url("/openapi/v2"))
	if err != nil {
		t.Fatalf("get OpenAPI v2 document: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from OpenAPI v2 endpoint, got %d: %s", resp.StatusCode, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read OpenAPI v2 document: %v", err)
	}

	document, err := openapi_v2.ParseDocument(body)
	if err != nil {
		t.Fatalf("parse OpenAPI v2 document: %v", err)
	}
	models, err := openapiproto.NewOpenAPIData(document)
	if err != nil {
		t.Fatalf("load OpenAPI v2 models: %v", err)
	}
	definitionName := strings.ReplaceAll(goTypeName(&testv1.Object{}), "/", "~1")
	schema := models.LookupModel(definitionName)
	if schema == nil {
		t.Fatalf("OpenAPI v2 document did not contain the Object schema")
	}

	original := []byte(`{"apiVersion":"test.orlop.gcp.managed.openshift.io/v1","kind":"Object","metadata":{"name":"test","namespace":"default"},"spec":{"publicField":"old"}}`)
	modified := []byte(`{"apiVersion":"test.orlop.gcp.managed.openshift.io/v1","kind":"Object","metadata":{"annotations":{"argocd.argoproj.io/tracking-id":"argocd:Channel:test"},"name":"test","namespace":"default"},"spec":{"publicField":"new"}}`)
	current := []byte(`{"apiVersion":"test.orlop.gcp.managed.openshift.io/v1","kind":"Object","metadata":{"annotations":{"private.orlop.gcp.managed.openshift.io/created-by":"user@example.com"},"name":"test","namespace":"default"},"spec":{"publicField":"old"}}`)

	if _, err := strategicpatch.CreateThreeWayMergePatch(original, modified, current, strategicpatch.NewPatchMetaFromOpenAPI(schema), true); err != nil {
		t.Fatalf("calculate strategic patch using served OpenAPI v2 schema: %v", err)
	}
}

func TestIntegration_APIDiscovery(t *testing.T) {
	env := setupIntegrationTest(t)

	resp, err := env.client.Get(env.url("/apis"))
	if err != nil {
		t.Fatalf("GET /apis failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 for /apis, got %d: %s", resp.StatusCode, string(body))
	}

	var groupList metav1.APIGroupList
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &groupList); err != nil {
		t.Fatalf("failed to decode APIGroupList: %v", err)
	}

	found := false
	for _, g := range groupList.Groups {
		if g.Name == testv1.GroupVersion.Group {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected API group %q in discovery, got groups: %v", testv1.GroupVersion.Group, groupList.Groups)
	}
}

func TestIntegration_CRUD(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	// CREATE
	obj := &testv1.Object{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testv1.GroupVersion.String(),
			Kind:       "Object",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "integration-test",
			Namespace: "default",
		},
		Spec: testv1.ObjectSpec{
			PublicField:   "test-value",
			InternalField: "internal",
		},
	}

	createBody, _ := json.Marshal(obj)
	resp, err := doRequest(env.client, http.MethodPost, env.url(basePath), createBody)
	if err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("CREATE expected 201, got %d: %s", resp.StatusCode, string(body))
	}

	var created testv1.Object
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := json.Unmarshal(respBody, &created); err != nil {
		t.Fatalf("failed to unmarshal created object: %v", err)
	}
	if created.Name != "integration-test" {
		t.Errorf("expected name %q, got %q", "integration-test", created.Name)
	}
	if created.UID == "" {
		t.Error("expected UID to be set on created object")
	}
	if created.Generation != 1 {
		t.Errorf("expected generation 1, got %d", created.Generation)
	}

	// GET
	resp, err = env.client.Get(env.url(basePath + "/integration-test"))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	var fetched testv1.Object
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	json.Unmarshal(respBody, &fetched)
	if fetched.Spec.PublicField != "test-value" {
		t.Errorf("GET expected spec.publicField %q, got %q", "test-value", fetched.Spec.PublicField)
	}

	// LIST
	resp, err = env.client.Get(env.url(basePath))
	if err != nil {
		t.Fatalf("LIST failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("LIST expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	var list testv1.ObjectList
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	json.Unmarshal(respBody, &list)
	if len(list.Items) != 1 {
		t.Errorf("LIST expected 1 item, got %d", len(list.Items))
	}

	// UPDATE
	fetched.Spec.PublicField = "updated-value"
	updateBody, _ := json.Marshal(fetched)
	resp, err = doRequest(env.client, http.MethodPut, env.url(basePath+"/integration-test"), updateBody)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("UPDATE expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	var updated testv1.Object
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	json.Unmarshal(respBody, &updated)
	if updated.Spec.PublicField != "updated-value" {
		t.Errorf("UPDATE expected spec.publicField %q, got %q", "updated-value", updated.Spec.PublicField)
	}
	if updated.Generation != 2 {
		t.Errorf("UPDATE expected generation 2, got %d", updated.Generation)
	}

	// DELETE
	resp, err = doRequest(env.client, http.MethodDelete, env.url(basePath+"/integration-test"), nil)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE expected 200, got %d: %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()

	// Verify deleted.
	resp, err = env.client.Get(env.url(basePath + "/integration-test"))
	if err != nil {
		t.Fatalf("GET after DELETE failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after DELETE expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_StatusSubresource(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	// Create object.
	obj := &testv1.Object{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testv1.GroupVersion.String(),
			Kind:       "Object",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "status-test",
			Namespace: "default",
		},
		Spec: testv1.ObjectSpec{
			PublicField:   "original",
			InternalField: "internal",
		},
	}

	createBody, _ := json.Marshal(obj)
	resp, err := doRequest(env.client, http.MethodPost, env.url(basePath), createBody)
	if err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CREATE expected 201, got %d: %s", resp.StatusCode, string(respBody))
	}

	var created testv1.Object
	json.Unmarshal(respBody, &created)

	// Update status subresource.
	created.Status.Conditions = []string{"Ready"}
	created.Spec.PublicField = "should-be-ignored"
	statusBody, _ := json.Marshal(created)
	resp, err = doRequest(env.client, http.MethodPut, env.url(basePath+"/status-test/status"), statusBody)
	if err != nil {
		t.Fatalf("STATUS UPDATE failed: %v", err)
	}
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("STATUS UPDATE expected 200, got %d: %s", resp.StatusCode, string(respBody))
	}

	var statusUpdated testv1.Object
	json.Unmarshal(respBody, &statusUpdated)

	if len(statusUpdated.Status.Conditions) != 1 || statusUpdated.Status.Conditions[0] != "Ready" {
		t.Errorf("expected status.conditions=[Ready], got %v", statusUpdated.Status.Conditions)
	}
	if statusUpdated.Spec.PublicField != "original" {
		t.Errorf("expected spec.publicField to remain %q, got %q", "original", statusUpdated.Spec.PublicField)
	}
}

func TestIntegration_FinalizerSoftDelete(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	// Create object with finalizer.
	obj := &testv1.Object{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testv1.GroupVersion.String(),
			Kind:       "Object",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "finalizer-test",
			Namespace:  "default",
			Finalizers: []string{"test.io/cleanup"},
		},
		Spec: testv1.ObjectSpec{
			PublicField:   "value",
			InternalField: "internal",
		},
	}

	createBody, _ := json.Marshal(obj)
	resp, _ := doRequest(env.client, http.MethodPost, env.url(basePath), createBody)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CREATE expected 201, got %d: %s", resp.StatusCode, string(respBody))
	}

	// Delete — should soft-delete (set deletionTimestamp).
	resp, _ = doRequest(env.client, http.MethodDelete, env.url(basePath+"/finalizer-test"), nil)
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE expected 200, got %d: %s", resp.StatusCode, string(respBody))
	}

	var softDeleted testv1.Object
	json.Unmarshal(respBody, &softDeleted)
	if softDeleted.DeletionTimestamp == nil {
		t.Error("expected deletionTimestamp to be set after soft delete")
	}

	// Object should still be retrievable.
	resp, _ = env.client.Get(env.url(basePath + "/finalizer-test"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET after soft delete expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Remove finalizer via update → triggers hard delete.
	resp, _ = env.client.Get(env.url(basePath + "/finalizer-test"))
	respBody, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var current testv1.Object
	json.Unmarshal(respBody, &current)
	current.Finalizers = nil
	updateBody, _ := json.Marshal(current)
	resp, _ = doRequest(env.client, http.MethodPut, env.url(basePath+"/finalizer-test"), updateBody)
	resp.Body.Close()

	// Now it should be gone.
	resp, _ = env.client.Get(env.url(basePath + "/finalizer-test"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after hard delete expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_GetNotFound(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	resp, err := env.client.Get(env.url(basePath + "/nonexistent"))
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestStorageHealthChecker(t *testing.T) {
	scheme := newTestScheme(t)
	factory := func(resourceType string, s *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
		return memory.NewMemoryStore(resourceType, s, gvk), nil
	}

	checker := NewStorageHealthChecker(factory, scheme, "objects", testGVK)

	if checker.Name() != "storage" {
		t.Errorf("expected name %q, got %q", "storage", checker.Name())
	}

	if err := checker.Check(nil); err != nil {
		t.Errorf("health check failed: %v", err)
	}
}

func TestStorageHealthChecker_FactoryError(t *testing.T) {
	scheme := newTestScheme(t)
	factory := func(resourceType string, s *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
		return nil, fmt.Errorf("connection refused")
	}

	checker := NewStorageHealthChecker(factory, scheme, "objects", testGVK)
	err := checker.Check(nil)
	if err == nil {
		t.Fatal("expected error from failing factory")
	}
}

func TestIntegration_CustomHealthCheck(t *testing.T) {
	env := setupIntegrationTestWithConfig(t, func(cfg *Config) {
		cfg.HealthCheckers = []healthz.HealthChecker{
			healthz.NamedCheck("custom-check", func(r *http.Request) error {
				return nil
			}),
		}
	})

	// Check that our custom health check is included.
	resp, err := env.client.Get(env.url("/healthz/custom-check"))
	if err != nil {
		t.Fatalf("GET /healthz/custom-check failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected 200 for /healthz/custom-check, got %d: %s", resp.StatusCode, string(body))
	}
}

func TestIntegration_DisableAuth_AllowsAnonymous(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	// With DisableAuth=true, anonymous requests should work.
	resp, err := env.client.Get(env.url(basePath))
	if err != nil {
		t.Fatalf("anonymous GET failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for anonymous request with DisableAuth, got %d", resp.StatusCode)
	}
}

func TestIntegration_Watch(t *testing.T) {
	env := setupIntegrationTest(t)
	basePath := fmt.Sprintf("/apis/%s/%s/namespaces/default/objects",
		testv1.GroupVersion.Group, testv1.GroupVersion.Version)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start watch.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.url(basePath+"?watch=true"), nil)
	resp, err := env.client.Do(req)
	if err != nil {
		t.Fatalf("WATCH failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("WATCH expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	// Create an object to trigger a watch event.
	obj := &testv1.Object{
		TypeMeta: metav1.TypeMeta{
			APIVersion: testv1.GroupVersion.String(),
			Kind:       "Object",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-test",
			Namespace: "default",
		},
		Spec: testv1.ObjectSpec{
			PublicField:   "watch-value",
			InternalField: "internal",
		},
	}
	createBody, _ := json.Marshal(obj)
	createResp, err := doRequest(env.client, http.MethodPost, env.url(basePath), createBody)
	if err != nil {
		t.Fatalf("CREATE during watch failed: %v", err)
	}
	createResp.Body.Close()

	// Read watch event.
	decoder := json.NewDecoder(resp.Body)
	var event struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
	}

	if err := decoder.Decode(&event); err != nil {
		t.Fatalf("failed to decode watch event: %v", err)
	}
	if event.Type != "ADDED" {
		t.Errorf("expected ADDED event, got %s", event.Type)
	}
}

func doRequest(client *http.Client, method, url string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = io.NopCloser(newBytesReader(body))
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return client.Do(req)
}

type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func keysOf(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
