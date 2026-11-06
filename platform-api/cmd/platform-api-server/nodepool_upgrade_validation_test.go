package main

import (
	"strings"
	"testing"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func validationFixtures(t *testing.T) (*nodePoolUpgradeValidator, *privatev1.NodePool) {
	t.Helper()
	scheme := getPrivateScheme()
	factory := func(resourceType string, scheme *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
		return memory.NewMemoryStore(resourceType, scheme, gvk), nil
	}
	validator, err := newNodePoolUpgradeValidator(factory, scheme)
	if err != nil {
		t.Fatal(err)
	}
	cluster := testCluster("customer", "example", "us-central1")
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.22.17"}
	release := &privatev1.Version{ObjectMeta: metav1.ObjectMeta{Name: "4.22.16"}, Spec: privatev1.VersionSpec{ChannelGroups: []string{"stable"}, ReleaseImage: "quay.io/example/release:4.22.16"}}
	if err := validator.clusters.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := validator.versions.Create(t.Context(), release); err != nil {
		t.Fatal(err)
	}
	pool := &privatev1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "customer"},
		Spec:       privatev1.NodePoolSpec{ClusterID: "example", Release: privatev1.ReleaseSpec{Version: "4.22.14", ChannelGroup: "stable"}},
		Status:     privatev1.NodePoolStatus{Version: "4.22.14"},
	}
	return validator, pool
}

func TestNodePoolUpgradeValidator(t *testing.T) {
	tests := []struct {
		name   string
		change func(*privatev1.NodePool)
		want   string
	}{
		{name: "valid target"},
		{name: "missing Cluster", change: func(p *privatev1.NodePool) { p.Spec.ClusterID = "missing" }, want: "Cluster does not exist"},
		{name: "outside channel", change: func(p *privatev1.NodePool) { p.Spec.Release.ChannelGroup = "fast" }, want: "channel group"},
		{name: "unknown observed version", change: func(p *privatev1.NodePool) { p.Status.Version = "" }, want: "must be observed"},
		{name: "pending target while pool is progressing", change: func(p *privatev1.NodePool) {
			p.Status.Conditions = []metav1.Condition{{Type: "NodePoolProgressing", Status: metav1.ConditionTrue}}
		}},
		{name: "target not newer", change: func(p *privatev1.NodePool) { p.Status.Version = "4.22.16"; p.Spec.Release.Version = "4.22.16" }, want: "newer than"},
		{name: "invalid observed version", change: func(p *privatev1.NodePool) { p.Status.Version = "bad"; p.Spec.Release.Version = "bad" }, want: "observed NodePool version is invalid"},
		{name: "unsupported minor step", change: func(p *privatev1.NodePool) { p.Status.Version = "4.20.14"; p.Spec.Release.Version = "4.20.14" }, want: "next-minor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator, pool := validationFixtures(t)
			if tt.change != nil {
				tt.change(pool)
			}
			err := validator.Validate(t.Context(), pool, "4.22.16")
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNodePoolUpgradeDatabaseRepresentation(t *testing.T) {
	validator, pool := validationFixtures(t)
	// Database Get returns Unstructured even when memory-backed fixtures are typed.
	for _, item := range []struct {
		store           storage.ResourceStore
		namespace, name string
	}{
		{validator.clusters, "customer", "example"}, {validator.versions, "", "4.22.16"},
	} {
		obj, err := item.store.Get(t.Context(), item.namespace, item.name)
		if err != nil {
			t.Fatal(err)
		}
		data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			t.Fatal(err)
		}
		if err := item.store.Update(t.Context(), &unstructured.Unstructured{Object: data}); err != nil {
			t.Fatal(err)
		}
	}
	if err := validator.Validate(t.Context(), pool, "4.22.16"); err != nil {
		t.Fatal(err)
	}

}

func TestNodePoolUpgradeTargetVersionBounds(t *testing.T) {
	tests := []struct{ name, current, target, controlPlane, channel, want string }{
		{"above control plane", "4.22.14", "4.22.18", "4.22.17", "stable", "not be newer"},
		{"outside skew", "4.18.1", "4.19.1", "4.23.1", "stable", "three minor"},
		{"at skew limit", "4.19.1", "4.20.1", "4.23.1", "stable", ""},
		{"EUS step", "4.20.1", "4.22.1", "4.22.17", "eus", ""},
		{"odd EUS step", "4.19.1", "4.21.1", "4.22.17", "eus", "next-minor"},
		{"downgrade", "4.22.17", "4.22.16", "4.22.17", "stable", "newer than"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, pool := validationFixtures(t)
			pool.Spec.Release.Version, pool.Status.Version = tt.current, tt.current
			pool.Spec.Release.ChannelGroup = tt.channel
			cluster := &privatev1.Cluster{Status: privatev1.ClusterStatus{HostedClusterResult: &privatev1.HostedClusterResult{Version: tt.controlPlane}}}
			release := &privatev1.Version{Spec: privatev1.VersionSpec{ChannelGroups: []string{tt.channel}}}
			err := privatev1.ValidateNodePoolUpgradeTarget(pool, cluster, release, tt.target)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestSharedMemoryStorageFactory(t *testing.T) {
	factory := newSharedMemoryStorageFactory()
	scheme := getPrivateScheme()
	gvk := privatev1.GroupVersion.WithKind("Cluster")
	first, err := factory("clusters", scheme, gvk)
	if err != nil {
		t.Fatal(err)
	}
	second, err := factory("clusters", scheme, gvk)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("API and validator received different stores")
	}
}
