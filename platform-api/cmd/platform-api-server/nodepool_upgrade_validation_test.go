package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

//nolint:kubeapilinter // Table-driven test inputs are not serialized API fields.
type nodePoolUpgradeValidationCase struct {
	name           string
	changeCluster  func(*privatev1.Cluster)
	changeNodePool func(*privatev1.NodePool)
	changeVersion  func(*privatev1.Version)
	omitCluster    bool
	omitNodePool   bool
	omitVersion    bool
	scheduled      bool
	wantErr        string
}

func TestNodePoolUpgradeValidator(t *testing.T) {
	tests := []nodePoolUpgradeValidationCase{
		{name: "valid target"},
		{name: "missing Cluster", omitCluster: true, wantErr: "cluster \"example\" does not exist"},
		{name: "missing NodePool", omitNodePool: true, wantErr: "nodepool \"workers\" does not exist"},
		{
			name:           "NodePool belongs to another Cluster",
			changeNodePool: func(pool *privatev1.NodePool) { pool.Spec.ClusterID = "other" },
			wantErr:        "does not belong to cluster",
		},
		{name: "missing version", omitVersion: true, wantErr: "not in the platform version catalog"},
		{
			name: "version outside channel group",
			changeVersion: func(version *privatev1.Version) {
				version.Spec.ChannelGroups = []string{"candidate"}
			},
			wantErr: "not available in channel group",
		},
		{
			name:           "missing observed NodePool version",
			changeNodePool: func(pool *privatev1.NodePool) { pool.Status.CurrentVersion = "" },
			wantErr:        "versions must be observed",
		},
		{
			name:           "NodePool already upgrading",
			changeNodePool: func(pool *privatev1.NodePool) { pool.Spec.Release.Version = "4.22.15" },
			wantErr:        "already has an upgrade in progress",
		},
		{
			name:           "scheduled request is rejected while NodePool is upgrading",
			changeNodePool: func(pool *privatev1.NodePool) { pool.Spec.Release.Version = "4.22.15" },
			scheduled:      true,
			wantErr:        "already has an upgrade in progress",
		},
		{
			name: "target not newer",
			changeNodePool: func(pool *privatev1.NodePool) {
				pool.Status.CurrentVersion = "4.22.16"
				pool.Spec.Release.Version = "4.22.16"
			},
			wantErr: "must be newer than the observed NodePool version",
		},
		{
			name: "target above control plane",
			changeCluster: func(cluster *privatev1.Cluster) {
				cluster.Status.HostedClusterResult.Version = "4.22.15"
			},
			wantErr: "must not be newer than the observed control-plane version",
		},
		{
			name: "scheduled target above control plane is rejected now",
			changeCluster: func(cluster *privatev1.Cluster) {
				cluster.Status.HostedClusterResult.Version = "4.22.15"
			},
			scheduled: true,
			wantErr:   "must not be newer than the observed control-plane version",
		},
		{
			name: "unsupported minor step",
			changeNodePool: func(pool *privatev1.NodePool) {
				pool.Status.CurrentVersion = "4.20.14"
				pool.Spec.Release.Version = "4.20.14"
			},
			wantErr: "must be a z-stream or next-minor upgrade",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestNodePoolUpgradeValidator(t, tt)
			request := &privatev1.NodePoolUpgradeRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "upgrade-workers", Namespace: "customer"},
				Spec: privatev1.NodePoolUpgradeRequestSpec{
					ClusterID: "example", NodePoolID: "workers", TargetVersion: "4.22.16",
				},
			}
			if tt.scheduled {
				start := metav1.NewTime(time.Now().Add(time.Hour))
				request.Spec.StartTime = &start
			}
			err := validator.Validate(t.Context(), request)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.wantErr)
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
		t.Fatal("the API server and validator received different in-memory Cluster stores")
	}
}

func newTestNodePoolUpgradeValidator(t *testing.T, tt nodePoolUpgradeValidationCase) *nodePoolUpgradeValidator {
	t.Helper()
	scheme := getPrivateScheme()
	stores := map[runtimeschema.GroupVersionKind]storage.ResourceStore{}
	factory := func(resourceType string, scheme *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
		if store, found := stores[gvk]; found {
			return store, nil
		}
		store := memory.NewMemoryStore(resourceType, scheme, gvk)
		stores[gvk] = store
		return store, nil
	}
	validator, err := newNodePoolUpgradeValidator(factory, scheme)
	if err != nil {
		t.Fatal(err)
	}
	cluster := testCluster("customer", "example", "us-central1")
	cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.22.17"}
	pool := &privatev1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "customer"},
		Spec: privatev1.NodePoolSpec{
			ClusterID: "example",
			Release:   privatev1.ReleaseSpec{Version: "4.22.14", ChannelGroup: "stable"},
		},
		Status: privatev1.NodePoolStatus{CurrentVersion: "4.22.14"},
	}
	version := &privatev1.Version{
		ObjectMeta: metav1.ObjectMeta{Name: "4.22.16"},
		Spec: privatev1.VersionSpec{
			ChannelGroups: []string{"stable"}, ReleaseImage: "quay.io/example/release:4.22.16",
		},
	}
	if tt.changeCluster != nil {
		tt.changeCluster(cluster)
	}
	if tt.changeNodePool != nil {
		tt.changeNodePool(pool)
	}
	if tt.changeVersion != nil {
		tt.changeVersion(version)
	}
	for _, item := range []struct {
		store   storage.ResourceStore
		object  client.Object
		omitted bool
	}{
		{validator.clusters, cluster, tt.omitCluster},
		{validator.nodePools, pool, tt.omitNodePool},
		{validator.versions, version, tt.omitVersion},
	} {
		if item.omitted {
			continue
		}
		if err := item.store.Create(context.Background(), item.object); err != nil {
			t.Fatal(err)
		}
	}
	return validator
}
