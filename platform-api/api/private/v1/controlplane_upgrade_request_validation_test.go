package v1

import (
	"strings"
	"testing"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func validControlPlaneUpgradeRequest(t *testing.T) (*ControlPlaneUpgradeRequest, *Cluster) {
	t.Helper()
	request := &ControlPlaneUpgradeRequest{Spec: ControlPlaneUpgradeRequestSpec{
		ClusterID: "example-cluster", TargetVersion: "4.22.15",
	}}
	request.SetNamespace("customer-project")
	cluster := &Cluster{}
	cluster.SetName("example-cluster")
	cluster.SetNamespace("customer-project")
	cluster.Spec.Release.Version = "4.22.14"
	cluster.Status.HostedClusterResult = &HostedClusterResult{
		Version: "4.22.14", DesiredVersion: "4.22.14", AvailableUpdates: []string{"4.22.15"},
	}
	return request, cluster
}

func TestControlPlaneUpgradeRequestValidateCreate(t *testing.T) {
	request, cluster := validControlPlaneUpgradeRequest(t)
	ctx := types.WithParentObject(t.Context(), cluster)
	if err := request.ValidateCreate(ctx); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestControlPlaneUpgradeRequestValidateCreate_UnstructuredParent(t *testing.T) {
	request, cluster := validControlPlaneUpgradeRequest(t)
	parentMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cluster)
	if err != nil {
		t.Fatal(err)
	}
	ctx := types.WithParentObject(t.Context(), &unstructured.Unstructured{Object: parentMap})
	if err := request.ValidateCreate(ctx); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestControlPlaneUpgradeRequestValidateCreate_InvalidRequest(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ControlPlaneUpgradeRequest, *Cluster)
		want   string
	}{
		{name: "missing target", mutate: func(r *ControlPlaneUpgradeRequest, _ *Cluster) {
			r.Spec.TargetVersion = ""
		}, want: "spec.targetVersion"},
		{name: "parent mismatch", mutate: func(r *ControlPlaneUpgradeRequest, _ *Cluster) {
			r.Spec.ClusterID = "another-cluster"
		}, want: "does not match"},
		{name: "missing feedback", mutate: func(_ *ControlPlaneUpgradeRequest, c *Cluster) {
			c.Status.HostedClusterResult = nil
		}, want: "no observed"},
		{name: "active upgrade", mutate: func(_ *ControlPlaneUpgradeRequest, c *Cluster) {
			c.Spec.Release.Version = "4.22.15"
		}, want: "already has"},
		{name: "target not advertised", mutate: func(r *ControlPlaneUpgradeRequest, _ *Cluster) {
			r.Spec.TargetVersion = "4.22.16"
		}, want: "not an available"},
		{name: "target not newer", mutate: func(r *ControlPlaneUpgradeRequest, _ *Cluster) {
			r.Spec.TargetVersion = "4.22.14"
		}, want: "must be newer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, cluster := validControlPlaneUpgradeRequest(t)
			tt.mutate(request, cluster)
			ctx := types.WithParentObject(t.Context(), cluster)
			if err := request.ValidateCreate(ctx); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateCreate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestControlPlaneUpgradeRequestValidateUpdate_SpecImmutable(t *testing.T) {
	request, _ := validControlPlaneUpgradeRequest(t)
	updated := request.DeepCopy()
	updated.Spec.TargetVersion = "4.22.16"
	if err := updated.ValidateUpdate(t.Context(), request); err == nil {
		t.Fatal("expected spec immutability error")
	}
}
