package v1

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNodePoolSchemaChannelGroup(t *testing.T) {
	processor := newSchemaProcessor(t, NodePoolSchemaYAML)
	for _, group := range []string{"stable", "fast", "candidate", "eus", "nightly", "dev-preview", "a", strings.Repeat("a", 63)} {
		t.Run("accept-"+group, func(t *testing.T) {
			if errs := processor.Process(context.Background(), nodePoolAsMap(t, group), nil); len(errs) > 0 {
				t.Fatalf("expected channelGroup %q to pass schema validation, got: %v", group, errs)
			}
		})
	}
	for _, group := range []string{"", "Nightly", "-nightly", "nightly-", "nightly_x", "nightly.4", strings.Repeat("a", 64)} {
		t.Run("reject-"+group, func(t *testing.T) {
			errs := processor.Process(context.Background(), nodePoolAsMap(t, group), nil)
			if len(errs) == 0 || !strings.Contains(errs.ToAggregate().Error(), "channelGroup") {
				t.Fatalf("expected channelGroup %q to fail schema validation, got: %v", group, errs)
			}
		})
	}
}

func TestNodePoolSchemaReleaseVersion(t *testing.T) {
	processor := newSchemaProcessor(t, NodePoolSchemaYAML)
	for _, tt := range []struct {
		version string
		valid   bool
	}{
		{version: "4.23.9", valid: true},
		{version: "5.0.0-ec.6", valid: true},
		{version: "4.23"},
		{version: "4"},
		{version: "v4.23.9"},
		{version: "4.23.9-01"},
	} {
		t.Run(tt.version, func(t *testing.T) {
			for _, operation := range []string{"create", "update"} {
				t.Run(operation, func(t *testing.T) {
					object := nodePoolAsMap(t, "stable")
					object["spec"].(map[string]any)["release"].(map[string]any)["version"] = tt.version
					var old map[string]any
					if operation == "update" {
						old = nodePoolAsMap(t, "stable")
					}
					errs := processor.Process(t.Context(), object, old)
					if tt.valid && len(errs) > 0 {
						t.Fatalf("expected version %q to pass schema validation, got: %v", tt.version, errs)
					}
					if !tt.valid && (len(errs) == 0 || !strings.Contains(errs.ToAggregate().Error(), "version must be")) {
						t.Fatalf("expected version %q to fail version validation, got: %v", tt.version, errs)
					}
				})
			}
		})
	}
}

func nodePoolAsMap(t *testing.T, group string) map[string]any {
	t.Helper()
	return objectAsMap(t, NodePool{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "NodePool"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-nodepool", Namespace: "test-namespace"},
		Spec: NodePoolSpec{
			ClusterID: "test-cluster",
			Platform: NodePoolPlatformSpec{
				Type: "GCP",
				GCP:  &GCPNodePoolPlatform{MachineType: "n2-standard-4", Zone: "us-central1-a", Subnet: "test-subnet"},
			},
			Release: NodePoolReleaseSpec{Version: "5.0.0-ec.6", ChannelGroup: group},
		},
	})
}
