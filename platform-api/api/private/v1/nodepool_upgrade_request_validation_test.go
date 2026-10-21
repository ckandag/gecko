package v1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validNodePoolUpgradeRequest() *NodePoolUpgradeRequest {
	return &NodePoolUpgradeRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-workers", Namespace: "customer"},
		Spec: NodePoolUpgradeRequestSpec{
			ClusterID: "example", NodePoolID: "workers", TargetVersion: "4.22.16",
		},
	}
}

func TestNodePoolUpgradeRequestValidateCreateUsesStorageValidator(t *testing.T) {
	SetValidatorDeps(ValidatorDeps{
		ValidateNodePoolUpgradeRequest: func(_ context.Context, request *NodePoolUpgradeRequest) error {
			if request.Spec.NodePoolID != "workers" {
				t.Fatalf("NodePoolID = %q, want workers", request.Spec.NodePoolID)
			}
			return errors.New("NodePool does not exist")
		},
	})
	t.Cleanup(func() { SetValidatorDeps(ValidatorDeps{}) })

	err := validNodePoolUpgradeRequest().ValidateCreate(t.Context())
	if err == nil || !strings.Contains(err.Error(), "NodePool does not exist") {
		t.Fatalf("ValidateCreate() error = %v, want NodePool error", err)
	}
}

func TestNodePoolUpgradeRequestValidation(t *testing.T) {
	ctx := context.Background()
	request := validNodePoolUpgradeRequest()
	if err := request.ValidateCreate(ctx); err != nil {
		t.Fatalf("valid create: %v", err)
	}
	invalid := request.DeepCopy()
	invalid.Spec.TargetVersion = "not-a-version"
	if err := invalid.ValidateCreate(ctx); err == nil {
		t.Fatal("expected invalid target version to be rejected")
	}
	changed := request.DeepCopy()
	changed.Spec.TargetVersion = "4.22.17"
	if err := changed.ValidateUpdate(ctx, request); err == nil {
		t.Fatal("expected immutable spec to be rejected")
	}
	if err := request.ValidateDelete(ctx); err != nil {
		t.Fatalf("unaccepted request should be cancellable: %v", err)
	}
	accepted := metav1.NewTime(time.Now())
	request.Status.AcceptedTime = &accepted
	if err := request.ValidateDelete(ctx); err == nil {
		t.Fatal("accepted request must not be deleted")
	}
}
