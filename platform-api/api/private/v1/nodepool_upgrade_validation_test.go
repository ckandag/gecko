package v1

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func upgradeValidationPool() *NodePool {
	return &NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "customer"},
		Spec:       NodePoolSpec{ClusterID: "example", Release: ReleaseSpec{Version: "4.22.14", ChannelGroup: "stable"}},
		Status:     NodePoolStatus{Version: "4.22.14"},
	}
}

func TestNodePoolReleaseValidationSkipsUnrelatedChanges(t *testing.T) {
	validations := 0
	SetValidatorDeps(ValidatorDeps{ValidateNodePoolUpgrade: func(_ context.Context, pool *NodePool, target string) error {
		validations++
		if pool.Status.Version != "4.22.14" || target != "4.22.16" {
			t.Fatal("unexpected validation input")
		}
		return nil
	}})
	t.Cleanup(func() { SetValidatorDeps(ValidatorDeps{}) })
	old := upgradeValidationPool()
	updated := old.DeepCopy()
	updated.Spec.Release.Version = "4.22.16"
	if err := updated.ValidateUpdate(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	scaled := updated.DeepCopy()
	scaled.Spec.NodeCount = ptr.To(int32(3))
	if err := scaled.ValidateUpdate(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	if validations != 1 {
		t.Fatalf("storage-backed checks = %d, want 1", validations)
	}
}

func TestNodePoolUpgradeUpdateRules(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name      string
		changeOld func(*NodePool)
		changeNew func(*NodePool)
		want      string
	}{
		{name: "direct release upgrade", changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.16" }},
		{name: "scheduled release upgrade", changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.16"; p.Spec.Release.UpgradeStartTime = &now }},
		{name: "pending cancellation", changeOld: func(p *NodePool) { p.Spec.Release.Version = "4.22.16"; p.Spec.Release.UpgradeStartTime = &now }, changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.14"; p.Spec.Release.UpgradeStartTime = nil }},
		{name: "invalid target", changeNew: func(p *NodePool) { p.Spec.Release.Version = "invalid" }, want: "semantic version"},
		{name: "invalid timestamp", changeNew: func(p *NodePool) {
			p.Spec.Release.Version = "4.22.16"
			p.Spec.Release.UpgradeStartTime = &metav1.Time{}
		}, want: "valid timestamp"},
		{name: "schedule without upgrade", changeNew: func(p *NodePool) { p.Spec.Release.UpgradeStartTime = &now }, want: "requires a version upgrade"},
		{name: "accepted cancellation", changeOld: func(p *NodePool) {
			p.Spec.Release.Version = "4.22.16"
			p.Status.Conditions = []metav1.Condition{{Type: NodePoolUpgradeProgressing, Status: metav1.ConditionTrue, Reason: "UpgradeAccepted"}}
		}, changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.14" }, want: "accepted upgrade is active"},
		{name: "accepted schedule change", changeOld: func(p *NodePool) {
			p.Spec.Release.Version = "4.22.16"
			p.Status.Conditions = []metav1.Condition{{Type: NodePoolUpgradeProgressing, Status: metav1.ConditionTrue, Reason: "UpgradeAccepted"}}
		}, changeNew: func(p *NodePool) { p.Spec.Release.UpgradeStartTime = &now }, want: "accepted upgrade is active"},
		{name: "accepted channel change", changeOld: func(p *NodePool) {
			p.Spec.Release.Version = "4.22.16"
			p.Status.Conditions = []metav1.Condition{{Type: NodePoolUpgradeProgressing, Status: metav1.ConditionTrue, Reason: "UpgradeAccepted"}}
		}, changeNew: func(p *NodePool) { p.Spec.Release.ChannelGroup = "fast" }, want: "accepted upgrade is active"},
		{name: "new target after completion", changeOld: func(p *NodePool) {
			p.Spec.Release.Version = "4.22.16"
			p.Status.Version = "4.22.16"
			p.Status.Conditions = []metav1.Condition{{Type: NodePoolUpgradeProgressing, Status: metav1.ConditionFalse, Reason: "UpgradeCompleted"}}
		}, changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.17" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := upgradeValidationPool()
			if tt.changeOld != nil {
				tt.changeOld(old)
			}
			updated := old.DeepCopy()
			tt.changeNew(updated)
			err := updated.ValidateUpdate(t.Context(), old)
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

func TestNodePoolInitialInstallationUnchanged(t *testing.T) {
	pool := upgradeValidationPool()
	pool.Status = NodePoolStatus{}
	if err := pool.ValidateCreate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !NodePoolReleaseApproved(pool) {
		t.Fatal("initial installation must not require upgrade approval")
	}
	pool.Spec.Release.UpgradeStartTime = ptr.To(metav1.Now())
	if err := pool.ValidateCreate(t.Context()); err == nil {
		t.Fatal("scheduling requires an installed pool")
	}
}
