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
		Status:     NodePoolStatus{CurrentVersion: "4.22.14"},
	}
}

func TestNodePoolUpgradeDefaultAndValidation(t *testing.T) {
	defaults, validations := 0, 0
	SetValidatorDeps(ValidatorDeps{
		DefaultNodePoolUpgrade: func(_ context.Context, pool *NodePool) error {
			defaults++
			pool.Spec.Upgrade.TargetVersion = ptr.To("4.22.16")
			return nil
		},
		ValidateNodePoolUpgrade: func(_ context.Context, pool *NodePool, target string) error {
			validations++
			if pool.Status.CurrentVersion != "4.22.14" || target != "4.22.16" {
				t.Fatal("unexpected validation input")
			}
			return nil
		},
	})
	t.Cleanup(func() { SetValidatorDeps(ValidatorDeps{}) })
	old := upgradeValidationPool()
	updated := old.DeepCopy()
	updated.Spec.Upgrade = &NodePoolUpgradeSpec{}
	if err := updated.Default(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := updated.ValidateUpdate(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if defaults != 1 || validations != 1 {
		t.Fatalf("callbacks = %d defaults, %d validations", defaults, validations)
	}
	// Once pinned, ordinary spec/status changes do not re-default or revalidate.
	scaled := updated.DeepCopy()
	scaled.Spec.NodeCount = ptr.To(int32(3))
	if err := scaled.Default(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := scaled.ValidateUpdate(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	if defaults != 1 || validations != 1 {
		t.Fatal("unrelated update repeated storage-backed checks")
	}
}

func TestNodePoolUpgradeUpdateRules(t *testing.T) {
	old := upgradeValidationPool()
	old.Spec.Upgrade = &NodePoolUpgradeSpec{TargetVersion: ptr.To("4.22.16")}
	now := metav1.Now()
	tests := []struct {
		name      string
		changeOld func(*NodePool)
		changeNew func(*NodePool)
		want      string
	}{
		{name: "pending cancellation", changeNew: func(p *NodePool) { p.Spec.Upgrade = nil }},
		{name: "invalid target", changeNew: func(p *NodePool) { p.Spec.Upgrade.TargetVersion = ptr.To("invalid") }, want: "semantic version"},
		{name: "missing default", changeNew: func(p *NodePool) { p.Spec.Upgrade.TargetVersion = nil }, want: "must be set or defaulted"},
		{name: "invalid timestamp", changeNew: func(p *NodePool) { p.Spec.Upgrade.StartTime = &metav1.Time{} }, want: "valid timestamp"},
		{name: "pending schedule cannot be bypassed", changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.16" }, want: "pending"},
		{name: "accepted cancellation", changeOld: func(p *NodePool) {
			p.Status.Upgrade = &NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now}
		}, changeNew: func(p *NodePool) { p.Spec.Upgrade = nil }, want: "accepted upgrade is active"},
		{name: "accepted target change", changeOld: func(p *NodePool) {
			p.Status.Upgrade = &NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now}
		}, changeNew: func(p *NodePool) { p.Spec.Upgrade.TargetVersion = ptr.To("4.22.17") }, want: "accepted upgrade is active"},
		{name: "accepted release submission", changeOld: func(p *NodePool) {
			p.Status.Upgrade = &NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now}
		}, changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.16" }},
		{name: "different release while accepted", changeOld: func(p *NodePool) {
			p.Status.Upgrade = &NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now}
		}, changeNew: func(p *NodePool) { p.Spec.Release.Version = "4.22.17" }, want: "match the accepted"},
		{name: "new target after completion", changeOld: func(p *NodePool) {
			p.Status.Upgrade = &NodePoolUpgradeResult{TargetVersion: "4.22.16", AcceptedTime: &now, CompletedTime: &now}
		}, changeNew: func(p *NodePool) { p.Spec.Upgrade.TargetVersion = ptr.To("4.22.17") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previous := old.DeepCopy()
			if tt.changeOld != nil {
				tt.changeOld(previous)
			}
			next := previous.DeepCopy()
			if tt.changeNew != nil {
				tt.changeNew(next)
			}
			err := next.ValidateUpdate(t.Context(), previous)
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
	if err := pool.Default(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := pool.ValidateCreate(t.Context()); err != nil {
		t.Fatal(err)
	}
	pool.Spec.Upgrade = &NodePoolUpgradeSpec{TargetVersion: ptr.To("4.22.16")}
	if err := pool.ValidateCreate(t.Context()); err == nil {
		t.Fatal("an upgrade should require an installed pool")
	}
}
