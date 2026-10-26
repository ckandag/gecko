package v1

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validControlPlaneUpgradePolicy() *ControlPlaneUpgradePolicy {
	exclusionStart := metav1.NewTime(time.Date(2026, time.December, 20, 0, 0, 0, 0, time.UTC))
	exclusionEnd := metav1.NewTime(time.Date(2027, time.January, 3, 0, 0, 0, 0, time.UTC))

	return &ControlPlaneUpgradePolicy{
		Spec: ControlPlaneUpgradePolicySpec{
			ClusterID: "example-cluster",
			MaintenanceWindow: &ControlPlaneMaintenanceWindow{
				Start:           metav1.NewTime(time.Date(2026, time.September, 26, 2, 0, 0, 0, time.UTC)),
				DurationMinutes: 240,
				Recurrence: ControlPlaneMaintenanceRecurrence{
					Frequency:  "weekly",
					DaysOfWeek: []string{"saturday"},
				},
			},
			MaintenanceExclusions: []ControlPlaneMaintenanceExclusion{
				{
					Name:  "holiday-freeze",
					Start: exclusionStart,
					End:   exclusionEnd,
				},
			},
		},
	}
}

func TestControlPlaneUpgradePolicyValidateCreate(t *testing.T) {
	if err := validControlPlaneUpgradePolicy().ValidateCreate(t.Context()); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestControlPlaneUpgradePolicyValidateCreate_InvalidPolicy(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ControlPlaneUpgradePolicy)
	}{
		{
			name: "non-positive duration",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.DurationMinutes = 0
			},
		},
		{
			name: "duration exceeds 24 hours",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.DurationMinutes = 1441
			},
		},
		{
			name: "unsupported frequency",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.Recurrence.Frequency = "daily"
			},
		},
		{
			name: "missing weekday",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.Recurrence.DaysOfWeek = nil
			},
		},
		{
			name: "invalid weekday",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.Recurrence.DaysOfWeek = []string{"weekend"}
			},
		},
		{
			name: "weekday must be lowercase",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.Recurrence.DaysOfWeek = []string{"Wednesday"}
			},
		},
		{
			name: "duplicate weekday",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceWindow.Recurrence.DaysOfWeek = []string{"saturday", "saturday"}
			},
		},
		{
			name: "empty exclusion name",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceExclusions[0].Name = " "
			},
		},
		{
			name: "duplicate exclusion name",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceExclusions = append(
					policy.Spec.MaintenanceExclusions,
					policy.Spec.MaintenanceExclusions[0],
				)
			},
		},
		{
			name: "exclusion end is not after start",
			mutate: func(policy *ControlPlaneUpgradePolicy) {
				policy.Spec.MaintenanceExclusions[0].End = policy.Spec.MaintenanceExclusions[0].Start
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := validControlPlaneUpgradePolicy()
			tt.mutate(policy)

			if err := policy.ValidateCreate(t.Context()); err == nil {
				t.Fatal("expected policy validation error")
			}
		})
	}
}

func TestControlPlaneUpgradePolicyValidateUpdate(t *testing.T) {
	oldPolicy := validControlPlaneUpgradePolicy()
	updatedPolicy := oldPolicy.DeepCopy()
	updatedPolicy.Spec.MaintenanceWindow.DurationMinutes = 300

	if err := updatedPolicy.ValidateUpdate(t.Context(), oldPolicy); err != nil {
		t.Fatalf("ValidateUpdate() error = %v", err)
	}
}

func TestControlPlaneUpgradePolicyValidateCreate_MaximumDuration(t *testing.T) {
	policy := validControlPlaneUpgradePolicy()
	policy.Spec.MaintenanceWindow.DurationMinutes = 1440
	if err := policy.ValidateCreate(t.Context()); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestControlPlaneUpgradePolicyValidateUpdate_ClusterIDImmutable(t *testing.T) {
	oldPolicy := validControlPlaneUpgradePolicy()
	updatedPolicy := oldPolicy.DeepCopy()
	updatedPolicy.Spec.ClusterID = "another-cluster"

	if err := updatedPolicy.ValidateUpdate(t.Context(), oldPolicy); err == nil {
		t.Fatal("expected clusterID immutability error")
	}
}
