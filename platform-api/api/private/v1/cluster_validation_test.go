package v1

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// validClusterWithWindow builds a Cluster with a valid maintenance window and one exclusion.
func validClusterWithWindow() *Cluster {
	c := &Cluster{}
	c.SetName("cluster-1")
	c.SetUID(types.UID("550e8400-e29b-41d4-a716-446655440000"))
	c.Spec.SafeName = DefaultSafeName(c.Name, c.UID)

	exclusionStart := metav1.NewTime(time.Date(2026, time.December, 20, 0, 0, 0, 0, time.UTC))
	exclusionEnd := metav1.NewTime(time.Date(2027, time.January, 3, 0, 0, 0, 0, time.UTC))

	exclusions := []ControlPlaneMaintenanceExclusion{
		{Name: "holiday-freeze", StartTime: exclusionStart, EndTime: exclusionEnd},
	}
	c.Spec.Maintenance = &ClusterMaintenanceSpec{
		Window: &ControlPlaneMaintenanceWindow{
			StartTime:       metav1.NewTime(time.Date(2026, time.September, 26, 2, 0, 0, 0, time.UTC)),
			DurationSeconds: 14400, // 4 hours
			Recurrence: ControlPlaneMaintenanceRecurrence{
				Frequency:  "weekly",
				DaysOfWeek: []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"},
			},
		},
		Exclusions: &exclusions,
	}
	return c
}

func TestClusterValidateCreate_ValidMaintenanceConfig(t *testing.T) {
	if err := validClusterWithWindow().ValidateCreate(t.Context()); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestClusterValidateCreate_InvalidMaintenanceConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Cluster)
	}{
		{
			name: "duration below minimum",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.DurationSeconds = 59
			},
		},
		{
			name: "duration exceeds 24 hours",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.DurationSeconds = 86401
			},
		},
		{
			name: "unsupported frequency",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.Recurrence.Frequency = "daily"
			},
		},
		{
			name: "missing weekday",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.Recurrence.DaysOfWeek = nil
			},
		},
		{
			name: "invalid weekday",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"weekend"}
			},
		},
		{
			name: "weekday must be lowercase",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"Saturday"}
			},
		},
		{
			name: "duplicate weekday",
			mutate: func(c *Cluster) {
				c.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"saturday", "saturday"}
			},
		},
		{
			name: "empty exclusion name",
			mutate: func(c *Cluster) {
				(*c.Spec.Maintenance.Exclusions)[0].Name = " "
			},
		},
		{
			name: "duplicate exclusion name",
			mutate: func(c *Cluster) {
				*c.Spec.Maintenance.Exclusions = append(
					*c.Spec.Maintenance.Exclusions,
					(*c.Spec.Maintenance.Exclusions)[0],
				)
			},
		},
		{
			name: "exclusion endTime not after startTime",
			mutate: func(c *Cluster) {
				(*c.Spec.Maintenance.Exclusions)[0].EndTime = (*c.Spec.Maintenance.Exclusions)[0].StartTime
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validClusterWithWindow()
			tt.mutate(c)
			if err := c.ValidateCreate(t.Context()); err == nil {
				t.Fatal("expected maintenance config validation error")
			}
		})
	}
}

func TestClusterValidateCreate_MaximumDuration(t *testing.T) {
	c := validClusterWithWindow()
	c.Spec.Maintenance.Window.DurationSeconds = 86400 // 24 hours
	if err := c.ValidateCreate(t.Context()); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestClusterValidateCreate_NoMaintenanceWindow(t *testing.T) {
	c := validClusterWithWindow()
	c.Spec.Maintenance = nil
	if err := c.ValidateCreate(t.Context()); err != nil {
		t.Fatalf("ValidateCreate() error = %v", err)
	}
}

func TestClusterValidateUpdate_MaintenanceConfigValid(t *testing.T) {
	old := validClusterWithWindow()
	updated := old.DeepCopy()
	updated.Spec.Maintenance.Window.DurationSeconds = 18000 // 5 hours

	if err := updated.ValidateUpdate(t.Context(), old); err != nil {
		t.Fatalf("ValidateUpdate() error = %v", err)
	}
}

func TestClusterValidateUpdate_InvalidMaintenanceConfig(t *testing.T) {
	old := validClusterWithWindow()
	updated := old.DeepCopy()
	updated.Spec.Maintenance.Window.Recurrence.Frequency = "monthly"

	if err := updated.ValidateUpdate(t.Context(), old); err == nil {
		t.Fatal("expected maintenance config validation error on update")
	}
}

func TestClusterValidateUpdate_LegacyMaintenance(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(old, updated *Cluster)
	}{
		{name: "unchanged update", mutate: func(_, _ *Cluster) {}},
		{
			name: "unrelated spec update",
			mutate: func(_, updated *Cluster) {
				updated.Spec.Release.Version = "4.22.20"
			},
		},
		{
			name: "status update",
			mutate: func(_, updated *Cluster) {
				updated.Status.Conditions = []metav1.Condition{{Type: "HostedClusterAvailable", Status: metav1.ConditionTrue}}
			},
		},
		{
			name: "critical upgrade pin",
			mutate: func(_, updated *Cluster) {
				updated.Spec.Maintenance.CriticalUpgrade = "4.22.20"
			},
		},
		{
			name: "safe name backfill",
			mutate: func(old, _ *Cluster) {
				old.Spec.SafeName = ""
			},
		},
		{
			name: "equivalent timestamp",
			mutate: func(_, updated *Cluster) {
				updated.Spec.Maintenance.Window.StartTime.Time = updated.Spec.Maintenance.Window.StartTime.In(time.FixedZone("offset", 3600))
			},
		},
		{
			name: "reordered weekdays",
			mutate: func(old, updated *Cluster) {
				old.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"saturday", "sunday"}
				updated.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"sunday", "saturday"}
			},
		},
		{
			name: "reordered exclusions",
			mutate: func(old, updated *Cluster) {
				first := (*old.Spec.Maintenance.Exclusions)[0]
				second := first
				second.Name = "other-freeze"
				old.Spec.Maintenance.Exclusions = &[]ControlPlaneMaintenanceExclusion{first, second}
				updated.Spec.Maintenance.Exclusions = &[]ControlPlaneMaintenanceExclusion{second, first}
			},
		},
		{
			name: "equivalent empty exclusions",
			mutate: func(old, updated *Cluster) {
				old.Spec.Maintenance.Exclusions = nil
				updated.Spec.Maintenance.Exclusions = &[]ControlPlaneMaintenanceExclusion{}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := validClusterWithWindow()
			old.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"saturday"}
			updated := old.DeepCopy()
			tc.mutate(old, updated)
			if err := updated.ValidateUpdate(t.Context(), old); err != nil {
				t.Fatalf("unchanged legacy timing blocked update: %v", err)
			}
		})
	}
}

func TestClusterValidateUpdate_LegacyExclusionLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Cluster)
	}{
		{
			name: "exclusion longer than thirty days",
			mutate: func(c *Cluster) {
				exclusion := &(*c.Spec.Maintenance.Exclusions)[0]
				exclusion.EndTime.Time = exclusion.StartTime.Add(31 * 24 * time.Hour)
			},
		},
		{
			name: "combined exclusions leave under forty eight hours",
			mutate: func(c *Cluster) {
				exclusion := (*c.Spec.Maintenance.Exclusions)[0]
				exclusion.EndTime.Time = exclusion.StartTime.Add(30 * 24 * time.Hour)
				extra := maintenanceTestExclusion("extra", exclusion.EndTime.Time, exclusion.EndTime.Add(time.Hour))
				c.Spec.Maintenance.Exclusions = &[]ControlPlaneMaintenanceExclusion{exclusion, extra}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := validClusterWithWindow()
			old.Spec.Maintenance.Window = nil
			tc.mutate(old)
			updated := old.DeepCopy()
			updated.Spec.Release.Version = "4.22.20"
			if err := updated.ValidateUpdate(t.Context(), old); err != nil {
				t.Fatalf("legacy exclusion limits blocked unrelated update: %v", err)
			}
			updated = old.DeepCopy()
			(*updated.Spec.Maintenance.Exclusions)[0].EndTime.Time = (*updated.Spec.Maintenance.Exclusions)[0].EndTime.Add(time.Hour)
			if err := updated.ValidateUpdate(t.Context(), old); err == nil {
				t.Fatal("changing noncompliant exclusions must fail")
			}
			updated = old.DeepCopy()
			(*updated.Spec.Maintenance.Exclusions)[0].EndTime.Time = (*updated.Spec.Maintenance.Exclusions)[0].StartTime.Add(time.Hour)
			if err := updated.ValidateUpdate(t.Context(), old); err != nil {
				t.Fatalf("correcting legacy exclusions must succeed: %v", err)
			}
		})
	}
}

func TestClusterValidateUpdate_LegacyMaintenanceChanged(t *testing.T) {
	old := validClusterWithWindow()
	old.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"saturday"}
	updated := old.DeepCopy()
	updated.Spec.Maintenance.Window.DurationSeconds = 18000
	assertMaintenanceValidationError(t, updated.ValidateUpdate(t.Context(), old), "at least 48 hours")
	assertMaintenanceValidationError(t, old.ValidateCreate(t.Context()), "at least 48 hours")
	updated = old.DeepCopy()
	updated.Spec.Maintenance.Window.Recurrence.DaysOfWeek = []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
	if err := updated.ValidateUpdate(t.Context(), old); err != nil {
		t.Fatalf("correcting the legacy window must succeed: %v", err)
	}
}

func TestClusterValidateUpdate_UnchangedStructuralErrors(t *testing.T) {
	old := validClusterWithWindow()
	old.Spec.Maintenance.Window.Recurrence.Frequency = "monthly"
	updated := old.DeepCopy()
	assertMaintenanceValidationError(t, updated.ValidateUpdate(t.Context(), old), "frequency must be weekly")
}

func TestClusterValidateUpdate_ReleaseIntent(t *testing.T) {
	for _, test := range []struct {
		name, version   string
		active, invalid bool
	}{
		{name: "minor", version: "4.23"}, {name: "exact", version: "4.23.9"},
		{name: "downgrade", version: "4.21.9", invalid: true}, {name: "unsupported minor", version: "4.24", invalid: true},
		{name: "active upgrade", version: "4.23", active: true, invalid: true},
		{name: "identical retry during upgrade", version: "4.22.19", active: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			old := validClusterWithWindow()
			old.Spec.Release = ReleaseSpec{Version: "4.22.19", ChannelGroup: "stable"}
			old.Status.HostedClusterResult = &HostedClusterResult{Version: "4.22.19", DesiredVersion: "4.22.19", AvailableUpdates: []string{"4.23.9"}}
			if test.active {
				old.Status.Conditions = append(old.Status.Conditions, metav1.Condition{Type: ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue})
			}
			changed := old.DeepCopy()
			changed.Spec.Release.Version = test.version
			err := changed.ValidateUpdate(t.Context(), old)
			if (err != nil) != test.invalid {
				t.Fatalf("ValidateUpdate() = %v, invalid=%v", err, test.invalid)
			}
		})
	}
}

func TestClusterInstallationVersion_RequiresMatchingApproval(t *testing.T) {
	cluster := &Cluster{Spec: ClusterSpec{Release: ReleaseSpec{Version: "4.23.9"}}, Status: ClusterStatus{HostedClusterResult: &HostedClusterResult{Version: "4.22.19"}}}
	if got := ClusterInstallationVersion(cluster); got != "4.22.19" {
		t.Fatalf("unapproved release leaked: %s", got)
	}
	cluster.Status.Conditions = []metav1.Condition{{Type: ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue}}
	cluster.Status.ControlPlaneUpgrade = &ControlPlaneUpgradeResult{TargetVersion: "4.23.8"}
	if ClusterReleaseApproved(cluster) {
		t.Fatal("approval for a different target must not authorize this spec")
	}
	cluster.Status.ControlPlaneUpgrade.TargetVersion = "4.23.9"
	if got := ClusterInstallationVersion(cluster); got != "4.23.9" {
		t.Fatalf("approved target = %s", got)
	}
	cluster.Generation++ // Unrelated edits must not release the admission lock.
	if !ClusterUpgradeActive(cluster) {
		t.Fatal("approval was lost after an unrelated generation change")
	}
}

func TestClusterUpgradeSubmissionIsBoundToApproval(t *testing.T) {
	cluster := &Cluster{Status: ClusterStatus{Conditions: []metav1.Condition{
		{Type: ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue, ObservedGeneration: 5},
		{Type: ControlPlaneUpgradeSubmitted, Status: metav1.ConditionTrue, ObservedGeneration: 3},
	}}}
	if ClusterUpgradeSubmissionStarted(cluster) {
		t.Fatal("previous rollout dispatch must not authorize this target")
	}
	cluster.Status.Conditions[1].ObservedGeneration = 5
	if !ClusterUpgradeSubmissionStarted(cluster) {
		t.Fatal("current dispatch must remain protected")
	}
	cluster.Generation = 8
	if !ClusterUpgradeSubmissionStarted(cluster) {
		t.Fatal("unrelated edits must not forget current dispatch")
	}
}
