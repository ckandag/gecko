package controlplaneupgrade

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMaintenancePermitted(t *testing.T) {
	window := &privatev1.ControlPlaneMaintenanceWindow{
		Start:           metav1.NewTime(time.Date(2026, time.October, 14, 23, 0, 0, 0, time.UTC)),
		DurationMinutes: 240,
		Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{
			Frequency:  "weekly",
			DaysOfWeek: []string{"wednesday"},
		},
	}
	exclusion := privatev1.ControlPlaneMaintenanceExclusion{
		Name:  "freeze",
		Start: metav1.NewTime(time.Date(2026, time.October, 15, 0, 0, 0, 0, time.UTC)),
		End:   metav1.NewTime(time.Date(2026, time.October, 15, 2, 0, 0, 0, time.UTC)),
	}
	overlappingExclusion := privatev1.ControlPlaneMaintenanceExclusion{
		Name:  "extended-freeze",
		Start: metav1.NewTime(time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC)),
		End:   metav1.NewTime(time.Date(2026, time.October, 15, 4, 0, 0, 0, time.UTC)),
	}
	policy := func(w *privatev1.ControlPlaneMaintenanceWindow, exclusions ...privatev1.ControlPlaneMaintenanceExclusion) *privatev1.ControlPlaneUpgradePolicy {
		return &privatev1.ControlPlaneUpgradePolicy{
			Spec: privatev1.ControlPlaneUpgradePolicySpec{
				MaintenanceWindow:     w,
				MaintenanceExclusions: exclusions,
			},
		}
	}
	tests := []struct {
		name       string
		policy     *privatev1.ControlPlaneUpgradePolicy
		now        time.Time
		permitted  bool
		reason     string
		nextWindow time.Time
	}{
		{name: "no policy", now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC), permitted: true},
		{name: "at window start", policy: policy(window), now: time.Date(2026, time.October, 14, 23, 0, 0, 0, time.UTC), permitted: true},
		{
			name: "before first configured occurrence", policy: policy(window), now: time.Date(2026, time.October, 7, 23, 30, 0, 0, time.UTC),
			reason: "OutsideMaintenanceWindow", nextWindow: window.Start.Time,
		},
		{
			name: "start date is not an allowed weekday", policy: policy(&privatev1.ControlPlaneMaintenanceWindow{
				Start: metav1.NewTime(time.Date(2026, time.October, 15, 23, 0, 0, 0, time.UTC)), DurationMinutes: 240,
				Recurrence: window.Recurrence,
			}), now: time.Date(2026, time.October, 15, 22, 0, 0, 0, time.UTC),
			reason: "OutsideMaintenanceWindow", nextWindow: time.Date(2026, time.October, 21, 23, 0, 0, 0, time.UTC),
		},
		{name: "overnight window", policy: policy(window), now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC), permitted: true},
		{
			name: "at window end", policy: policy(window), now: time.Date(2026, time.October, 15, 3, 0, 0, 0, time.UTC),
			reason: "OutsideMaintenanceWindow", nextWindow: time.Date(2026, time.October, 21, 23, 0, 0, 0, time.UTC),
		},
		{
			name: "exclusion within window", policy: policy(window, exclusion), now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC),
			reason: "MaintenanceExclusionActive", nextWindow: exclusion.End.Time,
		},
		{
			name: "exclusion without window", policy: policy(nil, exclusion), now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC),
			reason: "MaintenanceExclusionActive", nextWindow: exclusion.End.Time,
		},
		{
			name: "overlapping exclusion remains active", policy: policy(window, exclusion, overlappingExclusion),
			now:    time.Date(2026, time.October, 15, 2, 30, 0, 0, time.UTC),
			reason: "MaintenanceExclusionActive", nextWindow: overlappingExclusion.End.Time,
		},
		{name: "exclusion ends", policy: policy(nil, exclusion), now: exclusion.End.Time, permitted: true},
		{
			name: "unsupported frequency", policy: policy(&privatev1.ControlPlaneMaintenanceWindow{
				Start: window.Start, DurationMinutes: 240,
				Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{Frequency: "monthly", DaysOfWeek: []string{"wednesday"}},
			}), now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC), reason: "InvalidMaintenanceWindow",
		},
		{
			name: "invalid day", policy: policy(&privatev1.ControlPlaneMaintenanceWindow{
				Start: window.Start, DurationMinutes: 240,
				Recurrence: privatev1.ControlPlaneMaintenanceRecurrence{Frequency: "weekly", DaysOfWeek: []string{"WEDNESDAY"}},
			}), now: time.Date(2026, time.October, 15, 1, 0, 0, 0, time.UTC), reason: "InvalidMaintenanceWindow",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permitted, reason, _, next := maintenancePermitted(tt.policy, tt.now)
			require.Equal(t, tt.permitted, permitted)
			require.Equal(t, tt.reason, reason)
			if tt.nextWindow.IsZero() {
				require.Nil(t, next)
			} else {
				require.NotNil(t, next)
				require.Equal(t, tt.nextWindow, *next)
			}
		})
	}
}
