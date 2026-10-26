package v1

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

var validControlPlaneMaintenanceDays = map[string]struct{}{
	"monday":    {},
	"tuesday":   {},
	"wednesday": {},
	"thursday":  {},
	"friday":    {},
	"saturday":  {},
	"sunday":    {},
}

// ValidateCreate validates ControlPlaneUpgradePolicy creation.
func (p *ControlPlaneUpgradePolicy) ValidateCreate(_ context.Context) error {
	return p.validate()
}

// ValidateUpdate validates ControlPlaneUpgradePolicy updates.
func (p *ControlPlaneUpgradePolicy) ValidateUpdate(_ context.Context, oldObj runtime.Object) error {
	oldPolicy, ok := oldObj.(*ControlPlaneUpgradePolicy)
	if !ok {
		return fmt.Errorf("expected old object to be *ControlPlaneUpgradePolicy, got %T", oldObj)
	}

	if p.Spec.ClusterID != oldPolicy.Spec.ClusterID {
		return fmt.Errorf("spec.clusterID is immutable")
	}

	return p.validate()
}

// ValidateDelete validates ControlPlaneUpgradePolicy deletion.
func (p *ControlPlaneUpgradePolicy) ValidateDelete(_ context.Context) error {
	return nil
}

func (p *ControlPlaneUpgradePolicy) validate() error {
	if window := p.Spec.MaintenanceWindow; window != nil {
		if window.DurationMinutes < 1 || window.DurationMinutes > 1440 {
			return fmt.Errorf("spec.maintenanceWindow.durationMinutes must be between 1 and 1440")
		}
		if window.Recurrence.Frequency != "weekly" {
			return fmt.Errorf("spec.maintenanceWindow.recurrence.frequency must be weekly")
		}
		if len(window.Recurrence.DaysOfWeek) == 0 {
			return fmt.Errorf("spec.maintenanceWindow.recurrence.daysOfWeek must contain at least one day")
		}

		seenDays := make(map[string]struct{}, len(window.Recurrence.DaysOfWeek))
		for _, day := range window.Recurrence.DaysOfWeek {
			if _, valid := validControlPlaneMaintenanceDays[day]; !valid {
				return fmt.Errorf("spec.maintenanceWindow.recurrence.daysOfWeek contains invalid day %q", day)
			}
			if _, duplicate := seenDays[day]; duplicate {
				return fmt.Errorf("spec.maintenanceWindow.recurrence.daysOfWeek contains duplicate day %q", day)
			}
			seenDays[day] = struct{}{}
		}
	}

	seenExclusions := make(map[string]struct{}, len(p.Spec.MaintenanceExclusions))
	for i, exclusion := range p.Spec.MaintenanceExclusions {
		if strings.TrimSpace(exclusion.Name) == "" {
			return fmt.Errorf("spec.maintenanceExclusions[%d].name must not be empty", i)
		}
		if _, duplicate := seenExclusions[exclusion.Name]; duplicate {
			return fmt.Errorf("spec.maintenanceExclusions contains duplicate name %q", exclusion.Name)
		}
		if !exclusion.End.After(exclusion.Start.Time) {
			return fmt.Errorf("spec.maintenanceExclusions[%d].end must be after start", i)
		}
		seenExclusions[exclusion.Name] = struct{}{}
	}

	return nil
}
