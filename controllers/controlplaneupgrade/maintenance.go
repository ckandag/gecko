package controlplaneupgrade

import (
	"fmt"
	"time"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
)

// maintenancePermitted evaluates whether an upgrade may start at the given time
// according to the cluster's inline maintenance policy. It returns true if no
// maintenance window is configured (upgrades permitted at any time), or if the
// current time falls within a maintenance window and outside all exclusions.
func maintenancePermitted(cluster *privatev1.Cluster, now time.Time) (permitted bool, reason, message string, nextWindow *time.Time) {
	m := cluster.Spec.Maintenance
	if m == nil {
		return true, "", "", nil
	}

	// Check if we're currently in an exclusion period
	for _, exclusion := range derefExclusions(m.Exclusions) {
		start := exclusion.StartTime.Time
		end := exclusion.EndTime.Time
		if !now.Before(start) && now.Before(end) {
			endUTC := end.UTC()
			return false, "MaintenanceExclusionActive",
				fmt.Sprintf("Upgrade blocked by maintenance exclusion %q (ends %s)", exclusion.Name, endUTC.Format(time.RFC3339)),
				&endUTC
		}
	}
	if m.Window == nil {
		return true, "", "", nil
	}

	// A completed rollout consumes the current occurrence. A newly advertised
	// patch must wait for a new opening, even if this window is still open.
	if result := cluster.Status.ControlPlaneUpgrade; result != nil && result.CompletedTime != nil {
		if next := nextWindowAfter(m.Window, result.CompletedTime.Time); next != nil && now.Before(*next) {
			return false, "WaitingForNextMaintenanceWindow", "Waiting for the next maintenance-window opening", next
		}
	}

	// Check if we're currently in a maintenance window
	inWindow, next := inMaintenanceWindow(m.Window, now)
	if inWindow {
		return true, "", "", nil
	}
	if next == nil {
		return false, "InvalidMaintenanceWindow", "Maintenance window has an unsupported recurrence or no valid days", nil
	}

	// Not in window, not in exclusion: wait for next window
	message = fmt.Sprintf("Next maintenance window opens at %s", next.UTC().Format(time.RFC3339))
	return false, "OutsideMaintenanceWindow", message, next
}

// inMaintenanceWindow reports whether now falls within a maintenance window
// occurrence, and returns the next window start time if not currently in one.
func inMaintenanceWindow(window *privatev1.ControlPlaneMaintenanceWindow, now time.Time) (inWindow bool, nextStart *time.Time) {
	if window == nil {
		return false, nil
	}

	// Convert window start to UTC
	start := window.StartTime.UTC()
	duration := time.Duration(window.DurationSeconds) * time.Second

	// The policy API currently supports weekly recurrence only.
	if window.Recurrence.Frequency != "weekly" {
		return false, nil
	}

	// Build a map of allowed weekdays; skip any unrecognized names so that
	// a bypassed-validation entry does not silently match a valid weekday.
	allowedDays := make(map[time.Weekday]bool)
	for _, dayName := range window.Recurrence.DaysOfWeek {
		if day, ok := parseWeekday(dayName); ok {
			allowedDays[day] = true
		}
	}

	if len(allowedDays) == 0 {
		return false, nil
	}

	nowUTC := now.UTC()
	if nowUTC.Before(start) {
		for day := range allowedDays {
			daysAhead := (int(day) - int(start.Weekday()) + 7) % 7
			upcoming := start.AddDate(0, 0, daysAhead)
			if nextStart == nil || upcoming.Before(*nextStart) {
				nextStart = &upcoming
			}
		}
		return false, nextStart
	}
	for day := range allowedDays {
		// Find this day's most recent occurrence. Checking the previous week
		// also handles windows that extend past midnight or span several days.
		daysBack := (int(nowUTC.Weekday()) - int(day) + 7) % 7
		last := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(),
			start.Hour(), start.Minute(), start.Second(), 0, time.UTC).AddDate(0, 0, -daysBack)
		if last.After(nowUTC) {
			last = last.AddDate(0, 0, -7)
		}
		if !last.Before(start) && nowUTC.Before(last.Add(duration)) {
			return true, nil
		}

		upcoming := last.AddDate(0, 0, 7)
		if nextStart == nil || upcoming.Before(*nextStart) {
			nextStart = &upcoming
		}
	}
	return false, nextStart
}

// parseWeekday converts a lowercase day name to time.Weekday.
// The boolean return is false for any unrecognized name, making the
// sentinel explicit and preventing a stale -1 from being used as a weekday.
func parseWeekday(name string) (time.Weekday, bool) {
	switch name {
	case "sunday":
		return time.Sunday, true
	case "monday":
		return time.Monday, true
	case "tuesday":
		return time.Tuesday, true
	case "wednesday":
		return time.Wednesday, true
	case "thursday":
		return time.Thursday, true
	case "friday":
		return time.Friday, true
	case "saturday":
		return time.Saturday, true
	default:
		return 0, false
	}
}

// overrideCondition checks if maintenance windows/exclusions should be bypassed
// for the given upgrade candidate. The override fires only when all three
// conditions hold: (1) CriticalUpgrade exactly matches the candidate, (2) the
// pin is a valid z-stream update of completed (same major.minor, strictly newer).
// Condition (2) prevents a cross-minor pin that coincidentally matches a
// y-stream candidate from bypassing maintenance — the pin must be a z-stream
// patch or it has no bypass authority.
func overrideCondition(cluster *privatev1.Cluster, candidate, completed string) (override bool, reason, message string) {
	if candidate == "" || cluster.Spec.Maintenance == nil {
		return false, "", ""
	}
	pin := cluster.Spec.Maintenance.CriticalUpgrade
	if pin == candidate && isValidCriticalPin(pin, completed) {
		return true, "CriticalUpgradeRequired",
			fmt.Sprintf("Critical security or platform issue requires immediate upgrade to %s", candidate)
	}
	return false, "", ""
}

// derefExclusions safely dereferences a *[]ControlPlaneMaintenanceExclusion,
// returning nil (empty range) when the pointer is nil.
func derefExclusions(e *[]privatev1.ControlPlaneMaintenanceExclusion) []privatev1.ControlPlaneMaintenanceExclusion {
	if e == nil {
		return nil
	}
	return *e
}

// nextWindowAfter finds the first recurrence strictly after a completed rollout.
func nextWindowAfter(window *privatev1.ControlPlaneMaintenanceWindow, after time.Time) *time.Time {
	if window.Recurrence.Frequency != "weekly" {
		return nil
	}
	start := window.StartTime.UTC()
	day := after.UTC()
	for offset := 0; offset <= 7; offset++ {
		candidate := time.Date(day.Year(), day.Month(), day.Day(), start.Hour(), start.Minute(), start.Second(), 0, time.UTC).AddDate(0, 0, offset)
		if !candidate.After(after) || candidate.Before(start) {
			continue
		}
		for _, name := range window.Recurrence.DaysOfWeek {
			if weekday, ok := parseWeekday(name); ok && candidate.Weekday() == weekday {
				return &candidate
			}
		}
	}
	return nil
}
