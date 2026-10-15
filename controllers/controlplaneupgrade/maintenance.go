package controlplaneupgrade

import (
	"fmt"
	"time"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
)

// maintenancePermitted evaluates whether an upgrade may start at the given time
// according to the cluster's maintenance policy. It returns true if no policy is
// set (upgrades permitted at any time), or if the current time falls within a
// maintenance window and outside all exclusions.
func maintenancePermitted(policy *privatev1.ControlPlaneUpgradePolicy, now time.Time) (permitted bool, reason, message string, nextWindow *time.Time) {
	if policy == nil {
		return true, "", "", nil
	}

	// Check if we're currently in an exclusion period
	for _, exclusion := range policy.Spec.MaintenanceExclusions {
		start := exclusion.Start.Time
		end := exclusion.End.Time
		if !now.Before(start) && now.Before(end) {
			endUTC := end.UTC()
			return false, "MaintenanceExclusionActive",
				fmt.Sprintf("Upgrade blocked by maintenance exclusion %q (ends %s)", exclusion.Name, endUTC.Format(time.RFC3339)),
				&endUTC
		}
	}
	if policy.Spec.MaintenanceWindow == nil {
		return true, "", "", nil
	}

	// Check if we're currently in a maintenance window
	inWindow, next := inMaintenanceWindow(policy.Spec.MaintenanceWindow, now)
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
	start := window.Start.UTC()
	duration := time.Duration(window.DurationMinutes) * time.Minute

	// The policy API currently supports weekly recurrence only.
	if window.Recurrence.Frequency != "weekly" {
		return false, nil
	}

	// Build a map of allowed weekdays
	allowedDays := make(map[time.Weekday]bool)
	for _, dayName := range window.Recurrence.DaysOfWeek {
		if day := parseWeekday(dayName); day >= 0 {
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

// parseWeekday converts a day name to time.Weekday
func parseWeekday(name string) time.Weekday {
	switch name {
	case "sunday":
		return time.Sunday
	case "monday":
		return time.Monday
	case "tuesday":
		return time.Tuesday
	case "wednesday":
		return time.Wednesday
	case "thursday":
		return time.Thursday
	case "friday":
		return time.Friday
	case "saturday":
		return time.Saturday
	default:
		return -1
	}
}

// overrideCondition checks if maintenance windows/exclusions should be bypassed
// due to EOL proximity or critical security/platform issues.
func overrideCondition(cluster *privatev1.Cluster, isYStream bool) (override bool, reason, message string) {
	// Check for critical security or platform flag on the cluster
	// This would be set by platform operators for emergency patches
	if cluster.Annotations != nil {
		if cluster.Annotations["gecko.openshift.io/critical-upgrade"] == "true" {
			return true, "CriticalUpgradeRequired",
				"Critical security or platform issue requires immediate patching"
		}
	}

	// Y-stream only: check EOL proximity
	// TODO: This requires EOL date metadata, which would come from the Version API
	// or Channel status. For now, this is a placeholder for the future implementation.
	// The EOL check would look like:
	// if isYStream && cluster.Status.HostedClusterResult != nil {
	//     currentVersion := cluster.Status.HostedClusterResult.Version
	//     eolDate := getEOLDate(currentVersion) // from Version API
	//     if time.Until(eolDate) < 30*24*time.Hour {
	//         return true, "ApproachingEOL",
	//             fmt.Sprintf("Version %s reaches EOL on %s", currentVersion, eolDate.Format("2006-01-02"))
	//     }
	// }

	return false, "", ""
}
