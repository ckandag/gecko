package controlplaneupgrade

import (
	"fmt"

	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// selectZStreamCandidate returns the newest advertised patch release in the same
// major.minor stream as completed, or "" if none is newer than completed.
func selectZStreamCandidate(completed string, advertised []string) (string, error) {
	completedVersion, err := utilversion.ParseSemantic(completed)
	if err != nil {
		return "", fmt.Errorf("parse completed version %q: %w", completed, err)
	}

	var selected string
	var selectedVersion *utilversion.Version
	for _, candidate := range advertised {
		candidateVersion, err := utilversion.ParseSemantic(candidate)
		if err != nil {
			// AvailableUpdates is HyperShift-owned; skip anything Gecko cannot parse
			// rather than fail the whole reconciliation over one bad entry.
			continue
		}
		if candidateVersion.Major() != completedVersion.Major() || candidateVersion.Minor() != completedVersion.Minor() {
			continue
		}
		if !completedVersion.LessThan(candidateVersion) {
			continue
		}
		if selectedVersion == nil || selectedVersion.LessThan(candidateVersion) {
			selected = candidate
			selectedVersion = candidateVersion
		}
	}
	return selected, nil
}

// selectYStreamCandidate returns the next sequential minor version from advertised
// that does not exceed fleetMinorVersion authorization. Returns "" if no valid
// y-stream candidate exists.
//
// Y-stream upgrades are sequential (4.22 → 4.23 → 4.24), never skip a minor version.
// The fleetMinorVersion is the platform authorization boundary; it must be greater
// than or equal to the candidate's minor version.
func selectYStreamCandidate(completed string, advertised []string, fleetMinorVersion string) (string, error) {
	completedVersion, err := utilversion.ParseSemantic(completed)
	if err != nil {
		return "", fmt.Errorf("parse completed version %q: %w", completed, err)
	}

	fleetMinor, err := utilversion.ParseSemantic(fleetMinorVersion + ".0")
	if err != nil {
		return "", fmt.Errorf("parse fleet minor version %q: %w", fleetMinorVersion, err)
	}
	if fleetMinor.Major() != completedVersion.Major() {
		return "", fmt.Errorf("fleet minor version %q has a different major version than completed version %q", fleetMinorVersion, completed)
	}

	targetMinor := completedVersion.Minor() + 1

	// Find the newest patch in the next minor version that's authorized by fleetMinorVersion
	var selected string
	var selectedVersion *utilversion.Version
	for _, candidate := range advertised {
		candidateVersion, err := utilversion.ParseSemantic(candidate)
		if err != nil {
			continue
		}

		// Must be same major, next minor
		if candidateVersion.Major() != completedVersion.Major() {
			continue
		}
		if candidateVersion.Minor() != targetMinor {
			continue
		}

		// Must not exceed fleet authorization
		if candidateVersion.Minor() > fleetMinor.Minor() {
			continue
		}

		if selectedVersion == nil || selectedVersion.LessThan(candidateVersion) {
			selected = candidate
			selectedVersion = candidateVersion
		}
	}

	return selected, nil
}
