package firestore

import (
	"encoding/json"
	"fmt"

	"github.com/openshift-online/gecko/controllers/util/constants"
	"github.com/openshift-online/kube-applier-gcp/pkg/api/kubeapplier"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// aggregateConditions derives a single "Applied" metav1.Condition from the
// Successful conditions on a slice of ApplyDesire status documents.
// Applied=True only when every desire has Successful=True.
// Applied=False if any desire has Successful=False, or if the slice is empty,
// or if any desire has no Successful condition (still pending).
func aggregateConditions(desires []kubeapplier.ApplyDesire) []metav1.Condition {
	if len(desires) == 0 {
		return []metav1.Condition{{
			Type:    "Applied",
			Status:  metav1.ConditionFalse,
			Reason:  "NoApplyDesires",
			Message: "No ApplyDesire documents found",
		}}
	}

	allTrue := true
	anyPending := false
	for _, d := range desires {
		found := false
		for _, c := range d.Status.Conditions {
			if c.Type == kubeapplier.ConditionTypeSuccessful {
				found = true
				switch c.Status {
				case metav1.ConditionTrue:
					// applied successfully
				case metav1.ConditionUnknown:
					// kube-applier-gcp is still processing
					allTrue = false
					anyPending = true
				default:
					// ConditionFalse — apply failed
					allTrue = false
				}
				break
			}
		}
		if !found {
			// kube-applier-gcp has not yet processed this desire
			allTrue = false
			anyPending = true
		}
	}

	if allTrue {
		return []metav1.Condition{{
			Type:    "Applied",
			Status:  metav1.ConditionTrue,
			Reason:  "AllResourcesApplied",
			Message: fmt.Sprintf("All %d resources applied successfully", len(desires)),
		}}
	}

	if anyPending {
		return []metav1.Condition{{
			Type:    "Applied",
			Status:  metav1.ConditionFalse,
			Reason:  "Pending",
			Message: "One or more resources not yet processed by kube-applier-gcp",
		}}
	}

	return []metav1.Condition{{
		Type:    "Applied",
		Status:  metav1.ConditionFalse,
		Reason:  "ApplyFailed",
		Message: "One or more resources failed to apply",
	}}
}

// extractResourceStatuses parses ReadDesire status documents and returns
// per-resource status fields keyed by resource identity string.
// For HostedCluster resources it extracts: availableCondition, controlPlaneEndpoint,
// version, desiredVersion, availableVersions, and versionConditions.
// For NodePool resources it extracts: readyCondition, allNodesHealthyCondition.
// For Certificate resources it extracts: readyCondition.
// Other resources: empty inner map (no known fields to extract).
func extractResourceStatuses(reads []kubeapplier.ReadDesire) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string, len(reads))
	for _, rd := range reads {
		key := resourceKey(rd.Spec.TargetItem)
		fields := map[string]string{}

		if rd.Status.KubeContent == nil || len(rd.Status.KubeContent.Raw) == 0 {
			result[key] = fields
			continue
		}

		ref := rd.Spec.TargetItem
		var err error
		switch {
		case ref.Resource == "hostedclusters" && ref.Group == constants.HyperShiftGroup:
			fields, err = extractHCFields(rd.Status.KubeContent.Raw)
		case ref.Resource == "nodepools" && ref.Group == constants.HyperShiftGroup:
			fields, err = extractNPFields(rd.Status.KubeContent.Raw)
		case ref.Resource == "certificates" && ref.Group == "cert-manager.io":
			fields, err = extractCertFields(rd.Status.KubeContent.Raw)
		}
		if err != nil {
			return nil, fmt.Errorf("extract resource status %s: %w", key, err)
		}

		result[key] = fields
	}
	return result, nil
}

// extractHCFields extracts HostedCluster status fields from raw live-object JSON.
//   - availableCondition: .status.conditions[type=Available].status
//   - controlPlaneEndpoint: .status.controlPlaneEndpoint.host
//   - version: first .status.version.history[].version where state == "Completed"
//   - desiredVersion: .status.version.desired.version
//   - availableVersions: JSON-encoded .status.version.availableUpdates[].version
//   - versionConditions: JSON-encoded upgrade-related HostedCluster conditions
func extractHCFields(raw []byte) (map[string]string, error) {
	fields := map[string]string{}

	var obj struct {
		Status struct {
			Conditions           []metav1.Condition `json:"conditions"`
			ControlPlaneEndpoint struct {
				Host string `json:"host"`
			} `json:"controlPlaneEndpoint"`
			Version struct {
				Desired struct {
					Version string `json:"version"`
				} `json:"desired"`
				AvailableUpdates []struct {
					Version string `json:"version"`
				} `json:"availableUpdates"`
				History []struct {
					Version string `json:"version"`
					State   string `json:"state"`
				} `json:"history"`
			} `json:"version"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal HostedCluster live object: %w", err)
	}

	for _, c := range obj.Status.Conditions {
		if c.Type == "Available" {
			fields["availableCondition"] = string(c.Status)
			break
		}
	}

	if host := obj.Status.ControlPlaneEndpoint.Host; host != "" {
		fields["controlPlaneEndpoint"] = host
	}
	if desired := obj.Status.Version.Desired.Version; desired != "" {
		fields["desiredVersion"] = desired
	}

	availableVersions := make([]string, 0, len(obj.Status.Version.AvailableUpdates))
	for _, update := range obj.Status.Version.AvailableUpdates {
		if update.Version != "" {
			availableVersions = append(availableVersions, update.Version)
		}
	}
	availableJSON, err := json.Marshal(availableVersions)
	if err != nil {
		return nil, fmt.Errorf("marshal HostedCluster available versions: %w", err)
	}
	fields["availableVersions"] = string(availableJSON)

	versionConditions := make([]metav1.Condition, 0)
	for _, condition := range obj.Status.Conditions {
		switch condition.Type {
		case "ClusterVersionUpgradeable", "ClusterVersionProgressing", "ClusterVersionAvailable",
			"ClusterVersionReleaseAccepted", "ClusterVersionSucceeding", "Degraded":
			versionConditions = append(versionConditions, condition)
		}
	}
	conditionsJSON, err := json.Marshal(versionConditions)
	if err != nil {
		return nil, fmt.Errorf("marshal HostedCluster version conditions: %w", err)
	}
	fields["versionConditions"] = string(conditionsJSON)

	for _, h := range obj.Status.Version.History {
		if h.State == "Completed" {
			fields["version"] = h.Version
			break
		}
	}

	return fields, nil
}

// extractNPFields extracts NodePool status fields from raw live-object JSON.
//   - readyCondition:              .status.conditions[type=Ready].status
//   - allNodesHealthyCondition:    .status.conditions[type=AllNodesHealthy].status
//   - allMachinesReadyCondition:   .status.conditions[type=AllMachinesReady].status
//   - updatingConfigCondition:     .status.conditions[type=UpdatingConfig].status
//   - updatingVersionCondition:    .status.conditions[type=UpdatingVersion].status
func extractNPFields(raw []byte) (map[string]string, error) {
	fields := map[string]string{}

	var obj struct {
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal NodePool live object: %w", err)
	}

	for _, c := range obj.Status.Conditions {
		switch c.Type {
		case "Ready":
			fields["readyCondition"] = c.Status
		case "AllNodesHealthy":
			fields["allNodesHealthyCondition"] = c.Status
		case "AllMachinesReady":
			fields["allMachinesReadyCondition"] = c.Status
		case "UpdatingConfig":
			fields["updatingConfigCondition"] = c.Status
		case "UpdatingVersion":
			fields["updatingVersionCondition"] = c.Status
		}
	}

	return fields, nil
}

// extractCertFields extracts Certificate status fields from raw live-object JSON.
//   - readyCondition: .status.conditions[type=Ready].status
func extractCertFields(raw []byte) (map[string]string, error) {
	fields := map[string]string{}

	var obj struct {
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal Certificate live object: %w", err)
	}

	for _, c := range obj.Status.Conditions {
		if c.Type == "Ready" {
			fields["readyCondition"] = c.Status
			break
		}
	}

	return fields, nil
}
