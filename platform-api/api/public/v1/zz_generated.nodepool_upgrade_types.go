package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodePoolUpgradeSpec requests one customer-initiated upgrade.
type NodePoolUpgradeSpec struct {
	// targetVersion is the exact version to install. When omitted, it defaults
	// to the observed control-plane version when this upgrade is submitted.

	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion *string `json:"targetVersion,omitempty"`

	// startTime is the earliest time the upgrade may start. When omitted, the
	// upgrade is eligible immediately.

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
}

// NodePoolUpgradeResult records the current or most recent upgrade.
type NodePoolUpgradeResult struct {
	// targetVersion is the version selected for this upgrade.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion string `json:"targetVersion"`

	// requestedTime records when the upgrade was first observed.

	// +optional
	RequestedTime *metav1.Time `json:"requestedTime,omitempty"`

	// acceptedTime records when the target was accepted for execution. Pending
	// upgrade settings can no longer be changed after acceptance.

	// +optional
	AcceptedTime *metav1.Time `json:"acceptedTime,omitempty"`

	// startedTime records when the target was submitted for rollout.

	// +optional
	StartedTime *metav1.Time `json:"startedTime,omitempty"`

	// completedTime records successful completion of the version and health checks.

	// +optional
	CompletedTime *metav1.Time `json:"completedTime,omitempty"`

	// failureReason is a machine-readable explanation of a rejected or failed upgrade.

	// +optional
	// +kubebuilder:validation:MaxLength=63
	FailureReason *string `json:"failureReason,omitempty"`

	// failureMessage describes why the upgrade could not proceed.

	// +optional
	// +kubebuilder:validation:MaxLength=1024
	FailureMessage *string `json:"failureMessage,omitempty"`
}
