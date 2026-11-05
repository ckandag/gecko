package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodePoolUpgradeSpec requests one customer-initiated upgrade.
type NodePoolUpgradeSpec struct {
	// targetVersion is the exact version to install. When omitted, it defaults
	// to the observed control-plane version when this upgrade is submitted.
	// +orlop:public
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion *string `json:"targetVersion,omitempty"`

	// startTime is the earliest time the upgrade may start. When omitted, the
	// upgrade is eligible immediately.
	// +orlop:public
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
}

// NodePoolUpgradeResult records the current or most recent upgrade.
type NodePoolUpgradeResult struct {
	// targetVersion is the version selected for this upgrade.
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion string `json:"targetVersion"`

	// requestedTime records when the upgrade was first observed.
	// +orlop:public
	// +optional
	RequestedTime *metav1.Time `json:"requestedTime,omitempty"`

	// acceptedTime records when the target was accepted for execution. Pending
	// upgrade settings can no longer be changed after acceptance.
	// +orlop:public
	// +optional
	AcceptedTime *metav1.Time `json:"acceptedTime,omitempty"`

	// startedTime records when the target was submitted for rollout.
	// +orlop:public
	// +optional
	StartedTime *metav1.Time `json:"startedTime,omitempty"`

	// completedTime records successful completion of the version and health checks.
	// +orlop:public
	// +optional
	CompletedTime *metav1.Time `json:"completedTime,omitempty"`

	// failureReason is a machine-readable explanation of a rejected or failed upgrade.
	// +orlop:public
	// +optional
	// +kubebuilder:validation:MaxLength=63
	FailureReason *string `json:"failureReason,omitempty"`

	// failureMessage describes why the upgrade could not proceed.
	// +orlop:public
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	FailureMessage *string `json:"failureMessage,omitempty"`

	// intentID identifies the upgrade settings observed by the controller, so
	// unrelated spec changes do not restart a completed or rejected upgrade.
	// Private controller bookkeeping; not exposed to customers.
	// +optional
	// +kubebuilder:validation:MaxLength=80
	IntentID *string `json:"intentID,omitempty"`
}
