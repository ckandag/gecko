package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ControlPlaneUpgradePolicy defines when automatic control-plane upgrades may
// start for a Cluster. The policy does not select an upgrade target or track
// rollout execution.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
type ControlPlaneUpgradePolicy struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +orlop:public
	// +required
	Spec ControlPlaneUpgradePolicySpec `json:"spec"`

	// +orlop:public
	// +optional
	Status ControlPlaneUpgradePolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ControlPlaneUpgradePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// +orlop:public
	Items []ControlPlaneUpgradePolicy `json:"items"`
}

// ControlPlaneUpgradePolicySpec contains the customer's preferred timing for
// automatic control-plane upgrades.
type ControlPlaneUpgradePolicySpec struct {
	// ClusterID identifies the Cluster governed by this policy in the same namespace.
	// +orlop:public
	// +required
	ClusterID string `json:"clusterID"`

	// MaintenanceWindow defines when an automatic control-plane upgrade may start.
	// When omitted, the platform may start an automatic upgrade at any time.
	// +orlop:public
	// +optional
	MaintenanceWindow *ControlPlaneMaintenanceWindow `json:"maintenanceWindow,omitempty"`

	// MaintenanceExclusions are non-recurring periods during which automatic
	// control-plane upgrades must not start.
	// +orlop:public
	// +optional
	// +listType=map
	// +listMapKey=name
	MaintenanceExclusions []ControlPlaneMaintenanceExclusion `json:"maintenanceExclusions,omitempty"`
}

// ControlPlaneMaintenanceWindow defines one recurring maintenance window.
type ControlPlaneMaintenanceWindow struct {
	// Start is the first occurrence of the maintenance window, stored in UTC.
	// +orlop:public
	// +required
	Start metav1.Time `json:"start"`

	// DurationMinutes is how long each occurrence remains open, in minutes.
	// +orlop:public
	// +required
	DurationMinutes int32 `json:"durationMinutes"`

	// Recurrence describes how the window repeats using customer-friendly
	// fields. Gecko normalizes this structure to an RFC 5545 recurrence rule
	// when evaluating the maintenance window.
	// +orlop:public
	// +required
	Recurrence ControlPlaneMaintenanceRecurrence `json:"recurrence"`
}

// ControlPlaneMaintenanceRecurrence describes how a maintenance window repeats.
type ControlPlaneMaintenanceRecurrence struct {
	// Frequency is the recurrence frequency, for example "weekly".
	// +orlop:public
	// +required
	Frequency string `json:"frequency"`

	// DaysOfWeek contains lowercase UTC day names on which the window may start,
	// for example "monday" or "saturday". Other casing is rejected by validation.
	// +orlop:public
	// +required
	// +listType=set
	DaysOfWeek []string `json:"daysOfWeek"`
}

// ControlPlaneMaintenanceExclusion defines one non-recurring blackout period.
type ControlPlaneMaintenanceExclusion struct {
	// +orlop:public
	// +required
	Name string `json:"name"`

	// +orlop:public
	// +required
	Start metav1.Time `json:"start"`

	// +orlop:public
	// +required
	End metav1.Time `json:"end"`
}

// ControlPlaneUpgradePolicyStatus contains asynchronously evaluated policy status.
type ControlPlaneUpgradePolicyStatus struct {
	// Conditions report whether the policy is accepted and can be evaluated.
	// +orlop:public
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	register(&ControlPlaneUpgradePolicy{}, &ControlPlaneUpgradePolicyList{})
}
