package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NodePoolUpgradeRequest records a one-time customer-requested NodePool upgrade.
// A request may start immediately or no earlier than its startTime.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status

type NodePoolUpgradeRequest struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec identifies the NodePool, target version, and optional start time.

	// +required
	Spec NodePoolUpgradeRequestSpec `json:"spec"`

	// status reports the controller's decision and observed progress.

	// +optional
	Status NodePoolUpgradeRequestStatus `json:"status,omitempty"`
}

// NodePoolUpgradeRequestList is a list of NodePool upgrade requests.
// +kubebuilder:object:root=true
type NodePoolUpgradeRequestList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// items is the list of requests.

	Items []NodePoolUpgradeRequest `json:"items"`
}

// NodePoolUpgradeRequestSpec describes one immutable upgrade request.
type NodePoolUpgradeRequestSpec struct {
	// clusterID is the parent Cluster name in the same namespace.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClusterID string `json:"clusterID"`

	// nodePoolID is the NodePool name in the same namespace.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	NodePoolID string `json:"nodePoolID"`

	// targetVersion is the exact OpenShift version requested for the NodePool.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion string `json:"targetVersion"`

	// startTime is the earliest time at which the upgrade may start. When omitted,
	// the request is eligible immediately. This is a one-time start, not a
	// recurring maintenance window.

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
}

// NodePoolUpgradeRequestStatus reports scheduling and execution progress.
type NodePoolUpgradeRequestStatus struct {
	// conditions report whether the request is scheduled, accepted, progressing,
	// completed, or failed.

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// acceptedTime records when the controller accepted the target for execution.

	// +optional
	AcceptedTime *metav1.Time `json:"acceptedTime,omitempty"`

	// startedTime records when the NodePool target was submitted.

	// +optional
	StartedTime *metav1.Time `json:"startedTime,omitempty"`

	// completedTime records when HyperShift reports the target as applied.

	// +optional
	CompletedTime *metav1.Time `json:"completedTime,omitempty"`
}

func init() {
	register(&NodePoolUpgradeRequest{}, &NodePoolUpgradeRequestList{})
}
