package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ControlPlaneUpgradeRequest records a one-time, customer-initiated control-plane
// upgrade. Creating a request does not change the Cluster's release version;
// the control-plane-upgrade controller applies accepted requests.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status

type ControlPlaneUpgradeRequest struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec ControlPlaneUpgradeRequestSpec `json:"spec"`

	// +optional
	Status ControlPlaneUpgradeRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ControlPlaneUpgradeRequestList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:MaxItems=10000
	Items []ControlPlaneUpgradeRequest `json:"items"`
}

// ControlPlaneUpgradeRequestSpec identifies the Cluster and selected target.
type ControlPlaneUpgradeRequestSpec struct {
	// ClusterID is the Cluster name in the same namespace.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	ClusterID string `json:"clusterID"`

	// TargetVersion is an update advertised by the Cluster's HostedCluster.

	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion string `json:"targetVersion"`
}

// ControlPlaneUpgradeRequestStatus records the controller's decision on this
// request. Rollout progress remains on Cluster.status.controlPlaneUpgrade.
type ControlPlaneUpgradeRequestStatus struct {

	// decision is the outcome of the request: Accepted or Rejected. It is omitted
	// while the request is awaiting a decision.
	// +optional
	// +kubebuilder:validation:Enum=Accepted;Rejected
	// +kubebuilder:validation:MaxLength=8
	Decision string `json:"decision,omitempty"`

	// reason is a short code explaining the decision, such as UpgradeRequested
	// for an accepted request, or UpgradeInProgress or TargetNotAvailable for a
	// rejected request.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Reason string `json:"reason,omitempty"`

	// decidedTime is when the decision was recorded.
	// +optional
	DecidedTime *metav1.Time `json:"decidedTime,omitempty"`
}

func init() {
	register(&ControlPlaneUpgradeRequest{}, &ControlPlaneUpgradeRequestList{})
}
