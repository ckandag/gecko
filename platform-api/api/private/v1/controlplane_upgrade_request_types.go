package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ControlPlaneUpgradeRequest records a one-time, customer-initiated control-plane
// upgrade. Creating a request does not change the Cluster's release version;
// the control-plane-upgrade controller applies accepted requests.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
// +orlop:public-verbs: create,get,list
type ControlPlaneUpgradeRequest struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +orlop:public
	// +required
	Spec ControlPlaneUpgradeRequestSpec `json:"spec"`

	// +orlop:public
	// +optional
	Status ControlPlaneUpgradeRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ControlPlaneUpgradeRequestList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// +orlop:public
	Items []ControlPlaneUpgradeRequest `json:"items"`
}

// ControlPlaneUpgradeRequestSpec identifies the Cluster and selected target.
type ControlPlaneUpgradeRequestSpec struct {
	// ClusterID is the Cluster name in the same namespace.
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	ClusterID string `json:"clusterID"`

	// TargetVersion is an update advertised by the Cluster's HostedCluster.
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TargetVersion string `json:"targetVersion"`
}

// ControlPlaneUpgradeRequestStatus records the controller's decision on this
// request. Rollout progress remains on Cluster.status.controlPlaneUpgrade.
type ControlPlaneUpgradeRequestStatus struct {
	// +orlop:public
	// +optional
	// +kubebuilder:validation:Enum=Accepted;Rejected
	Decision string `json:"decision,omitempty"`
	// +orlop:public
	// +optional
	Reason string `json:"reason,omitempty"`
	// +orlop:public
	// +optional
	DecidedAt *metav1.Time `json:"decidedAt,omitempty"`
}

func init() {
	register(&ControlPlaneUpgradeRequest{}, &ControlPlaneUpgradeRequestList{})
}
