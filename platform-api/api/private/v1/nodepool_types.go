package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodePool enables declarative management of a pool of nodes.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Available",type=string,JSONPath=`.status.conditions[?(@.type=="NodePoolAvailable")].status`
// +kubebuilder:printcolumn:name="Healthy",type=string,JSONPath=`.status.conditions[?(@.type=="NodePoolHealthy")].status`
// +kubebuilder:printcolumn:name="Progressing",type=string,JSONPath=`.status.conditions[?(@.type=="NodePoolProgressing")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NodePool struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is the standard object metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired behavior of the NodePool.
	// +orlop:public
	// +required
	Spec NodePoolSpec `json:"spec,omitempty"`

	// status is the most recently observed status of the NodePool.
	// +orlop:public
	// +optional
	Status NodePoolStatus `json:"status,omitempty"`
}

// NodePoolList is a list of NodePool.
// +kubebuilder:object:root=true
type NodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is the standard list metadata.
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	// items is the list of NodePools.
	// +orlop:public
	Items []NodePool `json:"items"`
}

// NodePoolSpec defines the NodePool behavior.
// +kubebuilder:validation:XValidation:rule="!has(self.nodeCount) || !has(self.autoscaling)",message="nodeCount and autoscaling are mutually exclusive"
type NodePoolSpec struct {
	// clusterID is the name of the Cluster this NodePool belongs to.
	// If a Cluster with this name doesn't exist, the controller will no-op until it exists.
	// +orlop:public
	// +kubebuilder:validation:XValidation:rule="self == oldSelf", message="clusterID is immutable"
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')",message="clusterID must consist of lowercase alphanumeric characters or '-', start and end with an alphanumeric character, and be between 1 and 253 characters"
	// +required
	ClusterID string `json:"clusterID"`

	// platform specifies the underlying infrastructure provider for the NodePool
	// and is used to configure platform specific behavior.
	//
	// +orlop:public
	// +required
	Platform NodePoolPlatformSpec `json:"platform"`

	// release specifies what OpenShift release to use for the node pool.
	//
	// +orlop:public
	// +required
	Release NodePoolReleaseSpec `json:"release"`

	// upgrade requests a one-time upgrade of this pool. Omit this field to keep
	// the existing installation behavior. Set it to null to cancel a pending
	// upgrade before it is accepted for execution.
	// +orlop:public
	// +optional
	// +nullable
	Upgrade *NodePoolUpgradeSpec `json:"upgrade,omitempty"` //nolint:kubeapilinter // Explicit null cancels a pending upgrade; omission preserves it on public PUT.

	// nodeCount is the desired number of nodes the pool should maintain. If unset, the controller default value is 0.
	// nodeCount is mutually exclusive with autoscaling. If autoscaling is configured, nodeCount must be omitted and autoscaling will control the NodePool size internally.
	//
	// +orlop:public
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeCount *int32 `json:"nodeCount,omitempty"`

	// autoscaling specifies auto-scaling behavior for the NodePool.
	// autoscaling is mutually exclusive with nodeCount. If nodeCount is set, this field must be omitted.
	//
	// +orlop:public
	// +optional
	Autoscaling *AutoscalingSpec `json:"autoscaling,omitempty"`

	// nodeLabels propagates a list of labels to Nodes, only once on creation.
	// Valid values are those in https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
	//
	// +kubebuilder:validation:MaxProperties=50
	// +orlop:public
	// +optional
	NodeLabels map[string]string `json:"nodeLabels,omitempty"`

	// taints if specified, propagates a list of taints to Nodes, only once on creation.
	// These taints are additive to the ones applied by other controllers
	//
	// +kubebuilder:validation:MaxItems=50
	// +orlop:public
	// +optional
	// +listType=map
	// +listMapKey=key
	// +listMapKey=effect
	Taints []TaintSpec `json:"taints,omitempty"`
}

// NodePoolReleaseSpec defines the complete OpenShift release to install on a node pool.
type NodePoolReleaseSpec struct {
	// version is the OpenShift version to install.
	// Specify a complete semantic version, with an optional prerelease and without
	// a leading "v" (for example, "4.23.9" or "5.0.0-ec.6").
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule=`self.matches('^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?$')`,message="version must be a valid semantic version with an optional prerelease and without a leading 'v' (e.g., 4.14.9 or 5.0.0-ec.6)"
	// +example="4.23.9"
	Version string `json:"version"`

	// channelGroup selects a platform-managed Channel used to choose and resolve
	// the release (for example, stable, fast, eus, candidate, or nightly).
	// Available Channels depend on the environment; channel groups must be
	// lowercase DNS labels (not arbitrary DNS subdomain Channel names).
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	ChannelGroup string `json:"channelGroup"`
}

type NodePoolPlatformSpec struct {
	// +orlop:public
	// +required
	// +kubebuilder:validation:Enum=GCP
	Type string `json:"type"`

	// +orlop:public
	// +optional
	GCP *GCPNodePoolPlatform `json:"gcp,omitempty"`
}

type GCPNodePoolPlatform struct {
	// machineType is the GCP machine type for node instances (e.g. n2-standard-4).
	// Must follow GCP machine type naming conventions as documented at:
	// https://cloud.google.com/compute/docs/machine-resource#machine_type_comparison
	//
	// Valid machine type formats:
	//   - predefined: n1-standard-1, n2-highmem-4, c2-standard-8, etc.
	//   - custom: custom-{cpus}-{memory} (e.g. custom-4-8192)
	//   - custom with extended memory: custom-{cpus}-{memory}-ext (e.g. custom-2-13312-ext)
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-z0-9]+(-[a-z0-9]+)*$')",message="machineType must start and end with a lowercase letter or digit, and contain only lowercase letters, digits, and hyphens"
	MachineType string `json:"machineType,omitempty"`

	// diskSizeGB specifies the size of the boot disk in gigabytes.
	// Must be at least 20 GB for RHCOS images.
	//
	// +orlop:public
	// +optional
	// +kubebuilder:validation:Minimum=20
	// +default=64
	// +kubebuilder:validation:Maximum=65536
	DiskSizeGB int64 `json:"diskSizeGB,omitempty"`

	// diskType specifies the disk type for the boot disk.
	// Valid values include:
	//   - "pd-standard" - Standard persistent disk (magnetic)
	//   - "pd-ssd" - SSD persistent disk
	//   - "pd-balanced" - Balanced persistent disk (recommended)
	// If not specified, defaults to "pd-balanced".
	//
	// +default="pd-balanced"
	// +kubebuilder:validation:Enum=pd-standard;pd-ssd;pd-balanced
	// +orlop:public
	// +optional
	DiskType string `json:"diskType,omitempty"`

	// zone is the GCP zone where node instances will be created.
	// Must be a valid zone within the cluster's region.
	// Format: {region}-{zone} (e.g. us-central1-a, europe-west2-b)
	// See https://cloud.google.com/compute/docs/regions-zones for available zones.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-z]+(?:-[a-z0-9]+)*-[a-z]$')",message="zone must be in the form of region-zone (e.g., us-central1-a)"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="zone is immutable"
	Zone string `json:"zone,omitempty"`

	// subnet is the name of the subnet where node instances will be created.
	// Must be a subnet within the VPC network specified in the HostedCluster's
	// networkConfig and located in the same region as the zone.
	// The subnet must have enough IP addresses available for the expected number of nodes.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="subnet is immutable"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="subnet must be a valid GCP subnet name: start with a lowercase letter, contain only lowercase letters, digits, or hyphens, and end with a letter or digit"
	// +example="my-subnet"
	Subnet string `json:"subnet,omitempty"`

	// provisioningModel specifies the provisioning model for node instances.
	// Spot and Preemptible instances cost less but can be terminated by GCP with 30 seconds notice.
	// Spot instances are recommended over Preemptible as they have no maximum runtime limit.
	// Standard instances are regular VMs that run until explicitly stopped.
	// If not specified, defaults to "Standard".
	//
	// +orlop:public
	// +default="Standard"
	// +optional
	// +kubebuilder:validation:Enum=Standard;Spot;Preemptible
	ProvisioningModel string `json:"provisioningModel,omitempty"`

	// onHostMaintenance specifies the behavior when host maintenance occurs.
	// For Spot and Preemptible instances, this must be "TERMINATE".
	// For Standard instances, can be "MIGRATE" (live migrate) or "TERMINATE".
	// If not specified, defaults to "MIGRATE" for Standard instances and "TERMINATE" for Spot/Preemptible.
	//
	// +orlop:public
	// +optional
	// +kubebuilder:validation:Enum=MIGRATE;TERMINATE
	OnHostMaintenance string `json:"onHostMaintenance,omitempty"`

	// resourceLabels is an optional list of additional labels to apply to GCP node
	// instances and their associated resources (disks, etc.).
	// Labels will be merged with cluster-level resource labels, with NodePool labels
	// taking precedence in case of conflicts.
	//
	// Keys and values must conform to GCP labeling requirements:
	//   - Keys: 1–63 chars, must start with a lowercase letter; allowed [a-z0-9_-]
	//   - Values: empty or 1–63 chars; allowed [a-z0-9_-]
	//   - Maximum 60 user labels per resource (GCP limit is 64 total, with ~4 reserved)
	//
	// +orlop:public
	// +optional
	// +listType=map
	// +listMapKey=key
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=60
	ResourceLabels []GCPResourceLabel `json:"resourceLabels,omitempty"`

	// networkTags is an optional list of network tags to apply to node instances.
	// These tags are used by GCP firewall rules to control network access.
	// Tags must conform to GCP naming conventions:
	//   - 1-63 characters
	//   - Lowercase letters, numbers, and hyphens only
	//   - Must start with lowercase letter
	//   - Cannot end with hyphen
	//
	// +orlop:public
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	NetworkTags []string `json:"networkTags,omitempty"`
}

// TaintSpec is as v1 Core but without TimeAdded.
// https://github.com/kubernetes/kubernetes/blob/ed8cad1e80d096257921908a52ac69cf1f41a098/staging/src/k8s.io/api/core/v1/types.go#L3037-L3053
// Validation replicates the same validation as the upstream https://github.com/kubernetes/kubernetes/blob/9a2a7537f035969a68e432b4cc276dbce8ce1735/pkg/util/taints/taints.go#L273.
// See also https://kubernetes.io/docs/concepts/overview/working-with-objects/names/.
type TaintSpec struct {
	// key is the taint key to be applied to a node.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:XValidation:rule=`self.matches('^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\\/)?[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$')`,message="key must be a qualified name with an optional subdomain prefix e.g. example.com/MyName"
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`

	// value is the taint value corresponding to the taint key.
	//
	// +orlop:public
	// +optional
	// +kubebuilder:validation:XValidation:rule=`self.matches('^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$')`,message="Value must start and end with alphanumeric characters and can only contain '-', '_', '.' in the middle"
	// +kubebuilder:validation:MaxLength=63
	Value string `json:"value,omitempty"`

	// effect is the effect of the taint on pods
	// that do not tolerate the taint.
	// Valid effects are NoSchedule, PreferNoSchedule and NoExecute.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:Enum=NoSchedule;PreferNoSchedule;NoExecute
	Effect string `json:"effect"`
}

// +kubebuilder:validation:XValidation:rule="self.max >= self.min",message="max must be greater than or equal to min"
type AutoscalingSpec struct {
	// min is the minimum number of nodes to maintain in the pool.
	// Can be set to 0 to allow the pool to scale down completely.
	// Must be >= 0 and <= .Max.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:Minimum=0
	Min *int32 `json:"min,omitempty"`

	// max is the maximum number of nodes allowed in the pool. Must be >= 1 and >= Min.
	//
	// +orlop:public
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=500
	Max int32 `json:"max"`
}

// NodePoolStatus is written by controllers only.
type NodePoolStatus struct {
	// conditions represents the latest available observations of the node pool's
	// current state.
	//
	// +orlop:public
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// currentVersion is the last OpenShift version HyperShift reports as applied
	// to this NodePool. It does not advance merely because an upgrade was requested.
	// +orlop:public
	// +optional
	CurrentVersion string `json:"currentVersion,omitempty"`

	// upgrade reports the current or most recent upgrade, including its target,
	// execution times, and failure details.
	// +orlop:public
	// +optional
	Upgrade *NodePoolUpgradeResult `json:"upgrade,omitempty"`

	// VersionResolution is written by the nodepool-vr controller.
	// Not exposed on the public API.
	// +optional
	VersionResolution *VersionResolutionResult `json:"versionResolution,omitempty"`
}

func init() { register(&NodePool{}, &NodePoolList{}) }
