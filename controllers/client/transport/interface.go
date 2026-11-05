package transport

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceKey returns the identity key used in ResourceStatuses for a given resource.
// Format: "{group}/{version}/{resource}/{namespace}/{name}"
func ResourceKey(group, version, resource, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", group, version, resource, namespace, name)
}

// Status holds the status read back from the transport after applying resources.
type Status struct {
	// Conditions contains top-level conditions (Applied, Available, etc.).
	Conditions []metav1.Condition
	// ResourceStatuses contains statusFeedback values keyed by resource identity then by field name.
	// Key format: "{group}/{version}/{resource}/{namespace}/{name}"
	ResourceStatuses map[string]map[string]string
	// Stale is true when an expected status document is missing or does not yet
	// reflect the most recently written spec. Callers should requeue sooner and
	// avoid trusting condition values until Stale becomes false.
	Stale bool
}

// DeleteStatus holds the aggregated status of delete operations.
type DeleteStatus struct {
	// AllSuccessful is true when all DeleteDesires have Successful=True condition.
	AllSuccessful bool
	// PendingCount is the number of DeleteDesires without Successful=True.
	PendingCount int
	// TotalCount is the total number of DeleteDesires found.
	TotalCount int
	// ApplyDesiresCount is the number of ApplyDesires still present.
	// When > 0 and TotalCount == 0, Delete() must be called to enqueue deletion.
	ApplyDesiresCount int
}

// Client abstracts the transport layer for delivering resources to management clusters.
type Client interface {
	// Apply creates or updates resources on the target cluster and returns current status.
	Apply(ctx context.Context, targetCluster, groupKey string, manifests [][]byte) (*Status, error)
	// GetStatus compares the expected Desire documents in the specs database
	// with the statuses currently available for the given groupKey.
	GetStatus(ctx context.Context, targetCluster, groupKey string) (*Status, error)
	// Delete removes all resources for the given groupKey from the target cluster.
	Delete(ctx context.Context, targetCluster, groupKey string) error
	// GetDeleteStatus checks the status of delete operations for the given groupKey.
	GetDeleteStatus(ctx context.Context, targetCluster, groupKey string) (*DeleteStatus, error)
	// CleanupDeleteDesires removes DeleteDesire spec documents for the given groupKey.
	// kube-applier-gcp owns cleanup of the corresponding status documents.
	CleanupDeleteDesires(ctx context.Context, targetCluster, groupKey string) error
}
