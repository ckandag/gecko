// Package firestore implements transport.Client using Google Cloud Firestore
// as the transport layer via kube-applier-gcp desire documents.
package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"

	"github.com/openshift-online/gecko/controllers/client/transport"
	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/kube-applier-gcp/pkg/api/kubeapplier"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

const (
	collectionApplyDesires  = "applydesires"
	collectionReadDesires   = "readdesires"
	collectionDeleteDesires = "deletedesires"
)

const (
	specsDBName  = "specs"
	statusDBName = "status"

	// maxDeleteBatchSize is the maximum number of resources to delete per
	// Firestore transaction. Each resource requires 3 writes (Set DeleteDesire,
	// Delete ReadDesire, Delete ApplyDesire) and Firestore limits transactions
	// to 500 writes.
	maxDeleteBatchSize = 166
)

// mcClients caches the Firestore client pair for one management cluster.
type mcClients struct {
	specs  *firestore.Client // specs DB (apply/read/delete desires written here)
	status *firestore.Client // status DB (status read back from here)
}

// Client implements transport.Client using Firestore as the transport.
// One pair of *firestore.Client is maintained per management cluster.
// MCs are identified by their GCP project ID.
type Client struct {
	mu    sync.RWMutex
	cache map[string]*mcClients
	log   logger.Logger
	// dialOpts are extra grpc/firestore client options, used to inject emulator settings in tests.
	dialOpts []option.ClientOption
}

// Ensure Client implements transport.Client.
var _ transport.Client = (*Client)(nil)

// New creates a new Firestore transport client.
// The management cluster name passed to Apply/GetStatus/Delete is used directly
// as the GCP project ID.
// Use opts to inject emulator settings in tests (e.g. option.WithEndpoint).
func New(log logger.Logger, opts ...option.ClientOption) *Client {
	return &Client{
		cache:    make(map[string]*mcClients),
		log:      log,
		dialOpts: opts,
	}
}

// clients returns (or lazily creates) the Firestore client pair for the given MC.
func (c *Client) clients(ctx context.Context, mcName string) (*mcClients, error) {
	c.mu.RLock()
	if mc, ok := c.cache[mcName]; ok {
		c.mu.RUnlock()
		return mc, nil
	}
	c.mu.RUnlock()

	// Construct clients outside the lock to avoid blocking other goroutines
	// during gRPC dialing. MCs are identified by their GCP project ID.
	specsClient, err := firestore.NewClientWithDatabase(ctx, mcName, specsDBName, c.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("firestore transport: create specs client for MC %q: %w", mcName, err)
	}

	statusClient, err := firestore.NewClientWithDatabase(ctx, mcName, statusDBName, c.dialOpts...)
	if err != nil {
		specsClient.Close()
		return nil, fmt.Errorf("firestore transport: create status client for MC %q: %w", mcName, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Re-check after acquiring write lock — another goroutine may have populated
	// the cache while we were dialing. Close the duplicate pair if so.
	if mc, ok := c.cache[mcName]; ok {
		specsClient.Close()
		statusClient.Close()
		return mc, nil
	}

	mc := &mcClients{specs: specsClient, status: statusClient}
	c.cache[mcName] = mc
	return mc, nil
}

// Apply decomposes the manifests into individual resources and writes one
// ApplyDesire + one ReadDesire document per resource to the specs DB.
// Returns the current status by calling GetStatus after writing.
func (c *Client) Apply(ctx context.Context, targetCluster, groupKey string, manifests [][]byte) (*transport.Status, error) {
	mc, err := c.clients(ctx, targetCluster)
	if err != nil {
		return nil, err
	}

	batch := mc.specs.BulkWriter(ctx)

	var jobs []*firestore.BulkWriterJob

	for _, raw := range manifests {
		if len(raw) == 0 {
			continue
		}

		ref, unknownKind, err := parseManifest(raw)
		if err != nil {
			return nil, fmt.Errorf("firestore transport: Apply %s/%s: %w", targetCluster, groupKey, err)
		}
		if unknownKind {
			c.log.Infof(ctx, "firestore transport: Apply %s/%s: unknown Kind for resource %s/%s — using fallback pluralization, add it to kindToResource if incorrect", targetCluster, groupKey, ref.Namespace, ref.Name)
		}

		// Write ApplyDesire
		applyID, applyData, err := buildApplyDesireDoc(groupKey, targetCluster, ref, raw)
		if err != nil {
			return nil, fmt.Errorf("firestore transport: Apply %s/%s build apply desire: %w", targetCluster, groupKey, err)
		}
		applyRef := mc.specs.Collection(collectionApplyDesires).Doc(applyID)
		job, err := batch.Set(applyRef, applyData)
		if err != nil {
			return nil, fmt.Errorf("firestore transport: Apply %s/%s set apply desire: %w", targetCluster, groupKey, err)
		}
		jobs = append(jobs, job)

		// Write ReadDesire
		readID, readData := buildReadDesireDoc(groupKey, targetCluster, ref)
		readRef := mc.specs.Collection(collectionReadDesires).Doc(readID)
		job, err = batch.Set(readRef, readData)
		if err != nil {
			return nil, fmt.Errorf("firestore transport: Apply %s/%s set read desire: %w", targetCluster, groupKey, err)
		}
		jobs = append(jobs, job)
	}

	batch.Flush()

	for _, job := range jobs {
		if _, err := job.Results(); err != nil {
			return nil, fmt.Errorf("firestore transport: Apply %s/%s write error: %w", targetCluster, groupKey, err)
		}
	}

	c.log.Infof(ctx, "firestore transport: applied %d manifests for %s/%s", len(manifests), targetCluster, groupKey)

	return c.GetStatus(ctx, targetCluster, groupKey)
}

// GetStatus compares expected Desire documents with the ApplyDesire and
// ReadDesire statuses currently available for the given groupKey.
func (c *Client) GetStatus(ctx context.Context, targetCluster, groupKey string) (*transport.Status, error) {
	mc, err := c.clients(ctx, targetCluster)
	if err != nil {
		return nil, err
	}

	specsApplySnaps, err := mc.specs.Collection(collectionApplyDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetStatus %s/%s query specs apply desires: %w", targetCluster, groupKey, err)
	}

	statusApplySnaps, err := mc.status.Collection(collectionApplyDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetStatus %s/%s query status apply desires: %w", targetCluster, groupKey, err)
	}

	specsReadSnaps, err := mc.specs.Collection(collectionReadDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetStatus %s/%s query specs read desires: %w", targetCluster, groupKey, err)
	}

	statusReadSnaps, err := mc.status.Collection(collectionReadDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetStatus %s/%s query status read desires: %w", targetCluster, groupKey, err)
	}

	stale := false
	statusApplyByID := make(map[string]*firestore.DocumentSnapshot, len(statusApplySnaps))
	for _, snap := range statusApplySnaps {
		statusApplyByID[snap.Ref.ID] = snap
	}
	applyDesires := make([]kubeapplier.ApplyDesire, 0, len(specsApplySnaps))
	for _, specsSnap := range specsApplySnaps {
		var specsAD kubeapplier.ApplyDesire
		if err := specsSnap.DataTo(&specsAD); err != nil {
			return nil, fmt.Errorf("firestore transport: GetStatus %s/%s decode specs apply desire %s: %w", targetCluster, groupKey, specsSnap.Ref.ID, err)
		}
		snap, ok := statusApplyByID[specsSnap.Ref.ID]
		if !ok {
			applyDesires = append(applyDesires, kubeapplier.ApplyDesire{Spec: specsAD.Spec})
			stale = true
			continue
		}
		var ad kubeapplier.ApplyDesire
		if err := snap.DataTo(&ad); err != nil {
			return nil, fmt.Errorf("firestore transport: GetStatus %s/%s decode apply desire %s: %w", targetCluster, groupKey, snap.Ref.ID, err)
		}
		if ad.Status.ObservedDesireUpdateTime.Before(specsSnap.UpdateTime) {
			stale = true
		}
		applyDesires = append(applyDesires, ad)
	}

	statusReadByID := make(map[string]*firestore.DocumentSnapshot, len(statusReadSnaps))
	for _, snap := range statusReadSnaps {
		statusReadByID[snap.Ref.ID] = snap
	}
	readDesires := make([]kubeapplier.ReadDesire, 0, len(specsReadSnaps))
	for _, specsSnap := range specsReadSnaps {
		var specsRD kubeapplier.ReadDesire
		if err := specsSnap.DataTo(&specsRD); err != nil {
			return nil, fmt.Errorf("firestore transport: GetStatus %s/%s decode specs read desire %s: %w", targetCluster, groupKey, specsSnap.Ref.ID, err)
		}
		snap, ok := statusReadByID[specsSnap.Ref.ID]
		if !ok {
			readDesires = append(readDesires, kubeapplier.ReadDesire{Spec: specsRD.Spec})
			stale = true
			continue
		}
		var rd kubeapplier.ReadDesire
		if err := snap.DataTo(&rd); err != nil {
			return nil, fmt.Errorf("firestore transport: GetStatus %s/%s decode read desire %s: %w", targetCluster, groupKey, snap.Ref.ID, err)
		}
		if rd.Status.ObservedDesireUpdateTime.Before(specsSnap.UpdateTime) {
			stale = true
		}
		// Manually decode status_kubeContent (stored as map[string]any at doc root).
		if v, ok := snap.Data()["status_kubeContent"]; ok && v != nil {
			raw, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("firestore transport: GetStatus marshal status_kubeContent: %w", err)
			}
			rd.Status.KubeContent = &k8sruntime.RawExtension{Raw: raw}
		}
		readDesires = append(readDesires, rd)
	}

	resourceStatuses, err := extractResourceStatuses(readDesires)
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetStatus %s/%s: %w", targetCluster, groupKey, err)
	}

	return &transport.Status{
		Conditions:       aggregateConditions(applyDesires),
		ResourceStatuses: resourceStatuses,
		Stale:            stale,
	}, nil
}

// Delete writes one DeleteDesire document per resource and removes the
// corresponding ApplyDesire and ReadDesire documents from the specs DB.
// Resources are processed in batches to stay within Firestore's
// 500-write-per-transaction limit.
func (c *Client) Delete(ctx context.Context, targetCluster, groupKey string) error {
	mc, err := c.clients(ctx, targetCluster)
	if err != nil {
		return err
	}

	// Query all ApplyDesires for this groupKey.
	applySnaps, err := mc.specs.Collection(collectionApplyDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("firestore transport: Delete %s/%s query apply desires: %w", targetCluster, groupKey, err)
	}

	if len(applySnaps) == 0 {
		c.log.Infof(ctx, "firestore transport: Delete %s/%s: no apply desires found, nothing to delete", targetCluster, groupKey)
		return nil
	}

	// Process in chunks to stay within Firestore's 500-write-per-transaction
	// limit. Each resource requires 3 writes (Set DeleteDesire, Delete
	// ReadDesire, Delete ApplyDesire).
	var errs []error
	for i := 0; i < len(applySnaps); i += maxDeleteBatchSize {
		end := i + maxDeleteBatchSize
		if end > len(applySnaps) {
			end = len(applySnaps)
		}
		chunk := applySnaps[i:end]

		err := mc.specs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			for _, snap := range chunk {
				var ad kubeapplier.ApplyDesire
				if err := snap.DataTo(&ad); err != nil {
					return fmt.Errorf("decode apply desire: %w", err)
				}

				ref := ad.Spec.TargetItem

				// Write DeleteDesire.
				deleteID, deleteData := buildDeleteDesireDoc(groupKey, targetCluster, ref)
				deleteRef := mc.specs.Collection(collectionDeleteDesires).Doc(deleteID)
				if err := tx.Set(deleteRef, deleteData); err != nil {
					return fmt.Errorf("set delete desire: %w", err)
				}

				// Delete the ReadDesire doc (same document ID as the ApplyDesire).
				readRef := mc.specs.Collection(collectionReadDesires).Doc(snap.Ref.ID)
				if err := tx.Delete(readRef); err != nil {
					return fmt.Errorf("delete read desire: %w", err)
				}

				// Delete the ApplyDesire doc.
				if err := tx.Delete(snap.Ref); err != nil {
					return fmt.Errorf("delete apply desire: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("chunk %d-%d: %w", i, end-1, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("firestore transport: Delete %s/%s: %d chunk(s) failed: %w", targetCluster, groupKey, len(errs), errors.Join(errs...))
	}

	c.log.Infof(ctx, "firestore transport: deleted %d resources for %s/%s", len(applySnaps), targetCluster, groupKey)
	return nil
}

// GetDeleteStatus queries all DeleteDesire documents for the given groupKey
// and checks if all have Successful=True condition.
func (c *Client) GetDeleteStatus(ctx context.Context, targetCluster, groupKey string) (*transport.DeleteStatus, error) {
	mc, err := c.clients(ctx, targetCluster)
	if err != nil {
		return nil, err
	}

	// Query all DeleteDesires from specs DB.
	specsSnaps, err := mc.specs.Collection(collectionDeleteDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetDeleteStatus %s/%s query specs: %w", targetCluster, groupKey, err)
	}

	// Also query ApplyDesires to distinguish "never started" from "completed".
	applySnaps, err := mc.specs.Collection(collectionApplyDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetDeleteStatus %s/%s query apply desires: %w", targetCluster, groupKey, err)
	}

	if len(specsSnaps) == 0 {
		// No DeleteDesires found.
		// If ApplyDesires exist, deletion never started (controller must call Delete).
		// If no ApplyDesires, deletion completed.
		return &transport.DeleteStatus{
			AllSuccessful:     len(applySnaps) == 0,
			PendingCount:      0,
			TotalCount:        0,
			ApplyDesiresCount: len(applySnaps),
		}, nil
	}

	statusSnaps, err := mc.status.Collection(collectionDeleteDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("firestore transport: GetDeleteStatus %s/%s query status: %w", targetCluster, groupKey, err)
	}

	specUpdateTimes := make(map[string]time.Time, len(specsSnaps))
	for _, snap := range specsSnaps {
		specUpdateTimes[snap.Ref.ID] = snap.UpdateTime
	}

	successful := make(map[string]bool, len(statusSnaps))
	for _, snap := range statusSnaps {
		var dd kubeapplier.DeleteDesire
		if err := snap.DataTo(&dd); err != nil {
			return nil, fmt.Errorf("firestore transport: GetDeleteStatus %s/%s decode: %w", targetCluster, groupKey, err)
		}
		for _, cond := range dd.Status.Conditions {
			if cond.Type == kubeapplier.ConditionTypeSuccessful && cond.Status == "True" {
				if dd.Status.ObservedDesireUpdateTime.Equal(specUpdateTimes[snap.Ref.ID]) {
					successful[snap.Ref.ID] = true
				}
				break
			}
		}
	}

	pending := 0
	for _, snap := range specsSnaps {
		if !successful[snap.Ref.ID] {
			pending++
		}
	}

	return &transport.DeleteStatus{
		AllSuccessful:     pending == 0,
		PendingCount:      pending,
		TotalCount:        len(specsSnaps),
		ApplyDesiresCount: len(applySnaps),
	}, nil
}

// CleanupDeleteDesires removes all DeleteDesire spec documents for the given
// groupKey.
//
// Gecko has read-only access to the status database, so it must not delete
// status documents here. kube-applier-gcp removes the corresponding status
// documents after it observes that their spec documents are gone.
func (c *Client) CleanupDeleteDesires(ctx context.Context, targetCluster, groupKey string) error {
	mc, err := c.clients(ctx, targetCluster)
	if err != nil {
		return err
	}

	specsSnaps, err := mc.specs.Collection(collectionDeleteDesires).
		Where("spec.groupKey", "==", groupKey).
		Documents(ctx).GetAll()
	if err != nil {
		return fmt.Errorf("firestore transport: CleanupDeleteDesires %s/%s query specs: %w", targetCluster, groupKey, err)
	}

	if len(specsSnaps) == 0 {
		c.log.Infof(ctx, "firestore transport: CleanupDeleteDesires %s/%s: no delete desires found", targetCluster, groupKey)
		return nil
	}

	// Delete in batches (Firestore batch write limit = 500).
	specsBatch := mc.specs.BulkWriter(ctx)
	var specsJobs []*firestore.BulkWriterJob

	for _, snap := range specsSnaps {
		job, err := specsBatch.Delete(snap.Ref)
		if err != nil {
			return fmt.Errorf("firestore transport: CleanupDeleteDesires %s/%s delete specs: %w", targetCluster, groupKey, err)
		}
		specsJobs = append(specsJobs, job)
	}

	specsBatch.Flush()

	// Check job results.
	for _, job := range specsJobs {
		if _, err := job.Results(); err != nil {
			return fmt.Errorf("firestore transport: CleanupDeleteDesires %s/%s specs write error: %w", targetCluster, groupKey, err)
		}
	}

	c.log.Infof(ctx, "firestore transport: cleaned up %d delete desire specs for %s/%s", len(specsSnaps), targetCluster, groupKey)
	return nil
}

// Close closes all cached Firestore clients. Call on shutdown.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, mc := range c.cache {
		mc.specs.Close()
		mc.status.Close()
	}
	c.cache = make(map[string]*mcClients)
}
