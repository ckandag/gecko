package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/go-logr/logr"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type ControlSink interface {
	Publish(context.Context, []byte) error
}

// StageResult describes snapshot progress. A staged delivery is not a
// completed inventory; only the mirror can declare a capture complete.
type StageResult struct {
	Applied       int
	Skipped       int
	Pruned        int
	Completed     bool
	Abandoned     bool
	RequestResync bool
}

// Mirror applies source changes through the local API and assembles snapshots.
// Durable delete tombstones and transactional source-version tracking are a
// later safety improvement; the chart stays disabled until those are ready.
type Mirror interface {
	ApplyIncremental(context.Context, Event) (bool, error)
	StageSnapshot(context.Context, Event) (StageResult, error)
	ConfirmResyncRequested(string, string)
}

type Receiver struct {
	config    Config
	source    SubscriptionSource
	control   ControlSink
	store     Mirror
	metrics   *Metrics
	log       logr.Logger
	resources map[string]Resource
}

func NewReceiver(cfg Config, source SubscriptionSource, control ControlSink, store Mirror, metrics *Metrics, log logr.Logger) (*Receiver, error) {
	if cfg.Region == "" || cfg.LeaderRegion == "" || cfg.Generation == 0 || len(cfg.Resources) == 0 {
		return nil, fmt.Errorf("region, leader region, positive generation, and replicated resources are required")
	}
	if source == nil || metrics == nil {
		return nil, fmt.Errorf("data source and metrics are required")
	}
	if cfg.Region != cfg.LeaderRegion && (control == nil || store == nil) {
		return nil, fmt.Errorf("follower requires control sink and mirror store")
	}
	resources := make(map[string]Resource, len(cfg.Resources))
	for _, resource := range cfg.Resources {
		if _, ok := supportedResources[resource.ID]; !ok {
			return nil, fmt.Errorf("unsupported replicated resource %q", resource.ID)
		}
		resources[resource.ID] = resource
	}
	return &Receiver{config: cfg, source: source, control: control, store: store, metrics: metrics, log: log, resources: resources}, nil
}

func (r *Receiver) Run(ctx context.Context) error {
	receiveCtx, cancel := context.WithCancel(ctx)
	var bootstrapDone <-chan struct{}
	if r.config.Region != r.config.LeaderRegion {
		done := make(chan struct{})
		bootstrapDone = done
		go func() {
			defer close(done)
			if err := r.requestResync(receiveCtx, ""); err != nil && receiveCtx.Err() == nil {
				r.log.Error(err, "bootstrap resync request failed; data consumption continues")
			}
		}()
	}
	defer func() {
		cancel()
		if bootstrapDone != nil {
			<-bootstrapDone
		}
	}()
	for receiveCtx.Err() == nil {
		err := r.source.Receive(receiveCtx, r.Handle)
		if receiveCtx.Err() != nil {
			//nolint:nilerr // Context cancellation is a clean shutdown, even if Receive returned an error.
			return nil
		}
		if err != nil && r.config.Region != r.config.LeaderRegion && !retryablePubSubError(err) {
			return fmt.Errorf("data subscription stopped permanently: %w", err)
		}
		if err != nil {
			r.log.Error(err, "data subscription stopped; retrying", "region", r.config.Region)
		} else {
			r.log.Info("data subscription stopped; retrying")
		}
		if err := waitRetry(receiveCtx, retryDelay); err != nil {
			//nolint:nilerr // Context cancellation during backoff is a clean shutdown.
			return nil
		}
	}
	return nil
}

// Handle returns nil only when a delivery may be acknowledged. Invalid and
// stale deliveries are deliberately acknowledged; storage failures are retried.
func (r *Receiver) Handle(ctx context.Context, data []byte) error {
	if r.config.Region == r.config.LeaderRegion {
		r.metrics.EventsSkipped.WithLabelValues("leader", "").Inc()
		return nil
	}
	if len(data) > MaxEventBytes {
		r.metrics.EventsDropped.WithLabelValues("oversized", "").Inc()
		return nil
	}
	var event Event
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		r.metrics.EventsDropped.WithLabelValues("malformed", "").Inc()
		//nolint:nilerr // Malformed messages are counted and acknowledged to avoid endless redelivery.
		return nil
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		r.metrics.EventsDropped.WithLabelValues("malformed", "").Inc()
		return nil
	}
	metricKind := event.ResourceKind
	if _, ok := r.resources[metricKind]; !ok {
		metricKind = "unknown"
	}
	metricType := event.EventType
	if metricType != CreateOrUpdate && metricType != Delete && metricType != Inventory {
		metricType = "unknown"
	}
	if event.OriginRegion != r.config.LeaderRegion {
		r.metrics.EventsRejected.WithLabelValues(r.config.Region, metricType, metricKind, "wrong_origin", "other").Inc()
		return nil
	}
	if event.Generation != r.config.Generation {
		r.metrics.EventsRejected.WithLabelValues(r.config.Region, metricType, metricKind, "wrong_generation", "leader").Inc()
		return nil
	}
	resource, ok := r.resources[event.ResourceKind]
	if !ok || validateDataEvent(event, resource) != nil {
		r.metrics.EventsDropped.WithLabelValues("invalid", metricKind).Inc()
		//nolint:nilerr // Invalid messages are counted and acknowledged to avoid endless redelivery.
		return nil
	}
	if event.SyncID == "" {
		applied, err := r.store.ApplyIncremental(ctx, event)
		if err != nil {
			r.metrics.ApplyErrors.WithLabelValues(resource.ID).Inc()
			r.log.Error(err, "apply incremental event failed; delivery will be retried", "resourceKind", resource.ID, "eventType", event.EventType)
			return err
		}
		if applied {
			r.metrics.EventsApplied.WithLabelValues(event.EventType, resource.ID).Inc()
		} else {
			r.metrics.EventsSkipped.WithLabelValues("stale", resource.ID).Inc()
		}
		return nil
	}
	result, err := r.store.StageSnapshot(ctx, event)
	if err != nil {
		r.metrics.ApplyErrors.WithLabelValues(resource.ID).Inc()
		r.log.Error(err, "apply snapshot event failed; delivery will be retried", "resourceKind", resource.ID)
		return err
	}
	if result.Applied > 0 {
		r.metrics.EventsApplied.WithLabelValues("snapshot", resource.ID).Add(float64(result.Applied))
	}
	if result.Skipped > 0 {
		r.metrics.EventsSkipped.WithLabelValues("snapshot", resource.ID).Add(float64(result.Skipped))
	}
	if result.Pruned > 0 {
		r.metrics.ObjectsPruned.WithLabelValues(resource.ID).Add(float64(result.Pruned))
	}
	if result.Completed {
		r.metrics.InventorySyncs.WithLabelValues(resource.ID, "completed").Inc()
	}
	if result.Abandoned {
		r.metrics.InventorySyncs.WithLabelValues(resource.ID, "abandoned").Inc()
	}
	if result.RequestResync {
		if err := r.requestResync(ctx, resource.ID); err != nil {
			return err
		}
		r.store.ConfirmResyncRequested(resource.ID, event.SyncID)
	}
	return nil
}

func (r *Receiver) requestResync(ctx context.Context, resourceKind string) error {
	data, err := json.Marshal(Event{
		EventType: ResyncRequest, ResourceKind: resourceKind,
		OriginRegion: r.config.Region, Generation: r.config.Generation,
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	delay := retryDelay
	for ctx.Err() == nil {
		err := r.control.Publish(ctx, data)
		if err == nil {
			return nil
		}
		if !retryablePubSubError(err) {
			return fmt.Errorf("publish resync request: %w", err)
		}
		r.log.Error(err, "publish resync request failed; retrying", "resourceKind", resourceKind)
		if err := waitRetry(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
	return ctx.Err()
}

func validateDataEvent(event Event, resource Resource) error {
	if event.UpdatedAt.IsZero() || event.Name == "" && event.EventType != Inventory {
		return fmt.Errorf("missing event identity or timestamp")
	}
	if event.EventType == Inventory {
		return validateInventoryEvent(event)
	}
	return validateMutationEvent(event, resource)
}

func validateInventoryEvent(event Event) error {
	if event.SyncID == "" || event.SnapshotResourceVersion == "" || event.SourceResourceVersion != "" || len(event.Object) != 0 || event.PageCount == 0 || event.PageIndex >= event.PageCount {
		return fmt.Errorf("invalid inventory envelope")
	}
	boundary, ok := decimalVersion(event.SnapshotResourceVersion)
	if !ok {
		return fmt.Errorf("invalid inventory boundary")
	}
	seen := make(map[string]struct{}, len(event.Inventory))
	for _, ref := range event.Inventory {
		version, valid := decimalVersion(ref.SourceResourceVersion)
		if ref.Namespace == "" || ref.Name == "" || ref.SourceUID == "" || !valid || version.Sign() == 0 || version.Cmp(boundary) > 0 {
			return fmt.Errorf("invalid inventory reference")
		}
		key := ref.Namespace + "/" + ref.Name
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate inventory key")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateMutationEvent(event Event, resource Resource) error {
	if event.EventType != CreateOrUpdate && event.EventType != Delete {
		return fmt.Errorf("unsupported data event type")
	}
	version, ok := decimalVersion(event.SourceResourceVersion)
	if !ok || version.Sign() == 0 || event.Namespace == "" || event.Name == "" || len(event.Inventory) != 0 || event.PageCount != 0 || event.PageIndex != 0 {
		return fmt.Errorf("invalid mutation envelope")
	}
	if event.EventType == Delete {
		if len(event.Object) != 0 || event.SyncID != "" || event.SnapshotResourceVersion != "" {
			return fmt.Errorf("invalid delete payload")
		}
		return nil
	}
	if len(event.Object) == 0 || event.SyncID == "" && event.SnapshotResourceVersion != "" || event.SyncID != "" && event.SnapshotResourceVersion == "" {
		return fmt.Errorf("invalid upsert payload")
	}
	if event.SyncID != "" {
		boundary, valid := decimalVersion(event.SnapshotResourceVersion)
		if !valid || version.Cmp(boundary) > 0 {
			return fmt.Errorf("invalid snapshot boundary")
		}
	}
	var object unstructured.Unstructured
	if err := object.UnmarshalJSON(event.Object); err != nil {
		return fmt.Errorf("invalid object JSON")
	}
	if object.GetAPIVersion() != resource.GVR.Group+"/"+resource.GVR.Version || object.GetNamespace() != event.Namespace || object.GetName() != event.Name || object.GetResourceVersion() != event.SourceResourceVersion || object.GetUID() == "" {
		return fmt.Errorf("object identity does not match envelope")
	}
	if object.GetKind() != kindForResource(resource.ID) {
		return fmt.Errorf("object kind does not match resource")
	}
	return nil
}

func decimalVersion(value string) (*big.Int, bool) {
	if value == "" {
		return nil, false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return nil, false
		}
	}
	version, ok := new(big.Int).SetString(value, 10)
	return version, ok
}

func kindForResource(id string) string {
	switch id {
	case "roles.gcp.managed.openshift.io":
		return "Role"
	case "rolebindings.gcp.managed.openshift.io":
		return "RoleBinding"
	default:
		return ""
	}
}
