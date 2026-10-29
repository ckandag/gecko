package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

const (
	defaultPageSize = 64
	// Encoded bytes buffered in memory during one full snapshot capture. Keep
	// this well below the container memory limit (see the chart's
	// resources.limits.memory): the controller holds the encoded snapshot plus
	// Go runtime overhead while capturing.
	defaultMaxCaptureSize = 32 << 20
	retryDelay            = time.Second
	maxRetryDelay         = 30 * time.Second
	minRequestedResyncGap = 30 * time.Second
)

// DataSink publishes an immutable, already encoded event to the data topic.
type DataSink interface {
	Publish(context.Context, []byte) error
}

// ControlSource receives messages from the leader's control subscription.
// The callback must return after the request is queued for a resync.
type ControlSource interface {
	Receive(context.Context, func([]byte)) error
}

type Config struct {
	Region          string
	LeaderRegion    string
	Generation      uint64
	Resources       []Resource
	ResyncInterval  time.Duration
	MaxCaptureBytes int
	ListPageSize    int64
	// MinResyncGap rate-limits requested resyncs per resource type. Defaults to
	// minRequestedResyncGap when unset.
	MinResyncGap time.Duration
}

type Publisher struct {
	config  Config
	source  dynamic.Interface
	sink    DataSink
	control ControlSource
	metrics *Metrics
	log     logr.Logger

	requests map[string]chan struct{}
	locks    map[string]*sync.Mutex
}

func NewPublisher(cfg Config, source dynamic.Interface, sink DataSink, control ControlSource, metrics *Metrics, log logr.Logger) (*Publisher, error) {
	if cfg.Region == "" || cfg.LeaderRegion == "" || cfg.Generation == 0 || len(cfg.Resources) == 0 {
		return nil, fmt.Errorf("region, leader region, positive generation, and replicated resources are required")
	}
	if cfg.ResyncInterval <= 0 {
		return nil, fmt.Errorf("resync interval must be positive")
	}
	if source == nil || sink == nil || metrics == nil {
		return nil, fmt.Errorf("source, data sink, and metrics are required")
	}
	if cfg.Region == cfg.LeaderRegion && control == nil {
		return nil, fmt.Errorf("leader control subscription is required")
	}
	if cfg.MaxCaptureBytes == 0 {
		cfg.MaxCaptureBytes = defaultMaxCaptureSize
	}
	if cfg.MaxCaptureBytes < MaxEventBytes {
		return nil, fmt.Errorf("capture budget must be at least %d bytes", MaxEventBytes)
	}
	if cfg.ListPageSize == 0 {
		cfg.ListPageSize = defaultPageSize
	}
	if cfg.ListPageSize < 0 {
		return nil, fmt.Errorf("list page size cannot be negative")
	}
	if cfg.MinResyncGap <= 0 {
		cfg.MinResyncGap = minRequestedResyncGap
	}
	p := &Publisher{config: cfg, source: source, sink: sink, control: control, metrics: metrics, log: log, requests: make(map[string]chan struct{}), locks: make(map[string]*sync.Mutex)}
	for _, resource := range cfg.Resources {
		p.requests[resource.ID] = make(chan struct{}, 1)
		p.locks[resource.ID] = &sync.Mutex{}
	}
	return p, nil
}

func (p *Publisher) Run(ctx context.Context) error {
	if p.config.Region != p.config.LeaderRegion {
		p.log.Info("replication publisher disabled in follower region", "region", p.config.Region, "leaderRegion", p.config.LeaderRegion)
		<-ctx.Done()
		return nil
	}

	parentCtx := ctx
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return p.consumeControl(ctx) })
	for _, resource := range p.config.Resources {
		resource := resource
		group.Go(func() error { return p.runResource(ctx, resource) })
	}
	err := group.Wait()
	if parentCtx.Err() != nil {
		return nil
	}
	return err
}

func (p *Publisher) consumeControl(ctx context.Context) error {
	for ctx.Err() == nil {
		err := p.control.Receive(ctx, func(data []byte) {
			var event Event
			if err := json.Unmarshal(data, &event); err != nil {
				p.log.Error(err, "ignoring malformed control message")
				return
			}
			if event.EventType != ResyncRequest {
				p.log.Info("ignoring non-resync control message", "eventType", event.EventType)
				return
			}
			if event.Generation != p.config.Generation {
				p.log.Info("ignoring resync request from a different generation", "generation", event.Generation)
				return
			}
			if event.ResourceKind == "" {
				for _, ch := range p.requests {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
				return
			}
			ch, ok := p.requests[event.ResourceKind]
			if !ok {
				p.log.Info("ignoring resync request for unconfigured resource", "resourceKind", event.ResourceKind)
				return
			}
			select {
			case ch <- struct{}{}:
			default:
			}
		})
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !retryablePubSubError(err) {
			return fmt.Errorf("receive leader control subscription: %w", err)
		}
		if err == nil {
			p.log.Info("control subscription stopped; retrying")
		} else {
			p.log.Error(err, "control subscription stopped; retrying")
		}
		if err := waitRetry(ctx, retryDelay); err != nil {
			return nil
		}
	}
	return nil
}

// runResource keeps one resource replicating. A permanent per-resource failure
// (an oversized object, a capture over budget, or a permanent source API error
// such as Forbidden or NotFound) must not stop the other resources or crash the
// process, so it is surfaced and retried on the next resync interval rather than
// returned to the shared errgroup.
func (p *Publisher) runResource(ctx context.Context, resource Resource) error {
	for ctx.Err() == nil {
		err := p.serveResource(ctx, resource)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			continue
		}
		p.metrics.ResourceFailures.WithLabelValues(p.config.Region, resource.ID).Inc()
		p.log.Error(err, "replication for resource failed; retrying on resync interval", "resourceKind", resource.ID)
		if waitRetry(ctx, p.config.ResyncInterval) != nil {
			return nil
		}
	}
	return nil
}

// serveResource captures a snapshot, then runs the WATCH and periodic-snapshot
// loops until one fails or the context is cancelled.
func (p *Publisher) serveResource(ctx context.Context, resource Resource) error {
	lock := p.locks[resource.ID]
	lock.Lock()
	resourceVersion, err := p.retrySnapshot(ctx, resource)
	lock.Unlock()
	if err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return p.periodicSnapshots(ctx, resource, lock) })
	group.Go(func() error {
		for ctx.Err() == nil {
			var relist bool
			resourceVersion, relist, err = p.watchFrom(ctx, resource, resourceVersion)
			if err != nil {
				return err
			}
			if relist {
				lock.Lock()
				resourceVersion, err = p.retrySnapshot(ctx, resource)
				lock.Unlock()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return group.Wait()
}

func (p *Publisher) periodicSnapshots(ctx context.Context, resource Resource, lock *sync.Mutex) error {
	ticker := time.NewTicker(p.config.ResyncInterval)
	defer ticker.Stop()
	var pending *time.Timer
	var pendingCh <-chan time.Time
	var lastRequested time.Time
	defer func() {
		if pending != nil {
			pending.Stop()
		}
	}()
	for {
		requested := false
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// A scheduled full capture also satisfies a queued request.
			if pending != nil {
				pending.Stop()
				pending = nil
				pendingCh = nil
			}
		case <-p.requests[resource.ID]:
			if !lastRequested.IsZero() && time.Since(lastRequested) < p.config.MinResyncGap {
				if pending == nil {
					pending = time.NewTimer(p.config.MinResyncGap - time.Since(lastRequested))
					pendingCh = pending.C
				}
				continue
			}
			if pending != nil {
				pending.Stop()
				pending = nil
				pendingCh = nil
			}
			requested = true
		case <-pendingCh:
			pending = nil
			pendingCh = nil
			requested = true
		}
		lock.Lock()
		_, err := p.retrySnapshot(ctx, resource)
		lock.Unlock()
		if err != nil {
			return err
		}
		if requested {
			lastRequested = time.Now()
		}
	}
}

type capturedObject struct {
	data []byte
	ref  ObjectRef
}

func (p *Publisher) retrySnapshot(ctx context.Context, resource Resource) (string, error) {
	delay := retryDelay
	for ctx.Err() == nil {
		resourceVersion, err := p.snapshot(ctx, resource)
		if err == nil {
			return resourceVersion, nil
		}
		var oversized *oversizedEventError
		var budget *captureBudgetError
		if errors.As(err, &oversized) || errors.As(err, &budget) || permanentSourceError(err) {
			return "", err
		}
		p.log.Error(err, "snapshot failed; retrying", "resourceKind", resource.ID)
		if err := waitRetry(ctx, delay); err != nil {
			break
		}
		delay = min(delay*2, maxRetryDelay)
	}
	return "", ctx.Err()
}

func permanentSourceError(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsNotFound(err) ||
		apierrors.IsBadRequest(err) || apierrors.IsInvalid(err) || apierrors.IsMethodNotSupported(err)
}

func (p *Publisher) snapshot(ctx context.Context, resource Resource) (string, error) {
	started := time.Now()
	defer func() { p.metrics.ResyncDuration.WithLabelValues(resource.ID).Observe(time.Since(started).Seconds()) }()
	objects, boundary, err := p.capture(ctx, resource)
	if err != nil {
		return "", err
	}
	syncID := uuid.NewString()
	stamp := time.Now().UTC()
	refs := make([]ObjectRef, 0, len(objects))
	objectEvent := func(item capturedObject) Event {
		return Event{EventType: CreateOrUpdate, ResourceKind: resource.ID, OriginRegion: p.config.Region, Generation: p.config.Generation, SourceResourceVersion: item.ref.SourceResourceVersion, SnapshotResourceVersion: boundary, Namespace: item.ref.Namespace, Name: item.ref.Name, UpdatedAt: stamp, SyncID: syncID, Object: item.data}
	}
	// Check every object before publishing any part of the capture. Count the
	// encoded bytes the follower buffers, including inventory pages, rather
	// than relying on the raw-object estimate used while collecting the LIST.
	// Re-encoding each immutable event avoids a second full snapshot buffer.
	captureBytes := 0
	for _, item := range objects {
		data, err := p.encode(objectEvent(item))
		if err != nil {
			return "", err
		}
		captureBytes += len(data)
		refs = append(refs, item.ref)
	}
	pages, err := p.inventoryPages(resource, boundary, syncID, stamp, refs)
	if err != nil {
		return "", err
	}
	for _, page := range pages {
		captureBytes += len(page)
	}
	if captureBytes > p.config.MaxCaptureBytes {
		return "", &captureBudgetError{resource: resource.ID, budget: p.config.MaxCaptureBytes}
	}
	for _, item := range objects {
		data, err := p.encode(objectEvent(item))
		if err != nil {
			return "", err
		}
		if err := p.publishData(ctx, resource.ID, CreateOrUpdate, data); err != nil {
			return "", err
		}
	}
	for _, page := range pages {
		if err := p.publishData(ctx, resource.ID, Inventory, page); err != nil {
			return "", err
		}
	}
	p.log.Info("published replication snapshot", "resourceKind", resource.ID, "syncID", syncID, "snapshotResourceVersion", boundary, "objects", len(objects), "inventoryPages", len(pages))
	return boundary, nil
}

func (p *Publisher) capture(ctx context.Context, resource Resource) ([]capturedObject, string, error) {
	client := p.source.Resource(resource.GVR).Namespace(metav1.NamespaceAll)
	var objects []capturedObject
	var boundary, continuation string
	used := 0
	for {
		list, err := client.List(ctx, metav1.ListOptions{Limit: p.config.ListPageSize, Continue: continuation})
		if err != nil {
			// A stale continue token invalidates the whole capture, never just one page.
			return nil, "", err
		}
		if err := validCollectionVersion(list.GetResourceVersion()); err != nil {
			return nil, "", fmt.Errorf("LIST %s: %w", resource.ID, err)
		}
		if boundary == "" {
			boundary = list.GetResourceVersion()
		} else if boundary != list.GetResourceVersion() {
			return nil, "", fmt.Errorf("LIST %s changed snapshot version across pages: %s to %s", resource.ID, boundary, list.GetResourceVersion())
		}
		for i := range list.Items {
			item := list.Items[i]
			ref, err := reference(&item, boundary)
			if err != nil {
				return nil, "", fmt.Errorf("LIST %s: %w", resource.ID, err)
			}
			data, err := json.Marshal(item.Object)
			if err != nil {
				return nil, "", err
			}
			used += len(data) + len(ref.Name) + len(ref.Namespace) + len(ref.SourceUID) + len(ref.SourceResourceVersion) + 128
			if used > p.config.MaxCaptureBytes {
				return nil, "", &captureBudgetError{resource: resource.ID, budget: p.config.MaxCaptureBytes}
			}
			objects = append(objects, capturedObject{data: data, ref: ref})
		}
		continuation = list.GetContinue()
		if continuation == "" {
			return objects, boundary, nil
		}
	}
}

func reference(object *unstructured.Unstructured, boundary string) (ObjectRef, error) {
	if object.GetName() == "" || object.GetUID() == "" {
		return ObjectRef{}, fmt.Errorf("source object is missing name or UID")
	}
	version := object.GetResourceVersion()
	if err := validVersion(version); err != nil {
		return ObjectRef{}, fmt.Errorf("source object %s/%s: %w", object.GetNamespace(), object.GetName(), err)
	}
	objectNumber, _ := new(big.Int).SetString(version, 10)
	boundaryNumber, ok := new(big.Int).SetString(boundary, 10)
	if !ok || boundaryNumber.Sign() < 0 || objectNumber.Cmp(boundaryNumber) > 0 {
		return ObjectRef{}, fmt.Errorf("source object %s/%s version %s exceeds invalid snapshot boundary %s", object.GetNamespace(), object.GetName(), version, boundary)
	}
	return ObjectRef{Namespace: object.GetNamespace(), Name: object.GetName(), SourceResourceVersion: version, SourceUID: string(object.GetUID())}, nil
}

func validVersion(version string) error {
	n, ok := new(big.Int).SetString(version, 10)
	if !ok || n.Sign() <= 0 || strings.IndexFunc(version, func(r rune) bool { return r < '0' || r > '9' }) != -1 {
		return fmt.Errorf("invalid source resource version %q", version)
	}
	return nil
}

func validCollectionVersion(version string) error {
	n, ok := new(big.Int).SetString(version, 10)
	if !ok || n.Sign() < 0 || strings.IndexFunc(version, func(r rune) bool { return r < '0' || r > '9' }) != -1 {
		return fmt.Errorf("invalid collection resource version %q", version)
	}
	return nil
}

func (p *Publisher) inventoryPages(resource Resource, boundary, syncID string, stamp time.Time, refs []ObjectRef) ([][]byte, error) {
	base := Event{EventType: Inventory, ResourceKind: resource.ID, OriginRegion: p.config.Region, Generation: p.config.Generation, SnapshotResourceVersion: boundary, UpdatedAt: stamp, SyncID: syncID, PageIndex: ^uint32(0), PageCount: ^uint32(0)}
	// Measure the envelope once at its largest page-number width. Each ref's
	// encoded size plus a comma gives an exact running page size in linear time.
	base.Inventory = []ObjectRef{{}}
	probe, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	emptyRef, err := json.Marshal(ObjectRef{})
	if err != nil {
		return nil, err
	}
	pageOverhead := len(probe) - len(emptyRef)
	pageSize := pageOverhead
	pages := [][]ObjectRef{{}}
	for _, ref := range refs {
		current := pages[len(pages)-1]
		refJSON, err := json.Marshal(ref)
		if err != nil {
			return nil, err
		}
		additional := len(refJSON)
		if len(current) != 0 {
			additional++ // Comma between inventory entries.
		}
		if pageSize+additional > MaxEventBytes {
			if len(current) != 0 {
				pages = append(pages, nil)
				current = nil
				pageSize = pageOverhead
				additional = len(refJSON)
			}
			if pageSize+additional > MaxEventBytes {
				p.metrics.OversizedEvents.WithLabelValues(resource.ID).Inc()
				return nil, &oversizedEventError{size: pageSize + additional}
			}
		}
		pages[len(pages)-1] = append(current, ref)
		pageSize += additional
	}
	if uint64(len(pages)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("too many inventory pages for %s", resource.ID)
	}
	encoded := make([][]byte, 0, len(pages))
	for i, refs := range pages {
		base.Inventory = refs
		base.PageIndex = uint32(i)
		base.PageCount = uint32(len(pages))
		data, err := p.encode(base)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, data)
	}
	return encoded, nil
}

func (p *Publisher) watchFrom(ctx context.Context, resource Resource, resourceVersion string) (string, bool, error) {
	stream, err := p.source.Resource(resource.GVR).Namespace(metav1.NamespaceAll).Watch(ctx, metav1.ListOptions{ResourceVersion: resourceVersion, AllowWatchBookmarks: true})
	if apierrors.IsGone(err) || apierrors.IsResourceExpired(err) {
		return resourceVersion, true, nil
	}
	if err != nil {
		if permanentSourceError(err) {
			return resourceVersion, false, err
		}
		p.log.Error(err, "WATCH failed; retrying from last handled version", "resourceKind", resource.ID, "resourceVersion", resourceVersion)
		if err := waitRetry(ctx, retryDelay); err != nil {
			return resourceVersion, false, nil
		}
		return resourceVersion, false, nil
	}
	defer stream.Stop()
	for {
		select {
		case <-ctx.Done():
			return resourceVersion, false, nil
		case event, ok := <-stream.ResultChan():
			if !ok {
				if err := waitRetry(ctx, retryDelay); err != nil {
					return resourceVersion, false, nil
				}
				return resourceVersion, false, nil
			}
			if event.Type == watch.Error {
				if s, ok := event.Object.(*metav1.Status); ok {
					p.log.Info("WATCH reported an error; relisting", "resourceKind", resource.ID, "reason", s.Reason, "message", s.Message)
					if s.Reason == metav1.StatusReasonGone || s.Reason == metav1.StatusReasonExpired || s.Code == 410 {
						return resourceVersion, true, nil
					}
				}
				p.log.Info("WATCH reported an uncertain gap; relisting", "resourceKind", resource.ID)
				return resourceVersion, true, nil // Unknown gaps require a fresh authoritative LIST.
			}
			object, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				p.log.Info("WATCH returned an unexpected object; relisting", "resourceKind", resource.ID)
				return resourceVersion, true, nil
			}
			version := object.GetResourceVersion()
			if event.Type == watch.Bookmark {
				if err := validCollectionVersion(version); err != nil {
					p.log.Error(err, "invalid WATCH bookmark; relisting", "resourceKind", resource.ID)
					return resourceVersion, true, nil
				}
				if versionAfter(version, resourceVersion) {
					resourceVersion = version
				}
				continue
			}
			if err := validVersion(version); err != nil {
				p.log.Error(err, "invalid WATCH mutation version; relisting", "resourceKind", resource.ID)
				return resourceVersion, true, nil
			}
			var eventType string
			switch event.Type {
			case watch.Added, watch.Modified:
				eventType = CreateOrUpdate
			case watch.Deleted:
				eventType = Delete
			default:
				p.log.Info("WATCH returned an unexpected event type; relisting", "resourceKind", resource.ID, "eventType", event.Type)
				return resourceVersion, true, nil
			}
			if object.GetName() == "" {
				p.log.Info("WATCH mutation has no name; relisting", "resourceKind", resource.ID)
				return resourceVersion, true, nil
			}
			out := Event{EventType: eventType, ResourceKind: resource.ID, OriginRegion: p.config.Region, Generation: p.config.Generation, SourceResourceVersion: version, Namespace: object.GetNamespace(), Name: object.GetName(), UpdatedAt: time.Now().UTC()}
			if eventType == CreateOrUpdate {
				out.Object, err = json.Marshal(object.Object)
				if err != nil {
					return resourceVersion, false, err
				}
			}
			data, err := p.encode(out)
			if err != nil {
				return resourceVersion, false, err
			}
			if err := p.publishData(ctx, resource.ID, eventType, data); err != nil {
				return resourceVersion, false, err
			}
			if versionAfter(version, resourceVersion) {
				resourceVersion = version // Advance only after the immutable captured payload is handled.
			}
		}
	}
}

func versionAfter(candidate, current string) bool {
	a, okA := new(big.Int).SetString(candidate, 10)
	b, okB := new(big.Int).SetString(current, 10)
	return okA && okB && a.Cmp(b) > 0
}

type oversizedEventError struct{ size int }

func (e *oversizedEventError) Error() string {
	return fmt.Sprintf("encoded replication event is %d bytes (limit %d)", e.size, MaxEventBytes)
}

type captureBudgetError struct {
	resource string
	budget   int
}

func (e *captureBudgetError) Error() string {
	return fmt.Sprintf("LIST %s exceeds capture budget of %d bytes", e.resource, e.budget)
}

func (p *Publisher) encode(event Event) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxEventBytes {
		p.metrics.OversizedEvents.WithLabelValues(event.ResourceKind).Inc()
		return nil, &oversizedEventError{size: len(data)}
	}
	return data, nil
}

// publishData rejects data publication outside the configured leader. It keeps
// the exact encoded payload across retries, including its API-assigned version.
func (p *Publisher) publishData(ctx context.Context, resourceKind, eventType string, data []byte) error {
	if p.config.Region != p.config.LeaderRegion {
		p.metrics.EventsRejected.WithLabelValues(p.config.Region, eventType, resourceKind, "follower_publish", p.config.Region).Inc()
		return fmt.Errorf("region %q is not the configured leader %q", p.config.Region, p.config.LeaderRegion)
	}
	delay := retryDelay
	for ctx.Err() == nil {
		start := time.Now()
		err := p.sink.Publish(ctx, data)
		p.metrics.PublishDuration.WithLabelValues(eventType).Observe(time.Since(start).Seconds())
		if err == nil {
			p.metrics.EventsPublished.WithLabelValues(p.config.Region, eventType, resourceKind).Inc()
			return nil
		}
		p.metrics.PublishErrors.WithLabelValues(p.config.Region, resourceKind).Inc()
		if !retryablePubSubError(err) {
			return fmt.Errorf("publish %s %s: %w", eventType, resourceKind, err)
		}
		p.log.Error(err, "publish failed; retrying captured event", "eventType", eventType, "resourceKind", resourceKind)
		if err := waitRetry(ctx, delay); err != nil {
			break
		}
		delay = min(delay*2, maxRetryDelay)
	}
	return ctx.Err()
}

func retryablePubSubError(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented:
		return false
	default:
		return true
	}
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
