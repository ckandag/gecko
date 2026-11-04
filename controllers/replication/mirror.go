package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const (
	replicatedFromAnnotation   = "replication.gcp.managed.openshift.io/replicated-from"
	sourceUIDAnnotation        = "replication.gcp.managed.openshift.io/source-uid"
	sourceVersionAnnotation    = "replication.gcp.managed.openshift.io/source-version"
	sourceGenerationAnnotation = "replication.gcp.managed.openshift.io/source-generation"
	maxInvalidCaptures         = 4096
	maxSeenKeys                = 100000
	maxCachedNamespaces        = 1024
	invalidCaptureRetention    = 8 * 24 * time.Hour
)

var namespaceResource = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

type captureKey struct {
	kind, syncID string
}

type snapshotCapture struct {
	boundary  string
	pageCount uint32
	pages     map[uint32][]ObjectRef
	payloads  map[string]Event
	bytes     int
	started   time.Time
}

type invalidCapture struct {
	requestPending bool
	createdAt      time.Time
}

// APIMirror is a first-stage follower implementation using the existing local
// Kubernetes API. It deliberately does not claim durable delete tombstones or
// atomic source-version tracking; the chart remains disabled by default.
type APIMirror struct {
	api              dynamic.Interface
	config           Config
	generationString string
	mu               sync.Mutex
	seen             map[string]*big.Int
	completed        map[string]*big.Int
	lastSync         map[string]string
	captures         map[captureKey]*snapshotCapture
	invalid          map[captureKey]invalidCapture
	namespaces       map[string]struct{}
}

func NewAPIMirror(api dynamic.Interface, cfg Config) (*APIMirror, error) {
	if api == nil || cfg.Region == "" || cfg.LeaderRegion == "" || cfg.Generation == 0 {
		return nil, fmt.Errorf("API client, region, leader region, and positive generation are required")
	}
	if cfg.MaxCaptureBytes <= 0 {
		cfg.MaxCaptureBytes = defaultMaxCaptureSize
	}
	return &APIMirror{
		api:              api,
		config:           cfg,
		generationString: strconv.FormatUint(cfg.Generation, 10),
		seen:             make(map[string]*big.Int),
		completed:        make(map[string]*big.Int),
		lastSync:         make(map[string]string),
		captures:         make(map[captureKey]*snapshotCapture),
		invalid:          make(map[captureKey]invalidCapture),
		namespaces:       make(map[string]struct{}),
	}, nil
}

func mirrorKey(kind, namespace, name string) string {
	return kind + "\x00" + namespace + "\x00" + name
}

func objectKey(namespace, name string) string {
	return namespace + "\x00" + name
}

func (m *APIMirror) ApplyIncremental(ctx context.Context, event Event) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	version, valid := decimalVersion(event.SourceResourceVersion)
	if !valid {
		return false, fmt.Errorf("invalid source resource version %q", event.SourceResourceVersion)
	}
	// Gecko currently emits a DELETE with the deleted object's last version,
	// rather than a new deletion version. A delete may therefore arrive after a
	// completed snapshot even when its version is at or below that boundary.
	if event.EventType != Delete && m.completed[event.ResourceKind] != nil && version.Cmp(m.completed[event.ResourceKind]) <= 0 {
		return false, nil
	}
	return m.apply(ctx, event, false)
}

func (m *APIMirror) apply(ctx context.Context, event Event, repair bool) (bool, error) {
	version, _ := decimalVersion(event.SourceResourceVersion)
	key := mirrorKey(event.ResourceKind, event.Namespace, event.Name)
	if !repair && m.seen[key] != nil {
		comparison := version.Cmp(m.seen[key])
		if comparison < 0 || comparison == 0 && event.EventType != Delete {
			return false, nil
		}
	}
	resource := supportedResources[event.ResourceKind]
	client := m.api.Resource(resource).Namespace(event.Namespace)
	existing, err := client.Get(ctx, event.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("get local mirror: %w", err)
	}
	if apierrors.IsNotFound(err) {
		existing = nil
	}
	if existing != nil && existing.GetAnnotations()[sourceGenerationAnnotation] == m.generationString {
		if current := existing.GetAnnotations()[sourceVersionAnnotation]; current != "" {
			currentVersion, valid := decimalVersion(current)
			if valid && (currentVersion.Cmp(version) > 0 || !repair && event.EventType != Delete && currentVersion.Cmp(version) == 0) {
				return false, nil
			}
		}
	}
	if event.EventType == Delete {
		return m.applyDelete(ctx, client, event, existing, key, version)
	}
	return m.applyUpsert(ctx, client, event, existing, key)
}

func (m *APIMirror) applyUpsert(ctx context.Context, client dynamic.ResourceInterface, event Event, existing *unstructured.Unstructured, key string) (bool, error) {
	var source unstructured.Unstructured
	if err := source.UnmarshalJSON(event.Object); err != nil {
		return false, fmt.Errorf("decode source object: %w", err)
	}
	if err := m.ensureNamespace(ctx, event.Namespace); err != nil {
		return false, err
	}
	desired := source.DeepCopy()
	cleanSourceMetadata(desired)
	annotations := desired.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[replicatedFromAnnotation] = m.config.LeaderRegion
	annotations[sourceUIDAnnotation] = string(source.GetUID())
	annotations[sourceVersionAnnotation] = event.SourceResourceVersion
	annotations[sourceGenerationAnnotation] = m.generationString
	desired.SetAnnotations(annotations)
	if existing != nil && (existing.GetAnnotations()[sourceGenerationAnnotation] != m.generationString || existing.GetAnnotations()[sourceUIDAnnotation] != string(source.GetUID())) {
		if err := deleteAndConfirm(ctx, client, event.Name); err != nil {
			return false, fmt.Errorf("remove replaced local incarnation: %w", err)
		}
		existing = nil
	}
	if existing == nil {
		if _, err := client.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				delete(m.namespaces, event.Namespace)
			}
			return false, fmt.Errorf("create local mirror: %w", err)
		}
	} else {
		local := existing.DeepCopy()
		cleanSourceMetadata(local)
		if apiequality.Semantic.DeepEqual(local.Object, desired.Object) {
			delete(m.seen, key)
			return false, nil
		}
		desired.SetUID(existing.GetUID())
		desired.SetResourceVersion(existing.GetResourceVersion())
		desired.SetCreationTimestamp(existing.GetCreationTimestamp())
		desired.SetFinalizers(existing.GetFinalizers())
		if _, err := client.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				delete(m.namespaces, event.Namespace)
			}
			return false, fmt.Errorf("update local mirror: %w", err)
		}
	}
	// The existing object carries its source version in an annotation. Retain
	// in-memory versions only for absent objects until a full inventory covers
	// their deletion.
	delete(m.seen, key)
	return true, nil
}

func (m *APIMirror) applyDelete(ctx context.Context, client dynamic.ResourceInterface, event Event, existing *unstructured.Unstructured, key string, version *big.Int) (bool, error) {
	if existing == nil {
		if completed := m.completed[event.ResourceKind]; completed != nil && version.Cmp(completed) <= 0 {
			return false, nil
		}
		if seen := m.seen[key]; seen != nil && version.Cmp(seen) == 0 {
			return false, nil
		}
		if err := m.checkSeenCapacity(key); err != nil {
			return false, err
		}
		m.seen[key] = version
		return true, nil
	}
	if err := m.checkSeenCapacity(key); err != nil {
		return false, err
	}
	// The current private API does not enforce DeleteOptions preconditions.
	// One receiver worker and a Recreate deployment strategy avoid normal
	// overlap, but durable stale-delete protection remains follow-up work.
	if err := deleteAndConfirm(ctx, client, event.Name); err != nil {
		return false, fmt.Errorf("delete local mirror: %w", err)
	}
	m.seen[key] = version
	return true, nil
}

func (m *APIMirror) checkSeenCapacity(key string) error {
	if _, exists := m.seen[key]; !exists && len(m.seen) >= maxSeenKeys {
		return fmt.Errorf("mirror has %d unconfirmed deleted keys; a complete inventory is needed before more can be tracked", maxSeenKeys)
	}
	return nil
}

func deleteAndConfirm(ctx context.Context, client dynamic.ResourceInterface, name string) error {
	if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	_, err := client.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("deletion is pending")
}

func cleanSourceMetadata(object *unstructured.Unstructured) {
	for _, field := range []string{
		"uid", "resourceVersion", "generation", "creationTimestamp", "deletionTimestamp",
		"deletionGracePeriodSeconds", "finalizers", "ownerReferences", "managedFields", "selfLink",
	} {
		unstructured.RemoveNestedField(object.Object, "metadata", field)
	}
	unstructured.RemoveNestedField(object.Object, "status")
}

func (m *APIMirror) ensureNamespace(ctx context.Context, name string) error {
	if _, exists := m.namespaces[name]; exists {
		return nil
	}
	namespaces := m.api.Resource(namespaceResource)
	_, err := namespaces.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		m.rememberNamespace(name)
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get target namespace: %w", err)
	}
	_, err = namespaces.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": name},
	}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create target namespace: %w", err)
	}
	m.rememberNamespace(name)
	return nil
}

func (m *APIMirror) rememberNamespace(name string) {
	if len(m.namespaces) >= maxCachedNamespaces {
		m.namespaces = make(map[string]struct{})
	}
	m.namespaces[name] = struct{}{}
}

// StageSnapshot keeps a bounded capture until every inventory page and payload
// is available. It never prunes from an incomplete or conflicting capture.
func (m *APIMirror) StageSnapshot(ctx context.Context, event Event) (StageResult, error) {
	boundary, valid := decimalVersion(event.SnapshotResourceVersion)
	if !valid {
		return StageResult{}, fmt.Errorf("invalid snapshot resource version %q", event.SnapshotResourceVersion)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireInvalid(time.Now())
	if completed := m.completed[event.ResourceKind]; completed != nil && boundary.Cmp(completed) < 0 {
		return StageResult{Skipped: 1}, nil
	}
	if m.lastSync[event.ResourceKind] == event.SyncID {
		return StageResult{Skipped: 1}, nil
	}
	key := captureKey{kind: event.ResourceKind, syncID: event.SyncID}
	if rejected, exists := m.invalid[key]; exists {
		if rejected.requestPending {
			return StageResult{RequestResync: true}, nil
		}
		return StageResult{Skipped: 1}, nil
	}
	capture := m.captures[key]
	if capture == nil {
		// A subscription can deliver interleaved captures. Keep at most two
		// incomplete captures per kind so old partial work cannot accumulate.
		var oldestKey captureKey
		var oldestTime time.Time
		count := 0
		for candidateKey, candidate := range m.captures {
			if candidateKey.kind != key.kind {
				continue
			}
			if time.Since(candidate.started) > 2*m.config.ResyncInterval {
				if err := m.invalidateCapture(candidateKey, false); err != nil {
					return StageResult{}, err
				}
				continue
			}
			count++
			if oldestTime.IsZero() || candidate.started.Before(oldestTime) {
				oldestTime, oldestKey = candidate.started, candidateKey
			}
		}
		if count >= 2 {
			if err := m.invalidateCapture(oldestKey, false); err != nil {
				return StageResult{}, err
			}
		}
		capture = &snapshotCapture{boundary: event.SnapshotResourceVersion, pages: make(map[uint32][]ObjectRef), payloads: make(map[string]Event), started: time.Now()}
		m.captures[key] = capture
	}
	if capture.boundary != event.SnapshotResourceVersion || time.Since(capture.started) > 2*m.config.ResyncInterval {
		return m.abandon(key)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return StageResult{}, err
	}
	if event.EventType == Inventory {
		if capture.pageCount != 0 && capture.pageCount != event.PageCount {
			return m.abandon(key)
		}
		capture.pageCount = event.PageCount
		if previous, exists := capture.pages[event.PageIndex]; exists {
			if !sameJSON(previous, event.Inventory) {
				return m.abandon(key)
			}
		} else {
			capture.pages[event.PageIndex] = event.Inventory
			capture.bytes += len(encoded)
		}
	} else {
		objectID := objectKey(event.Namespace, event.Name)
		if previous, exists := capture.payloads[objectID]; exists {
			if !sameJSON(previous, event) {
				return m.abandon(key)
			}
		} else {
			capture.payloads[objectID] = event
			capture.bytes += len(encoded)
		}
	}
	if capture.bytes > m.config.MaxCaptureBytes {
		return m.abandon(key)
	}
	return m.tryComplete(ctx, key, capture)
}

func (m *APIMirror) abandon(key captureKey) (StageResult, error) {
	if err := m.invalidateCapture(key, true); err != nil {
		return StageResult{}, err
	}
	return StageResult{Abandoned: true, RequestResync: true}, nil
}

func (m *APIMirror) invalidateCapture(key captureKey, requestResync bool) error {
	if len(m.invalid) >= maxInvalidCaptures {
		return fmt.Errorf("too many rejected snapshot captures (%d); refusing to buffer another", maxInvalidCaptures)
	}
	delete(m.captures, key)
	m.invalid[key] = invalidCapture{requestPending: requestResync, createdAt: time.Now()}
	return nil
}

// ConfirmResyncRequested clears the pending request only after the control
// topic accepted it. A Nacked delivery can retry if publication failed.
func (m *APIMirror) ConfirmResyncRequested(kind, syncID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := captureKey{kind: kind, syncID: syncID}
	if rejected, exists := m.invalid[key]; exists {
		rejected.requestPending = false
		m.invalid[key] = rejected
	}
}

// The chart retains unacknowledged data messages for seven days. A capture
// without a pending resync request can be forgotten after that delivery window
// without reaccepting one of its old messages.
func (m *APIMirror) expireInvalid(now time.Time) {
	for key, rejected := range m.invalid {
		if !rejected.requestPending && now.Sub(rejected.createdAt) > invalidCaptureRetention {
			delete(m.invalid, key)
		}
	}
}

func sameJSON(previous, current any) bool {
	previousJSON, previousErr := json.Marshal(previous)
	currentJSON, currentErr := json.Marshal(current)
	return previousErr == nil && currentErr == nil && bytes.Equal(previousJSON, currentJSON)
}

func (m *APIMirror) tryComplete(ctx context.Context, key captureKey, capture *snapshotCapture) (StageResult, error) {
	if capture.pageCount == 0 || uint32(len(capture.pages)) != capture.pageCount {
		return StageResult{}, nil
	}
	expected := make(map[string]ObjectRef)
	for page := uint32(0); page < capture.pageCount; page++ {
		for _, ref := range capture.pages[page] {
			objectID := objectKey(ref.Namespace, ref.Name)
			if _, exists := expected[objectID]; exists {
				return m.abandon(key)
			}
			expected[objectID] = ref
		}
	}
	if len(capture.payloads) < len(expected) {
		return StageResult{}, nil
	}
	if len(capture.payloads) != len(expected) {
		return m.abandon(key)
	}
	for objectID, ref := range expected {
		payload, exists := capture.payloads[objectID]
		if !exists || payload.SourceResourceVersion != ref.SourceResourceVersion {
			return m.abandon(key)
		}
		var object unstructured.Unstructured
		if err := object.UnmarshalJSON(payload.Object); err != nil || string(object.GetUID()) != ref.SourceUID {
			return m.abandon(key)
		}
	}
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := StageResult{}
	boundary, _ := decimalVersion(capture.boundary)
	for _, objectID := range keys {
		payload := capture.payloads[objectID]
		key := mirrorKey(payload.ResourceKind, payload.Namespace, payload.Name)
		if seen := m.seen[key]; seen != nil && seen.Cmp(boundary) > 0 {
			result.Skipped++
			continue
		}
		applied, err := m.apply(ctx, payload, true)
		if err != nil {
			return StageResult{}, err
		}
		if applied {
			result.Applied++
		} else {
			result.Skipped++
		}
	}
	pruned, err := m.prune(ctx, key.kind, expected, boundary)
	if err != nil {
		return StageResult{}, err
	}
	result.Pruned = pruned
	result.Completed = true
	m.completed[key.kind] = boundary
	m.lastSync[key.kind] = key.syncID
	// The completed inventory supersedes tombstones at or below its boundary;
	// older CREATE_OR_UPDATE deliveries are rejected by ApplyIncremental.
	for seenKey, seenVersion := range m.seen {
		if strings.HasPrefix(seenKey, key.kind+"\x00") && seenVersion.Cmp(boundary) <= 0 {
			delete(m.seen, seenKey)
		}
	}
	delete(m.captures, key)
	for oldKey, oldCapture := range m.captures {
		if oldKey.kind == key.kind {
			oldBoundary, _ := decimalVersion(oldCapture.boundary)
			if oldBoundary.Cmp(boundary) < 0 {
				delete(m.captures, oldKey)
			}
		}
	}
	return result, nil
}

func (m *APIMirror) prune(ctx context.Context, kind string, expected map[string]ObjectRef, boundary *big.Int) (int, error) {
	client := m.api.Resource(supportedResources[kind]).Namespace(metav1.NamespaceAll)
	type localKey struct{ namespace, name string }
	var candidates []localKey
	var continuation, listVersion string
	for {
		list, err := client.List(ctx, metav1.ListOptions{Limit: defaultPageSize, Continue: continuation})
		if err != nil {
			return 0, fmt.Errorf("list local mirrors for pruning: %w", err)
		}
		if continuation != "" && list.GetResourceVersion() != listVersion {
			return 0, fmt.Errorf("local mirror LIST changed resource version between pages")
		}
		listVersion = list.GetResourceVersion()
		for _, object := range list.Items {
			if _, exists := expected[objectKey(object.GetNamespace(), object.GetName())]; exists {
				continue
			}
			// A complete inventory is authoritative for this replicated type,
			// including objects written by a former leader before promotion.
			key := mirrorKey(kind, object.GetNamespace(), object.GetName())
			if seen := m.seen[key]; seen != nil && seen.Cmp(boundary) > 0 {
				continue
			}
			if current := object.GetAnnotations()[sourceVersionAnnotation]; current != "" && object.GetAnnotations()[sourceGenerationAnnotation] == m.generationString {
				if currentVersion, valid := decimalVersion(current); valid && currentVersion.Cmp(boundary) > 0 {
					continue
				}
			}
			if len(candidates) >= maxSeenKeys {
				return 0, fmt.Errorf("local mirror has more than %d prune candidates; refusing to buffer more", maxSeenKeys)
			}
			candidates = append(candidates, localKey{namespace: object.GetNamespace(), name: object.GetName()})
		}
		continuation = list.GetContinue()
		if continuation == "" {
			break
		}
	}
	// Do not mutate the local API until every page has been read. Spanner
	// increments its collection version on delete, which would otherwise make
	// our own prune invalidate the next page's resource version.
	newTombstones := 0
	for _, candidate := range candidates {
		if _, exists := m.seen[mirrorKey(kind, candidate.namespace, candidate.name)]; !exists {
			newTombstones++
		}
	}
	if len(m.seen)+newTombstones > maxSeenKeys {
		return 0, fmt.Errorf("mirror cannot track %d deleted keys during pruning (limit %d)", len(m.seen)+newTombstones, maxSeenKeys)
	}
	count := 0
	for _, candidate := range candidates {
		if err := deleteAndConfirm(ctx, m.api.Resource(supportedResources[kind]).Namespace(candidate.namespace), candidate.name); err != nil {
			return count, fmt.Errorf("prune local mirror: %w", err)
		}
		m.seen[mirrorKey(kind, candidate.namespace, candidate.name)] = boundary
		count++
	}
	return count, nil
}
