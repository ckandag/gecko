package conversion

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// isNonNil returns true if obj is a non-nil interface holding a non-nil value.
// Handles the typed nil interface pitfall (e.g. (*Unstructured)(nil) passed as
// runtime.Object evaluates to != nil but dereferences panic).
func isNonNil(obj runtime.Object) bool {
	if obj == nil {
		return false
	}
	v := reflect.ValueOf(obj)
	// reflect.IsNil panics on non-nilable kinds (struct, int, etc.).
	// runtime.Object implementations are always pointer or interface,
	// but guard defensively.
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return !v.IsNil()
	default:
		// Non-nilable kind (e.g. struct value receiver) — treat as non-nil.
		return true
	}
}

const DefaultPrivatePrefix = "private.orlop.gcp.managed.openshift.io/"

// publicConditionTypes defines which condition types are visible on the public
// API, keyed by resource Kind (e.g. "Cluster", "NodePool"). During
// private-to-public conversion, only conditions whose type is in this set are
// kept; everything else is stripped. All conditions are private by default.
//
// To make a new condition public, add its type string to the appropriate Kind.
var publicConditionTypes = map[string]sets.Set[string]{
	"Cluster": sets.New[string](
		"HostedClusterAvailable",
		"ControlPlaneUpgradeAvailable",
		"ControlPlaneUpgradeProgressing",
		"ControlPlaneUpgradeDegraded",
	),
	"NodePool": sets.New[string]("NodePoolAvailable", "NodePoolHealthy", "NodePoolProgressing"),
	// Test-only resource type used in orlop tests
	"Object": sets.New[string]("Ready", "Available"),
}

// Converter handles conversion between private and public API types using scheme conversion.
type Converter struct {
	publicScheme  *runtime.Scheme
	privateScheme *runtime.Scheme
	privatePrefix string
}

// NewConverter creates a new converter.
// privatePrefix controls which labels, annotations, and condition types are
// stripped during private-to-public conversion. Pass "" to use DefaultPrivatePrefix.
func NewConverter(publicScheme, privateScheme *runtime.Scheme, privatePrefix string) *Converter {
	if privatePrefix == "" {
		privatePrefix = DefaultPrivatePrefix
	}
	return &Converter{
		publicScheme:  publicScheme,
		privateScheme: privateScheme,
		privatePrefix: privatePrefix,
	}
}

// PrivateToPublic converts a private API object to its public representation.
// Uses JSON round-trip for conversion since both types have the same GVK.
// Filters out private labels, annotations, and conditions.
func (c *Converter) PrivateToPublic(private runtime.Object) (runtime.Object, error) {
	// Get the GVK from the private object
	gvk := private.GetObjectKind().GroupVersionKind()

	// Marshal private object to JSON
	jsonData, err := json.Marshal(private)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private object: %w", err)
	}

	// Create a new public object of the same GVK
	public, err := c.publicScheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create public object for %s: %w", gvk, err)
	}

	// Unmarshal into public object (JSON will only populate fields that exist in public type)
	if err := json.Unmarshal(jsonData, public); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to public object: %w", err)
	}

	// Filter private labels and annotations
	c.filterPrivateMetadata(public)

	// Filter non-public conditions
	if err := c.filterNonPublicConditions(public, gvk.Kind); err != nil {
		return nil, fmt.Errorf("failed to filter non-public conditions: %w", err)
	}

	// Filter finalizers (not exposed on public API)
	c.filterFinalizers(public)

	// Preserve GVK
	public.GetObjectKind().SetGroupVersionKind(gvk)

	return public, nil
}

// filterPrivateMetadata removes labels and annotations with the configured private prefix.
func (c *Converter) filterPrivateMetadata(obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return
	}

	// Filter labels
	labels := accessor.GetLabels()
	if labels != nil {
		filtered := make(map[string]string)
		for k, v := range labels {
			if !strings.HasPrefix(k, c.privatePrefix) {
				filtered[k] = v
			}
		}
		accessor.SetLabels(filtered)
	}

	// Filter annotations
	annotations := accessor.GetAnnotations()
	if annotations != nil {
		filtered := make(map[string]string)
		for k, v := range annotations {
			if !strings.HasPrefix(k, c.privatePrefix) {
				filtered[k] = v
			}
		}
		accessor.SetAnnotations(filtered)
	}
}

// filterNonPublicConditions removes conditions that are not in the public allowlist
// for the given Kind. All conditions are private by default; only explicitly
// allowlisted conditions are kept. If the Kind is unknown (not in the allowlist),
// all conditions are stripped (safe default).
func (c *Converter) filterNonPublicConditions(obj runtime.Object, kind string) error {
	// Get the allowlist for this Kind
	allowed := publicConditionTypes[kind]
	// If Kind not in map, allowed is nil set — .Has() returns false for all conditions

	// Convert to map to access status.conditions
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object for condition filtering: %w", err)
	}

	var objMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &objMap); err != nil {
		return fmt.Errorf("failed to unmarshal object for condition filtering: %w", err)
	}

	// Check if status.conditions exists
	status, ok := objMap["status"].(map[string]interface{})
	if !ok {
		return nil // no status field - nothing to filter
	}

	conditions, ok := status["conditions"].([]interface{})
	if !ok {
		return nil // no conditions field - nothing to filter
	}

	// Filter conditions - handle both string arrays and object arrays
	var filtered []interface{}
	for _, cond := range conditions {
		// Try as string first (simple condition type)
		if condStr, ok := cond.(string); ok {
			// Only include conditions in the allowlist
			if allowed.Has(condStr) {
				filtered = append(filtered, cond)
			}
			continue
		}

		// Try as object with type field (complex condition)
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType, ok := condMap["type"].(string)
		if !ok {
			continue
		}
		// Only include conditions in the allowlist
		if allowed.Has(condType) {
			filtered = append(filtered, cond)
		}
	}

	// Update conditions in the status
	status["conditions"] = filtered

	// Marshal back and unmarshal into the object
	filteredJSON, err := json.Marshal(objMap)
	if err != nil {
		return fmt.Errorf("failed to marshal filtered conditions: %w", err)
	}

	if err := json.Unmarshal(filteredJSON, obj); err != nil {
		return fmt.Errorf("failed to unmarshal filtered conditions into object: %w", err)
	}

	return nil
}

// filterFinalizers removes all finalizers from metadata (finalizers not exposed on public API).
func (c *Converter) filterFinalizers(obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	accessor.SetFinalizers(nil)
}

// stripPrivateFieldsFromPublicInput removes private fields from a public API input object
// before conversion to private. This prevents clients from setting internal fields
// (finalizers, private labels/annotations) through the public API.
//
// Note: This creates new maps for labels/annotations rather than mutating the
// original maps, so the caller's references remain unaffected.
func (c *Converter) stripPrivateFieldsFromPublicInput(obj runtime.Object, kind string) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	// Strip deletionTimestamp — only the Delete handler may set it
	accessor.SetDeletionTimestamp(nil)

	// Strip finalizers — not settable via public API
	accessor.SetFinalizers(nil)

	// Strip private-prefixed labels (new map, no mutation of original)
	if labels := accessor.GetLabels(); len(labels) > 0 {
		filtered := make(map[string]string, len(labels))
		for k, v := range labels {
			if !strings.HasPrefix(k, c.privatePrefix) {
				filtered[k] = v
			}
		}
		accessor.SetLabels(filtered)
	}

	// Strip private-prefixed annotations (new map, no mutation of original)
	if annotations := accessor.GetAnnotations(); len(annotations) > 0 {
		filtered := make(map[string]string, len(annotations))
		for k, v := range annotations {
			if !strings.HasPrefix(k, c.privatePrefix) {
				filtered[k] = v
			}
		}
		accessor.SetAnnotations(filtered)
	}

	// Strip non-public conditions from status — prevents clients from
	// injecting internal condition state via Create/Update/Patch on public API.
	if err := c.filterNonPublicConditions(obj, kind); err != nil {
		return fmt.Errorf("failed to filter non-public conditions from public input: %w", err)
	}

	return nil
}

// reconcileMetadata fixes the additive map merge problem with json.Unmarshal.
// When seeding the private object from existing and then overlaying public input,
// json.Unmarshal merges maps (labels, annotations) additively — keys present in
// existing but absent from public input persist. For a PUT, the public input is
// the source of truth for non-private metadata. This function:
//   - Takes non-private labels/annotations from public (already stripped of private prefix)
//   - Appends private-prefixed labels/annotations from existing
//   - Sets the result on the converted private object
func (c *Converter) reconcileMetadata(public, existing, private runtime.Object) {
	publicAccessor, err := meta.Accessor(public)
	if err != nil {
		return
	}
	existingAccessor, err := meta.Accessor(existing)
	if err != nil {
		return
	}
	privateAccessor, err := meta.Accessor(private)
	if err != nil {
		return
	}

	// Reconcile labels: public non-private + existing private-prefixed
	reconciled := make(map[string]string)
	for k, v := range publicAccessor.GetLabels() {
		reconciled[k] = v
	}
	for k, v := range existingAccessor.GetLabels() {
		if strings.HasPrefix(k, c.privatePrefix) {
			reconciled[k] = v
		}
	}
	if len(reconciled) > 0 {
		privateAccessor.SetLabels(reconciled)
	} else {
		privateAccessor.SetLabels(nil)
	}

	// Reconcile annotations: public non-private + existing private-prefixed
	reconciled = make(map[string]string)
	for k, v := range publicAccessor.GetAnnotations() {
		reconciled[k] = v
	}
	for k, v := range existingAccessor.GetAnnotations() {
		if strings.HasPrefix(k, c.privatePrefix) {
			reconciled[k] = v
		}
	}
	if len(reconciled) > 0 {
		privateAccessor.SetAnnotations(reconciled)
	} else {
		privateAccessor.SetAnnotations(nil)
	}
}

// PublicToPrivate converts a public API object to its private representation.
// Uses JSON round-trip for conversion.
// The existing parameter can be used to preserve internal fields.
//
// json.Unmarshal replaces slices entirely (Go stdlib), so any slice fields
// present in the public JSON will overwrite the existing values seeded from
// the existing object. This affects status.conditions: public input lacks
// private-prefixed conditions, so the unmarshal wipes them. After the overlay
// step we re-inject private conditions from the existing object.
func (c *Converter) PublicToPrivate(public runtime.Object, existing runtime.Object) (runtime.Object, error) {
	// Get the GVK from the public object
	gvk := public.GetObjectKind().GroupVersionKind()

	// Strip private fields from public input before conversion
	if err := c.stripPrivateFieldsFromPublicInput(public, gvk.Kind); err != nil {
		return nil, fmt.Errorf("failed to strip private fields from public input: %w", err)
	}

	// Marshal public object to JSON
	jsonData, err := json.Marshal(public)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public object: %w", err)
	}

	// Create a new private object of the same GVK
	private, err := c.privateScheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create private object for %s: %w", gvk, err)
	}

	// If existing object provided, start with it to preserve internal fields.
	// Use isNonNil to handle typed nil interfaces (e.g. (*Unstructured)(nil)).
	hasExisting := isNonNil(existing)
	if hasExisting {
		// Marshal existing to get all fields
		existingJSON, err := json.Marshal(existing)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal existing object: %w", err)
		}
		// Unmarshal existing into private first — seeds internal fields
		if err := json.Unmarshal(existingJSON, private); err != nil {
			return nil, fmt.Errorf("failed to seed private object from existing: %w", err)
		}
	}

	// Unmarshal public data into private (will overwrite public fields).
	// WARNING: This replaces slice fields (e.g. status.conditions) entirely.
	// For maps (labels, annotations), json.Unmarshal merges additively —
	// existing keys NOT in the public input persist. We reconcile below.
	if err := json.Unmarshal(jsonData, private); err != nil {
		return nil, fmt.Errorf("failed to unmarshal to private object: %w", err)
	}

	// Reconcile labels and annotations: json.Unmarshal merges maps additively,
	// so a PUT that removes a label won't actually remove it. Fix by using
	// public input as the source of truth for non-private labels/annotations,
	// then appending private-prefixed entries from the existing object.
	if hasExisting {
		c.reconcileMetadata(public, existing, private)
	}

	// Re-inject non-public conditions that were destroyed by the slice replacement
	// in the previous step. The public input never contains non-public conditions
	// (stripped by PrivateToPublic on read, and not settable by clients), so the
	// unmarshal above replaces the conditions slice with public-only entries.
	// We restore non-public conditions from the existing object.
	if hasExisting {
		if err := c.preserveNonPublicConditions(existing, private, gvk.Kind); err != nil {
			return nil, fmt.Errorf("failed to preserve non-public conditions: %w", err)
		}
	}

	// Preserve GVK
	private.GetObjectKind().SetGroupVersionKind(gvk)

	return private, nil
}

// preserveNonPublicConditions re-injects non-public conditions from an
// existing object into a converted object. This is needed because
// json.Unmarshal replaces slices entirely, so the public→private overlay
// destroys any non-public conditions that were seeded from the existing object.
//
// Uses the same JSON→map round-trip pattern as filterNonPublicConditions.
func (c *Converter) preserveNonPublicConditions(existing, converted runtime.Object, kind string) error {
	nonPublicConditions := c.extractNonPublicConditions(existing, kind)
	if len(nonPublicConditions) == 0 {
		return nil
	}

	// Get the converted object as a map
	convertedJSON, err := json.Marshal(converted)
	if err != nil {
		return fmt.Errorf("failed to marshal converted object for condition preservation: %w", err)
	}

	var convertedMap map[string]interface{}
	if err := json.Unmarshal(convertedJSON, &convertedMap); err != nil {
		return fmt.Errorf("failed to unmarshal converted object for condition preservation: %w", err)
	}

	// Ensure status map exists
	status, ok := convertedMap["status"].(map[string]interface{})
	if !ok {
		status = make(map[string]interface{})
		convertedMap["status"] = status
	}

	// Get the allowlist for this Kind to filter out any existing non-public conditions
	// from the converted object before appending. This prevents duplicates when public
	// input omits status.conditions (converted still has conditions from seeded existing).
	allowed := publicConditionTypes[kind]
	conditions, _ := status["conditions"].([]interface{})

	// Keep only public conditions from converted
	var publicOnly []interface{}
	for _, cond := range conditions {
		// Try as string
		if condStr, ok := cond.(string); ok {
			if allowed.Has(condStr) {
				publicOnly = append(publicOnly, cond)
			}
			continue
		}
		// Try as object
		if condMap, ok := cond.(map[string]interface{}); ok {
			if condType, ok := condMap["type"].(string); ok && allowed.Has(condType) {
				publicOnly = append(publicOnly, cond)
			}
		}
	}

	// Append non-public conditions from existing
	publicOnly = append(publicOnly, nonPublicConditions...)
	status["conditions"] = publicOnly

	// Marshal back and unmarshal into the converted object
	mergedJSON, err := json.Marshal(convertedMap)
	if err != nil {
		return fmt.Errorf("failed to marshal merged conditions: %w", err)
	}
	if err := json.Unmarshal(mergedJSON, converted); err != nil {
		return fmt.Errorf("failed to unmarshal merged conditions into converted object: %w", err)
	}
	return nil
}

// extractNonPublicConditions returns conditions that are not in the public allowlist
// for the given Kind. Handles both string conditions and object conditions
// with a "type" field.
func (c *Converter) extractNonPublicConditions(obj runtime.Object, kind string) []interface{} {
	// Get the allowlist for this Kind
	allowed := publicConditionTypes[kind]

	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	var objMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &objMap); err != nil {
		return nil
	}

	status, ok := objMap["status"].(map[string]interface{})
	if !ok {
		return nil
	}

	conditions, ok := status["conditions"].([]interface{})
	if !ok {
		return nil
	}

	var nonPublic []interface{}
	for _, cond := range conditions {
		// String condition
		if condStr, ok := cond.(string); ok {
			// Keep conditions NOT in the allowlist
			if !allowed.Has(condStr) {
				nonPublic = append(nonPublic, cond)
			}
			continue
		}
		// Object condition with "type" field
		condMap, ok := cond.(map[string]interface{})
		if !ok {
			continue
		}
		condType, ok := condMap["type"].(string)
		if !ok {
			continue
		}
		// Keep conditions NOT in the allowlist
		if !allowed.Has(condType) {
			nonPublic = append(nonPublic, cond)
		}
	}

	return nonPublic
}

// TypedOldObject converts a stored object into the typed object registered in
// scheme for gvk, returning non-unstructured input unchanged.
//
// Storage backends decode persisted rows into *unstructured.Unstructured, but
// types.CustomValidator implementations are written against their concrete
// type and type-assert the old object they are handed. Without this conversion
// every such validator rejects every update.
func TypedOldObject(scheme *runtime.Scheme, gvk schema.GroupVersionKind, obj runtime.Object) (runtime.Object, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj, nil
	}
	typed, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("creating typed object for %s: %w", gvk, err)
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, typed); err != nil {
		return nil, fmt.Errorf("converting old object to %s: %w", gvk.Kind, err)
	}
	return typed, nil
}
