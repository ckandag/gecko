package handlers

import (
	"encoding/json"

	jsonpatch "github.com/evanphx/json-patch/v5"

	"k8s.io/apimachinery/pkg/runtime"
)

func (h *ConvertingResourceHandler) convertPublicInputToPrivate(publicObj, existing runtime.Object, public, oldPublic map[string]interface{}) (runtime.Object, error) {
	privateObj, err := h.converter.PublicToPrivate(publicObj, existing)
	if err != nil {
		return nil, err
	}
	return h.applyPublicSpecClears(privateObj, public, oldPublic)
}

// applyPublicSpecClears restores explicit nulls lost when an omitempty public
// pointer is marshaled during conversion. public is the schema-processed input.
// oldPublic is nil for PUT; PATCH supplies the stored public representation from
// before the patch, so removed public fields can be identified. Only public spec
// fields contribute deletions; private fields and controller status are preserved.
// PUT omissions retain their existing preservation semantics.
func (h *ConvertingResourceHandler) applyPublicSpecClears(obj runtime.Object, public, oldPublic map[string]interface{}) (runtime.Object, error) {
	spec, _ := public["spec"].(map[string]interface{})
	oldSpec, _ := oldPublic["spec"].(map[string]interface{})
	clears := publicFieldClears(spec, oldSpec)
	if len(clears) == 0 {
		return obj, nil
	}
	patch, err := json.Marshal(map[string]interface{}{"spec": clears})
	if err != nil {
		return nil, err
	}
	original, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	merged, err := jsonpatch.MergePatch(original, patch)
	if err != nil {
		return nil, err
	}
	// Decode into a fresh object: unmarshaling into the existing object would
	// preserve fields absent from the merged JSON, including deleted map keys.
	result, err := h.privateScheme.New(h.gvk)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(merged, result); err != nil {
		return nil, err
	}
	return result, nil
}

// publicFieldClears collects explicit nulls and keys removed from public object
// maps. previous is nil for PUT and contains the pre-patch public spec for PATCH.
// Array values pass through the existing conversion path; this helper does not
// collect deletions inside array elements.
func publicFieldClears(current, previous map[string]interface{}) map[string]interface{} {
	clears := make(map[string]interface{})
	for key, value := range current {
		if value == nil {
			clears[key] = nil
			continue
		}
		if child, ok := value.(map[string]interface{}); ok {
			oldChild, _ := previous[key].(map[string]interface{})
			if nested := publicFieldClears(child, oldChild); len(nested) > 0 {
				clears[key] = nested
			}
		}
	}
	for key := range previous {
		if _, found := current[key]; !found {
			clears[key] = nil
		}
	}
	return clears
}
