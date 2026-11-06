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
// pointer is marshaled during conversion. For merge patches, oldPublic also
// identifies fields removed by the patch. Both maps have passed public schema
// processing, so this cannot clear private-only fields or controller status.
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
