package handlers

import (
	"bytes"
	"encoding/json"

	"k8s.io/apimachinery/pkg/runtime"
)

func (h *ConvertingResourceHandler) convertPublicInputToPrivate(publicObj, existing runtime.Object, public, oldPublic map[string]interface{}) (runtime.Object, error) {
	privateObj, err := h.converter.PublicToPrivate(publicObj, existing)
	if err != nil {
		return nil, err
	}
	return h.applyPublicSpecClears(privateObj, existing, public, oldPublic)
}

// applyPublicSpecClears restores explicit nulls lost when an omitempty public
// pointer is marshaled during conversion. public is the schema-processed input.
// oldPublic is nil for PUT; PATCH supplies the stored public representation from
// before the patch, so removed public fields can be identified. Only public spec
// fields contribute deletions; private fields and controller status are preserved.
// Deleting a public object retains any private descendants from existing, even if
// conversion already replaced that object with nil. PUT omissions retain their
// existing preservation semantics.
func (h *ConvertingResourceHandler) applyPublicSpecClears(obj, existing runtime.Object, public, oldPublic map[string]interface{}) (runtime.Object, error) {
	spec, _ := public["spec"].(map[string]interface{})
	oldSpec, _ := oldPublic["spec"].(map[string]interface{})
	clears := publicFieldClears(spec, oldSpec)
	if len(clears) == 0 {
		return obj, nil
	}
	// Compare the stored private object with its public projection to identify
	// private descendants. The projection must come from existing, not the new
	// public input, which may have removed their parent object altogether.
	existingPublic, err := h.converter.PrivateToPublic(existing)
	if err != nil {
		return nil, err
	}
	privateMap, err := specClearObjectMap(existing)
	if err != nil {
		return nil, err
	}
	publicMap, err := specClearObjectMap(existingPublic)
	if err != nil {
		return nil, err
	}
	objectMap, err := specClearObjectMap(obj)
	if err != nil {
		return nil, err
	}
	privateSpec, _ := privateMap["spec"].(map[string]interface{})
	publicSpec, _ := publicMap["spec"].(map[string]interface{})
	objectSpec, _ := objectMap["spec"].(map[string]interface{})
	if objectSpec == nil {
		objectSpec = make(map[string]interface{})
		objectMap["spec"] = objectSpec
	}
	applyPublicFieldClears(objectSpec, clears, privateSpec, publicSpec)
	merged, err := json.Marshal(objectMap)
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

func specClearObjectMap(obj runtime.Object) (map[string]interface{}, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber() // Retained private descendants must keep exact integers.
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// applyPublicFieldClears narrows whole-object deletions to public members
// wherever the stored object also contains private state.
func applyPublicFieldClears(object, clears, private, public map[string]interface{}) {
	for key, value := range clears {
		if value == nil {
			if retained := privateObjectFields(private[key], public[key]); len(retained) > 0 {
				object[key] = retained
			} else {
				delete(object, key)
			}
			continue
		}
		if child, ok := value.(map[string]interface{}); ok {
			objectChild, _ := object[key].(map[string]interface{})
			privateChild, _ := private[key].(map[string]interface{})
			publicChild, _ := public[key].(map[string]interface{})
			if objectChild == nil {
				objectChild = make(map[string]interface{})
				object[key] = objectChild
			}
			applyPublicFieldClears(objectChild, child, privateChild, publicChild)
		}
	}
}

// privateObjectFields keeps only members absent from the stored object's public
// projection, including private descendants of otherwise public child objects.
func privateObjectFields(private, public interface{}) map[string]interface{} {
	privateObject, privateOK := private.(map[string]interface{})
	publicObject, publicOK := public.(map[string]interface{})
	if !privateOK || !publicOK {
		return nil
	}
	retained := make(map[string]interface{})
	for key, value := range privateObject {
		publicValue, found := publicObject[key]
		if !found {
			retained[key] = value
			continue
		}
		if child := privateObjectFields(value, publicValue); len(child) > 0 {
			retained[key] = child
		}
	}
	return retained
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
