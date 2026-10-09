package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/apply"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/constants"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/schema"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceHandler handles CRUD operations for a specific resource type.
type ResourceHandler struct {
	store         storage.ResourceStore
	processor     *schema.Processor
	gvk           runtimeschema.GroupVersionKind
	storageGVK    runtimeschema.GroupVersionKind // zero value = no version conversion
	resourceType  string
	scheme        *runtime.Scheme
	applyManager  *apply.Manager // Optional: for server-side apply support
	parentStore   storage.ResourceStore
	parentIDField string
	logger        logr.Logger
}

// SetParentStore configures creation-time validation of the parent named by idField.
func (h *ResourceHandler) SetParentStore(store storage.ResourceStore, idField string) {
	h.parentStore = store
	h.parentIDField = idField
}

// NewResourceHandler creates a new resource handler.
func NewResourceHandler(
	store storage.ResourceStore,
	processor *schema.Processor,
	gvk runtimeschema.GroupVersionKind,
	resourceType string,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *ResourceHandler {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	return &ResourceHandler{
		store:        store,
		processor:    processor,
		gvk:          gvk,
		resourceType: resourceType,
		scheme:       scheme,
		applyManager: nil, // Will be set by SetApplyManager if SSA is enabled
		logger:       logger,
	}
}

// SetApplyManager sets the apply manager for server-side apply support.
func (h *ResourceHandler) SetApplyManager(applyMgr *apply.Manager) {
	h.applyManager = applyMgr
}

// Create handles POST requests to create a new resource.
func (h *ResourceHandler) Create(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	h.logger.V(1).Info("Create request", "kind", h.gvk.Kind, "namespace", namespace)

	// Parse request body as map for schema processing
	var objMap map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&objMap); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if err := validateParentOnCreate(r.Context(), objMap); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Validate cross-namespace owner references before schema processing
	if err := validateOwnerReferencesFromMap(namespace, objMap); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid ownerReferences: %v", err))
		return
	}

	// Process object (prune, default, validate)
	if errs := h.processor.Process(r.Context(), objMap, nil); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", errs.ToAggregate()))
		return
	}

	parent, err := GetParentOnCreate(r.Context(), h.parentStore, namespace, h.parentIDField, objMap)
	if err != nil {
		status := http.StatusInternalServerError
		if isInvalidParentError(err) {
			status = http.StatusBadRequest
		}
		writeError(w, status, err.Error())
		return
	}
	if parent != nil {
		r = r.WithContext(types.WithParentObject(r.Context(), parent))
	}

	// Convert back to typed object
	objJSON, _ := json.Marshal(objMap)
	obj, err := h.scheme.New(h.gvk)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create object: %v", err))
		return
	}
	if err := json.Unmarshal(objJSON, obj); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to unmarshal object: %v", err))
		return
	}
	clientObj := obj.(client.Object)

	// Set metadata
	accessor, err := meta.Accessor(clientObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access metadata: %v", err))
		return
	}

	accessor.SetNamespace(namespace)
	accessor.SetUID(k8stypes.UID(uuid.New().String()))
	accessor.SetCreationTimestamp(metav1.Time{Time: time.Now()})
	accessor.SetGeneration(1)

	// Set GVK
	clientObj.GetObjectKind().SetGroupVersionKind(h.gvk)

	if d, ok := obj.(types.CustomDefaulter); ok {
		if err := d.Default(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("defaulting failed: %v", err))
			return
		}
	}

	name := accessor.GetName()
	if name == "" && accessor.GetGenerateName() == "" {
		writeError(w, http.StatusBadRequest, "metadata.name or metadata.generateName is required")
		return
	}

	if err := validateMetadata(accessor); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata: %v", err))
		return
	}

	// Validate owner references exist
	if err := h.validateOwnerReferences(r.Context(), clientObj); err != nil {
		h.logger.V(1).Info("Owner reference validation failed", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "error", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid ownerReferences: %v", err))
		return
	}

	if v, ok := obj.(types.CustomValidator); ok {
		if err := v.ValidateCreate(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}
	if !authorizeRequestObject(w, r, clientObj) {
		return
	}

	// Convert to storage version before storing
	storeObj, err := h.convertToStorageVersion(clientObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert object version: %v", err))
		return
	}

	// Store object
	if err := h.store.Create(r.Context(), storeObj); err != nil {
		h.logger.Error(err, "Create failed", "kind", h.gvk.Kind, "namespace", namespace, "name", accessor.GetName())
		if errors.IsAlreadyExists(err) {
			writeError(w, http.StatusConflict, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create object: %v", err))
		}
		return
	}

	// Convert back to serving version for response
	responseObj, err := h.convertToServingVersion(storeObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
		return
	}

	h.logger.Info("Created", "kind", h.gvk.Kind, "namespace", namespace, "name", responseObj.GetName())

	// Return created object
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(responseObj)
}

// Get handles GET requests to retrieve a single resource.
func (h *ResourceHandler) Get(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("Get request", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	obj, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		h.logger.Error(err, "Get failed", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get object: %v", err))
		}
		return
	}

	if !validateParentOwnership(r.Context(), obj) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if !authorizeRequestObject(w, r, obj) {
		return
	}

	obj, err = h.convertToServingVersion(obj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert object version: %v", err))
		return
	}

	h.logger.V(1).Info("Found", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// List handles GET requests to list resources.
func (h *ResourceHandler) List(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	if namespace == "" {
		h.logger.V(1).Info("List request", "kind", h.gvk.Kind, "scope", "cluster")
	} else {
		h.logger.V(1).Info("List request", "kind", h.gvk.Kind, "namespace", namespace)
	}

	// Build list options from query parameters
	opts := storage.ListOptions{
		Namespace: namespace,
	}
	applyItemFilterToListOptions(r.Context(), &opts)

	// Parse label selector from query parameter
	if labelSelectorStr := r.URL.Query().Get(constants.QueryParamLabelSelector); labelSelectorStr != "" {
		// Validate label selector syntax before passing to storage
		_, err := labels.Parse(labelSelectorStr)
		if err != nil {
			h.logger.V(1).Info("Invalid label selector", "error", err)
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid label selector: %v", err))
			return
		}
		opts.LabelSelector = labelSelectorStr
	}

	// Parse shard selector from query parameters
	shardSelector, err := storage.ParseShardSelector(
		r.URL.Query().Get(constants.QueryParamShardIndex),
		r.URL.Query().Get(constants.QueryParamShardCount),
	)
	if err != nil {
		h.logger.V(1).Info("Invalid shard selector", "error", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid shard selector: %v", err))
		return
	}
	opts.ShardSelector = shardSelector

	applyParentFilterToListOpts(r.Context(), &opts)

	// Parse limit parameter
	if limitStr := r.URL.Query().Get(constants.QueryParamLimit); limitStr != "" {
		limit, err := strconv.ParseInt(limitStr, 10, 64)
		if err != nil || limit < 0 {
			h.logger.V(1).Info("Invalid limit parameter", "error", err)
			writeError(w, http.StatusBadRequest, "invalid limit parameter")
			return
		}
		opts.Limit = limit
	}

	// Parse continue token
	opts.Continue = r.URL.Query().Get(constants.QueryParamContinue)

	// Check if this is a watch request
	if r.URL.Query().Get(constants.QueryParamWatch) == "true" {
		if namespace == "" {
			h.logger.V(1).Info("Watch request", "kind", h.gvk.Kind, "scope", "cluster", "shard", shardSelector, "uri", r.RequestURI)
		} else {
			h.logger.V(1).Info("Watch request", "kind", h.gvk.Kind, "namespace", namespace, "shard", shardSelector, "uri", r.RequestURI)
		}
		h.handleWatch(w, r, opts)
		return
	}

	list, err := h.store.List(r.Context(), opts)
	if err != nil {
		if namespace == "" {
			h.logger.Error(err, "List failed", "kind", h.gvk.Kind, "scope", "cluster")
		} else {
			h.logger.Error(err, "List failed", "kind", h.gvk.Kind, "namespace", namespace)
		}
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list objects: %v", err))
		return
	}

	list, err = h.convertListToServingVersion(list)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert list version: %v", err))
		return
	}

	// Set GVK on the list
	list.GetObjectKind().SetGroupVersionKind(h.gvk.GroupVersion().WithKind(h.gvk.Kind + "List"))

	// Log list operation
	listMeta, _ := meta.ListAccessor(list)
	count := 0
	if listMeta != nil {
		items, _ := meta.ExtractList(list)
		count = len(items)
	}

	if namespace == "" {
		if shardSelector != nil {
			h.logger.V(1).Info("Listed with shard filter", "kind", h.gvk.Kind, "scope", "cluster", "shard", shardSelector, "count", count)
		} else {
			h.logger.V(1).Info("Listed", "kind", h.gvk.Kind, "scope", "cluster", "count", count)
		}
	} else {
		if shardSelector != nil {
			h.logger.V(1).Info("Listed with shard filter", "kind", h.gvk.Kind, "namespace", namespace, "shard", shardSelector, "count", count)
		} else {
			h.logger.V(1).Info("Listed", "kind", h.gvk.Kind, "namespace", namespace, "count", count)
		}
	}

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(list)
}

// handleWatch handles watch requests using Server-Sent Events.

// Update handles PUT requests to update a resource.
func (h *ResourceHandler) Update(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("Update request", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Get existing object to compare spec
	existing, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get existing object: %v", err))
		}
		return
	}

	if !validateParentOwnership(r.Context(), existing) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	// Convert the existing object to the serving version once; it is reused
	// for authorization, CEL old-object validation, and CustomValidator.
	existingServing, authorized := h.authorizeStoredObject(w, r, existing)
	if !authorized {
		return
	}

	// Parse request body as map for schema processing
	var objMap map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&objMap); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Validate cross-namespace owner references before schema processing
	if err := validateOwnerReferencesFromMap(namespace, objMap); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid ownerReferences: %v", err))
		return
	}

	// Build an old-object map from the serving representation.
	oldObjMap, err := toObjMap(existingServing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("building old-object map: %v", err))
		return
	}

	// Process object (prune, default, validate)
	if errs := h.processor.Process(r.Context(), objMap, oldObjMap); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", errs.ToAggregate()))
		return
	}

	// Convert back to typed object
	objJSON, _ := json.Marshal(objMap)
	obj, err := h.scheme.New(h.gvk)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create object: %v", err))
		return
	}
	if err := json.Unmarshal(objJSON, obj); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to unmarshal object: %v", err))
		return
	}
	clientObj := obj.(client.Object)

	if d, ok := obj.(types.CustomDefaulter); ok {
		if err := d.Default(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("defaulting failed: %v", err))
			return
		}
	}

	if v, ok := obj.(types.CustomValidator); ok {
		// existingServing is already the serving version (computed above for CEL).
		existingTyped, err := conversion.TypedOldObject(h.scheme, h.gvk, existingServing)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("converting old object: %v", err))
			return
		}
		if err := v.ValidateUpdate(r.Context(), existingTyped); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}

	// Set metadata
	accessor, err := meta.Accessor(clientObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access metadata: %v", err))
		return
	}

	accessor.SetNamespace(namespace)
	accessor.SetName(name)

	if err := validateMetadata(accessor); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata: %v", err))
		return
	}

	// Authorize the effective object before any mutation side effects, including
	// finalizer-driven hard deletion.
	clientObj.GetObjectKind().SetGroupVersionKind(h.gvk)
	if !authorizeRequestObject(w, r, clientObj) {
		return
	}

	// Check if spec changed and increment generation if so.
	//
	// Compare against a copy of existing that has gone through the same
	// normalization as clientObj (above): schema prune/default processing AND
	// custom defaulting. Without this, existing objects fetched as
	// *unstructured.Unstructured (postgres/spanner backends) can look
	// permanently different from an identically-specified clientObj purely
	// because of asymmetric normalization, causing a spurious generation bump
	// on every update. All mutation happens on the copy; raw existing is never
	// modified. Fail-safe: on a processing error we fall back to raw existing
	// (and skip custom defaulting, which must not mutate the shared object);
	// that can cause a one-time spurious bump but never persists bad data.
	existingAccessor, _ := meta.Accessor(existing)
	existingForCmp := existingForCompare(r.Context(), h.processor, existing, h.logger)
	if specChanged(existingForCmp, clientObj) {
		accessor.SetGeneration(existingAccessor.GetGeneration() + 1)
	} else {
		accessor.SetGeneration(existingAccessor.GetGeneration())
	}

	// Preserve deletionTimestamp - it cannot be changed via Update
	// Only Delete operation can set it, and only finalizer removal can clear it
	if deletionTimestamp := existingAccessor.GetDeletionTimestamp(); deletionTimestamp != nil {
		accessor.SetDeletionTimestamp(deletionTimestamp)

		// If object is being deleted and all finalizers are removed, perform hard delete
		if len(accessor.GetFinalizers()) == 0 {
			if err := h.store.Delete(r.Context(), namespace, name); err != nil {
				h.logger.Error(err, "Failed to hard delete after finalizers removed", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
				if errors.IsNotFound(err) {
					writeError(w, http.StatusNotFound, err.Error())
				} else {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete object: %v", err))
				}
				return
			}

			h.logger.Info("Hard deleted after finalizers removed", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

			// Return success status for deletion
			status := metav1.Status{
				TypeMeta: metav1.TypeMeta{
					APIVersion: constants.APIVersionV1,
					Kind:       constants.KindStatus,
				},
				Status: metav1.StatusSuccess,
				Code:   http.StatusOK,
			}

			w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(status)
			return
		}
	}

	// Validate owner references exist
	if err := h.validateOwnerReferences(r.Context(), clientObj); err != nil {
		h.logger.V(1).Info("Owner reference validation failed", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "error", err)
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid ownerReferences: %v", err))
		return
	}

	// Convert to storage version before storing
	storeObj, err := h.convertToStorageVersion(clientObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert object version: %v", err))
		return
	}

	// Update object
	if err := h.store.Update(r.Context(), storeObj); err != nil {
		h.logger.Error(err, "Update failed", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else if errors.IsConflict(err) {
			writeError(w, http.StatusConflict, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
		}
		return
	}

	// Convert back to serving version for response
	responseObj, err := h.convertToServingVersion(storeObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
		return
	}

	h.logger.Info("Updated", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Return updated object
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responseObj)
}

// Patch handles PATCH requests to partially update a resource.

// ApplyPatch handles server-side apply PATCH requests.
// This implements the Kubernetes server-side apply protocol with field ownership tracking.
func (h *ResourceHandler) ApplyPatch(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)

	h.logger.V(1).Info("Apply request", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Check if apply manager is available
	if h.applyManager == nil {
		writeError(w, http.StatusNotImplemented, "Server-side apply is not enabled for this resource")
		return
	}

	// Extract field manager from query parameters (required)
	fieldManager := r.URL.Query().Get(constants.QueryParamFieldManager)
	if fieldManager == "" {
		writeError(w, http.StatusBadRequest, "fieldManager query parameter is required for server-side apply")
		return
	}

	// Extract force parameter (optional, defaults to false)
	force := r.URL.Query().Get(constants.QueryParamForce) == "true"

	// Read apply configuration body
	applyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to read request body: %v", err))
		return
	}

	// Get existing object (if it exists)
	existing, err := h.store.Get(r.Context(), namespace, name)
	if err != nil && !errors.IsNotFound(err) {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get object: %v", err))
		return
	}
	if existing != nil {
		if _, authorized := h.authorizeStoredObject(w, r, existing); !authorized {
			return
		}
	}

	// Perform server-side apply
	result, err := h.applyManager.Apply(existing, applyBytes, fieldManager, force)
	if err != nil {
		if errors.IsConflict(err) {
			writeError(w, http.StatusConflict, fmt.Sprintf("Apply conflict: %v", err))
		} else {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("Apply failed: %v", err))
		}
		return
	}

	// Ensure namespace is set
	accessor, err := meta.Accessor(result)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access metadata: %v", err))
		return
	}
	accessor.SetNamespace(namespace)
	if !authorizeRequestObject(w, r, result) {
		return
	}

	// Convert to storage version before storing
	storeObj, err := h.convertToStorageVersion(result)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert object version: %v", err))
		return
	}

	// Save to storage (create or update)
	if existing == nil {
		// This is a create via apply
		accessor.SetUID(k8stypes.UID(uuid.New().String()))
		accessor.SetCreationTimestamp(metav1.Time{Time: time.Now()})
		accessor.SetGeneration(1)

		if err := h.store.Create(r.Context(), storeObj); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create object: %v", err))
			return
		}

		responseObj, err := h.convertToServingVersion(storeObj)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
			return
		}

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(responseObj)
		h.logger.Info("Created via apply", "namespace", namespace, "name", name, "fieldManager", fieldManager)
	} else {
		// This is an update via apply
		if err := h.store.Update(r.Context(), storeObj); err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
			return
		}

		responseObj, err := h.convertToServingVersion(storeObj)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
			return
		}

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(responseObj)
		h.logger.Info("Updated via apply", "namespace", namespace, "name", name, "fieldManager", fieldManager, "force", force)
	}
}

func (h *ResourceHandler) authorizeStoredObject(w http.ResponseWriter, r *http.Request, stored client.Object) (client.Object, bool) {
	serving, err := h.convertToServingVersion(stored)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("converting existing object to serving version: %v", err))
		return nil, false
	}
	if !authorizeRequestObject(w, r, serving) {
		return nil, false
	}
	return serving, true
}

// Delete handles DELETE requests to delete a resource.
// Implements the Kubernetes finalizer deletion flow:
// 1. If object has finalizers, set deletionTimestamp and update (soft delete)
// 2. If object has no finalizers or deletionTimestamp is already set, delete immediately
func (h *ResourceHandler) Delete(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("Delete request", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Get the existing object to check for finalizers
	existing, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		h.logger.Error(err, "Delete failed - get object", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get object: %v", err))
		}
		return
	}

	if !validateParentOwnership(r.Context(), existing) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if _, authorized := h.authorizeStoredObject(w, r, existing); !authorized {
		return
	}

	if v, ok := existing.(types.CustomValidator); ok {
		if err := v.ValidateDelete(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}

	accessor, err := meta.Accessor(existing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access metadata: %v", err))
		return
	}

	// Parse propagation policy from query parameter
	// Values: Orphan, Background (default), Foreground
	propagationPolicy := r.URL.Query().Get(constants.QueryParamPropagationPolicy)
	if propagationPolicy == "" {
		propagationPolicy = "Background"
	}

	// Check if object has finalizers and is not already being deleted
	finalizers := accessor.GetFinalizers()
	deletionTimestamp := accessor.GetDeletionTimestamp()
	ownerUID := string(accessor.GetUID())

	// Handle propagation policy before finalizer check
	if deletionTimestamp == nil {
		switch propagationPolicy {
		case "Orphan":
			// Remove this owner from all dependent objects
			h.logger.Info("Orphaning dependents", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "policy", propagationPolicy)
			if err := h.removeOwnerReferencesFromDependents(r.Context(), namespace, name, ownerUID); err != nil {
				h.logger.Error(err, "Failed to orphan dependents", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
			}

		case "Foreground":
			// Delete dependents synchronously before deleting this object
			h.logger.Info("Cascade deleting dependents (foreground)", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "policy", propagationPolicy)
			if err := h.deleteDependents(r.Context(), namespace, name, ownerUID); err != nil {
				h.logger.Error(err, "Failed to delete dependents", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
			}

		case "Background":
			// Background deletion (default) - GC will clean up dependents asynchronously
			h.logger.V(1).Info("Using background cascade deletion (GC handles dependents)", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "policy", propagationPolicy)
		}
	}

	if len(finalizers) > 0 {
		if deletionTimestamp == nil {
			toStore, ok := existing.DeepCopyObject().(client.Object)
			if !ok {
				writeError(w, http.StatusInternalServerError, "object does not implement client.Object")
				return
			}

			now := metav1.Now()
			toStore.SetDeletionTimestamp(&now)
			toStore.GetObjectKind().SetGroupVersionKind(h.effectiveStorageGVK())

			// Verify conversion is possible before persisting.
			responseObj, err := h.convertToServingVersion(toStore)
			if err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
				return
			}

			if err := h.store.Update(r.Context(), toStore); err != nil {
				h.logger.Error(err, "Delete failed - update with deletionTimestamp", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
				if errors.IsConflict(err) {
					writeError(w, http.StatusConflict, err.Error())
				} else {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
				}
				return
			}

			h.logger.Info("Marked for deletion (finalizers present)", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "finalizers", finalizers, "propagationPolicy", propagationPolicy)

			w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(responseObj)
			return
		}

		h.logger.V(1).Info("Already marked for deletion", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "finalizers", finalizers)

		responseObj, err := h.convertToServingVersion(existing)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response version: %v", err))
			return
		}

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(responseObj)
		return
	}

	// Hard delete: no finalizers or already marked for deletion
	if err := h.store.Delete(r.Context(), namespace, name); err != nil {
		h.logger.Error(err, "Delete failed - hard delete", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete object: %v", err))
		}
		return
	}

	h.logger.Info("Deleted", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "propagationPolicy", propagationPolicy)

	// Return success status
	status := metav1.Status{
		TypeMeta: metav1.TypeMeta{
			APIVersion: constants.APIVersionV1,
			Kind:       constants.KindStatus,
		},
		Status: metav1.StatusSuccess,
		Code:   http.StatusOK,
	}

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// toObjMap marshals obj to JSON and unmarshals it into a map[string]interface{}.
// It is used to produce the old-object map for CEL oldSelf validation.
func toObjMap(obj interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshaling object: %w", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshaling object: %w", err)
	}
	return m, nil
}
