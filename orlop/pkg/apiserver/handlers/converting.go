package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/aggregated"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/constants"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/schema"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ConvertingResourceHandler wraps a ResourceHandler and performs conversions
// between public and private API types.
type ConvertingResourceHandler struct {
	store          storage.ResourceStore
	processor      *schema.Processor
	converter      *conversion.Converter
	gvk            runtimeschema.GroupVersionKind
	resourceType   string
	publicScheme   *runtime.Scheme // Scheme for public API types
	privateScheme  *runtime.Scheme // Scheme for private API types
	printerColumns []types.PrinterColumn
	parentStore    storage.ResourceStore
	parentIDField  string
	logger         logr.Logger
}

// SetParentStore configures creation-time validation of the parent named by idField.
func (h *ConvertingResourceHandler) SetParentStore(store storage.ResourceStore, idField string) {
	h.parentStore = store
	h.parentIDField = idField
}

// stripStatus removes the status field from a map, case-insensitively.
// Defense-in-depth: while schemas should reject case variants, this ensures
// status cannot bypass via "Status", "STATUS", etc.
func stripStatus(m map[string]interface{}) {
	for k := range m {
		if strings.EqualFold(k, "status") {
			delete(m, k)
			return // Only one status key expected
		}
	}
}

// NewConvertingResourceHandler creates a new converting resource handler.
func NewConvertingResourceHandler(
	store storage.ResourceStore,
	processor *schema.Processor,
	converter *conversion.Converter,
	gvk runtimeschema.GroupVersionKind,
	resourceType string,
	publicScheme *runtime.Scheme,
	privateScheme *runtime.Scheme,
	printerColumns []types.PrinterColumn,
	logger logr.Logger,
) *ConvertingResourceHandler {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	if processor == nil {
		// Fail loudly at construction time rather than at request time.
		// This is a programming error — every public API handler must have a schema processor.
		panic("ConvertingResourceHandler requires a non-nil schema processor")
	}
	return &ConvertingResourceHandler{
		store:          store,
		processor:      processor,
		converter:      converter,
		gvk:            gvk,
		resourceType:   resourceType,
		publicScheme:   publicScheme,
		privateScheme:  privateScheme,
		printerColumns: printerColumns,
		logger:         logger,
	}
}

// Create handles POST requests to create a new resource.
func (h *ConvertingResourceHandler) Create(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	h.logger.V(1).Info("Create request (converting)", "kind", h.gvk.Kind, "namespace", namespace)

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

	// Status is controller-managed; strip from public input.
	stripStatus(objMap)

	// Note: ownerReferences validation is intentionally omitted here.
	// The public metadata schema prunes ownerReferences from input, so
	// validateOwnerReferencesFromMap would always see an empty list.
	// Clients cannot set ownerReferences through the public API.

	// Process object (prune, default, validate) using public schema.
	// Fail-closed: reject request if processor is nil (misconfiguration)
	// rather than silently skipping all schema processing.
	if h.processor == nil {
		writeError(w, http.StatusInternalServerError, "schema processor not configured")
		return
	}
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

	// Convert to public typed object
	objJSON, _ := json.Marshal(objMap)
	publicObj, err := h.publicScheme.New(h.gvk)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create public object: %v", err))
		return
	}
	if err := json.Unmarshal(objJSON, publicObj); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to unmarshal object: %v", err))
		return
	}

	// Set metadata on public object
	accessor, err := meta.Accessor(publicObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access metadata: %v", err))
		return
	}

	name := accessor.GetName()
	if name == "" && accessor.GetGenerateName() == "" {
		writeError(w, http.StatusBadRequest, "metadata.name or metadata.generateName is required")
		return
	}

	accessor.SetNamespace(namespace)
	accessor.SetUID(k8stypes.UID(uuid.New().String()))
	accessor.SetCreationTimestamp(metav1.Time{Time: time.Now()})
	accessor.SetGeneration(1)

	if err := validateMetadata(accessor); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid metadata: %v", err))
		return
	}

	// Set GVK on public object
	publicObj.GetObjectKind().SetGroupVersionKind(h.gvk)
	if !authorizeRequestObject(w, r, publicObj.(client.Object)) {
		return
	}

	// Convert public to private (no existing object)
	privateObjRaw, err := h.privateScheme.New(h.gvk)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create private object: %v", err))
		return
	}
	privateObj, err := h.converter.PublicToPrivate(publicObj, privateObjRaw.(client.Object))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert to private: %v", err))
		return
	}

	// Only use the identity validated by the public API authn middleware. The
	// private prefix ensures it is automatically stripped from the public API
	// and cannot be set by clients.
	if email, ok := AuthenticatedUserFromContext(r.Context()); ok {
		privateAccessor, err := meta.Accessor(privateObj)
		if err == nil {
			annotations := privateAccessor.GetAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[constants.AnnotationCreatedBy] = email
			privateAccessor.SetAnnotations(annotations)
		}
	}

	if d, ok := privateObj.(types.CustomDefaulter); ok {
		if err := d.Default(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("defaulting failed: %v", err))
			return
		}
	}

	if v, ok := privateObj.(types.CustomValidator); ok {
		if err := v.ValidateCreate(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}
	// Set GVK on private object for storage
	privateObj.GetObjectKind().SetGroupVersionKind(h.gvk)

	// Store private object (cast to client.Object)
	if err := h.store.Create(r.Context(), privateObj.(client.Object)); err != nil {
		h.logger.Error(err, "Create failed (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", accessor.GetName())
		if errors.IsAlreadyExists(err) {
			writeError(w, http.StatusConflict, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create object: %v", err))
		}
		return
	}

	name = accessor.GetName()
	h.logger.Info("Created (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Convert stored private object back to public to get ResourceVersion
	responsePublic, err := h.convertToPublicObject(privateObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert to public: %v", err))
		return
	}

	// Return public representation
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(responsePublic)
}

// Get handles GET requests to retrieve a single resource.
func (h *ConvertingResourceHandler) Get(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("Get request (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Get private object from storage
	privateObj, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		h.logger.Error(err, "Get failed (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get object: %v", err))
		}
		return
	}

	if !validateParentOwnership(r.Context(), privateObj) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	// Convert to public
	publicObj, err := h.convertToPublicObject(privateObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert to public: %v", err))
		return
	}
	publicClientObj, ok := publicObj.(client.Object)
	if !ok {
		writeError(w, http.StatusInternalServerError, "public object does not implement client.Object")
		return
	}
	if !authorizeRequestObject(w, r, publicClientObj) {
		return
	}

	h.logger.V(1).Info("Found (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(publicObj)
}

// List handles GET requests to list resources.
func (h *ConvertingResourceHandler) List(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	h.logListRequest(namespace)

	opts, err := h.listOptions(r, namespace)
	if err != nil {
		h.logger.V(1).Info("Invalid list options", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check if this is a watch request
	if r.URL.Query().Get(constants.QueryParamWatch) == "true" {
		h.logWatchRequest(namespace, r.RequestURI)
		h.handleWatch(w, r, opts)
		return
	}

	// Get private objects list from storage
	privateList, err := h.store.List(r.Context(), opts)
	if err != nil {
		h.logListError(err, namespace)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list objects: %v", err))
		return
	}

	publicObjects, err := h.publicListObjects(privateList)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if h.writeTableList(w, r, privateList, publicObjects, namespace) {
		return
	}
	h.writeJSONList(w, privateList, publicObjects, namespace)
}

func (h *ConvertingResourceHandler) listOptions(r *http.Request, namespace string) (storage.ListOptions, error) {
	opts := storage.ListOptions{Namespace: namespace}
	if filter, ok := ItemFilterFromContext(r.Context()); ok {
		opts.ItemFilter = h.publicItemFilter(filter)
	}

	if labelSelector := r.URL.Query().Get(constants.QueryParamLabelSelector); labelSelector != "" {
		if _, err := labels.Parse(labelSelector); err != nil {
			return storage.ListOptions{}, fmt.Errorf("invalid label selector: %w", err)
		}
		opts.LabelSelector = labelSelector
	}
	shardSelector, err := storage.ParseShardSelector(
		r.URL.Query().Get(constants.QueryParamShardIndex),
		r.URL.Query().Get(constants.QueryParamShardCount),
	)
	if err != nil {
		return storage.ListOptions{}, fmt.Errorf("invalid shard selector: %w", err)
	}
	opts.ShardSelector = shardSelector
	applyParentFilterToListOpts(r.Context(), &opts)

	if limitString := r.URL.Query().Get(constants.QueryParamLimit); limitString != "" {
		limit, err := strconv.ParseInt(limitString, 10, 64)
		if err != nil || limit < 0 {
			return storage.ListOptions{}, fmt.Errorf("invalid limit parameter")
		}
		opts.Limit = limit
	}
	opts.Continue = r.URL.Query().Get(constants.QueryParamContinue)
	return opts, nil
}

func (h *ConvertingResourceHandler) publicItemFilter(filter storage.ItemFilter) storage.ItemFilter {
	return func(ctx context.Context, privateObj client.Object) (bool, error) {
		publicObj, err := h.convertToPublicObject(privateObj)
		if err != nil {
			return false, fmt.Errorf("convert object for item authorization: %w", err)
		}
		publicClientObj, ok := publicObj.(client.Object)
		if !ok {
			return false, fmt.Errorf("public object does not implement client.Object")
		}
		return filter(ctx, publicClientObj)
	}
}

func (h *ConvertingResourceHandler) publicListObjects(privateList client.ObjectList) ([]runtime.Object, error) {
	privateItems, err := meta.ExtractList(privateList)
	if err != nil {
		return nil, fmt.Errorf("extract list items: %w", err)
	}
	publicObjects := make([]runtime.Object, 0, len(privateItems))
	for _, privateObj := range privateItems {
		publicObj, err := h.convertToPublicObject(privateObj)
		if err != nil {
			continue
		}
		publicObjects = append(publicObjects, publicObj)
	}
	return publicObjects, nil
}

func (h *ConvertingResourceHandler) writeTableList(w http.ResponseWriter, r *http.Request, privateList client.ObjectList, publicObjects []runtime.Object, namespace string) bool {
	if !requestsTable(r) || len(h.printerColumns) == 0 {
		return false
	}

	gr := runtimeschema.GroupResource{Group: h.gvk.Group, Resource: h.resourceType}
	convertor := aggregated.NewCustomTableConvertor(gr, h.printerColumns)
	unstructuredList := &unstructured.UnstructuredList{}
	unstructuredList.SetAPIVersion(h.gvk.GroupVersion().String())
	unstructuredList.SetKind(h.gvk.Kind + "List")
	unstructuredList.SetResourceVersion(privateList.GetResourceVersion())
	if listMeta, err := meta.ListAccessor(privateList); err == nil {
		unstructuredList.SetContinue(listMeta.GetContinue())
		unstructuredList.SetRemainingItemCount(listMeta.GetRemainingItemCount())
	}
	for _, obj := range publicObjects {
		unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			h.logger.V(1).Info("Failed to convert object to unstructured for table", "error", err)
			continue
		}
		unstructuredList.Items = append(unstructuredList.Items, unstructured.Unstructured{Object: unstructuredObj})
	}
	table, err := convertor.ConvertToTable(r.Context(), unstructuredList, nil)
	if err != nil {
		h.logger.Error(err, "Table conversion failed", "kind", h.gvk.Kind)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert to table: %v", err))
		return true
	}
	h.logListed(namespace, len(publicObjects), "Listed as Table (converting)")
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(table); err != nil {
		h.logger.Error(err, "Failed to encode Table response", "kind", h.gvk.Kind)
	}
	return true
}

func requestsTable(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept"), ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		if mediaType == "application/json" && params["as"] == "Table" && params["q"] != "0" {
			return true
		}
	}
	return false
}

func (h *ConvertingResourceHandler) writeJSONList(w http.ResponseWriter, privateList client.ObjectList, publicObjects []runtime.Object, namespace string) {
	listResponse := map[string]interface{}{
		"apiVersion":            h.gvk.GroupVersion().String(),
		"kind":                  h.gvk.Kind + "List",
		constants.FieldMetadata: listMetadata(privateList),
		"items":                 publicObjects,
	}
	h.logListed(namespace, len(publicObjects), "Listed (converting)")
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(listResponse)
}

func listMetadata(list client.ObjectList) map[string]interface{} {
	metadata := map[string]interface{}{"resourceVersion": list.GetResourceVersion()}
	listMeta, err := meta.ListAccessor(list)
	if err != nil {
		return metadata
	}
	if continueToken := listMeta.GetContinue(); continueToken != "" {
		metadata["continue"] = continueToken
	}
	if remainingItemCount := listMeta.GetRemainingItemCount(); remainingItemCount != nil {
		metadata["remainingItemCount"] = *remainingItemCount
	}
	return metadata
}

func (h *ConvertingResourceHandler) logListRequest(namespace string) {
	if namespace == "" {
		h.logger.V(1).Info("List request (converting)", "kind", h.gvk.Kind, "scope", "cluster")
		return
	}
	h.logger.V(1).Info("List request (converting)", "kind", h.gvk.Kind, "namespace", namespace)
}

func (h *ConvertingResourceHandler) logWatchRequest(namespace, uri string) {
	if namespace == "" {
		h.logger.V(1).Info("Watch request (converting)", "kind", h.gvk.Kind, "scope", "cluster", "uri", uri)
		return
	}
	h.logger.V(1).Info("Watch request (converting)", "kind", h.gvk.Kind, "namespace", namespace, "uri", uri)
}

func (h *ConvertingResourceHandler) logListError(err error, namespace string) {
	if namespace == "" {
		h.logger.Error(err, "List failed (converting)", "kind", h.gvk.Kind, "scope", "cluster")
		return
	}
	h.logger.Error(err, "List failed (converting)", "kind", h.gvk.Kind, "namespace", namespace)
}

func (h *ConvertingResourceHandler) logListed(namespace string, count int, message string) {
	if namespace == "" {
		h.logger.V(1).Info(message, "kind", h.gvk.Kind, "scope", "cluster", "count", count)
		return
	}
	h.logger.V(1).Info(message, "kind", h.gvk.Kind, "namespace", namespace, "count", count)
}

// handleWatch handles watch requests using streaming JSON.
func (h *ConvertingResourceHandler) handleWatch(w http.ResponseWriter, r *http.Request, opts storage.ListOptions) {
	config := parseWatchConfig(r)

	h.logger.V(1).Info("Watch parameters (converting)",
		config.allowWatchBookmarks, config.sendInitialEvents, config.resourceVersionMatch, config.timeoutSeconds)

	// Apply timeout if specified
	ctx, timeoutCancel := applyWatchTimeout(r.Context(), config.timeoutSeconds)
	defer timeoutCancel()

	// Start watch
	eventCh, stop, err := h.store.Watch(ctx, opts, config.resourceVersion)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to start watch: %v", err))
		return
	}
	defer stop()

	// Get current resource version and check if list is empty
	var currentRV string
	var isEmpty bool
	list, err := h.store.List(ctx, opts)
	if err == nil {
		currentRV = list.GetResourceVersion()
		items, _ := meta.ExtractList(list)
		isEmpty = len(items) == 0
	} else {
		currentRV = "0"
		isEmpty = true
	}

	// Set up streaming response
	streamer, err := newWatchStreamer(w, h.gvk, currentRV, isEmpty)
	if err != nil {
		return
	}

	// Create transformer to convert private objects to public
	transformer := func(obj client.Object) (interface{}, error) {
		return h.convertToPublicObject(obj)
	}

	// Stream watch events with transformation
	streamWatch(ctx, streamer, eventCh, config, opts, h.store, transformer)
}

// Update handles PUT requests to update a resource.
func (h *ConvertingResourceHandler) Update(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("Update request (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Get existing private object
	existingPrivate, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get existing object: %v", err))
		}
		return
	}

	if !validateParentOwnership(r.Context(), existingPrivate) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	existingPublic, err := h.convertToPublicClientObject(existingPrivate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert existing object for authorization: %v", err))
		return
	}
	if !authorizeRequestObject(w, r, existingPublic) {
		return
	}

	// Parse request body
	var objMap map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&objMap); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Status is controller-managed; strip from public input.
	stripStatus(objMap)

	// Note: ownerReferences validation is intentionally omitted here.
	// The public metadata schema prunes ownerReferences from input, so
	// validateOwnerReferencesFromMap would always see an empty list.

	// Build an old-object map using the public representation so that it
	// matches the schema of objMap (which is the incoming public request).
	existingJSONForCEL, err := json.Marshal(existingPublic)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("marshaling old object: %v", err))
		return
	}
	var oldObjMap map[string]interface{}
	if err := json.Unmarshal(existingJSONForCEL, &oldObjMap); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("unmarshaling old object: %v", err))
		return
	}

	// Process object (prune, default, validate) using public schema.
	// Fail-closed: reject request if processor is nil (misconfiguration)
	// rather than silently skipping all schema processing.
	if h.processor == nil {
		writeError(w, http.StatusInternalServerError, "schema processor not configured")
		return
	}
	if errs := h.processor.Process(r.Context(), objMap, oldObjMap); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", errs.ToAggregate()))
		return
	}

	// Convert to public typed object
	objJSON, _ := json.Marshal(objMap)
	publicObj, err := h.publicScheme.New(h.gvk)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create public object: %v", err))
		return
	}
	if err := json.Unmarshal(objJSON, publicObj); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to unmarshal object: %v", err))
		return
	}

	// Set metadata
	accessor, err := meta.Accessor(publicObj)
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
	publicObj.GetObjectKind().SetGroupVersionKind(h.gvk)
	if !authorizeRequestObject(w, r, publicObj.(client.Object)) {
		return
	}

	// Convert public to private, preserving internal fields
	privateObj, err := h.converter.PublicToPrivate(publicObj, existingPrivate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert to private: %v", err))
		return
	}

	// Preserve deletionTimestamp from existing object
	// The converter's JSON unmarshal overwrites deletionTimestamp with nil since public API doesn't include it
	existingAccessor, err := meta.Accessor(existingPrivate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access existing metadata: %v", err))
		return
	}
	privateAccessor, err := meta.Accessor(privateObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to access private metadata: %v", err))
		return
	}
	if dt := existingAccessor.GetDeletionTimestamp(); dt != nil {
		privateAccessor.SetDeletionTimestamp(dt)
	}

	if d, ok := privateObj.(types.CustomDefaulter); ok {
		if err := d.Default(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("defaulting failed: %v", err))
			return
		}
	}

	if v, ok := privateObj.(types.CustomValidator); ok {
		existingTyped, err := conversion.TypedOldObject(h.privateScheme, h.gvk, existingPrivate)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("converting old object: %v", err))
			return
		}
		if err := v.ValidateUpdate(r.Context(), existingTyped); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}
	// Check if spec changed and increment generation if so.
	//
	// Compare against a copy of existingPrivate normalized the same way as
	// privateObj (custom defaulting), so identical content compares equal and
	// generation does not churn. The copy is comparison-only; existingPrivate
	// is never modified. Note: the public-schema prune/default applied to the
	// incoming object is not mirrored here — doing so would require a
	// public<->private round-trip. That asymmetry converges after one write and
	// is tracked as a follow-up.
	oldForCompare := existingForCompare(r.Context(), nil, existingPrivate, h.logger)
	if specChanged(oldForCompare, privateObj) {
		privateAccessor.SetGeneration(existingAccessor.GetGeneration() + 1)
	} else {
		privateAccessor.SetGeneration(existingAccessor.GetGeneration())
	}

	// Set GVK
	privateObj.GetObjectKind().SetGroupVersionKind(h.gvk)

	// Note: no finalizer-aware hard-delete here. Public API cannot modify finalizers
	// (they are stripped by stripPrivateFieldsFromPublicInput), so finalizer removal
	// only happens via the private API, which handles hard-delete in resource.go.

	// Update object in storage (cast to client.Object)
	if err := h.store.Update(r.Context(), privateObj.(client.Object)); err != nil {
		h.logger.Error(err, "Update failed (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else if errors.IsConflict(err) {
			writeError(w, http.StatusConflict, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
		}
		return
	}

	h.logger.Info("Updated (converting)", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

	// Convert back to public for response
	responsePublic, err := h.convertToPublicObject(privateObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response: %v", err))
		return
	}

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responsePublic)
}

// patchInput holds the outputs of the patch preparation step.
type patchInput struct {
	objMap          map[string]interface{}
	oldObjMap       map[string]interface{}
	existingPrivate client.Object
	existingPublic  client.Object
}

// Patch handles PATCH requests to partially update a resource.
func (h *ConvertingResourceHandler) Patch(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	contentType := r.Header.Get(constants.HeaderContentType)
	h.logger.V(1).Info("[PATCH-CONVERTING] %s namespace=%s name=%s content-type=%s", h.gvk.Kind, namespace, name, contentType)

	input, httpStatus, err := h.preparePatch(r, namespace, name)
	if err != nil {
		writeError(w, httpStatus, err.Error())
		return
	}
	if !authorizeRequestObject(w, r, input.existingPublic) {
		return
	}

	if h.processor == nil {
		writeError(w, http.StatusInternalServerError, "schema processor not configured")
		return
	}
	if errs := h.processor.Process(r.Context(), input.objMap, input.oldObjMap); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", errs.ToAggregate()))
		return
	}

	publicObj, httpStatus, err := h.patchPublicObject(namespace, name, input)
	if err != nil {
		writeError(w, httpStatus, err.Error())
		return
	}
	if !authorizeRequestObject(w, r, publicObj) {
		return
	}
	privateObj, httpStatus, err := h.convertPatchToPrivate(r, input, publicObj)
	if err != nil {
		writeError(w, httpStatus, err.Error())
		return
	}
	privateClientObj, ok := privateObj.(client.Object)
	if !ok {
		writeError(w, http.StatusInternalServerError, "patched object does not implement client.Object")
		return
	}

	if err := h.store.Update(r.Context(), privateClientObj); err != nil {
		h.logger.V(1).Info("[PATCH-CONVERTING] %s namespace=%s name=%s error=%v", h.gvk.Kind, namespace, name, err)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else if errors.IsConflict(err) {
			writeError(w, http.StatusConflict, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
		}
		return
	}

	h.logger.V(1).Info("[PATCH-CONVERTING] %s namespace=%s name=%s status=patched", h.gvk.Kind, namespace, name)

	responsePublic, err := h.convertToPublicObject(privateObj)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response: %v", err))
		return
	}

	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responsePublic)
}

// preparePatch fetches the existing object, applies the patch body to its
// public representation, and returns the merged object map along with an old-
// object map for CEL oldSelf validation.
func (h *ConvertingResourceHandler) preparePatch(r *http.Request, namespace, name string) (patchInput, int, error) {
	existingPrivate, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
		if errors.IsNotFound(err) {
			return patchInput{}, http.StatusNotFound, err
		}
		return patchInput{}, http.StatusInternalServerError, fmt.Errorf("failed to get object: %w", err)
	}

	if !validateParentOwnership(r.Context(), existingPrivate) {
		return patchInput{}, http.StatusNotFound, fmt.Errorf("not found")
	}

	existingPublic, err := h.convertToPublicClientObject(existingPrivate)
	if err != nil {
		return patchInput{}, http.StatusInternalServerError, fmt.Errorf("failed to convert to public: %w", err)
	}

	patchBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return patchInput{}, http.StatusBadRequest, fmt.Errorf("failed to read patch: %w", err)
	}

	// Status is controller-managed; strip from patch before applying so that
	// existing controller-set status is preserved from existingPublic.
	var patchMap map[string]interface{}
	if err := json.Unmarshal(patchBytes, &patchMap); err != nil {
		return patchInput{}, http.StatusBadRequest, fmt.Errorf("invalid patch JSON: %w", err)
	}
	stripStatus(patchMap)
	patchBytes, _ = json.Marshal(patchMap)

	existingJSON, err := json.Marshal(existingPublic)
	if err != nil {
		return patchInput{}, http.StatusInternalServerError, fmt.Errorf("failed to marshal existing object: %w", err)
	}

	patchedJSON, err := jsonMergePatch(existingJSON, patchBytes)
	if err != nil {
		return patchInput{}, http.StatusBadRequest, fmt.Errorf("patch failed: %w", err)
	}

	var objMap map[string]interface{}
	if err := json.Unmarshal(patchedJSON, &objMap); err != nil {
		return patchInput{}, http.StatusInternalServerError, fmt.Errorf("failed to unmarshal patched object: %w", err)
	}

	// Build an old-object map from the pre-patch state so that CEL rules
	// referencing oldSelf work correctly on update.
	var oldObjMap map[string]interface{}
	if err := json.Unmarshal(existingJSON, &oldObjMap); err != nil {
		return patchInput{}, http.StatusInternalServerError, fmt.Errorf("failed to unmarshal old object: %w", err)
	}

	return patchInput{
		objMap:          objMap,
		oldObjMap:       oldObjMap,
		existingPrivate: existingPrivate,
		existingPublic:  existingPublic,
	}, http.StatusOK, nil
}

func (h *ConvertingResourceHandler) convertToPublicObject(privateObj runtime.Object) (runtime.Object, error) {
	privateCopy := privateObj.DeepCopyObject()
	privateCopy.GetObjectKind().SetGroupVersionKind(h.gvk)
	return h.converter.PrivateToPublic(privateCopy)
}

func (h *ConvertingResourceHandler) convertToPublicClientObject(privateObj client.Object) (client.Object, error) {
	publicObj, err := h.convertToPublicObject(privateObj)
	if err != nil {
		return nil, err
	}
	publicClientObj, ok := publicObj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("public object does not implement client.Object")
	}
	return publicClientObj, nil
}

// patchPublicObject converts the validated, merged public object map into its
// typed public representation with server-controlled identity restored.
func (h *ConvertingResourceHandler) patchPublicObject(namespace, name string, input patchInput) (client.Object, int, error) {
	objJSON, _ := json.Marshal(input.objMap)
	publicObj, err := h.publicScheme.New(h.gvk)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create public object: %w", err)
	}
	if err := json.Unmarshal(objJSON, publicObj); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to unmarshal object: %w", err)
	}

	accessor, err := meta.Accessor(publicObj)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to access metadata: %w", err)
	}
	accessor.SetNamespace(namespace)
	accessor.SetName(name)

	if err := validateMetadata(accessor); err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("invalid metadata: %w", err)
	}
	publicObj.GetObjectKind().SetGroupVersionKind(h.gvk)
	clientObj, ok := publicObj.(client.Object)
	if !ok {
		return nil, http.StatusInternalServerError, fmt.Errorf("public object does not implement client.Object")
	}
	return clientObj, http.StatusOK, nil
}

// convertPatchToPrivate converts the effective public object into a private
// runtime.Object ready for storage, running custom defaulting and validation,
// preserving server-managed metadata, and computing the new generation.
func (h *ConvertingResourceHandler) convertPatchToPrivate(r *http.Request, input patchInput, publicObj client.Object) (runtime.Object, int, error) {

	privateObj, err := h.converter.PublicToPrivate(publicObj, input.existingPrivate)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to convert to private: %w", err)
	}

	if d, ok := privateObj.(types.CustomDefaulter); ok {
		if err := d.Default(r.Context()); err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("defaulting failed: %w", err)
		}
	}

	if v, ok := privateObj.(types.CustomValidator); ok {
		existingTyped, err := conversion.TypedOldObject(h.privateScheme, h.gvk, input.existingPrivate)
		if err != nil {
			return nil, http.StatusInternalServerError, fmt.Errorf("converting old object: %w", err)
		}
		if err := v.ValidateUpdate(r.Context(), existingTyped); err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("validation failed: %w", err)
		}
	}

	// Preserve deletionTimestamp — the converter's JSON round-trip overwrites
	// it with nil since the public API does not include it.
	existingAccessor, err := meta.Accessor(input.existingPrivate)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to access existing metadata: %w", err)
	}
	privateAccessor, err := meta.Accessor(privateObj)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to access private metadata: %w", err)
	}
	if dt := existingAccessor.GetDeletionTimestamp(); dt != nil {
		privateAccessor.SetDeletionTimestamp(dt)
	}

	// Increment generation when spec changes.
	// Compare against a copy of existingPrivate normalized the same way as
	// privateObj (custom defaulting), so identical content compares equal and
	// generation does not churn. The copy is comparison-only; existingPrivate
	// is never modified.
	oldForCompare := existingForCompare(r.Context(), nil, input.existingPrivate, h.logger)
	if specChanged(oldForCompare, privateObj) {
		privateAccessor.SetGeneration(existingAccessor.GetGeneration() + 1)
	} else {
		privateAccessor.SetGeneration(existingAccessor.GetGeneration())
	}

	// Note: no finalizer-aware hard-delete here. The public API cannot modify
	// finalizers (they are stripped by stripPrivateFieldsFromPublicInput), so
	// finalizer removal only happens via the private API.
	privateObj.GetObjectKind().SetGroupVersionKind(h.gvk)

	return privateObj, http.StatusOK, nil
}

// Delete handles DELETE requests to delete a resource.
func (h *ConvertingResourceHandler) Delete(w http.ResponseWriter, r *http.Request) {
	namespace := chi.URLParam(r, constants.URLParamNamespace)
	name := chi.URLParam(r, constants.URLParamName)
	h.logger.V(1).Info("[DELETE-CONVERTING] %s namespace=%s name=%s", h.gvk.Kind, namespace, name)

	existing, err := h.store.Get(r.Context(), namespace, name)
	if err != nil {
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
	authorizationPublic, err := h.convertToPublicObject(existing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert object for authorization: %v", err))
		return
	}
	publicClientObj, ok := authorizationPublic.(client.Object)
	if !ok {
		writeError(w, http.StatusInternalServerError, "public object does not implement client.Object")
		return
	}
	if !authorizeRequestObject(w, r, publicClientObj) {
		return
	}

	if v, ok := existing.(types.CustomValidator); ok {
		if err := v.ValidateDelete(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("validation failed: %v", err))
			return
		}
	}

	finalizers := existing.GetFinalizers()
	deletionTimestamp := existing.GetDeletionTimestamp()

	// Kubernetes finalizer deletion flow:
	// 1. If object has finalizers and is not marked for deletion, set deletionTimestamp (soft delete)
	// 2. If object has finalizers and is already marked for deletion, return object (idempotent)
	// 3. If object has no finalizers, delete immediately (hard delete)
	if len(finalizers) > 0 {
		if deletionTimestamp == nil {
			// First delete request - set deletionTimestamp (soft delete)
			toStore, ok := existing.DeepCopyObject().(client.Object)
			if !ok {
				writeError(w, http.StatusInternalServerError, "object does not implement client.Object")
				return
			}

			now := metav1.Now()
			toStore.SetDeletionTimestamp(&now)
			toStore.GetObjectKind().SetGroupVersionKind(h.gvk)

			if err := h.store.Update(r.Context(), toStore); err != nil {
				h.logger.Error(err, "Delete failed - update with deletionTimestamp", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
				if errors.IsConflict(err) {
					writeError(w, http.StatusConflict, err.Error())
				} else {
					writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to update object: %v", err))
				}
				return
			}

			h.logger.Info("Marked for deletion (finalizers present)", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "finalizers", finalizers)

			// Convert to public API for response
			responsePublic, err := h.convertToPublicObject(toStore)
			if err != nil {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response: %v", err))
				return
			}

			w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(responsePublic)
			return
		}

		// Already marked for deletion - return object idempotently
		h.logger.V(1).Info("Already marked for deletion", "kind", h.gvk.Kind, "namespace", namespace, "name", name, "finalizers", finalizers)

		responsePublic, err := h.convertToPublicObject(existing)
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to convert response: %v", err))
			return
		}

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(responsePublic)
		return
	}

	// Hard delete: no finalizers
	if err := h.store.Delete(r.Context(), namespace, name); err != nil {
		h.logger.Error(err, "Delete failed - hard delete", "kind", h.gvk.Kind, "namespace", namespace, "name", name)
		if errors.IsNotFound(err) {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete object: %v", err))
		}
		return
	}

	h.logger.Info("Deleted", "kind", h.gvk.Kind, "namespace", namespace, "name", name)

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
