package handlers

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ParentFilter holds the parent resource filter extracted from a nested route.
type ParentFilter struct {
	IDField string
	ID      string
}

type parentFilterKey struct{}

type invalidParentError struct {
	message string
}

func (e *invalidParentError) Error() string {
	return e.message
}

func isInvalidParentError(err error) bool {
	var parentErr *invalidParentError
	return stderrors.As(err, &parentErr)
}

// WithParentFilter returns a new context carrying the given ParentFilter.
func WithParentFilter(ctx context.Context, pf ParentFilter) context.Context {
	return context.WithValue(ctx, parentFilterKey{}, pf)
}

// ParentFilterFromContext retrieves the ParentFilter from the given context, or nil if absent.
func ParentFilterFromContext(ctx context.Context) *ParentFilter {
	pf, ok := ctx.Value(parentFilterKey{}).(ParentFilter)
	if !ok {
		return nil
	}
	return &pf
}

func applyParentFilterToListOpts(ctx context.Context, opts *storage.ListOptions) {
	pf := ParentFilterFromContext(ctx)
	if pf == nil {
		return
	}
	if opts.FieldFilters == nil {
		opts.FieldFilters = make(map[string]string)
	}
	opts.FieldFilters[pf.IDField] = pf.ID
}

func validateParentOnCreate(ctx context.Context, objMap map[string]interface{}) error {
	pf := ParentFilterFromContext(ctx)
	if pf == nil {
		return nil
	}
	if fieldValueFromMap(objMap, pf.IDField) != pf.ID {
		return fmt.Errorf("field %s must be %q when creating via nested route", pf.IDField, pf.ID)
	}
	return nil
}

// ValidateParentExists verifies that the parent referenced by a child exists
// and is not being deleted.
func ValidateParentExists(ctx context.Context, parentStore storage.ResourceStore, namespace, idField string, objMap map[string]interface{}) error {
	if parentStore == nil {
		return nil
	}

	parentID := fieldValueFromMap(objMap, idField)
	parent, err := parentStore.Get(ctx, namespace, parentID)
	if err != nil {
		if errors.IsNotFound(err) {
			return &invalidParentError{message: fmt.Sprintf("referenced parent %q not found", parentID)}
		}
		return fmt.Errorf("get referenced parent %q: %w", parentID, err)
	}
	if parent.GetDeletionTimestamp() != nil {
		return &invalidParentError{message: fmt.Sprintf("referenced parent %q is being deleted", parentID)}
	}
	return nil
}

func validateParentOwnership(ctx context.Context, obj client.Object) bool {
	pf := ParentFilterFromContext(ctx)
	if pf == nil {
		return true
	}
	objMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return false
	}
	return fieldValueFromMap(objMap, pf.IDField) == pf.ID
}

func fieldValueFromMap(m map[string]interface{}, path string) string {
	parts := strings.Split(path, ".")
	current := interface{}(m)
	for _, part := range parts {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return ""
		}
		current = cm[part]
	}
	s, _ := current.(string)
	return s
}
