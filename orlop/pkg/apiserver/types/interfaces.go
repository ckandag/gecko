package types

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type parentObjectKey struct{}

// WithParentObject makes the validated parent available to a child resource's
// create validator. The parent is a snapshot from the backing store.
func WithParentObject(ctx context.Context, parent client.Object) context.Context {
	return context.WithValue(ctx, parentObjectKey{}, parent)
}

// ParentObjectFromContext returns the validated parent, when the resource has one.
func ParentObjectFromContext(ctx context.Context) (client.Object, bool) {
	parent, ok := ctx.Value(parentObjectKey{}).(client.Object)
	return parent, ok
}

// CustomDefaulter defines an interface for setting defaults on API objects.
// Implement this on your API type to run custom defaulting logic
// after schema-based defaults have been applied.
type CustomDefaulter interface {
	Default(ctx context.Context) error
}

// CustomValidator defines an interface for validating API objects.
// Implement this on your API type to run custom validation logic
// after schema-based validation has passed.
type CustomValidator interface {
	ValidateCreate(ctx context.Context) error
	ValidateUpdate(ctx context.Context, oldObj runtime.Object) error
	ValidateDelete(ctx context.Context) error
}
