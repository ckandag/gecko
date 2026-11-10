package main

import (
	"context"
	"fmt"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/conversion"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
)

//nolint:kubeapilinter // Runtime storage dependencies are not serialized API fields.
type nodePoolUpgradeValidator struct {
	clusters storage.ResourceStore
	versions storage.ResourceStore
	scheme   *runtime.Scheme
}

func newNodePoolUpgradeValidator(factory apiserver.StorageFactory, scheme *runtime.Scheme) (*nodePoolUpgradeValidator, error) {
	getStore := func(kind string) (storage.ResourceStore, error) {
		gvk := privatev1.GroupVersion.WithKind(kind)
		store, err := factory(apiserver.GroupKindResourceType(gvk.GroupKind()), scheme, gvk)
		if err != nil {
			return nil, fmt.Errorf("create %s validation store: %w", kind, err)
		}
		if store == nil {
			return nil, fmt.Errorf("create %s validation store: nil store", kind)
		}
		return store, nil
	}
	clusters, err := getStore("Cluster")
	if err != nil {
		return nil, err
	}
	versions, err := getStore("Version")
	if err != nil {
		return nil, err
	}
	return &nodePoolUpgradeValidator{clusters: clusters, versions: versions, scheme: scheme}, nil
}

func (v *nodePoolUpgradeValidator) cluster(ctx context.Context, pool *privatev1.NodePool) (*privatev1.Cluster, error) {
	obj, err := v.clusters.Get(ctx, pool.Namespace, pool.Spec.ClusterID)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("the referenced Cluster does not exist")
	}
	if err != nil {
		return nil, fmt.Errorf("read Cluster for upgrade validation: %w", err)
	}
	typed, err := conversion.TypedOldObject(v.scheme, privatev1.GroupVersion.WithKind("Cluster"), obj)
	if err != nil {
		return nil, fmt.Errorf("decode Cluster for upgrade validation: %w", err)
	}
	cluster, ok := typed.(*privatev1.Cluster)
	if !ok {
		return nil, fmt.Errorf("the stored Cluster has an unexpected type")
	}
	return cluster, nil
}

func (v *nodePoolUpgradeValidator) Validate(ctx context.Context, pool *privatev1.NodePool, target string) error {
	cluster, err := v.cluster(ctx, pool)
	if err != nil {
		return err
	}
	obj, err := v.versions.Get(ctx, "", target)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("the target version is not in the platform version catalog")
	}
	if err != nil {
		return fmt.Errorf("read target version for upgrade validation: %w", err)
	}
	typed, err := conversion.TypedOldObject(v.scheme, privatev1.GroupVersion.WithKind("Version"), obj)
	if err != nil {
		return fmt.Errorf("decode target version for upgrade validation: %w", err)
	}
	release, ok := typed.(*privatev1.Version)
	if !ok {
		return fmt.Errorf("the stored Version has an unexpected type")
	}
	return privatev1.ValidateNodePoolUpgradeTarget(pool, cluster, release, target)
}
