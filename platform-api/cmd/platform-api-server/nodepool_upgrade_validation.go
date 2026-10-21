package main

import (
	"context"
	"fmt"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
)

const defaultNodePoolUpgradeChannelGroup = "candidate"

//nolint:kubeapilinter // Runtime storage dependencies are not serialized API fields.
type nodePoolUpgradeValidator struct {
	clusters  storage.ResourceStore
	nodePools storage.ResourceStore
	versions  storage.ResourceStore
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
	nodePools, err := getStore("NodePool")
	if err != nil {
		return nil, err
	}
	versions, err := getStore("Version")
	if err != nil {
		return nil, err
	}
	return &nodePoolUpgradeValidator{clusters: clusters, nodePools: nodePools, versions: versions}, nil
}

func (v *nodePoolUpgradeValidator) Validate(ctx context.Context, request *privatev1.NodePoolUpgradeRequest) error {
	clusterObj, err := v.clusters.Get(ctx, request.Namespace, request.Spec.ClusterID)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("cluster %q does not exist", request.Spec.ClusterID)
	}
	if err != nil {
		return fmt.Errorf("get Cluster %q: %w", request.Spec.ClusterID, err)
	}
	cluster, ok := clusterObj.(*privatev1.Cluster)
	if !ok {
		return fmt.Errorf("stored Cluster %q has unexpected type %T", request.Spec.ClusterID, clusterObj)
	}
	if cluster.DeletionTimestamp != nil {
		return fmt.Errorf("cluster %q is being deleted", request.Spec.ClusterID)
	}

	poolObj, err := v.nodePools.Get(ctx, request.Namespace, request.Spec.NodePoolID)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("nodepool %q does not exist", request.Spec.NodePoolID)
	}
	if err != nil {
		return fmt.Errorf("get NodePool %q: %w", request.Spec.NodePoolID, err)
	}
	pool, ok := poolObj.(*privatev1.NodePool)
	if !ok {
		return fmt.Errorf("stored NodePool %q has unexpected type %T", request.Spec.NodePoolID, poolObj)
	}
	if pool.DeletionTimestamp != nil {
		return fmt.Errorf("nodepool %q is being deleted", request.Spec.NodePoolID)
	}
	if pool.Spec.ClusterID != request.Spec.ClusterID {
		return fmt.Errorf("nodepool %q does not belong to cluster %q", request.Spec.NodePoolID, request.Spec.ClusterID)
	}

	releaseObj, err := v.versions.Get(ctx, "", request.Spec.TargetVersion)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("target version %q is not in the platform version catalog", request.Spec.TargetVersion)
	}
	if err != nil {
		return fmt.Errorf("get target version %q: %w", request.Spec.TargetVersion, err)
	}
	release, ok := releaseObj.(*privatev1.Version)
	if !ok {
		return fmt.Errorf("stored Version %q has unexpected type %T", request.Spec.TargetVersion, releaseObj)
	}

	channelGroup := pool.Spec.Release.ChannelGroup
	if channelGroup == "" {
		channelGroup = defaultNodePoolUpgradeChannelGroup
	}
	if !containsChannelGroup(release.Spec.ChannelGroups, channelGroup) {
		return fmt.Errorf("target version %q is not available in channel group %q", request.Spec.TargetVersion, channelGroup)
	}
	if pool.Status.CurrentVersion == "" || cluster.Status.HostedClusterResult == nil ||
		cluster.Status.HostedClusterResult.Version == "" {
		return fmt.Errorf("nodepool and control-plane versions must be observed before requesting an upgrade")
	}
	if pool.Spec.Release.Version != pool.Status.CurrentVersion {
		return fmt.Errorf("nodepool %q already has an upgrade in progress", request.Spec.NodePoolID)
	}

	current, err := utilversion.ParseSemantic(pool.Status.CurrentVersion)
	if err != nil {
		return fmt.Errorf("observed NodePool version %q is invalid: %w", pool.Status.CurrentVersion, err)
	}
	target, err := utilversion.ParseSemantic(request.Spec.TargetVersion)
	if err != nil {
		return fmt.Errorf("target version %q is invalid: %w", request.Spec.TargetVersion, err)
	}
	controlPlane, err := utilversion.ParseSemantic(cluster.Status.HostedClusterResult.Version)
	if err != nil {
		return fmt.Errorf("observed control-plane version %q is invalid: %w", cluster.Status.HostedClusterResult.Version, err)
	}
	if !current.LessThan(target) {
		return fmt.Errorf("target version must be newer than the observed NodePool version")
	}
	if controlPlane.LessThan(target) {
		return fmt.Errorf("target version must not be newer than the observed control-plane version")
	}
	if !privatev1.ValidNodePoolUpgradeStep(current, target, channelGroup) {
		return fmt.Errorf("target version must be a z-stream or next-minor upgrade, except an EUS-to-EUS step")
	}
	return nil
}

func containsChannelGroup(groups []string, channelGroup string) bool {
	for _, group := range groups {
		if group == channelGroup {
			return true
		}
	}
	return false
}
