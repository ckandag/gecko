package main

import (
	"fmt"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
	publicv1 "github.com/openshift-online/gecko/platform-api/api/public/v1"

	"k8s.io/apimachinery/pkg/runtime"
)

var parentResourcesByKind = map[string]types.ParentResourceInfo{
	"NodePool": {
		Plural:    "clusters",
		GroupKind: privatev1.GroupVersion.WithKind("Cluster").GroupKind(),
		IDField:   "spec.clusterID",
	},
}

// getPrivateResources returns the resource definitions for the private API.
func getPrivateResources() []types.ResourceInfo {
	return configureParentResources(privatev1.GetResourceInfos())
}

// getPublicResources returns the resource definitions for the public API.
func getPublicResources() []types.ResourceInfo {
	return configureParentResources(publicv1.GetResourceInfos())
}

func configureParentResources(resources []types.ResourceInfo) []types.ResourceInfo {
	for i := range resources {
		parent, found := parentResourcesByKind[resources[i].GVK.Kind]
		if found {
			parentCopy := parent
			resources[i].ParentResource = &parentCopy
		}
	}
	return resources
}

// getPrivateScheme creates and returns a runtime.Scheme with private API types registered.
func getPrivateScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := privatev1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("failed to register private API types: %v", err))
	}
	return scheme
}

// getPublicScheme creates and returns a runtime.Scheme with public API types registered.
func getPublicScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	if err := publicv1.AddToScheme(scheme); err != nil {
		panic(fmt.Sprintf("failed to register public API types: %v", err))
	}
	return scheme
}
