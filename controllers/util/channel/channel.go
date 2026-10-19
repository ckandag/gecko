// Package channel maps Channel changes to Clusters that use them.
package channel

import (
	"context"

	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// DefaultChannelGroup is used when a Cluster omits spec.release.channelGroup.
	DefaultChannelGroup = "stable"
	// ClusterChannelGroupField is the cache index for a Cluster's effective channel group.
	ClusterChannelGroupField = "spec.release.channelGroup"
)

// ClusterChannelGroup returns the effective channel group for cache indexing.
func ClusterChannelGroup(obj client.Object) []string {
	cluster, ok := obj.(*privatev1.Cluster)
	if !ok {
		return nil
	}
	group := cluster.Spec.Release.ChannelGroup
	if group == "" {
		group = DefaultChannelGroup
	}
	return []string{group}
}

// ToClusters maps a Channel change to Clusters in its indexed channel group.
func ToClusters(apiClient client.Client, log logger.Logger) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		changed, ok := obj.(*privatev1.Channel)
		if !ok {
			return nil
		}

		var clusters privatev1.ClusterList
		if err := apiClient.List(ctx, &clusters, client.MatchingFields{ClusterChannelGroupField: changed.Name}); err != nil {
			// MapFunc cannot return an error. Both consumers periodically requeue
			// Clusters, so a missed Channel event converges on their next reconcile.
			log.Errorf(ctx, "list Clusters for Channel %s: %v", changed.Name, err)
			return nil
		}

		requests := make([]reconcile.Request, 0, len(clusters.Items))
		for i := range clusters.Items {
			cluster := &clusters.Items[i]
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKey{
				Namespace: cluster.Namespace,
				Name:      cluster.Name,
			}})
		}
		return requests
	}
}
