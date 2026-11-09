package hc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestNodePoolToCluster(t *testing.T) {
	nodePool := &privatev1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "nodepool"},
		Spec:       privatev1.NodePoolSpec{ClusterID: "cluster"},
	}

	requests := nodePoolToCluster(context.Background(), nodePool)
	require.Len(t, requests, 1)
	require.Equal(t, "test", requests[0].Namespace)
	require.Equal(t, "cluster", requests[0].Name)
	require.Empty(t, nodePoolToCluster(context.Background(), &privatev1.NodePool{}))
}

func TestNodePoolClusterID(t *testing.T) {
	require.Equal(t, []string{"cluster"}, nodePoolClusterID(&privatev1.NodePool{
		Spec: privatev1.NodePoolSpec{ClusterID: "cluster"},
	}))
	require.Empty(t, nodePoolClusterID(&privatev1.NodePool{}))
}

func TestNodePoolDeletePredicate(t *testing.T) {
	predicate := nodePoolDeletePredicate()
	nodePool := &privatev1.NodePool{}

	require.False(t, predicate.Create(event.CreateEvent{Object: nodePool}))
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: nodePool, ObjectNew: nodePool}))
	require.False(t, predicate.Generic(event.GenericEvent{Object: nodePool}))
	require.True(t, predicate.Delete(event.DeleteEvent{Object: nodePool}))
}
