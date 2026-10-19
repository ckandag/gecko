package channel

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestClusterChannelGroup(t *testing.T) {
	tests := []struct {
		name     string
		object   client.Object
		expected []string
	}{
		{name: "omitted uses stable", object: &privatev1.Cluster{}, expected: []string{DefaultChannelGroup}},
		{name: "explicit channel", object: &privatev1.Cluster{Spec: privatev1.ClusterSpec{Release: privatev1.ReleaseSpec{ChannelGroup: "candidate"}}}, expected: []string{"candidate"}},
		{name: "other resource", object: &privatev1.Channel{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, ClusterChannelGroup(tt.object))
		})
	}
}

func TestToClustersUsesChannelIndex(t *testing.T) {
	apiClient := fake.NewClientBuilder().WithScheme(setup.NewScheme()).WithObjects(
		&privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "project-a", Name: "default-stable"}},
		&privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "project-b", Name: "explicit-stable"},
			Spec: privatev1.ClusterSpec{Release: privatev1.ReleaseSpec{ChannelGroup: "stable"}}},
		&privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "project-a", Name: "candidate"},
			Spec: privatev1.ClusterSpec{Release: privatev1.ReleaseSpec{ChannelGroup: "candidate"}}},
	).WithIndex(&privatev1.Cluster{}, ClusterChannelGroupField, ClusterChannelGroup).Build()
	log, err := logger.NewLogger(logger.Config{Level: "error", Format: logger.FormatText, Writer: &bytes.Buffer{}, Component: "test"})
	require.NoError(t, err)

	mapChannel := ToClusters(apiClient, log)
	require.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: client.ObjectKey{Namespace: "project-a", Name: "default-stable"}},
		{NamespacedName: client.ObjectKey{Namespace: "project-b", Name: "explicit-stable"}},
	}, mapChannel(context.Background(), &privatev1.Channel{ObjectMeta: metav1.ObjectMeta{Name: "stable"}}))
	require.Equal(t, []reconcile.Request{
		{NamespacedName: client.ObjectKey{Namespace: "project-a", Name: "candidate"}},
	}, mapChannel(context.Background(), &privatev1.Channel{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}}))
	require.Empty(t, mapChannel(context.Background(), &privatev1.Cluster{}))
}

type failingListClient struct{ client.Client }

func (failingListClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("list unavailable")
}

func TestToClustersLogsListError(t *testing.T) {
	var output bytes.Buffer
	log, err := logger.NewLogger(logger.Config{
		Level: "error", Format: logger.FormatText, Writer: &output, Component: "test",
	})
	require.NoError(t, err)

	requests := ToClusters(failingListClient{}, log)(context.Background(),
		&privatev1.Channel{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}})

	require.Empty(t, requests)
	require.Contains(t, output.String(), "list Clusters for Channel candidate: list unavailable")
}
