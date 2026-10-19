package controlplaneupgrade

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type failingListClient struct {
	client.Client
}

func (failingListClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("list unavailable")
}

func TestChannelToClusters_LogsListError(t *testing.T) {
	var output bytes.Buffer
	log, err := logger.NewLogger(logger.Config{
		Level: "error", Format: logger.FormatText, Writer: &output, Component: "test",
	})
	require.NoError(t, err)

	requests := channelToClusters(failingListClient{}, log)(context.Background(),
		&privatev1.Channel{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}})

	require.Empty(t, requests)
	require.Contains(t, output.String(), "list Clusters for Channel candidate: list unavailable")
}
