package versionresolution

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/util/logger"
	"github.com/openshift-online/gecko/controllers/util/setup"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ---- helpers ----------------------------------------------------------------

func newTestLogger(t *testing.T) logger.Logger {
	t.Helper()
	log, err := logger.NewLogger(logger.Config{
		Level:     "error",
		Format:    logger.FormatText,
		Component: "test",
		Version:   "test",
	})
	require.NoError(t, err)
	return log
}

// mockStatusWriter captures status update calls.
type mockStatusWriter struct {
	updateErr error
	called    bool
	captured  client.Object
}

func (m *mockStatusWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	m.called = true
	m.captured = obj
	return m.updateErr
}
func (m *mockStatusWriter) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return nil
}
func (m *mockStatusWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return nil
}
func (m *mockStatusWriter) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
	return nil
}

// mockStoreClient is a minimal client.Client backed by a fixed Cluster.
type mockStoreClient struct {
	cluster      *privatev1.Cluster
	channel      *privatev1.Channel
	version      *privatev1.Version
	versionErr   error
	getErr       error
	updateCalled bool
	statusWriter *mockStatusWriter
}

func (m *mockStoreClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m.getErr != nil {
		return m.getErr
	}
	switch typed := obj.(type) {
	case *privatev1.Cluster:
		if m.cluster == nil {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "cluster"}, key.Name)
		}
		m.cluster.DeepCopyInto(typed)
		return nil
	case *privatev1.Version:
		if m.versionErr != nil {
			return m.versionErr
		}
		if m.version == nil || m.version.Name != key.Name {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "versions"}, key.Name)
		}
		m.version.DeepCopyInto(typed)
		return nil
	case *privatev1.Channel:
		if m.channel == nil || m.channel.Name != key.Name {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "channel"}, key.Name)
		}
		m.channel.DeepCopyInto(typed)
		return nil
	default:
		return fmt.Errorf("unexpected type %T", obj)
	}
}

func (m *mockStoreClient) Update(_ context.Context, _ client.Object, _ ...client.UpdateOption) error {
	m.updateCalled = true
	return nil
}

func (m *mockStoreClient) Status() client.SubResourceWriter {
	if m.statusWriter == nil {
		m.statusWriter = &mockStatusWriter{}
	}
	return m.statusWriter
}

func (m *mockStoreClient) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}
func (m *mockStoreClient) Create(_ context.Context, _ client.Object, _ ...client.CreateOption) error {
	return nil
}
func (m *mockStoreClient) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return nil
}
func (m *mockStoreClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return nil
}
func (m *mockStoreClient) DeleteAllOf(_ context.Context, _ client.Object, _ ...client.DeleteAllOfOption) error {
	return nil
}
func (m *mockStoreClient) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return nil
}
func (m *mockStoreClient) SubResource(_ string) client.SubResourceClient { return nil }
func (m *mockStoreClient) Scheme() *runtime.Scheme                       { return nil }
func (m *mockStoreClient) RESTMapper() meta.RESTMapper                   { return nil }
func (m *mockStoreClient) GroupVersionKindFor(_ runtime.Object) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, nil
}
func (m *mockStoreClient) IsObjectNamespaced(_ runtime.Object) (bool, error) { return false, nil }

func clusterReq(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: "hyperfleet", Name: name}}
}

func catalogVersion(version, image string, groups ...string) *privatev1.Version {
	return &privatev1.Version{ObjectMeta: metav1.ObjectMeta{Name: version},
		Spec: privatev1.VersionSpec{ReleaseImage: image, ChannelGroups: groups}}
}

func buildReconciler(t *testing.T, cluster *privatev1.Cluster, release *privatev1.Version) (*Reconciler, *mockStoreClient) {
	t.Helper()
	store := &mockStoreClient{cluster: cluster, version: release}
	return NewReconciler(newTestLogger(t), store), store
}

func TestReconciler_HappyPath(t *testing.T) {
	for _, group := range []string{"", "stable", "fast", "candidate", "dev-preview"} {
		t.Run(group, func(t *testing.T) {
			effectiveGroup := group
			if effectiveGroup == "" {
				effectiveGroup = "stable"
			}
			cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "hyperfleet", Generation: 3}}
			cluster.Spec.Release = privatev1.ReleaseSpec{Version: "5.0.0-ec.6", ChannelGroup: group}
			release := catalogVersion("5.0.0-ec.6", "quay.io/ocp@sha256:original", effectiveGroup)
			r, store := buildReconciler(t, cluster, release)
			result, err := r.Reconcile(context.Background(), clusterReq(cluster.Name))
			require.NoError(t, err)
			require.Equal(t, requeueStable, result.RequeueAfter)
			require.False(t, store.updateCalled)
			updated := store.statusWriter.captured.(*privatev1.Cluster)
			require.Equal(t, &privatev1.VersionResolutionResult{ReleaseImage: release.Spec.ReleaseImage,
				ReleaseVersion: release.Name, ChannelGroup: effectiveGroup, CincinnatiChannel: effectiveGroup + "-5.0"}, updated.Status.VersionResolution)
			condition := meta.FindStatusCondition(updated.Status.Conditions, "VersionResolved")
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionTrue, condition.Status)
			require.Equal(t, int64(3), condition.ObservedGeneration)
		})
	}
}

func TestReconciler_ClusterNotFound(t *testing.T) {
	r, _ := buildReconciler(t, nil, nil)
	result, err := r.Reconcile(context.Background(), clusterReq("missing"))
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
}

func TestReconciler_ClusterGetError(t *testing.T) {
	r, store := buildReconciler(t, nil, nil)
	store.getErr = errors.New("API unavailable")
	_, err := r.Reconcile(context.Background(), clusterReq("cluster"))
	require.ErrorContains(t, err, "get cluster")
	require.Nil(t, store.statusWriter)
}

func TestReconciler_VersionNotSet(t *testing.T) {
	r, store := buildReconciler(t, &privatev1.Cluster{}, nil)
	result, err := r.Reconcile(context.Background(), clusterReq("cluster"))
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)
	updated := store.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, "ReleaseVersionNotSet", meta.FindStatusCondition(updated.Status.Conditions, "VersionResolved").Reason)
}

func TestReconciler_CatalogUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release *privatev1.Version
		reason  string
	}{
		{"missing", nil, "VersionNotFoundInCatalog"},
		{"wrong channel", catalogVersion("4.22.14", "image", "fast"), "VersionNotInChannelGroup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &privatev1.Cluster{}
			cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.22.14"}
			r, store := buildReconciler(t, cluster, tc.release)
			result, err := r.Reconcile(context.Background(), clusterReq("cluster"))
			require.NoError(t, err)
			require.Equal(t, reconcile.Result{}, result)
			updated := store.statusWriter.captured.(*privatev1.Cluster)
			require.Nil(t, updated.Status.VersionResolution)
			condition := meta.FindStatusCondition(updated.Status.Conditions, "VersionResolved")
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, tc.reason, condition.Reason)
			require.Contains(t, condition.Message, "4.22.14")
		})
	}
}

func TestReconciler_CatalogErrorIsRetryable(t *testing.T) {
	cluster := &privatev1.Cluster{}
	cluster.Spec.Release.Version = "4.22.14"
	r, store := buildReconciler(t, cluster, nil)
	store.versionErr = errors.New("cache not synced")
	_, err := r.Reconcile(context.Background(), clusterReq("cluster"))
	require.ErrorIs(t, err, store.versionErr)
	require.Nil(t, store.statusWriter, "infrastructure errors must not set a terminal False condition")

	store.versionErr = nil
	store.version = catalogVersion("4.22.14", "recovered-image", "stable")
	_, err = r.Reconcile(context.Background(), clusterReq("cluster"))
	require.NoError(t, err)
	updated := store.statusWriter.captured.(*privatev1.Cluster)
	require.Equal(t, "recovered-image", updated.Status.VersionResolution.ReleaseImage)
}

func TestReconciler_StatusUpdateError(t *testing.T) {
	for _, release := range []*privatev1.Version{nil, catalogVersion("4.22.14", "image", "stable")} {
		cluster := &privatev1.Cluster{}
		cluster.Spec.Release.Version = "4.22.14"
		r, store := buildReconciler(t, cluster, release)
		store.statusWriter = &mockStatusWriter{updateErr: errors.New("API unavailable")}
		_, err := r.Reconcile(context.Background(), clusterReq("cluster"))
		require.ErrorContains(t, err, "update cluster status")
	}
}

func TestReconciler_CatalogChangesPreservePinnedImage(t *testing.T) {
	ctx := context.Background()
	cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "hyperfleet"}}
	cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.22.14", ChannelGroup: "candidate"}
	release := catalogVersion("4.22.14", "original", "candidate")
	channel := &privatev1.Channel{ObjectMeta: metav1.ObjectMeta{Name: "candidate"}}
	channel.Spec.FleetMinorVersion = "4.22"
	apiClient := fake.NewClientBuilder().WithScheme(setup.NewScheme()).WithStatusSubresource(cluster).
		WithObjects(cluster, release, channel).Build()
	r := NewReconciler(newTestLogger(t), apiClient)
	_, err := r.Reconcile(ctx, clusterReq("cluster"))
	require.NoError(t, err)
	release.Spec.ReleaseImage = "changed"
	require.NoError(t, apiClient.Update(ctx, release))
	_, err = r.Reconcile(ctx, clusterReq("cluster"))
	require.NoError(t, err)
	require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	require.Equal(t, "original", cluster.Status.VersionResolution.ReleaseImage)
	channel.Spec.FleetMinorVersion = "4.24"
	require.NoError(t, apiClient.Update(ctx, channel))
	_, err = r.Reconcile(ctx, clusterReq("cluster"))
	require.NoError(t, err)
	require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	require.Equal(t, "candidate-4.23", cluster.Status.VersionResolution.CincinnatiChannel)
	require.Equal(t, "original", cluster.Status.VersionResolution.ReleaseImage)
	require.NoError(t, apiClient.Delete(ctx, release))
	_, err = r.Reconcile(ctx, clusterReq("cluster"))
	require.NoError(t, err)
	require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	require.Equal(t, "original", cluster.Status.VersionResolution.ReleaseImage)
}

func TestReconciler_CatalogRejectionRequiresResubmission(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			ctx := context.Background()
			cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "hyperfleet", Generation: 1}}
			cluster.Spec.Release = privatev1.ReleaseSpec{Version: "5.0.0-ec.6", ChannelGroup: "dev-preview"}
			release := catalogVersion("5.0.0-ec.6", "ci-image", "stable")
			objects := []client.Object{cluster}
			if !missing {
				objects = append(objects, release)
			}
			apiClient := fake.NewClientBuilder().WithScheme(setup.NewScheme()).WithStatusSubresource(cluster).WithObjects(objects...).Build()
			r := NewReconciler(newTestLogger(t), apiClient)
			result, err := r.Reconcile(ctx, clusterReq("cluster"))
			require.NoError(t, err)
			require.Equal(t, reconcile.Result{}, result, "catalog rejection must not schedule a retry")
			require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
			require.Nil(t, cluster.Status.VersionResolution)
			rejected := *meta.FindStatusCondition(cluster.Status.Conditions, "VersionResolved")
			require.Equal(t, metav1.ConditionFalse, rejected.Status)
			require.Equal(t, int64(1), rejected.ObservedGeneration)

			release.Spec.ChannelGroups = []string{"dev-preview"}
			if missing {
				require.NoError(t, apiClient.Create(ctx, release))
			} else {
				require.NoError(t, apiClient.Update(ctx, release))
			}
			// A new reconciler simulates restart or an unrelated status/Channel event.
			r = NewReconciler(newTestLogger(t), apiClient)
			result, err = r.Reconcile(ctx, clusterReq("cluster"))
			require.NoError(t, err)
			require.Equal(t, reconcile.Result{}, result)
			require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
			require.Nil(t, cluster.Status.VersionResolution, "publishing the release must not revive a rejected request")
			require.Equal(t, rejected, *meta.FindStatusCondition(cluster.Status.Conditions, "VersionResolved"))

			// A spec change starts a new resolution attempt.
			cluster.Spec.Release.ChannelGroup = "nightly"
			cluster.Generation++
			require.NoError(t, apiClient.Update(ctx, cluster))
			release.Spec.ChannelGroups = append(release.Spec.ChannelGroups, "nightly")
			require.NoError(t, apiClient.Update(ctx, release))
			result, err = r.Reconcile(ctx, clusterReq("cluster"))
			require.NoError(t, err)
			require.Equal(t, requeueStable, result.RequeueAfter)
			require.NoError(t, apiClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
			require.Equal(t, "ci-image", cluster.Status.VersionResolution.ReleaseImage)
			require.True(t, meta.IsStatusConditionTrue(cluster.Status.Conditions, "VersionResolved"))
		})
	}
}

func TestBuildChannel(t *testing.T) {
	cases := []struct {
		version string
		want    string
		wantErr bool
	}{
		{"4.22.0-ec.4", "candidate-4.22", false},
		{"4.16.3", "candidate-4.16", false},
		{"4.15.0", "candidate-4.15", false},
		{"invalid", "", true},
		{"4", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			got, err := buildChannel(tc.version, "candidate")
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestNextChannelVersion(t *testing.T) {
	cases := []struct {
		name       string
		version    string
		fleetMinor string
		want       string
	}{
		{name: "same minor", version: "4.22.14", fleetMinor: "4.22", want: "4.22.14"},
		{name: "next minor", version: "4.22.14", fleetMinor: "4.23", want: "4.23.0"},
		{name: "one minor at a time", version: "4.22.14", fleetMinor: "4.24", want: "4.23.0"},
		{name: "different major", version: "4.22.14", fleetMinor: "5.0", want: "4.22.14"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nextChannelVersion(tc.version, tc.fleetMinor)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReconciler_PendingReleaseKeepsObservedImage(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(fmt.Sprint(approved), func(t *testing.T) {
			cluster := &privatev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "hyperfleet", Generation: 2}}
			cluster.Spec.Release = privatev1.ReleaseSpec{Version: "4.23.9", ChannelGroup: "stable"}
			cluster.Status.HostedClusterResult = &privatev1.HostedClusterResult{Version: "4.22.19", DesiredVersion: "4.22.19"}
			target := "4.22.19"
			if approved {
				target = "4.23.9"
				cluster.Status.ControlPlaneUpgrade = &privatev1.ControlPlaneUpgradeResult{TargetVersion: target}
				cluster.Status.Conditions = []metav1.Condition{{Type: privatev1.ControlPlaneUpgradeAccepted, Status: metav1.ConditionTrue}}
			}
			r, store := buildReconciler(t, cluster, catalogVersion(target, "selected-image", "stable"))
			_, err := r.Reconcile(t.Context(), clusterReq(cluster.Name))
			require.NoError(t, err)
			resolved := store.statusWriter.captured.(*privatev1.Cluster).Status.VersionResolution
			require.Equal(t, target, resolved.ReleaseVersion)
			require.Equal(t, "selected-image", resolved.ReleaseImage)
			require.Equal(t, "4.23.9", cluster.Spec.Release.Version)
		})
	}
}
