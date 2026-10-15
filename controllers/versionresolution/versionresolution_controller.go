package versionresolution

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	adapterName = "version-resolution-controller"
	// DefaultChannelGroup is used when a Cluster does not specify a channel group.
	DefaultChannelGroup = "stable"
	requeueStable       = 5 * time.Minute
)

// Reconciler resolves the OCP release image for a cluster via Cincinnati.
type Reconciler struct {
	cincinnati *CincinnatiClient
	log        logger.Logger
	client     client.Client
}

// NewReconciler creates a new version-resolution Reconciler.
func NewReconciler(cincinnati *CincinnatiClient, log logger.Logger, c client.Client) *Reconciler {
	return &Reconciler{
		cincinnati: cincinnati,
		log:        log,
		client:     c,
	}
}

// Reconcile runs the version-resolution loop for one cluster event.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	clusterID := req.Name

	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.log.Infof(ctx, "vr: cluster %s not found, skipping", clusterID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("vr: get cluster %s: %w", clusterID, err)
	}

	if cluster.Spec.Release.Version == "" {
		r.log.Infof(ctx, "vr: cluster %s: release version not set, waiting for next event", clusterID)
		if meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "VersionResolved",
			Status:             metav1.ConditionUnknown,
			Reason:             "ReleaseVersionNotSet",
			Message:            "Release version not set in spec",
			ObservedGeneration: cluster.Generation,
		}) {
			if err := r.client.Status().Update(ctx, &cluster); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("vr: update cluster status %s: %w", clusterID, err)
			}
		}
		return reconcile.Result{}, nil
	}
	version := cluster.Spec.Release.Version

	channelGroup := DefaultChannelGroup
	if cluster.Spec.Release.ChannelGroup != "" {
		channelGroup = cluster.Spec.Release.ChannelGroup
	}

	channelVersion := version
	var platformChannel privatev1.Channel
	if err := r.client.Get(ctx, client.ObjectKey{Name: channelGroup}, &platformChannel); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("vr: get channel %s for cluster %s: %w", channelGroup, clusterID, err)
		}
		r.log.Infof(ctx, "vr: channel %s not found, resolving cluster %s in its current minor channel", channelGroup, clusterID)
	} else {
		// TODO: Revisit whether version-resolution should advance the concrete
		// channel directly when progressive rollout adds per-cluster admission.
		channelVersion, err = nextChannelVersion(version, platformChannel.Spec.FleetMinorVersion)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("vr: select channel version for cluster %s: %w", clusterID, err)
		}
	}

	channel, err := buildChannel(channelVersion, channelGroup)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("vr: build channel for cluster %s: %w", clusterID, err)
	}

	// Check already resolved — version, channel group, and concrete Cincinnati
	// channel must all match to skip re-resolution.
	if vr := cluster.Status.VersionResolution; vr != nil &&
		vr.ReleaseVersion == version &&
		vr.ChannelGroup == channelGroup &&
		vr.CincinnatiChannel == channel {
		r.log.Infof(ctx, "vr: cluster %s: version %s channel %s already resolved, checking again later", clusterID, version, channelGroup)
		return reconcile.Result{RequeueAfter: requeueStable}, nil
	}
	r.log.Infof(ctx, "vr: cluster %s: resolving version %s via channel %s", clusterID, version, channel)

	info, err := r.cincinnati.Resolve(ctx, version, channel)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("vr: cincinnati resolve for cluster %s: %w", clusterID, err)
	}
	if info == nil {
		r.log.Warnf(ctx, "vr: cluster %s: version %s not found in Cincinnati, retrying later", clusterID, version)
		if meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               "VersionResolved",
			Status:             metav1.ConditionFalse,
			Reason:             "VersionNotFoundInCincinnati",
			Message:            fmt.Sprintf("Version %s not found in Cincinnati channel %s", version, channel),
			ObservedGeneration: cluster.Generation,
		}) {
			if err := r.client.Status().Update(ctx, &cluster); err != nil && !apierrors.IsConflict(err) {
				return reconcile.Result{}, fmt.Errorf("vr: update cluster status %s: %w", clusterID, err)
			}
		}
		return reconcile.Result{RequeueAfter: requeueStable}, nil
	}

	// Write VR result and VersionResolved condition to status.
	cluster.Status.VersionResolution = &privatev1.VersionResolutionResult{
		ReleaseImage:      info.Payload,
		ReleaseVersion:    info.Version,
		CincinnatiChannel: channel,
		ChannelGroup:      channelGroup,
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "VersionResolved",
		Status:             metav1.ConditionTrue,
		Reason:             "VersionResolved",
		Message:            fmt.Sprintf("Version %s resolved to image %s", version, info.Payload),
		ObservedGeneration: cluster.Generation,
	})
	if err := r.client.Status().Update(ctx, &cluster); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("vr: update cluster status %s: %w", clusterID, err)
	}

	r.log.Infof(ctx, "vr: cluster %s: resolved version %s", clusterID, version)
	return reconcile.Result{RequeueAfter: requeueStable}, nil
}

// buildChannel constructs the Cincinnati channel name from a version string and channel group.
// e.g. "4.22.0-ec.4" + "stable" → "stable-4.22"
func buildChannel(version, channelGroup string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid version %q: expected at least major.minor", version)
	}
	return fmt.Sprintf("%s-%s.%s", channelGroup, parts[0], parts[1]), nil
}

// nextChannelVersion returns a version in the minor channel that CVO should
// query. Minor upgrades are exposed one at a time even when the fleet minor is
// more than one release ahead of the cluster.
func nextChannelVersion(version, fleetMinorVersion string) (string, error) {
	if fleetMinorVersion == "" {
		return version, nil
	}

	current, err := utilversion.ParseSemantic(version)
	if err != nil {
		return "", fmt.Errorf("parse current version %q: %w", version, err)
	}
	fleetMinor, err := utilversion.ParseSemantic(fleetMinorVersion + ".0")
	if err != nil {
		return "", fmt.Errorf("parse fleet minor version %q: %w", fleetMinorVersion, err)
	}

	if current.Major() != fleetMinor.Major() || current.Minor() >= fleetMinor.Minor() {
		return version, nil
	}

	return fmt.Sprintf("%d.%d.0", current.Major(), current.Minor()+1), nil
}
