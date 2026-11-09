// Package controlplaneupgrade coordinates automatic and customer-requested
// control-plane upgrades through Cluster release intent and conditions.
package controlplaneupgrade

import (
	"context"
	"fmt"
	"time"

	channelutil "github.com/openshift-online/gecko/controllers/util/channel"
	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	adapterName           = "control-plane-upgrade-controller"
	conditionAvailable    = "ControlPlaneUpgradeAvailable"
	conditionProgressing  = "ControlPlaneUpgradeProgressing"
	conditionDegraded     = "ControlPlaneUpgradeDegraded"
	targetSourceAutomatic = "automatic"
	targetSourceCustomer  = "customer"
	requeuePending        = 15 * time.Second
	requeueConflict       = time.Second
	requeueObserving      = 30 * time.Second
	requeueStable         = 5 * time.Minute
)

// AdmissionGate is the integration boundary for progressive rollout admission.
// A future capacity coordinator must make admission idempotent for this Cluster
// and target. False preserves desired spec and leaves all installers unapproved.
// No gate means unrestricted capacity, preserving current fleet behavior.
type AdmissionGate func(context.Context, *privatev1.Cluster, string) (bool, error)

// Reconciler owns upgrade decisions and conditions; HC feedback remains owned
// by the HostedCluster installer. Release normalization never grants approval.
type Reconciler struct {
	apiClient client.Client
	log       logger.Logger
	now       func() time.Time
	admission AdmissionGate
}

func NewReconciler(apiClient client.Client, log logger.Logger) *Reconciler {
	return &Reconciler{apiClient: apiClient, log: log, now: time.Now}
}

// WithAdmissionGate connects a capacity coordinator without changing the API.
func (r *Reconciler) WithAdmissionGate(gate AdmissionGate) *Reconciler { r.admission = gate; return r }

func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var cluster privatev1.Cluster
	if err := r.apiClient.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	hc := cluster.Status.HostedClusterResult
	if hc == nil || hc.Version == "" {
		if privatev1.IsMinorRelease(cluster.Spec.Release.Version) {
			return r.normalizeMinor(ctx, &cluster)
		}
		return r.writeStatus(ctx, &cluster, cluster.Status.ControlPlaneUpgrade, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionUnknown, "WaitingForHostedClusterFeedback", "Waiting for observed HostedCluster version feedback"),
			newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested", "Initial installation is awaiting version feedback"),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
	}
	completed, desired := hc.Version, hc.DesiredVersion
	if meta.IsStatusConditionTrue(cluster.Status.Conditions, privatev1.ControlPlaneUpgradeAccepted) {
		return r.observeUpgrade(ctx, &cluster)
	}
	// Existing HyperShift rollouts are observed, never replaced by new intent.
	if active, target := feedbackTarget(completed, desired); active {
		return r.observeExisting(ctx, &cluster, target)
	}
	if degraded := meta.FindStatusCondition(cluster.Status.Conditions, conditionDegraded); degraded != nil && degraded.Status == metav1.ConditionTrue && degraded.ObservedGeneration == cluster.Generation && degraded.Reason == "PreparationFailed" {
		return reconcile.Result{RequeueAfter: requeueStable}, nil
	}
	result := recordCompletion(cluster.Status.ControlPlaneUpgrade, completed)
	if !equality.Semantic.DeepEqual(cluster.Status.ControlPlaneUpgrade, result) {
		return r.writeStatus(ctx, &cluster, result, requeuePending,
			newCondition(conditionProgressing, metav1.ConditionFalse, "UpgradeCompleted", "The control plane reached its target version"))
	}
	if privatev1.IsMinorRelease(cluster.Spec.Release.Version) {
		return r.normalizeMinor(ctx, &cluster)
	}
	if cluster.Spec.Release.Version != completed {
		source := targetSourceCustomer
		if result != nil && result.TargetVersion == cluster.Spec.Release.Version && result.TargetSource == targetSourceAutomatic && result.CompletedTime == nil {
			source = targetSourceAutomatic
		}
		return r.approveUpgrade(ctx, &cluster, source)
	}
	return r.reconcileAutomatic(ctx, &cluster, result)
}

func (r *Reconciler) reconcileAutomatic(ctx context.Context, cluster *privatev1.Cluster, result *privatev1.ControlPlaneUpgradeResult) (reconcile.Result, error) {
	log := r.log
	hc := cluster.Status.HostedClusterResult
	completed, observed := hc.Version, hc.ObservedConditions
	available, err := r.eligibleUpdates(ctx, cluster)
	if err != nil {
		return reconcile.Result{}, err
	}
	// Prefer a newer patch in the Cluster's current major.minor stream. Z-stream
	// selection does not depend on Channel fleet-minor authorization.
	zCandidate, err := selectZStreamCandidate(completed, available)
	if err != nil {
		return r.writeStatus(ctx, cluster, result, requeueStable,
			newCondition(conditionAvailable, metav1.ConditionFalse, "TargetSelectionFailed", err.Error()),
			newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
				"No control-plane upgrade has been requested"),
			newCondition(conditionDegraded, metav1.ConditionTrue, "TargetSelectionFailed", err.Error()),
		)
	}

	candidate := zCandidate
	isYStream := false

	// Channel is consulted for Y-stream authorization even when a Z-stream update
	// is also available. Prefer an authorized Y-stream target so a patch update does
	// not consume an upgrade edge that is needed to enter the next minor version.
	var channel privatev1.Channel
	channelName := cluster.Spec.Release.ChannelGroup
	if channelName == "" {
		channelName = channelutil.DefaultChannelGroup
	}
	if err := r.apiClient.Get(ctx, client.ObjectKey{Name: channelName}, &channel); err != nil {
		if apierrors.IsNotFound(err) {
			if candidate == "" {
				log.Infof(ctx, "channel %q not found, skipping y-stream target selection", channelName)
				return r.writeStatus(ctx, cluster, result, requeuePending,
					newCondition(conditionAvailable, metav1.ConditionUnknown, "ChannelNotFound",
						fmt.Sprintf("Channel %q not found", channelName)),
					newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
						"No control-plane upgrade has been requested"),
					newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
				)
			}
			log.Infof(ctx, "channel %q not found, proceeding with z-stream target %s", channelName, candidate)
		} else {
			return reconcile.Result{}, fmt.Errorf("%s: get channel %q: %w", adapterName, channelName, err)
		}
	} else {
		yCandidate, err := selectYStreamCandidate(completed, available, channel.Spec.FleetMinorVersion)
		if err != nil {
			return r.writeStatus(ctx, cluster, result, requeueStable,
				newCondition(conditionAvailable, metav1.ConditionFalse, "TargetSelectionFailed", err.Error()),
				newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
					"No control-plane upgrade has been requested"),
				newCondition(conditionDegraded, metav1.ConditionTrue, "TargetSelectionFailed", err.Error()),
			)
		}
		// If CriticalUpgrade pins an exact version that is advertised, prefer it
		// over the y-stream candidate so the emergency patch is not blocked by
		// maintenance while the controller selects the minor-version upgrade instead.
		// Otherwise prefer the y-stream target so a patch update does not consume
		// an upgrade edge needed to enter the next minor version.
		// TODO: Revisit target priority when progressive rollout introduces an
		// explicit admission policy for choosing between available streams.
		criticalPin := ""
		if cluster.Spec.Maintenance != nil {
			criticalPin = cluster.Spec.Maintenance.CriticalUpgrade
		}
		// Apply the pin only when it is a valid z-stream update of completed
		// (same major.minor, strictly newer). This prevents an operator-set pin
		// in a different minor version from bypassing y-stream health gates and
		// Channel FleetMinorVersion authorization.
		if isValidCriticalPin(criticalPin, completed) && isAdvertised(criticalPin, available) {
			candidate = criticalPin
			isYStream = false
		} else if yCandidate != "" {
			candidate = yCandidate
			isYStream = true
		}
	}

	if candidate == "" {
		return r.writeStatus(ctx, cluster, result, requeueStable,
			newCondition(conditionAvailable, metav1.ConditionFalse, "NoUpgradeAvailable",
				fmt.Sprintf("No newer update is advertised for %s", completed)),
			newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
				"No control-plane upgrade has been requested"),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
	}

	// Health gates apply to both z-stream and y-stream
	if ok, reason, message := healthOK(cluster.Status.Conditions, observed, isYStream); !ok {
		return r.writeStatus(ctx, cluster, result, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionFalse, reason, message),
			newCondition(conditionProgressing, metav1.ConditionFalse, "UpgradeBlocked",
				fmt.Sprintf("Control-plane upgrade to %s is waiting for HostedCluster health", candidate)),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
	}

	// Check for critical security or platform overrides.
	override, _, _ := overrideCondition(cluster, candidate, completed)

	// Evaluate maintenance window/exclusions unless override applies
	if !override {
		permitted, reason, message, next := maintenancePermitted(cluster, r.now())
		if !permitted {
			return r.writeMaintenanceBlocked(ctx, cluster, result, completed, candidate, isYStream, reason, message, next)
		}
	}

	// Persist concrete intent before the approval/capacity gate. Application
	// remains blocked until approveUpgrade records acceptance.
	return r.stageAutomaticUpgrade(ctx, cluster, result, candidate)
}

func (r *Reconciler) writeMaintenanceBlocked(
	ctx context.Context,
	cluster *privatev1.Cluster,
	result *privatev1.ControlPlaneUpgradeResult,
	completed, candidate string,
	isYStream bool,
	reason, message string,
	next *time.Time,
) (reconcile.Result, error) {
	// Policy and Cluster watches wake us for changes; otherwise poll at a stable
	// interval or at the next known opening, whichever is sooner.
	requeue := requeueStable
	if next != nil {
		untilNext := next.Sub(r.now())
		if untilNext > 0 && untilNext < requeueStable {
			requeue = untilNext
		}
	}
	streamType := "z-stream"
	if isYStream {
		streamType = "y-stream"
	}
	availableStatus := metav1.ConditionTrue
	availableMessage := fmt.Sprintf("%s %s update %s is available but %s", streamType, completed, candidate, message)
	degradedStatus := metav1.ConditionFalse
	degradedReason := "AsExpected"
	degradedMessage := ""
	if reason == "InvalidMaintenanceWindow" {
		availableStatus = metav1.ConditionFalse
		availableMessage = fmt.Sprintf("Cannot start %s update to %s: %s", streamType, candidate, message)
		degradedStatus = metav1.ConditionTrue
		degradedReason = reason
		degradedMessage = message
	}
	return r.writeStatus(ctx, cluster, result, requeue,
		newCondition(conditionAvailable, availableStatus, reason, availableMessage),
		newCondition(conditionProgressing, metav1.ConditionFalse, reason, message),
		newCondition(conditionDegraded, degradedStatus, degradedReason, degradedMessage),
	)
}

// upgradeFailureReason reports only an explicit release rejection. Generic
// HostedCluster degradation and ClusterVersionSucceeding=False are expected to be
// transient during a healthy rollout and are not terminal upgrade signals.
func upgradeFailureReason(observed []metav1.Condition) (reason, message string, failed bool) {
	if c := meta.FindStatusCondition(observed, "ClusterVersionReleaseAccepted"); c != nil && c.Status == metav1.ConditionFalse {
		return "ReleaseNotAccepted", c.Message, true
	}
	return "", "", false
}

// healthOK reports whether a new upgrade target may be requested. Both z-stream and
// y-stream upgrades require HostedCluster availability and positive confirmation of
// no degradation. Y-stream upgrades additionally require ClusterVersionUpgradeable=True.
// An absent or Unknown HostedClusterDegraded reading blocks selection just like a
// confirmed True does — deny by default when there isn't enough evidence to confirm
// the cluster is healthy.
func healthOK(clusterConditions, observed []metav1.Condition, isYStream bool) (ok bool, reason, message string) {
	if !meta.IsStatusConditionTrue(clusterConditions, "HostedClusterAvailable") {
		return false, "HostedClusterUnavailable", "HostedCluster must be available before an upgrade can start"
	}
	degraded := meta.FindStatusCondition(observed, "HostedClusterDegraded")
	if degraded == nil || degraded.Status != metav1.ConditionFalse {
		message := "HostedCluster has not reported HostedClusterDegraded=False"
		if degraded != nil && degraded.Message != "" {
			message = degraded.Message
		}
		return false, "HostedClusterDegraded", message
	}

	// Y-stream upgrades require ClusterVersionUpgradeable=True
	if isYStream {
		upgradeable := meta.FindStatusCondition(observed, "ClusterVersionUpgradeable")
		if upgradeable == nil || upgradeable.Status != metav1.ConditionTrue {
			message := "Y-stream upgrade requires ClusterVersionUpgradeable=True"
			if upgradeable != nil && upgradeable.Message != "" {
				message = upgradeable.Message
			}
			return false, "ClusterVersionNotUpgradeable", message
		}
	}

	return true, "", ""
}

// recordActiveTarget returns the ControlPlaneUpgradeResult to report while target is
// active. If previous already tracks target, its RequestedTime is preserved and any
// failure recorded for it is cleared (the caller re-populates FailureReason/Message
// this reconcile if the failure is still present). Otherwise this is a fresh attempt.
func recordActiveTarget(previous *privatev1.ControlPlaneUpgradeResult, target string) *privatev1.ControlPlaneUpgradeResult {
	if previous != nil && previous.TargetVersion == target {
		result := previous.DeepCopy()
		result.FailureReason = ""
		result.FailureMessage = ""
		return result
	}
	now := metav1.Now()
	return &privatev1.ControlPlaneUpgradeResult{
		TargetVersion: target,
		TargetSource:  targetSourceAutomatic,
		RequestedTime: &now,
	}
}

// recordCompletion marks previous complete when it tracked the version that just
// completed. It never resets currentVersion/targetVersion tracking on its own —
// only a fresh call to recordActiveTarget starts a new attempt.
func recordCompletion(previous *privatev1.ControlPlaneUpgradeResult, completed string) *privatev1.ControlPlaneUpgradeResult {
	if previous == nil {
		return nil
	}
	if previous.TargetVersion == completed && previous.CompletedTime == nil {
		result := previous.DeepCopy()
		now := metav1.Now()
		result.CompletedTime = &now
		result.FailureReason = ""
		result.FailureMessage = ""
		return result
	}
	return previous
}

// writeStatus applies conditions and result to cluster and, if anything changed,
// performs a single status update. Returns the given requeueAfter on success.
func (r *Reconciler) writeStatus(
	ctx context.Context,
	cluster *privatev1.Cluster,
	result *privatev1.ControlPlaneUpgradeResult,
	requeueAfter time.Duration,
	conditions ...metav1.Condition,
) (reconcile.Result, error) {
	gen := cluster.Generation
	changed := false
	for _, cond := range conditions {
		cond.ObservedGeneration = gen
		changed = meta.SetStatusCondition(&cluster.Status.Conditions, cond) || changed
	}
	if !equality.Semantic.DeepEqual(cluster.Status.ControlPlaneUpgrade, result) {
		cluster.Status.ControlPlaneUpgrade = result
		changed = true
	}
	if changed {
		if err := r.apiClient.Status().Update(ctx, cluster); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{RequeueAfter: requeueConflict}, nil
			}
			return reconcile.Result{}, fmt.Errorf("%s: update cluster status: %w", adapterName, err)
		}
	}
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func newCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message}
}
