// Package controlplaneupgrade implements the control-plane-upgrade-controller
// reconciler.
//
// This controller selects automatic and accepts one-time manual control-plane
// upgrades (both z-stream patch and y-stream minor). It reads HC-observed HostedCluster feedback
// from Cluster.status.hostedClusterResult and requests an upgrade by setting
// Cluster.spec.release.version; it never applies a release image directly —
// the existing version-resolution and hc controllers do that.
//
// Z-stream upgrades select the newest patch in the current minor version.
// Y-stream upgrades select the next sequential minor version, authorized by
// Channel.spec.fleetMinorVersion. Both upgrade types respect maintenance windows
// and exclusions from Cluster.spec.maintenance.window and
// Cluster.spec.maintenance.exclusions, with override conditions for EOL proximity
// (y-stream) and critical security/platform issues via
// Cluster.spec.maintenance.criticalUpgrade (z-stream).
package controlplaneupgrade

import (
	"context"
	"fmt"
	"sort"
	"time"

	channelutil "github.com/openshift-online/gecko/controllers/util/channel"
	"github.com/openshift-online/gecko/controllers/util/logger"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	adapterName = "control-plane-upgrade-controller"
	// RequestClusterIDField is the cache index for one-time manual requests.
	RequestClusterIDField = "spec.clusterID"

	conditionAvailable   = "ControlPlaneUpgradeAvailable"
	conditionProgressing = "ControlPlaneUpgradeProgressing"
	conditionDegraded    = "ControlPlaneUpgradeDegraded"

	targetSourceAutomatic = "automatic"
	targetSourceCustomer  = "customer"

	// requeuePending is used while waiting on feedback or a health/readiness gate.
	requeuePending = 15 * time.Second
	// requeueConflict retries a stale version write after a fresh reconciliation.
	requeueConflict = time.Second
	// requeueObserving is used while a requested upgrade is rolling out.
	requeueObserving = 30 * time.Second
	// requeueStable is used once there is nothing to do until the next event.
	requeueStable = 5 * time.Minute
)

// Reconciler selects and observes control-plane upgrades for one Cluster
// per reconciliation. It owns Cluster.status.controlPlaneUpgrade and the
// ControlPlaneUpgrade* conditions; it never writes Cluster.status.hostedClusterResult,
// which is owned exclusively by the hc-controller.
type Reconciler struct {
	apiClient client.Client
	log       logger.Logger
}

// NewReconciler creates a new control-plane-upgrade Reconciler.
func NewReconciler(apiClient client.Client, log logger.Logger) *Reconciler {
	return &Reconciler{apiClient: apiClient, log: log}
}

// Reconcile evaluates one Cluster. Different Clusters may reconcile concurrently.
// Version changes use resource-version optimistic locking, so a competing update
// to the same Cluster causes a fresh reconciliation instead of overwriting it.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := r.log.With("controller", adapterName).With("cluster_id", req.Name)

	var cluster privatev1.Cluster
	if err := r.apiClient.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof(ctx, "cluster not found, skipping")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: get cluster: %w", adapterName, err)
	}

	// This controller does not own the Cluster lifecycle or any finalizer.
	if !cluster.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	hc := cluster.Status.HostedClusterResult
	if hc == nil || hc.Version == "" {
		return r.writeStatus(ctx, &cluster, cluster.Status.ControlPlaneUpgrade, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionUnknown, "WaitingForHostedClusterFeedback",
				"Waiting for observed HostedCluster version feedback"),
			newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
				"No control-plane upgrade has been requested"),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
	}

	completed := hc.Version
	desired := hc.DesiredVersion
	observed := hc.ObservedConditions
	requests, acceptedRequests, err := r.clusterRequests(ctx, &cluster)
	if err != nil {
		return reconcile.Result{}, err
	}

	// An upgrade is already active — either HyperShift is progressing toward a target,
	// or we (or a prior reconcile) requested one that HyperShift feedback has not yet
	// caught up to. Only observe; never select another target.
	if active, target := activeTarget(completed, desired, cluster.Spec.Release.Version); active {
		return r.reconcileActiveUpgrade(ctx, &cluster, requests, acceptedRequests, completed, desired, observed, target)
	}

	result, handled, statusResult, err := r.reconcileCompletedOrManual(ctx, &cluster, requests, acceptedRequests, completed, observed)
	if handled {
		return statusResult, err
	}

	// Prefer a newer patch in the Cluster's current major.minor stream. Z-stream
	// selection does not depend on Channel fleet-minor authorization.
	zCandidate, err := selectZStreamCandidate(completed, hc.AvailableUpdates)
	if err != nil {
		return r.writeStatus(ctx, &cluster, result, requeueStable,
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
				return r.writeStatus(ctx, &cluster, result, requeuePending,
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
		yCandidate, err := selectYStreamCandidate(completed, hc.AvailableUpdates, channel.Spec.FleetMinorVersion)
		if err != nil {
			return r.writeStatus(ctx, &cluster, result, requeueStable,
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
		if isValidCriticalPin(criticalPin, completed) && isAdvertised(criticalPin, hc.AvailableUpdates) {
			candidate = criticalPin
			isYStream = false
		} else if yCandidate != "" {
			candidate = yCandidate
			isYStream = true
		}
	}

	if candidate == "" {
		return r.writeStatus(ctx, &cluster, result, requeueStable,
			newCondition(conditionAvailable, metav1.ConditionFalse, "NoUpgradeAvailable",
				fmt.Sprintf("No newer update is advertised for %s", completed)),
			newCondition(conditionProgressing, metav1.ConditionFalse, "NoUpgradeRequested",
				"No control-plane upgrade has been requested"),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
	}

	// Health gates apply to both z-stream and y-stream
	if ok, reason, message := healthOK(cluster.Status.Conditions, observed, isYStream); !ok {
		return r.writeStatus(ctx, &cluster, result, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionFalse, reason, message),
			newCondition(conditionProgressing, metav1.ConditionFalse, "UpgradeBlocked",
				fmt.Sprintf("Control-plane upgrade to %s is waiting for HostedCluster health", candidate)),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
	}

	// Check for critical security or platform overrides.
	override, overrideReason, overrideMsg := overrideCondition(&cluster, candidate, completed)

	// Evaluate maintenance window/exclusions unless override applies
	if !override {
		permitted, reason, message, next := maintenancePermitted(&cluster, time.Now())
		if !permitted {
			return r.writeMaintenanceBlocked(ctx, &cluster, result, completed, candidate, isYStream, reason, message, next)
		}
	}

	// A future progressive-rollout admission gate belongs here, after target,
	// health, and maintenance eligibility and before mutating the Cluster spec.
	return r.requestAutomaticUpgrade(ctx, &cluster, result, completed, candidate, isYStream, override, overrideReason, overrideMsg, log)
}

// reconcileCompletedOrManual handles a completed target before considering a new
// automatic upgrade, then gives pending manual requests priority over automation.
func (r *Reconciler) reconcileCompletedOrManual(
	ctx context.Context,
	cluster *privatev1.Cluster,
	requests, acceptedRequests []privatev1.ControlPlaneUpgradeRequest,
	completed string,
	observed []metav1.Condition,
) (*privatev1.ControlPlaneUpgradeResult, bool, reconcile.Result, error) {
	result := recordCompletion(cluster.Status.ControlPlaneUpgrade, completed)
	if result != nil && result.TargetVersion == completed && hasAcceptedManualRequest(acceptedRequests, completed) &&
		result.TargetSource != targetSourceCustomer {
		result = result.DeepCopy()
		result.TargetSource = targetSourceCustomer
	}
	// Persist completion before considering another target. In particular, a
	// missing maintenance window must not leave a completed manual upgrade marked
	// as progressing indefinitely.
	if !equality.Semantic.DeepEqual(cluster.Status.ControlPlaneUpgrade, result) {
		message := fmt.Sprintf("Control-plane upgrade to %s completed", completed)
		statusResult, err := r.writeStatus(ctx, cluster, result, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeCompleted", message),
			newCondition(conditionProgressing, metav1.ConditionFalse, "UpgradeCompleted", message),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		)
		return result, true, statusResult, err
	}
	if len(requests) > 0 {
		statusResult, err := r.reconcileManualRequest(ctx, cluster, result, requests, completed, observed)
		return result, true, statusResult, err
	}
	return result, false, reconcile.Result{}, nil
}

func (r *Reconciler) requestAutomaticUpgrade(
	ctx context.Context,
	cluster *privatev1.Cluster,
	result *privatev1.ControlPlaneUpgradeResult,
	completed, candidate string,
	isYStream, override bool,
	overrideReason, overrideMsg string,
	log logger.Logger,
) (reconcile.Result, error) {
	// Request the upgrade with the Cluster's observed resourceVersion. A
	// concurrent writer wins or loses explicitly instead of being overwritten.
	cluster.Spec.Release.Version = candidate
	if err := r.apiClient.Update(ctx, cluster); err != nil {
		if apierrors.IsConflict(err) {
			// Re-read the Cluster and re-evaluate requests, health and maintenance.
			return reconcile.Result{RequeueAfter: requeueConflict}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: request version %s for cluster %s: %w", adapterName, candidate, cluster.Name, err)
	}

	streamType := "z-stream"
	upgradeReason := "UpdateSelected"
	if isYStream {
		streamType = "y-stream"
	}
	if override {
		upgradeReason = overrideReason
		log.Infof(ctx, "requested %s control-plane upgrade to %s (override: %s)", streamType, candidate, overrideMsg)
	} else {
		log.Infof(ctx, "requested %s control-plane upgrade to %s", streamType, candidate)
	}

	result = recordActiveTarget(result, candidate)
	return r.writeStatus(ctx, cluster, result, requeueObserving,
		newCondition(conditionAvailable, metav1.ConditionTrue, upgradeReason,
			fmt.Sprintf("Selected advertised control-plane update %s", candidate)),
		newCondition(conditionProgressing, metav1.ConditionTrue, "UpgradeRequested",
			fmt.Sprintf("Requested control-plane upgrade from %s to %s", completed, candidate)),
		newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
	)
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
		untilNext := time.Until(*next)
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

// activeTarget reports whether an upgrade is already active and, if so, its target.
// An upgrade is active when HyperShift is progressing toward a desired version that
// differs from the completed version, or when spec.release.version has been changed
// but HostedCluster feedback has not yet caught up to it.
func activeTarget(completed, desired, specVersion string) (active bool, target string) {
	if desired != "" && desired != completed {
		completedVersion, completedErr := utilversion.ParseSemantic(completed)
		desiredVersion, desiredErr := utilversion.ParseSemantic(desired)
		// HostedCluster desired feedback can lag after the new version has
		// completed. An older desired version is not a new active target.
		if completedErr != nil || desiredErr != nil || completedVersion.LessThan(desiredVersion) {
			return true, desired
		}
	}
	if specVersion != completed {
		return true, specVersion
	}
	return false, ""
}

func (r *Reconciler) reconcileActiveUpgrade(
	ctx context.Context,
	cluster *privatev1.Cluster,
	requests []privatev1.ControlPlaneUpgradeRequest,
	acceptedRequests []privatev1.ControlPlaneUpgradeRequest,
	completed, desired string,
	observed []metav1.Condition,
	target string,
) (reconcile.Result, error) {
	result := recordActiveTarget(cluster.Status.ControlPlaneUpgrade, target)
	accepted := hasAcceptedManualRequest(acceptedRequests, target)
	if accepted {
		result.TargetSource = targetSourceCustomer
	}
	type requestDecision struct {
		request  *privatev1.ControlPlaneUpgradeRequest
		decision string
		reason   string
	}
	decisions := make([]requestDecision, 0, len(requests))
	for i := range requests {
		request := &requests[i]
		previous := cluster.Status.ControlPlaneUpgrade
		// The Cluster spec update can succeed even if recording the customer's
		// target in status fails. A pending request matching an untracked target
		// recovers that partially recorded manual upgrade on the next reconcile.
		untracked := previous == nil || previous.TargetVersion != target
		customerTracked := previous != nil && previous.TargetVersion == target &&
			previous.TargetSource == targetSourceCustomer && previous.RequestedTime != nil &&
			!request.CreationTimestamp.After(previous.RequestedTime.Time)
		if !accepted && request.Spec.TargetVersion == target &&
			(untracked || customerTracked) {
			accepted = true
			result.TargetSource = targetSourceCustomer
			decisions = append(decisions, requestDecision{request, "Accepted", "UpgradeRequested"})
		} else {
			decisions = append(decisions, requestDecision{request, "Rejected", "UpgradeInProgress"})
		}
	}

	var conditions []metav1.Condition
	requeueAfter := requeueObserving
	if reason, message, failed := upgradeFailureReason(observed); failed {
		result.FailureReason = reason
		result.FailureMessage = message
		requeueAfter = requeueStable
		conditions = []metav1.Condition{
			newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeInProgress",
				fmt.Sprintf("Control-plane target is %s", target)),
			newCondition(conditionProgressing, metav1.ConditionFalse, reason, message),
			newCondition(conditionDegraded, metav1.ConditionTrue, reason, message),
		}
	} else {
		reason := "WaitingForHostedCluster"
		message := fmt.Sprintf("Waiting for HostedCluster to accept control-plane version %s", target)
		if desired == target {
			reason = "HostedClusterProgressing"
			message = fmt.Sprintf("HostedCluster is upgrading the control plane from %s to %s", completed, target)
		}
		conditions = []metav1.Condition{
			newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeInProgress",
				fmt.Sprintf("Control-plane target is %s", target)),
			newCondition(conditionProgressing, metav1.ConditionTrue, reason, message),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""),
		}
	}
	statusResult, err := r.writeStatus(ctx, cluster, result, requeueAfter, conditions...)
	if err != nil || statusResult.RequeueAfter == 0 {
		return statusResult, err
	}
	for _, decision := range decisions {
		if err := r.recordRequestDecision(ctx, decision.request, decision.decision, decision.reason); err != nil {
			return reconcile.Result{}, err
		}
	}
	return statusResult, nil
}

func (r *Reconciler) clusterRequests(ctx context.Context, cluster *privatev1.Cluster) ([]privatev1.ControlPlaneUpgradeRequest, []privatev1.ControlPlaneUpgradeRequest, error) {
	var list privatev1.ControlPlaneUpgradeRequestList
	if err := r.apiClient.List(ctx, &list, client.InNamespace(cluster.Namespace),
		client.MatchingFields{RequestClusterIDField: cluster.Name}); err != nil {
		return nil, nil, fmt.Errorf("%s: list upgrade requests for cluster %s: %w", adapterName, cluster.Name, err)
	}
	requests := make([]privatev1.ControlPlaneUpgradeRequest, 0, len(list.Items))
	accepted := make([]privatev1.ControlPlaneUpgradeRequest, 0)
	for _, request := range list.Items {
		switch request.Status.Decision {
		case "":
			requests = append(requests, request)
		case "Accepted":
			accepted = append(accepted, request)
		}
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].CreationTimestamp.Equal(&requests[j].CreationTimestamp) {
			return requests[i].Name < requests[j].Name
		}
		return requests[i].CreationTimestamp.Before(&requests[j].CreationTimestamp)
	})
	return requests, accepted, nil
}

func hasAcceptedManualRequest(requests []privatev1.ControlPlaneUpgradeRequest, target string) bool {
	for _, request := range requests {
		if request.Spec.TargetVersion == target {
			return true
		}
	}
	return false
}

func (r *Reconciler) recordRequestDecision(ctx context.Context, request *privatev1.ControlPlaneUpgradeRequest, decision, reason string) error {
	request.Status.Decision = decision
	request.Status.Reason = reason
	now := metav1.Now()
	request.Status.DecidedTime = &now
	if err := r.apiClient.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("%s: update request %s status: %w", adapterName, request.Name, err)
	}
	return nil
}

func (r *Reconciler) reconcileManualRequest(
	ctx context.Context,
	cluster *privatev1.Cluster,
	result *privatev1.ControlPlaneUpgradeResult,
	requests []privatev1.ControlPlaneUpgradeRequest,
	completed string,
	observed []metav1.Condition,
) (reconcile.Result, error) {
	request := &requests[0]
	for i := 1; i < len(requests); i++ {
		if err := r.recordRequestDecision(ctx, &requests[i], "Rejected", "UpgradeRequestAlreadyPending"); err != nil {
			return reconcile.Result{}, err
		}
	}

	target := request.Spec.TargetVersion
	advertised := false
	for _, available := range cluster.Status.HostedClusterResult.AvailableUpdates {
		if available == target {
			advertised = true
			break
		}
	}
	if !advertised {
		if err := r.recordRequestDecision(ctx, request, "Rejected", "TargetNotAvailable"); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}
	currentVersion, err := utilversion.ParseSemantic(completed)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("parse observed version %q: %w", completed, err)
	}
	targetVersion, err := utilversion.ParseSemantic(target)
	if err != nil || !currentVersion.LessThan(targetVersion) {
		if err := r.recordRequestDecision(ctx, request, "Rejected", "InvalidTargetVersion"); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: requeuePending}, nil
	}
	isYStream := currentVersion.Major() != targetVersion.Major() || currentVersion.Minor() != targetVersion.Minor()
	if ok, reason, message := healthOK(cluster.Status.Conditions, observed, isYStream); !ok {
		return r.writeStatus(ctx, cluster, result, requeuePending,
			newCondition(conditionAvailable, metav1.ConditionTrue, "ManualUpgradeRequested", fmt.Sprintf("Requested control-plane update %s", target)),
			newCondition(conditionProgressing, metav1.ConditionFalse, reason, message),
			newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
	}

	cluster.Spec.Release.Version = target
	if err := r.apiClient.Update(ctx, cluster); err != nil {
		if apierrors.IsConflict(err) {
			// Do not decide the request until a fresh reconcile wins the update.
			return reconcile.Result{RequeueAfter: requeueConflict}, nil
		}
		return reconcile.Result{}, fmt.Errorf("%s: request manual version %s for cluster %s: %w", adapterName, target, cluster.Name, err)
	}
	result = recordActiveTarget(result, target)
	result.TargetSource = targetSourceCustomer
	statusResult, err := r.writeStatus(ctx, cluster, result, requeueObserving,
		newCondition(conditionAvailable, metav1.ConditionFalse, "UpgradeInProgress", fmt.Sprintf("Control-plane target is %s", target)),
		newCondition(conditionProgressing, metav1.ConditionTrue, "UpgradeRequested", fmt.Sprintf("Requested control-plane upgrade from %s to %s", completed, target)),
		newCondition(conditionDegraded, metav1.ConditionFalse, "AsExpected", ""))
	if err != nil || statusResult.RequeueAfter == 0 {
		return reconcile.Result{}, err
	}
	if err := r.recordRequestDecision(ctx, request, "Accepted", "UpgradeRequested"); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: requeueObserving}, nil
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
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("%s: update cluster status: %w", adapterName, err)
		}
	}
	return reconcile.Result{RequeueAfter: requeueAfter}, nil
}

func newCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message}
}
