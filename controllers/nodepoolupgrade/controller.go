// Package nodepoolupgrade coordinates customer-requested NodePool upgrades.
package nodepoolupgrade

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	waitInterval         = 30 * time.Second
	retryInterval        = time.Second
	maxScheduleWait      = time.Hour
	conditionScheduled   = "NodePoolUpgradeScheduled"
	conditionProgressing = "NodePoolUpgradeProgressing"
	conditionDegraded    = "NodePoolUpgradeDegraded"
)

// Reconciler owns upgrade scheduling and status. Installation controllers retain
// ownership of release resolution, resource application, and observed pool health.
type Reconciler struct {
	client client.Client
	now    func() time.Time
}

func NewReconciler(c client.Client) *Reconciler { return &Reconciler{client: c, now: time.Now} }

func (r *Reconciler) Reconcile(ctx context.Context, key reconcile.Request) (reconcile.Result, error) {
	var pool privatev1.NodePool
	if err := r.client.Get(ctx, key.NamespacedName, &pool); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !pool.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	before := pool.DeepCopy()
	if privatev1.NodePoolUpgradeActive(pool.Status.Upgrade) {
		return r.reconcileAccepted(ctx, before, &pool)
	}
	if pool.Status.CurrentVersion != "" && pool.Spec.Release.Version != pool.Status.CurrentVersion {
		return r.reconcileDirect(ctx, before, &pool)
	}
	if pool.Spec.Upgrade == nil {
		return r.cancelPending(ctx, before, &pool)
	}
	if pool.Spec.Upgrade.TargetVersion == nil {
		return r.defaultTarget(ctx, &pool)
	}
	id := intentID(pool.Spec.Upgrade)
	if pool.Status.Upgrade == nil || ptr.Deref(pool.Status.Upgrade.IntentID, "") != id {
		pool.Status.Upgrade = r.newResult(*pool.Spec.Upgrade.TargetVersion, id)
	}
	if terminal(pool.Status.Upgrade) {
		return reconcile.Result{}, nil
	}
	if start := pool.Spec.Upgrade.StartTime; start != nil && r.now().Before(start.Time) {
		remaining := start.Sub(r.now())
		if remaining > maxScheduleWait {
			remaining = maxScheduleWait
		}
		conditions(&pool, metav1.ConditionTrue, metav1.ConditionFalse, "WaitingForStartTime", "Waiting for the requested start time")
		return r.persist(ctx, before, &pool, remaining)
	}
	return r.accept(ctx, before, &pool)
}

// intentID tracks the upgrade settings, rather than the whole object generation:
// scaling, status updates, and our own release patch must not create new attempts.
func intentID(upgrade *privatev1.NodePoolUpgradeSpec) string {
	if upgrade == nil {
		return ""
	}
	start := ""
	if upgrade.StartTime != nil {
		start = upgrade.StartTime.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(ptr.Deref(upgrade.TargetVersion, "")+"\n"+start)))
}

func (r *Reconciler) newResult(target, id string) *privatev1.NodePoolUpgradeResult {
	now := metav1.NewTime(r.now().UTC())
	return &privatev1.NodePoolUpgradeResult{TargetVersion: target, IntentID: ptr.To(id), RequestedTime: &now}
}

func (r *Reconciler) defaultTarget(ctx context.Context, pool *privatev1.NodePool) (reconcile.Result, error) {
	// Admission normally pins the default. Also recover persisted objects written
	// without that defaulter, before scheduling or starting their upgrade.
	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Spec.ClusterID}, &cluster); err != nil {
		return reconcile.Result{}, fmt.Errorf("read Cluster to default upgrade target: %w", err)
	}
	if cluster.Status.HostedClusterResult == nil || cluster.Status.HostedClusterResult.Version == "" {
		return reconcile.Result{RequeueAfter: waitInterval}, nil
	}
	before := pool.DeepCopy()
	pool.Spec.Upgrade.TargetVersion = ptr.To(cluster.Status.HostedClusterResult.Version)
	return patchResult(r.client.Patch(ctx, pool, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})))
}

func (r *Reconciler) accept(ctx context.Context, before, pool *privatev1.NodePool) (reconcile.Result, error) {
	if pool.Status.CurrentVersion == "" || !rolloutComplete(pool) {
		conditions(pool, metav1.ConditionFalse, metav1.ConditionFalse, "WaitingForNodePool", "Waiting for observed NodePool version and health")
		return r.persist(ctx, before, pool, waitInterval)
	}
	if err := r.validate(ctx, pool, pool.Status.Upgrade.TargetVersion); err != nil {
		return r.validationResult(ctx, before, pool, err)
	}
	now := metav1.NewTime(r.now().UTC())
	pool.Status.Upgrade.AcceptedTime = &now
	conditions(pool, metav1.ConditionFalse, metav1.ConditionTrue, "UpgradeAccepted", "Upgrade target accepted for execution")
	// Persist acceptance before changing the installation target. Admission then
	// prevents concurrent cancellation or replacement, including across restarts.
	return r.persist(ctx, before, pool, retryInterval)
}

func (r *Reconciler) reconcileAccepted(ctx context.Context, before, pool *privatev1.NodePool) (reconcile.Result, error) {
	result := pool.Status.Upgrade
	if !strings.HasPrefix(ptr.Deref(result.IntentID, ""), "release:") && ptr.Deref(result.IntentID, "") != intentID(pool.Spec.Upgrade) {
		return r.fail(ctx, before, pool, "UpgradeChanged", "The accepted upgrade settings changed")
	}
	if pool.Spec.Release.Version == result.TargetVersion {
		return r.observe(ctx, before, pool)
	}
	if pool.Spec.Release.Version != pool.Status.CurrentVersion {
		return r.fail(ctx, before, pool, "ConcurrentUpgrade", "Another release change is already in progress")
	}
	if !rolloutComplete(pool) {
		conditions(pool, metav1.ConditionFalse, metav1.ConditionTrue, "WaitingForNodePool", "Waiting for the NodePool to become healthy")
		return r.persist(ctx, before, pool, waitInterval)
	}
	// Revalidate after acceptance too: another actor may have completed a newer
	// upgrade while this reconciler was stopped. Never overwrite that with an old target.
	if err := r.validate(ctx, pool, result.TargetVersion); err != nil {
		return r.validationResult(ctx, before, pool, err)
	}
	pool.Spec.Release.Version = result.TargetVersion
	return patchResult(r.client.Patch(ctx, pool, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})))
}

func (r *Reconciler) reconcileDirect(ctx context.Context, before, pool *privatev1.NodePool) (reconcile.Result, error) {
	id := "release:" + pool.Spec.Release.Version
	if pool.Status.Upgrade == nil || ptr.Deref(pool.Status.Upgrade.IntentID, "") != id {
		pool.Status.Upgrade = r.newResult(pool.Spec.Release.Version, id)
		now := metav1.NewTime(r.now().UTC())
		pool.Status.Upgrade.AcceptedTime = &now
	}
	if terminal(pool.Status.Upgrade) {
		return reconcile.Result{}, nil
	}
	return r.observe(ctx, before, pool)
}

func (r *Reconciler) observe(ctx context.Context, before, pool *privatev1.NodePool) (reconcile.Result, error) {
	result := pool.Status.Upgrade
	if result.StartedTime == nil {
		now := metav1.NewTime(r.now().UTC())
		result.StartedTime = &now
	}
	if c := meta.FindStatusCondition(pool.Status.Conditions, "NodePoolVersionResolved"); c != nil && c.ObservedGeneration == pool.Generation && c.Status == metav1.ConditionFalse {
		return r.fail(ctx, before, pool, "VersionResolutionFailed", "The target release could not be resolved")
	}
	if pool.Status.CurrentVersion == result.TargetVersion && rolloutComplete(pool) {
		now := metav1.NewTime(r.now().UTC())
		result.CompletedTime = &now
		conditions(pool, metav1.ConditionFalse, metav1.ConditionFalse, "UpgradeCompleted", "The NodePool reached the target version and is healthy")
		return r.persist(ctx, before, pool, 0)
	}
	if current, err := utilversion.ParseSemantic(pool.Status.CurrentVersion); err == nil {
		if target, err := utilversion.ParseSemantic(result.TargetVersion); err == nil && target.LessThan(current) {
			return r.fail(ctx, before, pool, "TargetSuperseded", "The observed NodePool version is newer than this upgrade target")
		}
	}
	conditions(pool, metav1.ConditionFalse, metav1.ConditionTrue, "WaitingForNodePool", "Waiting for the target version and a healthy rollout")
	return r.persist(ctx, before, pool, waitInterval)
}

func rolloutComplete(pool *privatev1.NodePool) bool {
	for _, expected := range []struct {
		name   string
		status metav1.ConditionStatus
	}{
		{"NodePoolAvailable", metav1.ConditionTrue}, {"NodePoolHealthy", metav1.ConditionTrue}, {"NodePoolProgressing", metav1.ConditionFalse},
	} {
		c := meta.FindStatusCondition(pool.Status.Conditions, expected.name)
		if c == nil || c.Status != expected.status || c.ObservedGeneration != pool.Generation {
			return false
		}
	}
	return true
}

type validationError struct{ message string }

func (e *validationError) Error() string { return e.message }

func (r *Reconciler) validate(ctx context.Context, pool *privatev1.NodePool, target string) error {
	var cluster privatev1.Cluster
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Spec.ClusterID}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return &validationError{"The referenced Cluster does not exist"}
		}
		return fmt.Errorf("read Cluster for upgrade: %w", err)
	}
	var release privatev1.Version
	if err := r.client.Get(ctx, types.NamespacedName{Name: target}, &release); err != nil {
		if apierrors.IsNotFound(err) {
			return &validationError{"The target version is not in the platform catalog"}
		}
		return fmt.Errorf("read Version for upgrade: %w", err)
	}
	if err := privatev1.ValidateNodePoolUpgradeTarget(pool, &cluster, &release, target); err != nil {
		return &validationError{err.Error()}
	}
	return nil
}

func (r *Reconciler) validationResult(ctx context.Context, before, pool *privatev1.NodePool, err error) (reconcile.Result, error) {
	var invalid *validationError
	if !errors.As(err, &invalid) {
		return reconcile.Result{}, err
	}
	return r.fail(ctx, before, pool, "TargetRejected", invalid.message)
}

func (r *Reconciler) fail(ctx context.Context, before, pool *privatev1.NodePool, reason, message string) (reconcile.Result, error) {
	pool.Status.Upgrade.FailureReason = ptr.To(reason)
	pool.Status.Upgrade.FailureMessage = ptr.To(message)
	conditions(pool, metav1.ConditionFalse, metav1.ConditionFalse, reason, message)
	setCondition(pool, conditionDegraded, metav1.ConditionTrue, reason, message)
	return r.persist(ctx, before, pool, 0)
}

func (r *Reconciler) cancelPending(ctx context.Context, before, pool *privatev1.NodePool) (reconcile.Result, error) {
	if pool.Status.Upgrade == nil || pool.Status.Upgrade.IntentID == nil {
		return reconcile.Result{}, nil
	}
	pool.Status.Upgrade.IntentID = nil
	if !terminal(pool.Status.Upgrade) {
		conditions(pool, metav1.ConditionFalse, metav1.ConditionFalse, "UpgradeCanceled", "The pending upgrade was canceled")
	}
	return r.persist(ctx, before, pool, 0)
}

func conditions(pool *privatev1.NodePool, scheduled, progressing metav1.ConditionStatus, reason, message string) {
	setCondition(pool, conditionScheduled, scheduled, reason, message)
	setCondition(pool, conditionProgressing, progressing, reason, message)
	setCondition(pool, conditionDegraded, metav1.ConditionFalse, "AsExpected", "")
}

func setCondition(pool *privatev1.NodePool, typ string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message, ObservedGeneration: pool.Generation,
	})
}

func terminal(result *privatev1.NodePoolUpgradeResult) bool {
	return result.CompletedTime != nil || result.FailureReason != nil
}

func (r *Reconciler) persist(ctx context.Context, before, pool *privatev1.NodePool, after time.Duration) (reconcile.Result, error) {
	if !apiequality.Semantic.DeepEqual(before.Status, pool.Status) {
		if err := r.client.Status().Patch(ctx, pool, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return reconcile.Result{RequeueAfter: retryInterval}, nil
			}
			return reconcile.Result{}, fmt.Errorf("patch NodePool upgrade status: %w", err)
		}
	}
	return reconcile.Result{RequeueAfter: after}, nil
}

func patchResult(err error) (reconcile.Result, error) {
	if err != nil && !apierrors.IsConflict(err) {
		return reconcile.Result{}, fmt.Errorf("patch NodePool upgrade target: %w", err)
	}
	return reconcile.Result{RequeueAfter: retryInterval}, nil
}
