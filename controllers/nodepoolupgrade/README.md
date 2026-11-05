# NodePool upgrades

NodePool upgrades use the existing NodePool API. The `nodepool-upgrade`
controller watches NodePools and coordinates customer-requested upgrades. It
runs separately from version resolution and installation.

## Customer flow

Update a NodePool through its normal resource path:

```text
/apis/gcp.managed.openshift.io/v1/namespaces/{namespace}/nodepools/{name}
```

The existing Cluster-nested NodePool route is also supported. The caller needs
`nodepool.update` to request, revise, or cancel an upgrade and `nodepool.get`
to view its status.

Request an immediate upgrade:

```sh
kubectl -n customer patch nodepool workers --type merge \
  -p '{"spec":{"upgrade":{"targetVersion":"4.22.16"}}}'
```

For a one-time scheduled start, include an RFC 3339 timestamp:

```yaml
spec:
  release:
    version: "4.22.14"
    channelGroup: stable
  upgrade:
    targetVersion: "4.22.16"
    startTime: "2026-11-06T03:00:00Z"
```

An empty upgrade object defaults `targetVersion` to the observed control-plane
version at submission. The stored target stays pinned if the control plane
advances before the scheduled start. Omit `startTime` for immediate eligibility.
Upgrade requests require an installed pool with observed version feedback.

While the upgrade waits, `spec.release.version` remains unchanged. When the
upgrade becomes eligible, the coordinator validates it again and changes
`spec.release.version`. The existing version-resolution controller resolves the
release image and the installation controller applies it.

Watch the NodePool:

```sh
kubectl -n customer get nodepool workers -o yaml --watch
```

- `status.currentVersion` is reported from the live pool's version feedback.
- `status.upgrade.targetVersion` identifies the tracked upgrade.
- `status.upgrade.requestedTime`, `acceptedTime`, `startedTime`, and
  `completedTime` describe execution progress.
- `status.upgrade.failureReason` and `failureMessage` describe a rejection or failure.
- `NodePoolUpgradeScheduled`, `NodePoolUpgradeProgressing`, and
  `NodePoolUpgradeDegraded` are public status conditions.

A matching current and target version alone does not complete an upgrade. The
pool must also report available, healthy, and not progressing for its current
generation. The status contains the current or most recent attempt, without
an attempt-history list.

## Pending changes and cancellation

Before acceptance, change the target or start time by updating `spec.upgrade`.
Omitting upgrade from an ordinary public PUT preserves it. To cancel a pending
upgrade, explicitly clear it:

```sh
kubectl -n customer patch nodepool workers --type merge \
  -p '{"spec":{"upgrade":null}}'
```

After `acceptedTime` is recorded, the settings cannot be replaced or canceled
until the upgrade completes or fails. Pool deletion remains available through
its normal lifecycle. To retry a rejected target, submit changed upgrade
settings (for example, a new start time), or clear the pending intent and wait
for cancellation to be observed before submitting it again.

## Controller ownership and recovery

The upgrade coordinator owns `status.upgrade` and only its three upgrade
conditions. The installation controller owns `currentVersion` and the existing
availability/health/progress conditions. It does not implement scheduling.

Acceptance is persisted before submitting a release change. Both status and
spec writes use resource-version preconditions. Restarts resume from persisted
spec/status; an older accepted target is revalidated before it can change the
release version. Repeated reconciles do not restart a completed attempt, and
unrelated changes such as scaling do not create new attempts.

Direct updates to `spec.release.version` still use the existing rollout path.
The upgrade controller observes their progress as well. An active accepted
upgrade rejects conflicting target changes. A pending schedule must be changed
or canceled before directly changing the release version.

## Validation and current scope

Validation uses the platform database, without network calls to Cincinnati or
management clusters during admission. It checks catalog/channel membership,
forward version movement, the control-plane ceiling, the supported N-3 target
skew, sequential minor steps, and EUS-to-EUS steps. Database objects are decoded
from both typed and unstructured representations.

The Version catalog does not currently store Cincinnati upgrade edges. Catalog
membership and version-step checks do not prove that an individual graph edge
exists; full graph eligibility remains separate work. The platform notification
pipeline and upgrade metrics described in the design are also separate from
this API/controller refactor.
