# NodePool upgrades

NodePool upgrades use the existing release configuration. Customers own
`spec.release.version` (the desired version), `channelGroup`, and optional
`upgradeStartTime`. Controllers only write observed state and execution progress to
status. The separate `nodepool-upgrade` controller coordinates upgrades without
changing the customer's spec.

## Customer flow

Request an immediate upgrade through the normal NodePool resource path:

```sh
kubectl -n customer patch nodepool workers --type merge \
  -p '{"spec":{"release":{"version":"4.22.16","upgradeStartTime":null}}}'
```

For a one-time scheduled start:

```yaml
spec:
  release:
    version: "4.22.16"        # Desired version, including while scheduled
    channelGroup: stable
    upgradeStartTime: "2026-11-07T03:00:00Z"
status:
  version: "4.22.14"   # Last observed version
```

`upgradeStartTime` is the earliest permitted start, not a guarantee of the exact start
time. Omit it for immediate eligibility. Initial installation still uses
`release.version` directly; `upgradeStartTime` is supported only after the pool has an
observed installed version. Cluster scheduling continues to use
`spec.maintenance`; Cluster admission rejects `release.upgradeStartTime`.

The upgrade coordinator waits for the start time and fresh healthy pool
feedback, revalidates the target, and records approval through the
`NodePoolUpgradeProgressing` condition. Version resolution and installation require
that approval before using a different release. Until then, installation
continues reconciling the observed release so scaling and health reporting
remain functional. No controller copies a target into `spec.release.version`.

The resource path is:

```text
/apis/gcp.managed.openshift.io/v1/namespaces/{namespace}/nodepools/{name}
```

The Cluster-nested NodePool route is also supported. The caller needs
`nodepool.update` to request, revise, or cancel an upgrade and `nodepool.get`
to view its status. `gcphcpctl nodepool upgrade workers --version 4.22.16`
updates this same release configuration; `--start-time` adds a schedule.

## Progress and cancellation

Watch the NodePool:

```sh
kubectl -n customer get nodepool workers -o yaml --watch
```

- `status.version` reports the observed pool version.
- `spec.release.version` remains the desired target during scheduling and rollout.
- `NodePoolUpgradeScheduled=True` means the request is waiting for its start time.
- `NodePoolUpgradeProgressing=True` means the coordinator approved the rollout.
  Successful completion sets it to `False` with reason `UpgradeCompleted`.
- `NodePoolUpgradeDegraded=True` reports rejection or failure; its reason and
  message explain the problem.
- Each condition's `lastTransitionTime` records when that condition changed state.

A matching current and target version alone does not complete an upgrade. The
pool must report available, healthy, and not progressing for its current
generation. Conditions report the current or latest outcome without a separate public
`status.upgrade` result or an attempt-history list.

Before acceptance, revise `release.version` or `release.upgradeStartTime`. Clearing
only `upgradeStartTime` makes the desired upgrade immediately eligible. An ordinary
public PUT omitting `upgradeStartTime` preserves it; explicit null clears it. To cancel
a pending upgrade, restore the observed version and clear the schedule:

```sh
kubectl -n customer patch nodepool workers --type merge \
  -p '{"spec":{"release":{"version":"4.22.14","upgradeStartTime":null}}}'
```

After acceptance, changes to the desired version, channel, or start time are
rejected synchronously. Repeating identical settings is idempotent. Scaling and
pool deletion remain available. A confirmed validation, resolution, or resource
preparation failure before dispatch sets the degraded condition and releases
the lock. The customer can then restore `spec.release.version` to
`status.version` and clear `upgradeStartTime`; Gecko does not rewrite customer
intent automatically.

## Ownership and recovery

The coordinator owns the scheduled, progressing, and degraded upgrade conditions.
Version resolution owns `status.versionResolution` and its resolution condition.
Installation owns `status.version`, pool availability/health/progress, and the
`NodePoolUpgradeSubmitted` dispatch condition. There is no upgrade-result object
or private upgrade-state field. Customer status input cannot bypass admission.

Approval is persisted using a resource-version precondition before a different
release may be resolved or installed. Admission locks release settings while
`NodePoolUpgradeProgressing=True`. This approval remains valid across unrelated
scaling generations; health/completion conditions must nevertheless describe
the current generation. Conflicts and transient storage errors retry without
opening the gate. Rejected conditions describe the current spec generation and
are reevaluated when the desired configuration changes.

Before dispatch, installation persists `NodePoolUpgradeSubmitted=Unknown`.
Only then may it write the target through transport. A successful dispatch
records `True`; this acknowledges handoff, not completed installation in
HyperShift. `False` means the desired upgrade has not been dispatched. Unknown
is treated conservatively across transport errors and restarts, preventing an
unsafe revert when a write may already have happened. Recovery after dispatch
is deliberately deferred; the active release remains locked until healthy
completion. Confirmed pre-submission failures can be canceled as described above.

## Validation and scope

Admission and coordinator validation use platform database objects without
network calls to Cincinnati, management clusters, or GCP. Checks include catalog
and channel membership, forward versions, the control-plane ceiling, N-3 target
skew, sequential minor steps, and EUS-to-EUS steps. Database objects are decoded
from both typed and unstructured representations.

The Version catalog does not store Cincinnati upgrade edges. Catalog membership
and version-step checks do not prove that a particular graph edge exists.
Full graph eligibility, the notification pipeline, and upgrade metrics remain
separate work.

## Deployment health and networking

The controller exposes `/healthz` and `/readyz` on port 8081 by default. The Helm
chart configures liveness and readiness probes against these handlers. Override
`healthProbePort` in chart values to change the port, or use the controller's
`--health-probe-bind-address` flag when running it directly.

The chart enables a NetworkPolicy selecting this controller's pods. It permits
health-probe ingress, DNS egress on TCP/UDP 53, and API egress on TCP 443. Set
`networkPolicy.apiServerCIDRs` to the API server ranges when available. For a direct
`orlopURL`, configure `networkPolicy.apiServerPort` and the destination CIDRs to
match that endpoint. `networkPolicy.extraEgress` supports environment-specific
rules; deployments using a namespace-managed policy can disable this chart's
policy with `networkPolicy.enabled=false`.
