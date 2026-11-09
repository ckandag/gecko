# Control-plane version upgrades

Customers update the existing Cluster's `spec.release.version`. A `major.minor`
request such as `4.23` selects the newest eligible advertised release in that
minor and the selected channel. Complete versions, including prereleases, are
also accepted. Initial installation selects minor requests from the mirrored
Version catalog because no HostedCluster upgrade feedback exists yet.

The controller normalizes a minor request to its concrete target in spec before
approval. `status.hostedClusterResult.version` remains the actual observed
version. Clients can display that as **Version**, and a differing spec version as
**Target Version**, including while the upgrade waits for health or capacity.
A normalized target stays pinned; newly advertised patches do not replace it.

`ControlPlaneUpgradeAccepted=True` and the exact
`status.controlPlaneUpgrade.targetVersion` authorize rollout. Version resolution
and HostedCluster application continue maintaining the observed release until
this approval is durably recorded. Accepted release settings are locked by API
validation; pending settings remain editable. Returning a pending target to the
observed full version cancels it. Unrelated Cluster changes remain permitted.

Manual intent bypasses maintenance timing, but retains health and advertised-edge
and catalog/channel checks. Automatic selection retains fleet-minor authorization,
maintenance exclusions, and the exact critical-patch override. After a rollout
completes, a later automatic patch waits for the next configured window opening,
even if the previous window is still open. Without a window, upgrades remain
permitted at any time outside exclusions.

The HostedCluster installer records `ControlPlaneUpgradeSubmitted=Unknown`
before dispatch and `True` after acknowledgement. A confirmed preparation failure
before dispatch releases the approval lock so a customer can correct or cancel
the target. Unknown or successful dispatch keeps the lock until healthy
completion. Recovery after a dispatched, failed upgrade requires operational
intervention; changing the desired release is not an implicit rollback.

`WithAdmissionGate` is the internal integration point for future progressive
rollout capacity. A denied gate retains the normalized target in spec and reports
`WaitingForUpgradeCapacity`. The default permits upgrades: this change does not
implement a fleet-wide semaphore, quota, reservation, or release coordinator.
A future gate must reserve capacity idempotently for the Cluster and target,
including retries after an approval-write conflict.

This API represents current desired configuration and conditions. It has no
separate upgrade-request resource, operation queue, or attempt history.
