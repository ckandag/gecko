# Control-plane version upgrades

Customers update the existing Cluster's `spec.release.version`. A `major.minor`
request such as `4.23` selects the newest eligible advertised release in that
minor and the selected channel. Complete versions, including prereleases, are
also accepted. Initial installation selects minor requests from the mirrored
Version catalog because no HostedCluster upgrade feedback exists yet.

Cluster uses `ClusterReleaseSpec`, independently of `NodePoolReleaseSpec`.
NodePools continue to require complete versions until their resolver supports
minor selection. Both release objects keep the same JSON fields.

The controller normalizes a customer minor request to its concrete target in spec before
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

Automation records its source and approval before changing spec. During
`PreparingAutomaticUpgrade`, the release is locked but installers keep the
observed version. Normalization then records final approval against the new
spec generation. A failed write or restart cannot turn that automatic operation
into a manual maintenance bypass. Before automatic admission, a customer's
version change is always manual, even if an older automatic selection names the
same target. A matching historical version never determines request source.

The HostedCluster installer records `ControlPlaneUpgradeSubmitted=Unknown`
before dispatch and `True` after acknowledgement. A confirmed preparation failure
before dispatch releases the approval lock so a customer can correct or cancel
the target. Manual desired versions stay saved. For an automatic preparation
failure, the controller restores the observed version in spec before unlocking;
the unsubmitted platform-selected target cannot later be mistaken for manual intent. Unknown or successful dispatch keeps the lock until healthy
completion. Recovery after a dispatched, failed upgrade requires operational
intervention; changing the desired release is not an implicit rollback.

`WithAdmissionGate` is the internal integration point for future progressive
rollout capacity. A denied gate retains normalized customer intent in spec and reports
`WaitingForUpgradeCapacity`. The default permits upgrades: this change does not
implement a fleet-wide semaphore, quota, reservation, or release coordinator.
A future gate must reserve capacity idempotently for the Cluster and target,
including retries after an approval-write conflict.

This API represents current desired configuration and conditions. It has no
separate upgrade-request resource, operation queue, or attempt history.
