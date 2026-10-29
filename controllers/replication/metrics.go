package replication

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	EventsPublished  *prometheus.CounterVec
	EventsRejected   *prometheus.CounterVec
	PublishErrors    *prometheus.CounterVec
	PublishDuration  *prometheus.HistogramVec
	ResyncDuration   *prometheus.HistogramVec
	OversizedEvents  *prometheus.CounterVec
	ResourceFailures *prometheus.CounterVec
	EventsApplied    *prometheus.CounterVec
	ApplyErrors      *prometheus.CounterVec
	EventsSkipped    *prometheus.CounterVec
	EventsDropped    *prometheus.CounterVec
	ObjectsPruned    *prometheus.CounterVec
	InventorySyncs   *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsPublished:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_published_total", Help: "Successfully published replication events."}, []string{"region", "event_type", "resource_kind"}),
		EventsRejected:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_rejected_total", Help: "Rejected replication events."}, []string{"region", "event_type", "resource_kind", "reason", "origin_region"}),
		PublishErrors:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_publish_errors_total", Help: "Failed publish attempts."}, []string{"region", "resource_kind"}),
		PublishDuration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "replication_publish_duration_seconds", Help: "Time to publish one event.", Buckets: prometheus.DefBuckets}, []string{"event_type"}),
		ResyncDuration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "replication_resync_duration_seconds", Help: "Time for a complete source snapshot.", Buckets: prometheus.DefBuckets}, []string{"resource_kind"}),
		OversizedEvents:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_oversized_events_total", Help: "Events exceeding the encoded application limit."}, []string{"resource_kind"}),
		ResourceFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_resource_failures_total", Help: "Per-resource replication failures that degraded one resource without stopping others."}, []string{"region", "resource_kind"}),
		EventsApplied:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_applied_total", Help: "Source events applied to a regional mirror."}, []string{"event_type", "resource_kind"}),
		ApplyErrors:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_apply_errors_total", Help: "Local mirror application attempts that failed and will be retried."}, []string{"resource_kind"}),
		EventsSkipped:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_skipped_total", Help: "Source events skipped by a regional receiver."}, []string{"reason", "resource_kind"}),
		EventsDropped:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_dropped_total", Help: "Invalid source events acknowledged without application."}, []string{"reason", "resource_kind"}),
		ObjectsPruned:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_objects_pruned_total", Help: "Local objects pruned after a validated inventory."}, []string{"resource_kind"}),
		InventorySyncs:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_inventory_syncs_total", Help: "Inventory reconciliation outcomes."}, []string{"resource_kind", "outcome"}),
	}
	reg.MustRegister(m.EventsPublished, m.EventsRejected, m.PublishErrors, m.PublishDuration, m.ResyncDuration, m.OversizedEvents, m.ResourceFailures, m.EventsApplied, m.ApplyErrors, m.EventsSkipped, m.EventsDropped, m.ObjectsPruned, m.InventorySyncs)
	return m
}
