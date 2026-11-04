package replication

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	EventsPublished      *prometheus.CounterVec
	EventsRejected       *prometheus.CounterVec
	PublishErrors        *prometheus.CounterVec
	PublishDuration      *prometheus.HistogramVec
	ResyncDuration       *prometheus.HistogramVec
	OversizedEvents      *prometheus.CounterVec
	ResourceFailures     *prometheus.CounterVec
	ControlReceiveErrors *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsPublished:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_published_total", Help: "Successfully published replication events."}, []string{"region", "event_type", "resource_kind"}),
		EventsRejected:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_events_rejected_total", Help: "Rejected replication events."}, []string{"region", "event_type", "resource_kind", "reason", "origin_region"}),
		PublishErrors:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_publish_errors_total", Help: "Failed publish attempts."}, []string{"region", "resource_kind"}),
		PublishDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "replication_publish_duration_seconds", Help: "Time to publish one event.", Buckets: prometheus.DefBuckets}, []string{"event_type"}),
		ResyncDuration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "replication_resync_duration_seconds", Help: "Time for a complete source snapshot.", Buckets: prometheus.DefBuckets}, []string{"resource_kind"}),
		OversizedEvents:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_oversized_events_total", Help: "Events exceeding the encoded application limit."}, []string{"resource_kind"}),
		ResourceFailures:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_resource_failures_total", Help: "Per-resource replication failures that degraded one resource without stopping others."}, []string{"region", "resource_kind"}),
		ControlReceiveErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "replication_control_receive_errors_total", Help: "Failures receiving resync requests from the leader control subscription."}, []string{"region", "code"}),
	}
	reg.MustRegister(m.EventsPublished, m.EventsRejected, m.PublishErrors, m.PublishDuration, m.ResyncDuration, m.OversizedEvents, m.ResourceFailures, m.ControlReceiveErrors)
	return m
}
