package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics encapsulates all Prometheus collectors for ForgeFlow.
type Metrics struct {
	JobsEnqueuedTotal         *prometheus.CounterVec
	JobsProcessedTotal        *prometheus.CounterVec
	JobExecutionDurationSecs  *prometheus.HistogramVec
	JobQueueLatencySecs       *prometheus.HistogramVec
	ActiveWorkers             prometheus.Gauge
	QueueDepth                *prometheus.GaugeVec
	DeadLetterJobsTotal       prometheus.Counter
	LeaseRecoverySweepsTotal  prometheus.Counter
	HTTPRequestsTotal         *prometheus.CounterVec
	HTTPRequestDurationSecs   *prometheus.HistogramVec
}

var globalMetrics *Metrics

// InitMetrics initializes and registers Prometheus metric collectors.
func InitMetrics() *Metrics {
	if globalMetrics != nil {
		return globalMetrics
	}

	globalMetrics = &Metrics{
		JobsEnqueuedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "forgeflow",
				Name:      "jobs_enqueued_total",
				Help:      "Total number of jobs submitted and enqueued",
			},
			[]string{"queue", "task_type"},
		),

		JobsProcessedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "forgeflow",
				Name:      "jobs_processed_total",
				Help:      "Total number of jobs processed by workers",
			},
			[]string{"queue", "status", "task_type"},
		),

		JobExecutionDurationSecs: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "forgeflow",
				Name:      "job_execution_duration_seconds",
				Help:      "Histogram of job task execution duration in seconds",
				Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0},
			},
			[]string{"queue", "task_type"},
		),

		JobQueueLatencySecs: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "forgeflow",
				Name:      "job_queue_latency_seconds",
				Help:      "Time duration spent by a job waiting in queue before claim",
				Buckets:   []float64{0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0},
			},
			[]string{"queue"},
		),

		ActiveWorkers: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "forgeflow",
				Name:      "active_workers",
				Help:      "Current number of registered active worker nodes",
			},
		),

		QueueDepth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "forgeflow",
				Name:      "queue_depth",
				Help:      "Current number of pending/enqueued messages in the queue",
			},
			[]string{"queue"},
		),

		DeadLetterJobsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "forgeflow",
				Name:      "dead_letter_jobs_total",
				Help:      "Total number of jobs transitioned to terminal DEAD state",
			},
		),

		LeaseRecoverySweepsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "forgeflow",
				Name:      "lease_recovery_sweeps_total",
				Help:      "Total number of lease recovery sweep cycles executed",
			},
		),

		HTTPRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "forgeflow",
				Name:      "http_requests_total",
				Help:      "Total number of HTTP API requests handled",
			},
			[]string{"method", "path", "status"},
		),

		HTTPRequestDurationSecs: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "forgeflow",
				Name:      "http_request_duration_seconds",
				Help:      "Histogram of HTTP request latency in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "path"},
		),
	}

	return globalMetrics
}

// GetMetrics returns the singleton metrics instance.
func GetMetrics() *Metrics {
	if globalMetrics == nil {
		return InitMetrics()
	}
	return globalMetrics
}
