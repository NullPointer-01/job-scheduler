package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsSubmitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_submitted_total",
		Help: "Total number of jobs submitted.",
	})

	JobsCancelled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_cancelled_total",
		Help: "Total number of jobs cancelled.",
	})

	JobsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_jobs_running_total",
		Help: "Total number of currently running jobs",
	})

	JobsSucceeded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_succeeded_total",
		Help: "Total number of jobs that completed successfully.",
	})

	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_failed_total",
		Help: "Total number of jobs that failed after retries.",
	})

	JobsRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_retried_total",
		Help: "Total number of job retries",
	})

	JobsRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_recovered_total",
		Help: "Total number of crashed jobs recovered at startup.",
	})

	JobsExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_jobs_expired_total",
		Help: "Total number of jobs recovered after lease expiry.",
	})

	ProcessingLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "jobs_processing_duration_seconds",
		Help: "Job processing time by workers in seconds",
	}, []string{"job_type", "status"})
)
