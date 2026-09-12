package worker

import (
	"context"
	"distributed-job-scheduler/internal/metrics"
	"distributed-job-scheduler/internal/store"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

type Handler func(ctx context.Context, data json.RawMessage) error

type Pool struct {
	poolId        string
	store         store.Store
	queue         <-chan store.Job
	handlers      map[string]Handler
	size          int
	leaseDuration time.Duration
}

func New(poolId string, store store.Store, queue <-chan store.Job, size int, leaseDuration time.Duration) *Pool {
	return &Pool{
		poolId:        poolId,
		store:         store,
		queue:         queue,
		size:          size,
		leaseDuration: leaseDuration,
		handlers:      make(map[string]Handler),
	}
}

func (p *Pool) Run(ctx context.Context) {
	p.initHandlers()

	var wg sync.WaitGroup

	for i := 0; i < p.size; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			p.runWorker(ctx)
		}()
	}

	wg.Wait()
}

func (p *Pool) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-p.queue:
			if !ok {
				return
			}

			p.executeJob(ctx, job)
		}
	}
}

func (p *Pool) executeJob(ctx context.Context, job store.Job) {
	if err := p.store.MarkJobRunning(ctx, job.Id, p.poolId, job.FencingToken, p.leaseDuration); err != nil {
		slog.Warn("Job reclaimed or stale, aborting", "id", job.Id, "err", err)
		return
	}

	metrics.JobsRunning.Inc()
	defer metrics.JobsRunning.Dec()

	defer func() {
		if r := recover(); r != nil {
			slog.Error("recovered from panic", "err", r)
			retried, _ := p.store.MarkJobFailed(ctx, job.Id, p.poolId, job.FencingToken)
			if retried {
				metrics.JobsRetried.Inc()
			} else {
				metrics.JobsFailed.Inc()
			}
		}
	}()

	handler, ok := p.handlers[job.Type]
	if !ok {
		retried, _ := p.store.MarkJobFailed(ctx, job.Id, p.poolId, job.FencingToken)
		if retried {
			metrics.JobsRetried.Inc()
		} else {
			metrics.JobsFailed.Inc()
		}

		slog.Error("No handler registered for type" + job.Type)
		return
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutMillis)*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := handler(execCtx, job.Data)
	elapsed := time.Since(start).Seconds()

	if err != nil {
		slog.Error("Job execution failed", "err", err)
		retried, _ := p.store.MarkJobFailed(ctx, job.Id, p.poolId, job.FencingToken)
		if retried {
			metrics.JobsRetried.Inc()
		} else {
			metrics.JobsFailed.Inc()
		}

		metrics.ProcessingLatency.WithLabelValues(job.Type, "failure").Observe(elapsed)
		return
	}

	p.store.MarkJobSucceeded(ctx, job.Id, p.poolId, job.FencingToken)
	metrics.JobsSucceeded.Inc()
	metrics.ProcessingLatency.WithLabelValues(job.Type, "success").Observe(elapsed)
}

func (p *Pool) initHandlers() {
	p.handlers["email"] = func(ctx context.Context, data json.RawMessage) error {
		slog.Info("Executing email job: ", "data", string(data))
		select {
		case <-ctx.Done():
			slog.Info("Job timed out")
			return ctx.Err()
		default:
			slog.Info("Email successfully sent")
			return nil
		}
	}
}
