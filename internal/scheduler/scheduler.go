package scheduler

import (
	"context"
	"distributed-job-scheduler/internal/metrics"
	"distributed-job-scheduler/internal/store"
	"log/slog"
	"time"
)

type Scheduler struct {
	schedulerId  string
	store        store.Store
	queue        chan<- store.Job
	batch        int
	tickInterval time.Duration

	leaseDuration     time.Duration
	leaseRecoveryTick time.Duration
}

func New(schedulerId string, store store.Store, queue chan<- store.Job, batch int, tickInterval time.Duration,
	leaseDuration time.Duration, leaseRecoveryTick time.Duration) *Scheduler {

	return &Scheduler{
		schedulerId:  schedulerId,
		store:        store,
		queue:        queue,
		batch:        batch,
		tickInterval: tickInterval,

		leaseDuration:     leaseDuration,
		leaseRecoveryTick: leaseRecoveryTick,
	}
}

func (sch *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(sch.tickInterval)
	defer ticker.Stop()

	leaseRecoveryTicker := time.NewTicker(sch.leaseRecoveryTick)
	defer leaseRecoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sch.tick(ctx)
		case <-leaseRecoveryTicker.C:
			sch.recoverExpiredJobs(ctx)
		}
	}
}

func (sch *Scheduler) tick(ctx context.Context) {
	jobs, err := sch.store.ClaimPendingJobs(ctx, sch.schedulerId, sch.leaseDuration, sch.batch)
	if err != nil {
		slog.Error("Scheduler tick failed: ", "err", err)
		return
	}

	for _, job := range jobs {
		select {
		case sch.queue <- job:
		case <-ctx.Done():
			return
		}
	}
}

func (sch *Scheduler) recoverExpiredJobs(ctx context.Context) {
	n, err := sch.store.RecoverExpiredLeases(ctx)
	if err != nil {
		slog.Error("Scheduler tick failed: ", "err", err)
		return
	}

	if n > 0 {
		metrics.JobsExpired.Add(float64(n))
	}
}
