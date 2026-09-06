package scheduler

import (
	"context"
	"distributed-job-scheduler/internal/store"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Scheduler struct {
	store        store.Store
	queue        chan<- uuid.UUID
	batch        int
	tickInterval time.Duration
}

func New(store store.Store, queue chan<- uuid.UUID, batch int, tickInterval time.Duration) *Scheduler {
	return &Scheduler{
		store:        store,
		queue:        queue,
		batch:        batch,
		tickInterval: tickInterval,
	}
}

func (sch *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(sch.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sch.tick(ctx)
		}
	}
}

func (sch *Scheduler) tick(ctx context.Context) {
	jobs, err := sch.store.GetPendingJobs(ctx, sch.batch)
	if err != nil {
		slog.Error("Scheduler tick failed: ", "err", err)
		return
	}

	for _, job := range jobs {
		select {
		case sch.queue <- job.Id:
		case <-ctx.Done():
			return
		}
	}
}
