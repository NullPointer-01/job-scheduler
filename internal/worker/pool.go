package worker

import (
	"context"
	"distributed-job-scheduler/internal/store"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Handler func(ctx context.Context, data json.RawMessage) error

type Pool struct {
	store    store.Store
	queue    <-chan uuid.UUID
	handlers map[string]Handler
	size     int
}

func New(store store.Store, queue <-chan uuid.UUID, size int) *Pool {
	return &Pool{
		store:    store,
		queue:    queue,
		size:     size,
		handlers: make(map[string]Handler),
	}
}

func (p *Pool) Run(ctx context.Context) {
	p.initHandlers()

	var wg sync.WaitGroup

	for i := 0; i < p.size; i++ {
		wg.Add(1)

		go func() {
			wg.Done()
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
		case jobId, ok := <-p.queue:
			if !ok {
				return
			}

			p.executeJob(ctx, jobId)
		}
	}
}

func (p *Pool) executeJob(ctx context.Context, jobId uuid.UUID) {
	job, err := p.store.GetJob(ctx, jobId)
	if err != nil {
		slog.Error("Job execution failed", "err", err)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("recovered from panic", "err", r)
		}
	}()

	handler, ok := p.handlers[job.Type]
	if !ok {
		p.store.MarkJobFailed(ctx, jobId)
		slog.Error("No handler registered for type" + job.Type)
		return
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutMillis)*time.Millisecond)
	defer cancel()

	err = handler(execCtx, job.Data)
	if err != nil {
		slog.Error("Job execution failed", "err", err)
		p.store.MarkJobFailed(ctx, jobId)
		return
	}

	p.store.MarkJobSucceeded(ctx, jobId)
}

func (p *Pool) initHandlers() {
	p.handlers["email"] = func(ctx context.Context, data json.RawMessage) error {
		slog.Info("Executing email job: ", "data", string(data))
		select {
		case <-ctx.Done():
			slog.Info("Job timed out")
			return nil
		default:
			slog.Info("Email successfully sent")
			return nil
		}
	}
}
