package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrNotFound         = errors.New("Job not found")
	ErrDenyCancellation = errors.New("Job cannot be cancelled")
	ErrUnknown          = errors.New("Unknown error")
)

type Store interface {
	CreateJob(ctx context.Context, job Job) (Job, error)

	GetJob(ctx context.Context, id uuid.UUID) (Job, error)

	CancelJob(ctx context.Context, id uuid.UUID) error
}

type JobState string

const (
	StateScheduled JobState = "scheduled"
	StateReady     JobState = "ready"
	StateRunning   JobState = "running"
	StateCancelled JobState = "cancelled"
	StateSuccess   JobState = "success"
	StateFailed    JobState = "failed"
)

type Job struct {
	Id         uuid.UUID       `db:"id"`
	Type       string          `db:"type"`
	Data       json.RawMessage `db:"data"`
	Status     JobState        `db:"status"`
	RunAt      time.Time       `db:"run_at"`
	CreatedAt  time.Time       `db:"created_at"`
	ModifiedAt time.Time       `db:"modified_at"`
}
