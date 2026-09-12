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
	ErrStaleFenceToken  = errors.New("Fencing token is stale")
	ErrUnknown          = errors.New("Unknown error")
)

type Store interface {
	CreateJob(ctx context.Context, job Job) (Job, error)

	ClaimPendingJobs(ctx context.Context, ownerId string, leaseDuration time.Duration, batch int) ([]Job, error)

	RenewLease(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int, leaseDuration time.Duration) error

	GetJob(ctx context.Context, id uuid.UUID) (Job, error)

	CancelJob(ctx context.Context, id uuid.UUID) error

	MarkJobRunning(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int, leaseDuration time.Duration) error

	MarkJobSucceeded(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int) error

	MarkJobFailed(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int) (bool, error)

	RecoverCrashedJobs(ctx context.Context) (int, error)

	RecoverExpiredLeases(ctx context.Context) (int, error)
}

type JobState string

const (
	StateScheduled  JobState = "scheduled"
	StateRunning    JobState = "running"
	StateDispatched JobState = "dispatched"
	StateCancelled  JobState = "cancelled"
	StateSuccess    JobState = "success"
	StateFailed     JobState = "failed"
)

type Job struct {
	Id            uuid.UUID       `db:"id"`
	Type          string          `db:"type"`
	Data          json.RawMessage `db:"data"`
	Status        JobState        `db:"status"`
	RunAt         time.Time       `db:"run_at"`
	RetryCount    int             `db:"retry_count"`
	MaxRetries    int             `db:"max_retries"`
	TimeoutMillis int             `db:"timeout_millis"`
	CreatedAt     time.Time       `db:"created_at"`
	ModifiedAt    time.Time       `db:"modified_at"`
	LeaseOwner    *string         `db:"lease_owner"`
	LeaseExpiry   *time.Time      `db:"lease_expiry"`
	FencingToken  int             `db:"fencing_token"`
}
