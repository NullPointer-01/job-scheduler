package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type PostgresStore struct {
	db *sqlx.DB
}

func NewPostgresStore(db *sqlx.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

const (
	backoffBaseTime = 5 * time.Second
	backoffMaxTime  = 30 * time.Minute
)

func (s *PostgresStore) CreateJob(ctx context.Context, job Job) (Job, error) {
	const q = `
		INSERT INTO jobs (id, type, data, status, run_at, retry_count, max_retries, timeout_millis, created_at, modified_at, fencing_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING *`

	var createdJob Job

	err := s.db.GetContext(ctx, &createdJob, q,
		job.Id, job.Type, job.Data, job.Status, job.RunAt, job.RetryCount, job.MaxRetries, job.TimeoutMillis, job.CreatedAt, job.ModifiedAt, 0)

	if err != nil {
		return Job{}, err
	}

	return createdJob, nil
}

func (s *PostgresStore) ClaimPendingJobs(ctx context.Context, ownerId string, leaseDuration time.Duration, limit int) ([]Job, error) {
	const q = `UPDATE jobs SET status = $1, modified_at = $2, lease_owner = $3, lease_expiry = $4, fencing_token = fencing_token + 1 WHERE id IN (
		SELECT id FROM jobs WHERE status = $5 AND run_at <= $6 ORDER BY run_at LIMIT $7 FOR UPDATE SKIP LOCKED)
		RETURNING *`

	var jobs []Job

	now := time.Now().Truncate(time.Second)
	leaseExpiry := now.Add(leaseDuration).Truncate(time.Second)

	err := s.db.SelectContext(ctx, &jobs, q, StateDispatched, now, ownerId, leaseExpiry, StateScheduled, now, limit)
	if err != nil {
		return []Job{}, err
	}

	return jobs, nil
}

func (s *PostgresStore) RenewLease(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int, leaseDuration time.Duration) error {
	const q = "UPDATE jobs SET lease_expiry = $1, modified_at = $2 WHERE id = $3 and status = $4 AND lease_owner = $5 and fencing_token = $6"

	now := time.Now().Truncate(time.Second)
	leaseExpiry := now.Add(leaseDuration).Truncate(time.Second)

	rows, err := s.db.ExecContext(ctx, q, leaseExpiry, now, id, StateRunning, ownerId, fencingToken)
	if err != nil {
		return fmt.Errorf("renew-lease: %w", err)
	}

	if n, _ := rows.RowsAffected(); n == 0 {
		return ErrStaleFenceToken
	}

	return nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id uuid.UUID) (Job, error) {
	const q = "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}

	if err != nil {
		return Job{}, fmt.Errorf("get %w:", err)
	}

	return job, nil
}

func (s *PostgresStore) CancelJob(ctx context.Context, id uuid.UUID) error {
	const q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE id = $3 AND status = $4"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateCancelled, now, id, StateScheduled)
	if err != nil {
		return fmt.Errorf("cancel: %w", err)
	}

	if n, _ := res.RowsAffected(); n == 0 {
		var job Job
		err = s.db.GetContext(ctx, &job, "SELECT id FROM jobs WHERE id = $1", id)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return ErrDenyCancellation
	}
	return nil
}

func (s *PostgresStore) MarkJobRunning(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int, leaseDuration time.Duration) error {
	const q = `UPDATE jobs SET status = $1, modified_at = $2, lease_expiry = $3
		WHERE id = $4 AND status = $5 AND lease_owner = $6 AND fencing_token = $7`

	now := time.Now().Truncate(time.Second)
	leaseExpiry := now.Add(leaseDuration).Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateRunning, now, leaseExpiry, id, StateDispatched, ownerId, fencingToken)
	if err != nil {
		return fmt.Errorf("mark-running: %w", err)
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleFenceToken
	}

	return nil
}

func (s *PostgresStore) MarkJobSucceeded(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int) error {
	q := "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}

	if err != nil {
		return fmt.Errorf("failed to fetch job: %w", err)
	}

	q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE id = $3 AND lease_owner = $4 AND fencing_token = $5"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateSuccess, now, id, ownerId, fencingToken)
	if err != nil {
		return fmt.Errorf("failed to mark job as success: %w", err)
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return ErrStaleFenceToken
	}

	return nil
}

func (s *PostgresStore) MarkJobFailed(ctx context.Context, id uuid.UUID, ownerId string, fencingToken int) (bool, error) {
	q := "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	}

	if err != nil {
		return false, fmt.Errorf("failed to fetch job: %w", err)
	}

	now := time.Now().Truncate(time.Second)
	cannotRetry := job.MaxRetries == 0 || job.RetryCount >= job.MaxRetries

	if cannotRetry {
		q = "UPDATE jobs SET status = $1, modified_at = $2, lease_owner = NULL, lease_expiry = NULL WHERE id = $3 AND lease_owner = $4 AND fencing_token = $5"
		rows, err := s.db.ExecContext(ctx, q, StateFailed, now, id, ownerId, fencingToken)

		if err != nil {
			return false, fmt.Errorf("Exception while marking job as failed: %w", err)
		}

		if n, _ := rows.RowsAffected(); n == 0 {
			return false, ErrStaleFenceToken
		}

		return false, nil
	}

	q = `UPDATE jobs
		SET status = $1, modified_at = $2, lease_owner = NULL, lease_expiry = NULL,
		run_at = $3, retry_count = retry_count + 1
		WHERE id = $4 AND lease_owner = $5 AND fencing_token = $6`
	rows, err := s.db.ExecContext(ctx, q, StateScheduled, now, calculateNextRunTime(job.RetryCount, now), id, ownerId, fencingToken)

	if err != nil {
		return true, fmt.Errorf("Exception while marking job as failed: %w", err)
	}

	if n, _ := rows.RowsAffected(); n == 0 {
		return true, ErrStaleFenceToken
	}

	return true, nil
}

func (s *PostgresStore) RecoverCrashedJobs(ctx context.Context) (int, error) {
	// Jobs that are past twice their timeout are considered crashed
	const q = "UPDATE jobs SET status = $1, modified_at = $2, lease_owner = NULL, lease_expiry = NULL WHERE status = $3 AND modified_at + (2 * timeout_millis * interval '1 millisecond') < $4"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateScheduled, now, StateRunning, now)
	if err != nil {
		return 0, err
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *PostgresStore) RecoverExpiredLeases(ctx context.Context) (int, error) {
	const q = "UPDATE jobs SET status = $1, lease_owner = NULL, lease_expiry = NULL, modified_at = $2 WHERE status IN ($3, $4) AND lease_expiry < $5"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateScheduled, now, StateDispatched, StateRunning, now)
	if err != nil {
		return 0, err
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}

func calculateNextRunTime(retryCount int, now time.Time) time.Time {
	nextTime := backoffBaseTime * time.Duration(math.Pow(2, float64(retryCount-1)))
	nextTime = min(nextTime, backoffMaxTime)

	return now.Add(nextTime)
}
