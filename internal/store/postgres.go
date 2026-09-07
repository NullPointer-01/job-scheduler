package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

func (s *PostgresStore) CreateJob(ctx context.Context, job Job) (Job, error) {
	const q = `
		INSERT INTO jobs (id, type, data, status, run_at, timeout_millis, created_at, modified_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING *`

	var createdJob Job

	err := s.db.GetContext(ctx, &createdJob, q,
		job.Id, job.Type, job.Data, job.Status, job.RunAt, job.TimeoutMillis, job.CreatedAt, job.ModifiedAt)

	if err != nil {
		return Job{}, err
	}

	return createdJob, nil
}

func (s *PostgresStore) GetPendingJobs(ctx context.Context, limit int) ([]Job, error) {
	const q1 = "SELECT * FROM jobs WHERE status = $1 ORDER BY run_at LIMIT $2 FOR UPDATE SKIP LOCKED"
	var jobs []Job

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return []Job{}, err
	}
	defer tx.Rollback()

	err = tx.SelectContext(ctx, &jobs, q1, StateScheduled, limit)
	if err != nil {
		return []Job{}, err
	}

	if len(jobs) == 0 {
		return []Job{}, tx.Commit()
	}

	jobIds := make([]uuid.UUID, 0, len(jobs))
	for _, job := range jobs {
		jobIds = append(jobIds, job.Id)
	}

	q2, args, err := sqlx.In(
		"UPDATE jobs SET status = ?, modified_at = ? WHERE id IN (?)",
		StateRunning,
		time.Now().Truncate(time.Second),
		jobIds,
	)
	q2 = tx.Rebind(q2)

	_, err = tx.ExecContext(ctx, q2, args...)
	if err != nil {
		return []Job{}, err
	}

	if err := tx.Commit(); err != nil {
		return []Job{}, err
	}

	// Update jobs status
	for i := range jobs {
		jobs[i].Status = StateRunning
	}
	return jobs, nil
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
	q := "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if job.Status != StateScheduled {
		return ErrDenyCancellation
	}

	q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE id = $3 AND status = $4"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateCancelled, now, id, StateScheduled)
	if err != nil {
		return fmt.Errorf("cancel: %w", err)
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return ErrUnknown
	}
	return nil
}

func (s *PostgresStore) MarkJobSucceeded(ctx context.Context, id uuid.UUID) error {
	q := "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}

	if err != nil {
		return fmt.Errorf("failed to fetch job: %w", err)
	}

	q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE id = $3"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateSuccess, now, id)
	if err != nil {
		return fmt.Errorf("failed to mark job as success: %w", err)
	}

	if n, _ := res.RowsAffected(); n > 0 {
		return ErrUnknown
	}

	return nil
}

func (s *PostgresStore) MarkJobFailed(ctx context.Context, id uuid.UUID) error {
	q := "SELECT * FROM jobs WHERE id = $1"

	var job Job
	err := s.db.GetContext(ctx, &job, q, id)

	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}

	if err != nil {
		return fmt.Errorf("failed to fetch job: %w", err)
	}

	q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE id = $3"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateFailed, now, id)
	if err != nil {
		return fmt.Errorf("failed to mark job as success: %w", err)
	}

	if n, _ := res.RowsAffected(); n > 0 {
		return ErrUnknown
	}

	return nil
}

func (s *PostgresStore) RecoverCrashedJobs(ctx context.Context) (int, error) {
	// Jobs that are past twice their timeout are considered crashed
	const q = "UPDATE jobs SET status = $1, modified_at = $2 WHERE status = $3 AND modified_at + (2 * timeout_millis * interval '1 millisecond') < $4"
	now := time.Now().Truncate(time.Second)

	res, err := s.db.ExecContext(ctx, q, StateScheduled, now, StateRunning, now)
	if err != nil {
		return 0, err
	}

	n, _ := res.RowsAffected()
	return int(n), nil
}
