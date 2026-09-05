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
		INSERT INTO jobs (id, type, data, status, run_at, created_at, modified_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING *`

	var createdJob Job

	err := s.db.GetContext(ctx, &createdJob, q,
		job.Id, job.Type, job.Data, job.Status, job.RunAt, job.CreatedAt, job.ModifiedAt)

	if err != nil {
		return Job{}, err
	}

	return createdJob, nil
}

func (s *PostgresStore) GetJobs(ctx context.Context, batch int) ([]Job, error) {
	const q = "SELECT * FROM jobs LIMIT $1"

	var jobs []Job
	err := s.db.SelectContext(ctx, &jobs, q, batch)

	if err != nil {
		return []Job{}, err
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
