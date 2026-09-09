CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE Jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    data JSONB NOT NULL,
    status TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    retry_count INT NULL,
    max_retries INT NULL,
    timeout_millis INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL
)

CREATE INDEX idx_jobs_scheduled ON jobs (run_at ASC) WHERE status = 'scheduled';