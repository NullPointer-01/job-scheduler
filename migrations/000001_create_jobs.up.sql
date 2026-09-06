CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE Jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type TEXT NOT NULL,
    data JSONB NOT NULL,
    status TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    timeout_millis INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL
)