ALTER TABLE jobs
    ADD COLUMN lease_owner   TEXT,
    ADD COLUMN lease_expiry  TIMESTAMPTZ,
    ADD COLUMN fencing_token INT NOT NULL DEFAULT 0;

CREATE INDEX idx_jobs_running ON jobs (lease_expiry) WHERE status = 'running';