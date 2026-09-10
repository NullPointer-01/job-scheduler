DROP INDEX IF EXISTS idx_jobs_running;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS fencing_token,
    DROP COLUMN IF EXISTS lease_expiry,
    DROP COLUMN IF EXISTS lease_owner;