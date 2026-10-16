-- Apply before deploying the updated import workers and API.
-- Existing jobs have NULL breakdowns because their historical skip reasons are unknown.
ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS duplicate_rows INTEGER;
ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS non_duplicate_skipped_rows INTEGER;
ALTER TABLE import_jobs ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS import_job_issues (
    id SERIAL PRIMARY KEY,
    import_job_id INTEGER NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    sheet_name VARCHAR(255),
    reason_code VARCHAR(80) NOT NULL,
    message TEXT NOT NULL,
    field_name VARCHAR(255),
    context JSONB,
    row_data JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_import_job_issues_import_job_id
    ON import_job_issues (import_job_id);
CREATE INDEX IF NOT EXISTS ix_import_job_issues_job_attempt_reason
    ON import_job_issues (import_job_id, attempt_number, reason_code);
