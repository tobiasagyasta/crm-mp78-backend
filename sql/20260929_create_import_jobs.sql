CREATE TABLE IF NOT EXISTS import_jobs (
    id SERIAL PRIMARY KEY,
    report_type VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    original_filename VARCHAR(255),
    storage_provider VARCHAR(50) NOT NULL DEFAULT 'railway_bucket',
    storage_bucket VARCHAR(255) NOT NULL,
    storage_key VARCHAR(500) NOT NULL,
    total_rows INTEGER NOT NULL DEFAULT 0,
    processed_rows INTEGER NOT NULL DEFAULT 0,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    skipped_rows INTEGER NOT NULL DEFAULT 0,
    failed_rows INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    extra_data JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_import_jobs_status ON import_jobs (status);
CREATE INDEX IF NOT EXISTS ix_import_jobs_report_type ON import_jobs (report_type);
CREATE INDEX IF NOT EXISTS ix_import_jobs_storage_key ON import_jobs (storage_key);
