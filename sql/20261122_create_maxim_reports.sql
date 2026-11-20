-- Apply before deploying Maxim report support.
CREATE TABLE IF NOT EXISTS maxim_reports (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    brand_name VARCHAR,
    branch VARCHAR,
    outlet_code VARCHAR,
    gross_value NUMERIC,
    nett_value NUMERIC,
    created_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_maxim_reports_order_id
    ON maxim_reports (order_id);
