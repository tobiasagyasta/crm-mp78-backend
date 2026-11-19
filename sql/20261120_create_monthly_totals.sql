-- Apply before deploying the version that refreshes monthly totals.
CREATE TABLE IF NOT EXISTS monthly_totals (
    outlet_id VARCHAR NOT NULL,
    month DATE NOT NULL,
    report_type VARCHAR NOT NULL,
    brand VARCHAR(100),
    total_gross NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_net NUMERIC(18, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (outlet_id, month, report_type),
    CONSTRAINT ck_monthly_totals_month_start CHECK (EXTRACT(DAY FROM month) = 1)
);

CREATE INDEX IF NOT EXISTS ix_monthly_totals_brand_month_platform
    ON monthly_totals (brand, month, report_type);
