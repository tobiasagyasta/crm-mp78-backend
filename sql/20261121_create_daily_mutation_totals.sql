CREATE TABLE IF NOT EXISTS daily_mutation_totals (
    bank_date DATE NOT NULL,
    platform VARCHAR(20) NOT NULL,
    transaction_date DATE NOT NULL,
    attribution_key VARCHAR(180) NOT NULL,
    attribution VARCHAR(20) NOT NULL,
    outlet_code VARCHAR(100),
    brand VARCHAR(100),
    amount NUMERIC(20, 2) NOT NULL DEFAULT 0,
    mutation_count INTEGER NOT NULL DEFAULT 0,
    missing_amount_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bank_date, platform, transaction_date, attribution_key)
);

CREATE INDEX IF NOT EXISTS ix_daily_mutation_totals_transaction_brand_platform
    ON daily_mutation_totals (transaction_date, brand, platform);
