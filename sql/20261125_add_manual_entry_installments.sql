-- Apply before deploying the installment-aware application.
BEGIN;

ALTER TABLE manual_entries
    ALTER COLUMN amount TYPE NUMERIC(18, 2),
    ADD COLUMN IF NOT EXISTS installment_group_id VARCHAR(36),
    ADD COLUMN IF NOT EXISTS installment_number INTEGER,
    ADD COLUMN IF NOT EXISTS installment_count INTEGER;

CREATE INDEX IF NOT EXISTS ix_manual_entries_installment_group_id
    ON manual_entries (installment_group_id);

INSERT INTO expense_categories (name)
SELECT 'Pembayaran Termin'
WHERE NOT EXISTS (
    SELECT 1 FROM expense_categories WHERE name = 'Pembayaran Termin'
);

COMMIT;
