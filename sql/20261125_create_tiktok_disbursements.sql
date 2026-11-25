CREATE TABLE tiktok_disbursements (
    id SERIAL PRIMARY KEY,
    statement_no VARCHAR NOT NULL,
    statement_order_id VARCHAR NOT NULL,
    item_order_id VARCHAR NOT NULL,
    store_id VARCHAR NOT NULL,
    store_name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    order_creation_date TIMESTAMP NOT NULL,
    order_payment_date DATE NOT NULL,
    total_settlement_amount NUMERIC(18, 2) NOT NULL,
    total_commission NUMERIC(18, 2) NOT NULL,
    total_platform_fees NUMERIC(18, 2) NOT NULL,
    creator_commission NUMERIC(18, 2) NOT NULL,
    payment_amount NUMERIC(18, 2) NOT NULL,
    refund_amount NUMERIC(18, 2) NOT NULL,
    platform_incentive NUMERIC(18, 2) NOT NULL,
    platform_incentive_refunds NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_tiktok_disbursements_statement_item_type
        UNIQUE (statement_no, item_order_id, type)
);

CREATE INDEX ix_tiktok_disbursements_statement_order_id ON tiktok_disbursements (statement_order_id);
CREATE INDEX ix_tiktok_disbursements_item_order_id ON tiktok_disbursements (item_order_id);
CREATE INDEX ix_tiktok_disbursements_store_id ON tiktok_disbursements (store_id);
CREATE INDEX ix_tiktok_disbursements_order_payment_date ON tiktok_disbursements (order_payment_date);
