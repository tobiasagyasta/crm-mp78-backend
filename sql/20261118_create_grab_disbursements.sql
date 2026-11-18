CREATE TABLE grab_disbursements (
    id SERIAL PRIMARY KEY,
    disbursement_id VARCHAR NOT NULL UNIQUE,
    store_id VARCHAR NOT NULL,
    store_name VARCHAR NOT NULL,
    transfer_date TIMESTAMP NOT NULL,
    net_amount NUMERIC(18, 2) NOT NULL,
    status VARCHAR NOT NULL,
    rekening_id INTEGER NOT NULL REFERENCES rekenings(id),
    account_suffix VARCHAR(4) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX ix_grab_disbursements_store_id ON grab_disbursements (store_id);
CREATE INDEX ix_grab_disbursements_transfer_date ON grab_disbursements (transfer_date);
CREATE INDEX ix_grab_disbursements_rekening_id ON grab_disbursements (rekening_id);
