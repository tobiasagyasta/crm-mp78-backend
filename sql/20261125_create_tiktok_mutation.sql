CREATE TABLE tiktok_mutation (
    id SERIAL PRIMARY KEY,
    rekening_number VARCHAR NOT NULL,
    tanggal DATE NOT NULL,
    transaksi VARCHAR NOT NULL,
    transaction_type VARCHAR NOT NULL,
    transaction_id VARCHAR NOT NULL UNIQUE,
    transaction_amount NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX ix_tiktok_mutation_tanggal ON tiktok_mutation (tanggal);
