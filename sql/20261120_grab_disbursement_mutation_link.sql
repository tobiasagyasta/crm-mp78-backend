ALTER TABLE grab_disbursements
    ADD COLUMN mutation_id INTEGER REFERENCES bank_mutations(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX uq_grab_disbursements_mutation_id
    ON grab_disbursements (mutation_id);
