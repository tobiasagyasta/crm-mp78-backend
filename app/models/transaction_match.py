from datetime import datetime

from app.extensions import db


class TransactionMatch(db.Model):
    __tablename__ = 'transaction_matches'

    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(50), nullable=False)
    outlet_code = db.Column(db.String(100), db.ForeignKey('outlets.outlet_code'), nullable=True)
    report_date = db.Column(db.Date, nullable=False)

    daily_total_outlet_id = db.Column(db.String, nullable=True)
    daily_total_date = db.Column(db.Date, nullable=True)
    daily_total_report_type = db.Column(db.String, nullable=True)

    mutation_id = db.Column(db.Integer, db.ForeignKey('bank_mutations.id'), nullable=True)
    platform_code = db.Column(db.String, nullable=True)
    platform_amount = db.Column(db.Numeric(12, 2), nullable=True)
    mutation_amount = db.Column(db.Numeric(12, 2), nullable=True)
    difference = db.Column(db.Numeric(12, 2), nullable=True)

    status = db.Column(db.String(50), nullable=False)
    match_method = db.Column(db.String(50), nullable=True)
    notes = db.Column(db.String(255), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    outlet = db.relationship('Outlet', foreign_keys=[outlet_code])
    mutation = db.relationship('BankMutation', foreign_keys=[mutation_id])

    __table_args__ = (
        db.ForeignKeyConstraint(
            ['daily_total_outlet_id', 'daily_total_date', 'daily_total_report_type'],
            ['daily_merchant_totals.outlet_id', 'daily_merchant_totals.date', 'daily_merchant_totals.report_type'],
            name='fk_transaction_matches_daily_total',
        ),
        db.CheckConstraint(
            "status IN ('matched', 'unmatched_platform', 'unmatched_mutation', 'ignored')",
            name='valid_transaction_match_status',
        ),
        db.UniqueConstraint(
            'platform', 'daily_total_outlet_id', 'daily_total_date', 'daily_total_report_type',
            name='uq_transaction_matches_daily_total',
        ),
        db.Index('ix_transaction_matches_platform_report_date', 'platform', 'report_date'),
        db.Index('ix_transaction_matches_outlet_report_date', 'outlet_code', 'report_date'),
        db.Index('ix_transaction_matches_status', 'status'),
        db.Index('ix_transaction_matches_mutation_id', 'mutation_id'),
    )

    def __repr__(self):
        return f'<TransactionMatch {self.platform} {self.outlet_code} {self.report_date} {self.status}>'
