from datetime import datetime

from app.extensions import db


class DailyMutationTotal(db.Model):
    """Bank-date aggregates; unattributed rows use an H+1 candidate date."""

    __tablename__ = 'daily_mutation_totals'

    bank_date = db.Column(db.Date, primary_key=True)
    platform = db.Column(db.String(20), primary_key=True)
    transaction_date = db.Column(db.Date, primary_key=True)
    attribution_key = db.Column(db.String(180), primary_key=True)
    attribution = db.Column(db.String(20), nullable=False)
    outlet_code = db.Column(db.String(100), nullable=True)
    brand = db.Column(db.String(100), nullable=True)
    amount = db.Column(db.Numeric(20, 2), nullable=False, default=0)
    mutation_count = db.Column(db.Integer, nullable=False, default=0)
    missing_amount_count = db.Column(db.Integer, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.Index('ix_daily_mutation_totals_transaction_brand_platform',
                 'transaction_date', 'brand', 'platform'),
    )
