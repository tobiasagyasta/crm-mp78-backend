from datetime import datetime

from app.extensions import db


class MonthlyTotal(db.Model):
    """Calendar-month platform totals for an outlet; month is the first day."""

    __tablename__ = 'monthly_totals'

    outlet_id = db.Column(db.String, primary_key=True)
    month = db.Column(db.Date, primary_key=True)
    report_type = db.Column(db.String, primary_key=True)
    brand = db.Column(db.String(100), nullable=True)
    total_gross = db.Column(db.Numeric(18, 2), nullable=False, default=0)
    total_net = db.Column(db.Numeric(18, 2), nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.Index('ix_monthly_totals_brand_month_platform', 'brand', 'month', 'report_type'),
    )
