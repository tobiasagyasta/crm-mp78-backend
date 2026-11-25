from datetime import datetime

from app.extensions import db


class TiktokDisbursement(db.Model):
    __tablename__ = 'tiktok_disbursements'
    __table_args__ = (
        db.UniqueConstraint('statement_no', 'item_order_id', 'type',
                            name='uq_tiktok_disbursements_statement_item_type'),
    )

    id = db.Column(db.Integer, primary_key=True)
    statement_no = db.Column(db.String, nullable=False)
    statement_order_id = db.Column(db.String, nullable=False, index=True)
    item_order_id = db.Column(db.String, nullable=False, index=True)
    store_id = db.Column(db.String, nullable=False, index=True)
    store_name = db.Column(db.String, nullable=False)
    type = db.Column(db.String, nullable=False)
    order_creation_date = db.Column(db.DateTime, nullable=False)
    order_payment_date = db.Column(db.Date, nullable=False, index=True)
    total_settlement_amount = db.Column(db.Numeric(18, 2), nullable=False)
    total_commission = db.Column(db.Numeric(18, 2), nullable=False)
    total_platform_fees = db.Column(db.Numeric(18, 2), nullable=False)
    creator_commission = db.Column(db.Numeric(18, 2), nullable=False)
    payment_amount = db.Column(db.Numeric(18, 2), nullable=False)
    refund_amount = db.Column(db.Numeric(18, 2), nullable=False)
    platform_incentive = db.Column(db.Numeric(18, 2), nullable=False)
    platform_incentive_refunds = db.Column(db.Numeric(18, 2), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
