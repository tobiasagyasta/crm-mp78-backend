from app.extensions import db
from datetime import datetime

class ShopeepayReport(db.Model):
    __tablename__ = 'shopeepay_reports'

    no = db.Column(db.Integer, primary_key=True, autoincrement=True)
    brand_name = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    
    merchant_host = db.Column(db.String, nullable=True)
    partner_merchant_id = db.Column(db.String, nullable=True)
    merchant_store_name = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=True)
    merchant_scope = db.Column(db.String, nullable=True)
    transaction_id = db.Column(db.String, nullable=True)
    reference_id = db.Column(db.String, nullable=True)
    parent_id = db.Column(db.String, nullable=True)
    external_reference_id = db.Column(db.String, nullable=True)
    issuer_identifier = db.Column(db.String, nullable=True)
    transaction_amount = db.Column(db.Numeric, nullable=True)
    fee_mdr = db.Column(db.Numeric, nullable=True)
    settlement_amount = db.Column(db.Numeric, nullable=True)
    terminal_id = db.Column(db.String, nullable=True)
    create_time = db.Column(db.DateTime, nullable=True)
    update_time = db.Column(db.DateTime, nullable=True)
    adjustment_reason = db.Column(db.String, nullable=True)
    entity_id = db.Column(db.String, nullable=True)
    fee_cofunding = db.Column(db.Numeric, nullable=True)
    reward_amount = db.Column(db.Numeric, nullable=True)
    reward_type = db.Column(db.String, nullable=True)
    promo_type = db.Column(db.String, nullable=True)
    payment_method = db.Column(db.String, nullable=True)
    currency_code = db.Column(db.String, nullable=True)
    voucher_promotion_event_name = db.Column(db.String, nullable=True)
    payment_option = db.Column(db.String, nullable=True)
    fee_withdrawal = db.Column(db.Numeric, nullable=True)
    fee_handling = db.Column(db.Numeric, nullable=True)

    def __repr__(self):
        return f"<ShopeepayReport {self.transaction_id}, {self.create_time}>"