from app.extensions import db
from datetime import datetime

class ShopeeReport(db.Model):
    __tablename__ = 'shopee_reports'

    no = db.Column(db.Integer, primary_key=True, autoincrement=True)
    brand_name = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    transaction_type = db.Column(db.String, nullable=False)
    order_id = db.Column(db.String, nullable=False)
    order_pick_up_id = db.Column(db.String, nullable=True)
    store_id = db.Column(db.String, nullable=True)
    store_name = db.Column(db.String, nullable=True)
    order_create_time = db.Column(db.DateTime, nullable=False)
    order_complete_cancel_time = db.Column(db.DateTime, nullable=True)
    order_amount = db.Column(db.Numeric, nullable=True)
    merchant_service_charge = db.Column(db.Numeric, nullable=True)
    pb1 = db.Column(db.Numeric, nullable=True)
    merchant_surcharge_fee = db.Column(db.Numeric, nullable=True)
    merchant_shipping_fee_voucher_subsidy = db.Column(db.Numeric, nullable=True)
    food_direct_discount = db.Column(db.Numeric, nullable=True)
    merchant_food_voucher_subsidy = db.Column(db.Numeric, nullable=True)
    subtotal = db.Column(db.Numeric, nullable=True)
    total = db.Column(db.Numeric, nullable=True)
    commission = db.Column(db.Numeric, nullable=True)
    net_income = db.Column(db.Numeric, nullable=True)
    order_status = db.Column(db.String, nullable=True)
    order_type = db.Column(db.String, nullable=True)

    def __repr__(self):
        return f"<ShopeeReport {self.order_id}, {self.order_create_time}>"