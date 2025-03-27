from app.extensions import db

class GojekReport(db.Model):
    __tablename__ = 'gojek_reports'

    brand_name = db.Column(db.String, nullable=False)
    outlet_code = db.Column(db.String, nullable=False)
    transaction_id = db.Column(db.String, nullable=False)
    transaction_date = db.Column(db.Date, nullable=False)
    stan = db.Column(db.String, nullable=True)
    nett_amount = db.Column(db.Numeric, nullable=True)
    currency = db.Column(db.String, nullable=True)
    amount = db.Column(db.Numeric, nullable=True)
    transaction_time = db.Column(db.Time, nullable=True)
    transaction_status = db.Column(db.String, nullable=True)
    transaction_reference = db.Column(db.String, nullable=True)
    order_id = db.Column(db.String, nullable=True)
    settlement_time = db.Column(db.Time, nullable=True)
    settlement_date = db.Column(db.Date, nullable=True)
    feature = db.Column(db.String, nullable=True)
    payment_type = db.Column(db.String, nullable=True)
    gopay_transaction_id_reference = db.Column(db.String, nullable=True)
    merchant_name = db.Column(db.String, nullable=True)
    merchant_id = db.Column(db.String, nullable=True)
    promo_type = db.Column(db.String, nullable=True)
    promo_name = db.Column(db.String, nullable=True)
    gopay_promo = db.Column(db.Numeric, nullable=True)
    gofood_discount = db.Column(db.Numeric, nullable=True)
    voucher_commission = db.Column(db.Numeric, nullable=True)
    tax = db.Column(db.Numeric, nullable=True)
    witholding_tax = db.Column(db.Numeric, nullable=True)

    __table_args__ = (
        db.PrimaryKeyConstraint('transaction_id', name='pk_transaction_id'),
    )

    def __repr__(self):
        return f"<Report {self.transaction_id}, {self.transaction_date}>"
