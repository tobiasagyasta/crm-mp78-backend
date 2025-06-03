from app.extensions import db
from datetime import date, datetime

class DailyMerchantTotal(db.Model):
    __tablename__ = 'daily_merchant_totals'

    outlet_id = db.Column(db.String, primary_key=True)
    date = db.Column(db.Date, primary_key=True)
    report_type = db.Column(db.String, primary_key=True)
    total_gross = db.Column(db.Numeric(precision=12, scale=2), default=0)
    total_net = db.Column(db.Numeric(precision=12, scale=2), default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<DailyMerchantTotal {self.outlet_id} {self.date} {self.report_type}>"
