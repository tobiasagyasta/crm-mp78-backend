from app.extensions import db
from datetime import datetime

class CashReport(db.Model):
    __tablename__ = 'cash_reports'

    id = db.Column(db.Integer, primary_key=True)
    tanggal = db.Column(db.DateTime, nullable=False)
    outlet_code = db.Column(db.String(50), nullable=False)
    brand_name = db.Column(db.String(50), nullable=False)
    type = db.Column(db.Enum('income', 'expense', name='cash_entry_types'), nullable=False)
    details = db.Column(db.String(255))
    total = db.Column(db.Numeric(10, 2), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CashReport {self.id}, {self.tanggal}, {self.type}, {self.details}, {self.total}>"

    def to_dict(self):
        return {
            "id": self.id,
            "tanggal": self.tanggal.isoformat(),
            "outlet_code": self.outlet_code,
            "brand_name": self.brand_name,  
            "type": self.type,
            "details": self.details,
            "total": float(self.total),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

