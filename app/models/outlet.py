from app.extensions import db
from datetime import datetime

class Outlet(db.Model):
    __tablename__ = "outlets"

    id = db.Column(db.Integer, primary_key=True)
    outlet_name = db.Column(db.String(255), nullable=False)
    
    # Foreign Key to Partner
    partner_id = db.Column(db.Integer, db.ForeignKey("partners.id"), nullable=False)
    
    partner_name = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Enum("Active", "Inactive", name="outlet_status"), nullable=False, default="Active")
    closing_date = db.Column(db.DateTime, nullable=True)
    address = db.Column(db.String(500), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "outlet_name": self.outlet_name,
            "partner_id": self.partner_id,
            "partner_name": self.partner_name,
            "status": self.status,
            "closing_date": self.closing_date.isoformat() if self.closing_date else None,
            "address": self.address,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
