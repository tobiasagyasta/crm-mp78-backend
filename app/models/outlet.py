from app.extensions import db
from datetime import datetime

class Outlet(db.Model):
    __tablename__ = "outlets"

    id = db.Column(db.Integer, primary_key=True)
    outlet_code = db.Column(db.String(50), unique=True, nullable=False)
    outlet_name_gojek = db.Column(db.String(255), nullable=True)
    outlet_name_grab = db.Column(db.String(255), nullable=True)
    outlet_phone = db.Column(db.String(20), nullable=True)
    outlet_email = db.Column(db.String(255), nullable=True)
    area = db.Column(db.String(100), nullable=False)
    service_area = db.Column(db.String(100), nullable=True)
    city_grouping = db.Column(db.String(100), nullable=True)
    address = db.Column(db.String(500), nullable=True)
    brand = db.Column(db.String(100), nullable=True)
    
    # Partner Information
    partner_name = db.Column(db.String(255), nullable=True)
    partner_phone = db.Column(db.String(20), nullable=True)
    pic_partner_name = db.Column(db.String(255), nullable=True)
    pic_phone = db.Column(db.String(20), nullable=True)
    
    # Operational Information
    status = db.Column(db.Enum("Active", "Inactive", name="outlet_status"), nullable=False, default="Active")
    closing_date = db.Column(db.String(10), nullable=True) 
    operating_hours = db.Column(db.String(255), nullable=True)
    coordinator_avenger = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "outlet_code": self.outlet_code,
            "outlet_name_gojek": self.outlet_name_gojek,
            "outlet_name_grab": self.outlet_name_grab,
            "outlet_phone": self.outlet_phone,
            "outlet_email": self.outlet_email,
            "area": self.area,
            "service_area": self.service_area,
            "city_grouping": self.city_grouping,
            "address": self.address,
            "brand": self.brand,
            "partner_name": self.partner_name,
            "partner_phone": self.partner_phone,
            "pic_partner_name": self.pic_partner_name,
            "pic_phone": self.pic_phone,
            "status": self.status,
            "closing_date": self.closing_date,
            "operating_hours": self.operating_hours,
            "coordinator_avenger": self.coordinator_avenger,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
