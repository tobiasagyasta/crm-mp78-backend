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
    
    # Store IDs for different platforms
    store_id_gojek = db.Column(db.String(50), nullable=True, unique=True)
    store_id_grab = db.Column(db.String(50), nullable=True, unique=True)
    store_id_shopee = db.Column(db.String(50), nullable=True, unique=True)
    
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

    # Admin Credentials
    gojek_admin_email = db.Column(db.String(255), nullable=True)
    gojek_admin_password = db.Column(db.String(255), nullable=True)
    grab_admin_email = db.Column(db.String(255), nullable=True)
    grab_admin_password = db.Column(db.String(255), nullable=True)
    shopee_admin_email = db.Column(db.String(255), nullable=True)
    shopee_admin_password = db.Column(db.String(255), nullable=True)

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
            "store_id_gojek": self.store_id_gojek,
            "store_id_grab": self.store_id_grab,
            "store_id_shopee": self.store_id_shopee,
            # Add admin credentials
            "gojek_admin_email": self.gojek_admin_email,
            "gojek_admin_password": self.gojek_admin_password,
            "grab_admin_email": self.grab_admin_email,
            "grab_admin_password": self.grab_admin_password,
            "shopee_admin_email": self.shopee_admin_email,
            "shopee_admin_password": self.shopee_admin_password,
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
