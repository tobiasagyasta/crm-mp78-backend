from app.extensions import db
from datetime import datetime

class Pukis(db.Model):
    __tablename__ = 'pukis'

    id = db.Column(db.Integer, primary_key=True)
    tanggal = db.Column(db.DateTime, nullable=False)
    outlet_code = db.Column(db.String(50), nullable=False)
    brand_name = db.Column(db.String(50), nullable=False)
    pukis_inventory_type = db.Column(db.Enum('produksi', 'terjual', 'retur', 'free', name='pukis_inventory_types'))
    pukis_product_type = db.Column(db.Enum('jumbo', 'klasik', name='pukis_product_types'))
    amount = db.Column(db.Numeric(precision=12, scale=2), default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
   

    def __repr__(self):
        return (f"<Pukis id={self.id}, tanggal={self.tanggal}, outlet_code={self.outlet_code}, "
                f"brand_name={self.brand_name}, inventory_type={self.pukis_inventory_type}, "
                f"product_type={self.pukis_product_type}, amount={self.amount}>")

    def to_dict(self):
        return {
            "id": self.id,
            "tanggal": self.tanggal.isoformat() if self.tanggal else None,
            "outlet_code": self.outlet_code,
            "brand_name": self.brand_name,
            "pukis_inventory_type": self.pukis_inventory_type,
            "pukis_product_type": self.pukis_product_type,
            "amount": float(self.amount) if self.amount is not None else 0.0,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }