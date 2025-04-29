from app.extensions import db
from datetime import datetime

class PukisReport(db.Model):
    __tablename__ = 'pukis_reports'

    id = db.Column(db.Integer, primary_key=True)
    tanggal = db.Column(db.DateTime, nullable=False)
    outlet_code = db.Column(db.String(50), nullable=False)
    brand_name = db.Column(db.String(50), nullable=False)
    pukis_terjual_total_jumbo = db.Column(db.Integer, nullable=True)
    pukis_terjual_total_klasik = db.Column(db.Integer, nullable=True)
    pukis_sisa = db.Column(db.Integer, nullable=True)
    pukis_sisa_klasik = db.Column(db.Integer, nullable=True)
    pukis_free = db.Column(db.Integer, nullable=True)
    pukis_sisa_klasik_free = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<PukisReport {self.id}, {self.tanggal}, {self.outlet_code}, {self.brand_name}>"

    def to_dict(self):
        return {
            "id": self.id,
            "tanggal": self.tanggal.isoformat(),
            "outlet_code": self.outlet_code,
            "brand_name": self.brand_name,
            "pukis_terjual_total_jumbo": self.pukis_terjual_total_jumbo,
            "pukis_terjual_total_klasik": self.pukis_terjual_total_klasik,
            "pukis_sisa": self.pukis_sisa,
            "pukis_sisa_klasik": self.pukis_sisa_klasik,
            "pukis_free": self.pukis_free,
            "pukis_sisa_klasik_free": self.pukis_sisa_klasik_free,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }