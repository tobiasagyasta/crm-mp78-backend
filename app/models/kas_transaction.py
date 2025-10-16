from app.extensions import db
from sqlalchemy import Date, String, Float, Integer

class KasTransaction(db.Model):
    __tablename__ = 'kas_transactions'

    id = db.Column(Integer, primary_key=True)
    tanggal = db.Column(Date, nullable=False)
    keterangan = db.Column(String(255), nullable=False)
    tipe = db.Column(String(50), nullable=False)  # 'Masuk' or 'Keluar'
    jumlah = db.Column(Float, nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'tanggal': self.tanggal.isoformat(),
            'keterangan': self.keterangan,
            'tipe': self.tipe,
            'jumlah': self.jumlah
        }