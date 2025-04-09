from app.extensions import db
from datetime import datetime

class BankMutation(db.Model):
    __tablename__ = 'bank_mutations'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rekening_number = db.Column(db.String, nullable=False)
    tanggal = db.Column(db.DateTime, nullable=False)
    keterangan = db.Column(db.String, nullable=True)
    masuk = db.Column(db.Numeric, nullable=True)
    keluar = db.Column(db.Numeric, nullable=True)
    cr_cb = db.Column(db.String, nullable=True)
    saldo = db.Column(db.Numeric, nullable=True)
    transaksi = db.Column(db.String, nullable=True)
    outlet = db.Column(db.String, nullable=True)
    closing = db.Column(db.String, nullable=True)
    harga_per_outlet = db.Column(db.String, nullable=True)
    ket = db.Column(db.String, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<BankMutation {self.tanggal} {self.keterangan}>"