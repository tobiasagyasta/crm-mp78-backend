from app.extensions import db

class GojekReport(db.Model):
    __tablename__ = 'gojek_reports'

    brand_name = db.Column(db.String, nullable=False)
    outlet_code = db.Column(db.String, nullable=False)
    serial_no = db.Column(db.String, nullable=False)
    waktu_transaksi = db.Column(db.DateTime, nullable=False)
    nomor_pesanan = db.Column(db.String, nullable=False)
    currency = db.Column(db.String, nullable=True)
    gross_sales = db.Column(db.Numeric, nullable=True)
    komisi_program = db.Column(db.Numeric, nullable=True)
    nama_program = db.Column(db.String, nullable=True)
    biaya_komisi = db.Column(db.Numeric, nullable=True)
    diskon_ditanggung_mitra = db.Column(db.Numeric, nullable=True)
    voucher_commission = db.Column(db.Numeric, nullable=True)
    total_biaya_komisi = db.Column(db.Numeric, nullable=True)
    nett_sales = db.Column(db.Numeric, nullable=True)

    __table_args__ = (
        db.PrimaryKeyConstraint('waktu_transaksi', 'nomor_pesanan', name='pk_waktu_nomor'),
    )

    def __repr__(self):
        return f"<Report {self.nomor_pesanan}, {self.waktu_transaksi}>"
