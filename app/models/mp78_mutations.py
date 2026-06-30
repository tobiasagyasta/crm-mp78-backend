from datetime import datetime

from app.extensions import db


class MP78Mutation(db.Model):
    __tablename__ = 'mp78_mutations'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    rekening_number = db.Column(db.String, nullable=False)
    tanggal = db.Column(db.Date, nullable=True)
    transaksi = db.Column(db.String, nullable=True)
    transaction_type = db.Column(db.String, nullable=True)
    transaction_amount = db.Column(db.Numeric, nullable=True)
    mp78_code = db.Column(db.String(10), nullable=False)
    outlet_code = db.Column(db.String(50), nullable=True)
    transaction_id = db.Column(db.String(64), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.Index('ix_mp78_mutations_date_code', 'tanggal', 'mp78_code'),
        db.Index('ix_mp78_mutations_date_amount', 'tanggal', 'transaction_amount'),
    )

    def __repr__(self):
        return f"<MP78Mutation {self.tanggal} {self.mp78_code} {self.transaction_amount}>"
