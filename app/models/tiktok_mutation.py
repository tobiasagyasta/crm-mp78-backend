from datetime import datetime

from app.extensions import db


class TikTokMutation(db.Model):
    """An account-level TikTok earnings credit from a bank statement."""

    __tablename__ = 'tiktok_mutation'

    id = db.Column(db.Integer, primary_key=True)
    rekening_number = db.Column(db.String, nullable=False)
    tanggal = db.Column(db.Date, nullable=False, index=True)
    transaksi = db.Column(db.String, nullable=False)
    transaction_type = db.Column(db.String, nullable=False)
    transaction_id = db.Column(db.String, nullable=False, unique=True)
    transaction_amount = db.Column(db.Numeric(18, 2), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
