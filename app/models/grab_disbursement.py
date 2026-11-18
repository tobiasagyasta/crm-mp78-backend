from datetime import datetime

from app.extensions import db


class GrabDisbursement(db.Model):
    __tablename__ = 'grab_disbursements'

    id = db.Column(db.Integer, primary_key=True)
    disbursement_id = db.Column(db.String, nullable=False, unique=True, index=True)
    store_id = db.Column(db.String, nullable=False, index=True)
    store_name = db.Column(db.String, nullable=False)
    transfer_date = db.Column(db.DateTime, nullable=False, index=True)
    net_amount = db.Column(db.Numeric(18, 2), nullable=False)
    status = db.Column(db.String, nullable=False)
    rekening_id = db.Column(db.Integer, db.ForeignKey('rekenings.id'), nullable=False, index=True)
    mutation_id = db.Column(db.Integer, db.ForeignKey('bank_mutations.id', ondelete='SET NULL'), unique=True, nullable=True)
    account_suffix = db.Column(db.String(4), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    rekening = db.relationship('Rekening')

    @property
    def rekening_name(self):
        return self.rekening.name if self.rekening else None
