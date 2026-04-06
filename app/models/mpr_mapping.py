from datetime import datetime

from app.extensions import db


class MprMapping(db.Model):
    __tablename__ = "mpr_mappings"

    id = db.Column(db.Integer, primary_key=True)
    mp78_outlet_code = db.Column(db.String(50), nullable=False)
    mpr_outlet_code = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<MprMapping mp78_outlet_code={self.mp78_outlet_code} "
            f"mpr_outlet_code={self.mpr_outlet_code}>"
        )
