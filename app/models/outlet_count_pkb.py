from app.extensions import db
from datetime import date

class OutletCountPKB(db.Model):
    __tablename__ = 'outlet_count_pkb'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=False)
    outlet_count = db.Column(db.Integer, nullable=False)

    def __repr__(self):
        return f"<OutletCountPKB id={self.id} range={self.start_date}→{self.end_date} count={self.outlet_count}>"

    def to_dict(self):
        return {
            'id': self.id,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'outlet_count': self.outlet_count
        }