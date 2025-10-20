from app.extensions import db
from datetime import datetime
from app.models.outlet import Outlet
import re

class PKBMutation(db.Model):
    __tablename__ = 'pkb_mutations'

    id = db.Column(db.Integer, primary_key=True)
    outlet_code = db.Column(db.String(50), db.ForeignKey('outlet.outlet_code'), nullable=False)
    mutation_date = db.Column(db.Date, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    description = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    outlet = db.relationship('Outlet', backref=db.backref('pkb_mutations', lazy=True))

    def __repr__(self):
        return f"<PKBMutation id={self.id} outlet_code={self.outlet_code} mutation_date={self.mutation_date} amount={self.amount}>"

    @staticmethod
    def parse_minus_date(description):
        indo_months = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MEI': 5, 'JUN': 6,
            'JUL': 7, 'AGU': 8, 'SEP': 9, 'OKT': 10, 'NOV': 11, 'DES': 12
        }
        match = re.search(r'MINUS(\d{1,2})([A-Z]{3})', description.upper())
        if match:
            day = int(match.group(1))
            month_str = match.group(2)
            month = indo_months.get(month_str)
            if month:
                year = datetime.now().year
                try:
                    return datetime(year, month, day).date()
                except Exception:
                    return None
        return None