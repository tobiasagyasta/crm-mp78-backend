from app.extensions import db


class MaximReport(db.Model):
    __tablename__ = "maxim_reports"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.String, nullable=False, index=True, unique=True)
    brand_name = db.Column(db.String, nullable=True)
    branch = db.Column(db.String, nullable=True)
    outlet_code = db.Column(db.String, nullable=True)
    gross_value = db.Column(db.Numeric, nullable=True)
    nett_value = db.Column(db.Numeric, nullable=True)
    created_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f"<MaximReport {self.order_id}, {self.created_at}>"
