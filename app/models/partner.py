from app.extensions import db
from datetime import datetime

class Partner(db.Model):
    __tablename__ = "partners"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    number_of_outlets = db.Column(db.Integer, nullable=False, default=0)
    rekening_number = db.Column(db.String(50), nullable=False)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship with Outlet
    outlets = db.relationship("Outlet", backref="partner", cascade="all, delete", lazy=True)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "number_of_outlets": self.number_of_outlets,
            "rekening_number": self.rekening_number,
            "last_updated": self.last_updated.isoformat(),
            "outlets": [outlet.to_dict() for outlet in self.outlets]  # Include related outlets
        }
