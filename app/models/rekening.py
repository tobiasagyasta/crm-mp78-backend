from app.extensions import db


class Rekening(db.Model):
    __tablename__ = "rekenings"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    rekening_type = db.Column(db.String, nullable=True)
    rekening_number = db.Column(db.String, nullable=False, unique=True, index=True)

    def __repr__(self):
        return f"<Rekening {self.name}, {self.rekening_number}>"
