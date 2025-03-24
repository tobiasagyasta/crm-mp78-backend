from app.extensions import db

class IncomeCategory(db.Model):
    __tablename__ = "income_categories"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name
        }
