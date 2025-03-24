from app.extensions import db
from datetime import datetime
from app.models.income_category import IncomeCategory
from app.models.expense_category import ExpenseCategory

class ManualEntry(db.Model):
    __tablename__ = 'manual_entries'

    id = db.Column(db.Integer, primary_key=True)
    outlet_code = db.Column(db.String(100), nullable = False)
    entry_type = db.Column(db.Enum('income', 'expense', name='entry_types'), nullable=False)
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    description = db.Column(db.String(255))
    start_date = db.Column(db.Date, nullable=False)  # Replace date with start_date
    end_date = db.Column(db.Date, nullable=False)    # Add end_date
    category_id = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Define separate foreign keys for income and expense categories
    income_category = db.relationship('IncomeCategory', foreign_keys=[category_id],
                                    primaryjoin="and_(ManualEntry.category_id==IncomeCategory.id, "
                                              "ManualEntry.entry_type=='income')")
    expense_category = db.relationship('ExpenseCategory', foreign_keys=[category_id],
                                     primaryjoin="and_(ManualEntry.category_id==ExpenseCategory.id, "
                                               "ManualEntry.entry_type=='expense')")

    __table_args__ = (
        db.CheckConstraint(
            "entry_type IN ('income', 'expense')",
            name='valid_entry_type'
        ),
    )

    def __repr__(self):
        return f'<ManualEntry {self.outlet_code} {self.entry_type} {self.amount}>'

    def to_dict(self):
        category_name = None
        if self.entry_type == 'income' and self.income_category:
            category_name = self.income_category.name
        elif self.entry_type == 'expense' and self.expense_category:
            category_name = self.expense_category.name

        return {
            "id": self.id,
            "outlet_code": self.outlet_code,
            "entry_type": self.entry_type,
            "amount": float(self.amount),
            "description": self.description,
            "start_date": self.start_date.strftime('%Y-%m-%d'),  # Update date to start_date
            "end_date": self.end_date.strftime('%Y-%m-%d'),      # Add end_date
            "category_id": self.category_id,
            "category_name": category_name,
            "created_at": self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            "updated_at": self.updated_at.strftime('%Y-%m-%d %H:%M:%S')
        }