from datetime import date
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch
from types import SimpleNamespace

from flask import Flask

from app.controllers.expense_category_controller import expense_category_bp
from app.controllers.manual_entry_controller import manual_entries_bp
from app.extensions import db
from app.models.expense_category import ExpenseCategory
from app.models.income_category import IncomeCategory
from app.models.manual_entry import ManualEntry


class ManualEntryInstallmentTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI="sqlite://", TESTING=True)
        db.init_app(app)
        app.register_blueprint(manual_entries_bp)
        app.register_blueprint(expense_category_bp)
        self.context = app.app_context()
        self.context.push()
        ExpenseCategory.__table__.create(db.engine)
        IncomeCategory.__table__.create(db.engine)
        ManualEntry.__table__.create(db.engine)
        db.session.add_all([
            ExpenseCategory(id=1, name="Pembayaran Termin"),
            ExpenseCategory(id=2, name="Other"),
        ])
        db.session.commit()
        self.client = app.test_client()
        self.outlet = SimpleNamespace(closing_date="26-25")
        self.outlet_query = patch(
            "app.services.manual_entry_installment_service.Outlet.query"
        )
        self.outlet_query.start().filter_by.return_value.first.return_value = self.outlet
        self.today = patch("app.services.manual_entry_installment_service.date", wraps=date)
        self.mock_date = self.today.start()
        self.mock_date.today.return_value = date(2026, 11, 25)

    def tearDown(self):
        self.outlet_query.stop()
        self.today.stop()
        db.session.remove()
        ManualEntry.__table__.drop(db.engine)
        IncomeCategory.__table__.drop(db.engine)
        ExpenseCategory.__table__.drop(db.engine)
        self.context.pop()

    def payload(self, **changes):
        result = {
            "outlet_code": "OUT1", "brand_name": "Brand", "entry_type": "expense",
            "category_id": 1, "amount": "25000000.00",
            "installment_count": 5,
        }
        result.update(changes)
        return result

    def test_schedule_and_existing_listing_and_delete(self):
        response = self.client.post("/manual-entries/", json=self.payload())
        self.assertEqual(response.status_code, 201, response.json)
        entries = response.json["entries"]
        self.assertEqual(len(entries), 5)
        self.assertEqual(entries[0]["start_date"], "2026-10-26")
        self.assertEqual(entries[0]["end_date"], "2026-11-25")
        self.assertEqual(entries[1]["start_date"], "2026-11-26")
        self.assertEqual(entries[4]["end_date"], "2027-03-25")
        self.assertTrue(all(e["amount"] == 5000000 for e in entries))
        listing = self.client.get("/manual-entries/?outlet_code=OUT1&start_date=2026-10-26&end_date=2026-11-25")
        self.assertEqual(listing.json["totals"]["total_expense"], 5000000)
        entry_id = entries[0]["id"]
        self.assertEqual(self.client.put(f"/manual-entries/{entry_id}", json={"amount": 1}).status_code, 400)
        self.assertEqual(self.client.delete(f"/manual-entries/{entry_id}").status_code, 200)
        self.assertEqual(ManualEntry.query.count(), 0)

    def test_fallback_and_rounding(self):
        self.outlet.closing_date = None
        self.mock_date.today.return_value = date(2028, 1, 31)
        response = self.client.post("/manual-entries/", json=self.payload(
            amount="100.00", installment_count=3))
        self.assertEqual(response.status_code, 201, response.json)
        entries = response.json["entries"]
        self.assertEqual([e["amount"] for e in entries], [33.34, 33.33, 33.33])
        self.assertEqual([(e["start_date"], e["end_date"]) for e in entries], [
            ("2028-01-01", "2028-01-31"),
            ("2028-02-01", "2028-02-29"),
            ("2028-03-01", "2028-03-31"),
        ])
        self.assertEqual(sum((row.amount for row in ManualEntry.query.all()), Decimal(0)), Decimal("100"))

    def test_short_month_closing_dates(self):
        self.outlet.closing_date = "31-30"
        self.mock_date.today.return_value = date(2028, 2, 29)
        response = self.client.post("/manual-entries/", json=self.payload(
            installment_count=2))
        self.assertEqual(response.status_code, 201, response.json)
        self.assertEqual(response.json["entries"][0]["start_date"], "2028-02-29")
        self.assertEqual(response.json["entries"][1]["start_date"], "2028-03-31")

    def test_current_period_ignores_supplied_dates(self):
        self.mock_date.today.return_value = date(2026, 11, 26)
        response = self.client.post("/manual-entries/", json=self.payload(
            start_date="2000-01-01", end_date="2000-01-31", installment_count=1))
        self.assertEqual(response.status_code, 201, response.json)
        self.assertEqual(response.json["entries"][0]["start_date"], "2026-11-26")
        self.assertEqual(response.json["entries"][0]["end_date"], "2026-12-25")

    def test_invalid_input_does_not_create_rows(self):
        for changes in [
            {"installment_count": 0}, {"installment_count": True},
            {"installment_count": "5"}, {"amount": "NaN"}, {"amount": "0.01"},
            {"amount": "1.001"},
        ]:
            response = self.client.post("/manual-entries/", json=self.payload(**changes))
            self.assertEqual(response.status_code, 400, (changes, response.json))
        self.assertEqual(ManualEntry.query.count(), 0)

    def test_regular_entries_and_system_category(self):
        data = self.payload(category_id=2, amount=100, start_date="2026-11-01", end_date="2026-11-30")
        data.pop("installment_count")
        response = self.client.post("/manual-entries/", json=data)
        self.assertEqual(response.status_code, 201, response.json)
        self.assertIsNone(response.json["installment_group_id"])
        self.assertEqual(self.client.put("/manual-entries/1", json={"category_id": 1}).status_code, 400)
        self.assertEqual(self.client.put("/expense_categories/1", json={"name": "Other name"}).status_code, 400)
        self.assertEqual(self.client.delete("/expense_categories/1").status_code, 400)
