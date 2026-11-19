from datetime import date
from decimal import Decimal
from unittest import TestCase

from flask import Flask
from sqlalchemy import text

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.monthly_totals import MonthlyTotal
from app.services.monthly_totals_service import rebuild_monthly_totals


class MonthlyTotalsTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        db.session.execute(text('CREATE TABLE outlets (outlet_code VARCHAR PRIMARY KEY, brand VARCHAR)'))
        DailyMerchantTotal.__table__.create(db.engine)
        MonthlyTotal.__table__.create(db.engine)
        db.session.execute(text("INSERT INTO outlets VALUES ('a', 'MP78'), ('b', 'MPR')"))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        MonthlyTotal.__table__.drop(db.engine)
        DailyMerchantTotal.__table__.drop(db.engine)
        db.session.execute(text('DROP TABLE outlets'))
        db.session.commit()
        self.context.pop()

    def test_full_month_platform_brand_and_idempotence(self):
        db.session.add_all([
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1), report_type='gojek',
                               total_gross=100, total_net=90),
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 31), report_type='gojek',
                               total_gross=200, total_net=180),
            DailyMerchantTotal(outlet_id='b', date=date(2026, 1, 10), report_type='grab',
                               total_gross=50, total_net=45),
            DailyMerchantTotal(outlet_id='missing', date=date(2026, 1, 10), report_type='shopee',
                               total_gross=20, total_net=19),
            DailyMerchantTotal(outlet_id='a', date=date(2026, 2, 1), report_type='gojek',
                               total_gross=999, total_net=999),
        ])
        db.session.commit()
        for _ in range(2):
            rebuild_monthly_totals(date(2026, 1, 15), date(2026, 1, 16))
            db.session.commit()
        rows = {(r.outlet_id, r.report_type): r for r in MonthlyTotal.query.all()}
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows['a', 'gojek'].total_net, Decimal('270'))
        self.assertEqual(rows['a', 'gojek'].brand, 'MP78')
        self.assertEqual(rows['b', 'grab'].brand, 'MPR')
        self.assertIsNone(rows['missing', 'shopee'].brand)

    def test_zero_stale_and_platform_filter(self):
        db.session.add_all([
            MonthlyTotal(outlet_id='a', month=date(2026, 1, 1), report_type='gojek',
                         brand='MP78', total_gross=10, total_net=10),
            MonthlyTotal(outlet_id='a', month=date(2026, 1, 1), report_type='grab',
                         brand='MP78', total_gross=20, total_net=20),
        ])
        db.session.commit()
        rebuild_monthly_totals(date(2026, 1, 1), date(2026, 1, 31), ['gojek'])
        db.session.commit()
        rows = {r.report_type: r for r in MonthlyTotal.query.all()}
        self.assertEqual(rows['gojek'].total_net, 0)
        self.assertEqual(rows['grab'].total_net, 20)

    def test_invalid_platform_and_range(self):
        with self.assertRaises(ValueError):
            rebuild_monthly_totals(date(2026, 1, 2), date(2026, 1, 1))
        with self.assertRaises(ValueError):
            rebuild_monthly_totals(date(2026, 1, 1), date(2026, 1, 1), ['shopeepay'])
