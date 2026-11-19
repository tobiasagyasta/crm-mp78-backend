from datetime import date, datetime
from decimal import Decimal
from unittest import TestCase

from flask import Flask

from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.gojek_reports import GojekReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.shopee_reports import ShopeeReport
from app.services.consolidation_service import rebuild_daily_totals


class DailyTotalsRebuildTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        for model in (DailyMerchantTotal, GojekReport, GrabFoodReport, ShopeeReport):
            model.__table__.create(db.engine)

    def tearDown(self):
        db.session.remove()
        for model in (ShopeeReport, GrabFoodReport, GojekReport, DailyMerchantTotal):
            model.__table__.drop(db.engine)
        self.context.pop()

    def test_rebuild_is_idempotent_and_preserves_other_platforms(self):
        day = date(2026, 1, 5)
        db.session.add_all([
            GojekReport(transaction_id='g', brand_name='MP78', outlet_code='a',
                        transaction_date=day, amount=100, nett_amount=90),
            GrabFoodReport(brand_name='MPR', outlet_code='b', id_toko='store',
                           tanggal_dibuat=datetime(2026, 1, 5, 12), amount=200, total=180),
            ShopeeReport(outlet_code='a', order_id='s', transaction_type='order',
                         order_create_time=datetime(2026, 1, 5, 12), order_status='Completed',
                         order_amount=300, net_income=270),
            ShopeeReport(outlet_code='a', order_id='cancel', transaction_type='order',
                         order_create_time=datetime(2026, 1, 5), order_status='Cancelled',
                         order_amount=900, net_income=900),
            DailyMerchantTotal(outlet_id='a', date=day, report_type='shopeepay',
                               total_gross=42, total_net=42),
            DailyMerchantTotal(outlet_id='old', date=day, report_type='gojek',
                               total_gross=50, total_net=50),
        ])
        db.session.commit()
        for _ in range(2):
            self.assertEqual(rebuild_daily_totals(day, day), {'gojek': 2, 'grab': 1, 'shopee': 1})
            db.session.commit()
        totals = {(row.outlet_id, row.report_type): row for row in DailyMerchantTotal.query.all()}
        self.assertEqual(totals['a', 'gojek'].total_net, Decimal('90'))
        self.assertEqual(totals['b', 'grab'].total_net, Decimal('180'))
        self.assertEqual(totals['a', 'shopee'].total_net, Decimal('270'))
        self.assertEqual(totals['a', 'shopeepay'].total_net, Decimal('42'))
        self.assertEqual(totals['old', 'gojek'].total_net, Decimal('0'))

    def test_validation(self):
        with self.assertRaises(ValueError):
            rebuild_daily_totals(date(2026, 2, 1), date(2026, 1, 1))
        with self.assertRaises(ValueError):
            rebuild_daily_totals(date(2026, 1, 1), date(2026, 1, 1), ['shopeepay'])

    def test_progress_reports_partitions_and_empty_platform(self):
        day = date(2026, 1, 5)
        db.session.add(GojekReport(transaction_id='g', brand_name='MP78', outlet_code='a',
                                   transaction_date=day, amount=100, nett_amount=90))
        db.session.commit()
        events = []
        result = rebuild_daily_totals(day, day, ['gojek', 'shopee'],
                                    progress=lambda *event: events.append(event))
        self.assertEqual(result, {'gojek': 1, 'shopee': 0})
        self.assertEqual(events, [
            ('platform_start', 'gojek', 0, None),
            ('partitions_found', 'gojek', 0, 1),
            ('partition_processed', 'gojek', 1, 1),
            ('platform_complete', 'gojek', 1, 1),
            ('platform_start', 'shopee', 0, None),
            ('partitions_found', 'shopee', 0, 0),
            ('platform_complete', 'shopee', 0, 0),
        ])
