from contextlib import ExitStack
from datetime import date, datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from flask import Flask
from openpyxl import Workbook

from app.extensions import db
from app.models.outlet import Outlet
from app.models.qpon_reports import QponReport
from app.models.tiktok_reports import TiktokReport
from app.services.excel_export import data_service
from app.services.excel_export.sheets.closing_sheet import ClosingSheet


class TiktokClosingShiftTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        for model in (TiktokReport, QponReport):
            model.__table__.create(db.engine)

    def tearDown(self):
        db.session.remove()
        for model in (QponReport, TiktokReport):
            model.__table__.drop(db.engine)
        self.context.pop()

    def test_brand_shift_queries_prior_period_for_tiktok_and_qpon(self):
        brands = ('MP78', 'MPR', 'MPR Mandiri', 'MPR Non MP78', ' mpr mandiri ', 'Other')
        for index, brand in enumerate(brands):
            code = str(index)
            db.session.add(TiktokReport(
                outlet_code=code, outlet_order_id=f'previous-{code}',
                order_time=datetime(2026, 10, 25), net_amount=100, gross_amount=120,
            ))
            db.session.add(TiktokReport(
                outlet_code=code, outlet_order_id=f'current-{code}',
                order_time=datetime(2026, 11, 1), net_amount=200, gross_amount=240,
            ))
            db.session.add(QponReport(
                outlet_code=code, billing_id=f'previous-qpon-{code}',
                bill_created_at=datetime(2026, 10, 25), gross_amount=25,
            ))
            db.session.add(QponReport(
                outlet_code=code, billing_id=f'qpon-{code}',
                bill_created_at=datetime(2026, 11, 1), gross_amount=50,
            ))
        db.session.commit()

        mocks = (
            patch.object(data_service.GojekReport, 'query'),
            patch.object(data_service.GrabFoodReport, 'query'),
            patch.object(data_service.ShopeeReport, 'query'),
            patch.object(data_service.ShopeepayReport, 'query'),
            patch.object(data_service.WebshopReport, 'query'),
            patch.object(data_service.MaximReport, 'query'),
            patch.object(data_service.VoucherReport, 'query'),
            patch.object(data_service.CashReport, 'query'),
            patch.object(data_service.ManualEntry, 'query'),
            patch.object(data_service.MP78Mutation, 'query'),
            patch.object(data_service, '_get_grab_disbursements', return_value=[]),
            patch.object(data_service, '_get_tiktok_disbursements', return_value=[]),
            patch.object(data_service, '_match_grab_disbursement_mutations'),
            patch.object(data_service, '_aggregate_qpon_mutations'),
            patch.object(data_service, '_match_mutations'),
            patch.object(data_service, 'get_minus_manual_entries', return_value=[]),
        )
        with ExitStack() as stack:
            outlet_query = stack.enter_context(patch.object(Outlet, 'query'))
            for mock in mocks:
                patched = stack.enter_context(mock)
                if hasattr(patched, 'filter'):
                    patched.filter.return_value.all.return_value = []
                    patched.filter.return_value.outerjoin.return_value.outerjoin.return_value.add_entity.return_value.add_entity.return_value.all.return_value = []
                    patched.filter.return_value.order_by.return_value.all.return_value = []

            for index, brand in enumerate(brands):
                with self.subTest(brand=brand):
                    outlet_query.filter_by.return_value.first.return_value = SimpleNamespace(
                        brand=brand, store_id_grab=None
                    )
                    data = data_service.get_report_data(
                        str(index), datetime(2026, 11, 1), datetime(2026, 11, 8)
                    )
                    first, eighth = date(2026, 11, 1), date(2026, 11, 8)
                    daily = data['daily_totals']
                    shifted = brand != 'Other'
                    self.assertEqual(daily[first]['Tiktok_Closing_Net'], 100 if shifted else 200)
                    self.assertEqual(daily[eighth]['Tiktok_Closing_Net'], 200 if shifted else 0)
                    self.assertEqual(daily[first]['Tiktok_Net'], 200)
                    self.assertEqual(daily[first]['Qpon_Closing_Net'], 25 if shifted else 50)
                    self.assertEqual(daily[eighth]['Qpon_Closing_Net'], 50 if shifted else 0)
                    sheet = ClosingSheet(Workbook(), data)
                    self.assertAlmostEqual(
                        sheet._get_tiktok_closing_display_value('main', eighth),
                        190 if shifted else 0,
                    )
