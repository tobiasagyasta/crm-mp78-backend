from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from flask import Flask
from openpyxl import Workbook

from app.extensions import db
from app.models.tiktok_disbursement import TiktokDisbursement
from app.models.tiktok_reports import TiktokReport
from app.services.excel_export.data_service import (
    _aggregate_tiktok_disbursements, _calculate_grand_totals,
    _get_tiktok_disbursements, _init_daily_total,
)
from app.services.excel_export.sheets.daily_sheet import DailySheet


class DailyTiktokDisbursementTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        TiktokDisbursement.__table__.create(db.engine)

    def tearDown(self):
        db.session.remove()
        TiktokDisbursement.__table__.drop(db.engine)
        self.context.pop()

    def disbursement(self, item_id, amount, statement='statement', entry_type='Order'):
        return TiktokDisbursement(
            statement_no=statement, statement_order_id='order', item_order_id=item_id,
            store_id='store', store_name='Store', type=entry_type,
            order_creation_date=datetime(2026, 11, 13), order_payment_date=date(2026, 11, 17),
            total_settlement_amount=Decimal(amount), total_commission=0, total_platform_fees=0,
            creator_commission=0, payment_amount=0, refund_amount=0,
            platform_incentive=0, platform_incentive_refunds=0,
        )

    def test_matching_order_date_sheet_and_preview(self):
        db.session.add_all([
            self.disbursement('item-1', '100.50'),
            self.disbursement('item-1', '-10.50', statement='refund', entry_type='Refund'),
            self.disbursement('item-2', '0'),
            self.disbursement('other-outlet', '999'),
        ])
        db.session.commit()
        days = [date(2026, 11, 13), date(2026, 11, 14), date(2026, 11, 15)]
        reports = [
            SimpleNamespace(outlet_order_id=' item-1 ', order_time=datetime(2026, 11, 13)),
            SimpleNamespace(outlet_order_id='item-2', order_time=datetime(2026, 11, 14)),
            SimpleNamespace(outlet_order_id='missing', order_time=datetime(2026, 11, 15)),
        ]
        daily = {day: _init_daily_total() for day in days}
        _aggregate_tiktok_disbursements(daily, reports, _get_tiktok_disbursements(reports))
        self.assertEqual([daily[day]['Tiktok_Disbursement'] for day in days], [90, 0, None])

        for brand in ('MP78', 'MPR', 'Pukis & Martabak Kota Baru',
                      'Es Ce Hun Tiau & Bongko Wendy', 'Other'):
            with self.subTest(brand=brand):
                data = {'outlet': SimpleNamespace(brand=brand, outlet_name_gojek='Outlet'),
                        'start_date': days[0], 'end_date': days[-1], 'all_dates': days,
                        'daily_totals': daily, 'grand_totals': _calculate_grand_totals(daily),
                        'minusan_by_date': {}}
                sheet = DailySheet(Workbook(), data)
                sheet.generate()
                headers = sheet._get_headers()
                column = headers.index('Tiktok Pencairan')
                self.assertEqual(column, headers.index('Tiktok Net') + 1)
                self.assertEqual([sheet.ws.cell(row, column + 1).value for row in (6, 7, 8, 9)],
                                 [90, 0, None, 90])
                preview = sheet.build_preview()
                self.assertEqual([row[column] for row in preview['rows']], [90, 0, None])
                self.assertEqual(preview['totals']['Tiktok Pencairan'], 90)

    def test_no_match_leaves_daily_and_grand_total_blank(self):
        day = date(2026, 11, 13)
        daily = {day: _init_daily_total()}
        self.assertEqual(_get_tiktok_disbursements([]), [])
        _aggregate_tiktok_disbursements(daily, [], [])
        data = {'outlet': SimpleNamespace(brand='Other'), 'all_dates': [day],
                'daily_totals': daily, 'grand_totals': _calculate_grand_totals(daily),
                'minusan_by_date': {}}
        preview = DailySheet(None, data).build_preview()
        self.assertIsNone(preview['rows'][0][preview['columns'].index('Tiktok Pencairan')])
        self.assertIsNone(preview['totals']['Tiktok Pencairan'])
