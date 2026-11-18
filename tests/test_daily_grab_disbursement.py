from datetime import date, datetime
from types import SimpleNamespace
from unittest import TestCase

from flask import Flask
from openpyxl import Workbook

from app.extensions import db
from app.models.grab_disbursement import GrabDisbursement
from app.models.rekening import Rekening
from app.services.excel_export.data_service import (
    _aggregate_grab_disbursements, _calculate_grand_totals,
    _get_grab_disbursements, _init_daily_total,
)
from app.services.excel_export.sheets.daily_sheet import DailySheet


class DailyGrabDisbursementTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        Rekening.__table__.create(db.engine)
        GrabDisbursement.__table__.create(db.engine)
        db.session.add(Rekening(id=1, name='Bank', rekening_number='1230385'))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        GrabDisbursement.__table__.drop(db.engine)
        Rekening.__table__.drop(db.engine)
        self.context.pop()

    def test_next_day_store_status_boundaries_and_sheets(self):
        def report(identifier, store, day, amount, status='Completed'):
            return GrabDisbursement(disbursement_id=identifier, store_id=store, store_name='Store',
                                    transfer_date=datetime(2026, 12, day), net_amount=amount,
                                    status=status, rekening_id=1, account_suffix='0385')

        db.session.add_all([
            report('a', 'store', 1, 100), report('b', 'store', 1, 50),
            report('c', 'store', 2, 200), report('other', 'other', 1, 999),
            report('pending', 'store', 1, 999, 'Pending'),
            report('late', 'store', 3, 999),
        ])
        db.session.commit()
        days = [date(2026, 11, 30), date(2026, 12, 1)]
        daily = {day: _init_daily_total() for day in days}
        records = _get_grab_disbursements('store', datetime(2026, 11, 30), datetime(2026, 12, 1))
        _aggregate_grab_disbursements(daily, records)
        self.assertEqual([daily[day]['Grab_Disbursement'] for day in days], [150, 200])
        self.assertEqual(_get_grab_disbursements(None, datetime(2026, 11, 30), datetime(2026, 12, 1)), [])

        for brand in ('MP78', 'MPR', 'Pukis & Martabak Kota Baru', 'Es Ce Hun Tiau & Bongko Wendy', 'Other'):
            with self.subTest(brand=brand):
                data = {'outlet': SimpleNamespace(brand=brand, outlet_name_gojek='Outlet'),
                        'start_date': days[0], 'end_date': days[-1], 'all_dates': days,
                        'daily_totals': daily, 'grand_totals': _calculate_grand_totals(daily),
                        'minusan_by_date': {}}
                sheet = DailySheet(Workbook(), data)
                sheet.generate()
                headers = sheet._get_headers()
                index = headers.index('Grab Pencairan')
                self.assertEqual(index, headers.index('Grab Net') + 1)
                self.assertEqual([sheet.ws.cell(row, index + 1).value for row in (6, 7, 8)], [150, 200, 350])
                self.assertEqual(sheet.build_preview()['totals']['Grab Pencairan'], 350)

    def test_missing_disbursement_is_blank(self):
        daily = {date(2026, 11, 1): _init_daily_total()}
        _aggregate_grab_disbursements(daily, [])
        self.assertIsNone(daily[date(2026, 11, 1)]['Grab_Disbursement'])
