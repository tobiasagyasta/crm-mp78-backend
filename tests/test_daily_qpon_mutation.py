from datetime import date, datetime
from types import SimpleNamespace
from unittest import TestCase

from flask import Flask
from openpyxl import Workbook

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.services.excel_export.data_service import (
    _aggregate_qpon_closing, _aggregate_qpon_mutations, _calculate_grand_totals, _init_daily_total,
)
from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.sheets.pukis_closing_sheet import PukisClosingSheet
from app.services.excel_export.sheets.daily_sheet import DailySheet


class DailyQponMutationTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        BankMutation.__table__.create(db.engine)

    def tearDown(self):
        db.session.remove()
        BankMutation.__table__.drop(db.engine)
        self.context.pop()

    def test_billing_matches_sum_on_report_date_and_appear_in_sheet(self):
        first, second = date(2026, 11, 1), date(2026, 11, 2)
        reports = [SimpleNamespace(billing_id=key, bill_created_at=datetime(2026, 11, 1))
                   for key in ('a', 'b', 'missing')]
        db.session.add_all([
            BankMutation(rekening_number='1', tanggal=date(2026, 11, 16), platform_name='Qpon',
                         platform_code='a', transaction_amount=17100),
            BankMutation(rekening_number='1', tanggal=date(2026, 11, 17), platform_name='Qpon',
                         platform_code='b', transaction_amount=8550),
            BankMutation(rekening_number='1', tanggal=date(2026, 11, 17), platform_name='Qpon',
                         platform_code='a', transaction_amount=17100),
            BankMutation(rekening_number='1', tanggal=first, platform_name='Gojek',
                         platform_code='missing', transaction_amount=999),
        ])
        db.session.commit()
        daily = {first: _init_daily_total(), second: _init_daily_total()}
        daily[first]['Qpon_Net'] = 26000
        _aggregate_qpon_mutations(daily, reports)
        self.assertEqual(daily[first]['Qpon_Mutation'], 25650)
        self.assertEqual(daily[first]['Qpon_Difference'], -350)
        self.assertIsNone(daily[second]['Qpon_Mutation'])
        self.assertIsNone(daily[second]['Qpon_Difference'])

        for brand in ('MP78', 'MPR', 'Pukis & Martabak Kota Baru', 'Es Ce Hun Tiau & Bongko Wendy'):
            with self.subTest(brand=brand):
                data = {'outlet': SimpleNamespace(brand=brand, outlet_name_gojek='Outlet'),
                        'start_date': first, 'end_date': second, 'all_dates': [first, second],
                        'daily_totals': daily, 'grand_totals': _calculate_grand_totals(daily),
                        'minusan_by_date': {}}
                sheet = DailySheet(Workbook(), data)
                sheet.generate()
                headers = sheet._get_headers()
                self.assertEqual(headers.index('Qpon Mutation'), headers.index('Qpon Net') + 1)
                col = headers.index('Qpon Mutation') + 1
                self.assertEqual(sheet.ws.cell(6, col).value, 25650)
                self.assertIsNone(sheet.ws.cell(7, col).value)
                self.assertEqual(sheet.ws.cell(8, col).value, 25650)
                self.assertEqual(sheet.build_preview()['totals']['Qpon Mutation'], 25650)
                self.assertEqual(headers[col], 'Qpon Difference')
                self.assertEqual(sheet.ws.cell(6, col + 1).value, -350)
                self.assertIsNone(sheet.ws.cell(7, col + 1).value)
                self.assertEqual(sheet.ws.cell(8, col + 1).value, -350)
                self.assertEqual(sheet.build_preview()['totals']['Qpon Difference'], -350)

    def test_closing_uses_shifted_mutation_or_daily_fallback(self):
        days = [date(2026, 11, day) for day in (8, 9, 10)]
        daily = {day: _init_daily_total() for day in days}
        reports = [
            SimpleNamespace(billing_id=key, bill_created_at=datetime(2026, 11, day), gross_amount=amount)
            for key, day, amount in [('matched', 1, 100), ('missing', 2, 200), ('zero', 3, 50)]
        ]
        db.session.add_all([
            BankMutation(rekening_number='1', tanggal=date(2026, 11, 20), platform_name='Qpon',
                         platform_code='matched', transaction_amount=90),
            BankMutation(rekening_number='1', tanggal=date(2026, 11, 20), platform_name='Qpon',
                         platform_code='zero', transaction_amount=0),
        ])
        db.session.commit()
        _aggregate_qpon_closing(daily, reports)
        _aggregate_qpon_mutations(daily, reports, closing=True)
        data = {'outlet': SimpleNamespace(brand='MP78'), 'daily_totals': daily,
                'grand_totals': _calculate_grand_totals(daily)}
        sheet = ClosingSheet(Workbook(), data)
        self.assertEqual([sheet._get_qpon_closing_display_value('main', day) for day in days],
                         [90, 200, 0])
        self.assertEqual(sheet._get_qpon_closing_display_value('main'), 290)
        self.assertAlmostEqual(sheet._get_qpon_closing_ac_display_value('main'), 287.1)
        data['mpr_report_data'] = {'outlet': SimpleNamespace(brand='MPR'),
                                   'daily_totals': daily, 'grand_totals': data['grand_totals']}
        self.assertAlmostEqual(sheet._get_qpon_closing_ac_display_value('mpr'), 287.1)

    def test_pkb_closing_uses_daily_fallback_in_total(self):
        days = [date(2026, 11, 1), date(2026, 11, 2)]
        daily = {day: _init_daily_total() for day in days}
        daily[days[0]].update(Qpon_Net=100, Qpon_Mutation=90)
        daily[days[1]].update(Qpon_Net=200)
        data = {'all_dates': days, 'daily_totals': daily, 'pukis_reports': [],
                'grand_totals': _calculate_grand_totals(daily)}
        sheet = PukisClosingSheet(Workbook(), data)
        sheet._write_data()
        self.assertEqual([sheet.ws.cell(row, 7).value for row in (1, 2, 3)], [90, 200, 290])
