from datetime import date, datetime
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask
from openpyxl import Workbook

from app.extensions import db
from app.models.grab_disbursement import GrabDisbursement
from app.models.bank_mutations import BankMutation
from app.models.rekening import Rekening
from app.services.excel_export.data_service import (
    _aggregate_grab_disbursements, _calculate_grand_totals,
    _get_grab_disbursements, _init_daily_total, _match_grab_disbursement_mutations,
    _match_mutations,
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
        BankMutation.__table__.create(db.engine)
        db.session.add(Rekening(id=1, name='Bank', rekening_number='1230385'))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        BankMutation.__table__.drop(db.engine)
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

    def test_flag_enables_exactly_one_grab_matching_path(self):
        for enabled, expected in ((True, ['gojek', 'shopee', 'shopeepay']),
                                  (False, ['gojek', 'grab', 'shopee', 'shopeepay'])):
            with self.subTest(enabled=enabled), \
                    patch('app.services.excel_export.data_service.GRAB_DISBURSEMENT_FIRST_MATCHING', enabled), \
                    patch('app.services.excel_export.data_service.TransactionMatcher') as matcher:
                matcher.return_value.get_mutations_query.return_value.all.return_value = []
                _match_mutations({}, 'outlet', datetime(2026, 11, 1), datetime(2026, 11, 2))
                self.assertEqual([call.args[0] for call in matcher.call_args_list], expected)

    def test_no_disbursements_skip_bank_query(self):
        with patch.object(BankMutation, 'query', new_callable=MagicMock) as query:
            _match_grab_disbursement_mutations({}, [])
            query.filter.assert_not_called()

    def test_mutations_match_closest_account_date_and_not_other_stores(self):
        transfer = datetime(2026, 12, 1)
        db.session.add_all([
            GrabDisbursement(disbursement_id='a', store_id='store', store_name='Store',
                             transfer_date=transfer, net_amount=100000, status='Completed',
                             rekening_id=1, account_suffix='0385'),
            GrabDisbursement(disbursement_id='b', store_id='other', store_name='Other',
                             transfer_date=transfer, net_amount=200000, status='Completed',
                             rekening_id=1, account_suffix='0385'),
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100000),
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=200000),
            BankMutation(rekening_number='wrong', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100000),
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='DB', transaction_amount=100000),
            BankMutation(rekening_number='1230385', tanggal=date(2026, 12, 2), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100000),
        ])
        db.session.commit()
        day = date(2026, 11, 30)
        daily = {day: _init_daily_total()}
        daily[day]['Grab_Net'] = 90000
        reports = _get_grab_disbursements('store', datetime(2026, 11, 30), datetime(2026, 11, 30))
        _match_grab_disbursement_mutations(daily, reports)
        self.assertEqual(daily[day]['Grab_Mutation'], 100000)
        saved = GrabDisbursement.query.filter_by(disbursement_id='a').one()
        self.assertIsNotNone(saved.mutation_id)
        self.assertEqual(db.session.get(BankMutation, saved.mutation_id).platform_code, 'store')
        daily_repeat = {day: _init_daily_total()}
        _match_grab_disbursement_mutations(daily_repeat, reports)
        self.assertEqual(daily_repeat[day]['Grab_Mutation'], 100000)
        data = {'outlet': SimpleNamespace(brand='MP78', outlet_name_gojek='Outlet'),
                'start_date': day, 'end_date': day, 'all_dates': [day],
                'daily_totals': daily, 'grand_totals': _calculate_grand_totals(daily),
                'minusan_by_date': {}}
        sheet = DailySheet(Workbook(), data)
        sheet.generate()
        headers = sheet._get_headers()
        col = headers.index('Grab Mutation') + 1
        self.assertEqual(col, headers.index('Grab Pencairan') + 2)
        self.assertEqual(sheet.ws.cell(6, col).value, 100000)
        self.assertEqual(sheet.build_preview()['totals']['Grab Mutation'], 100000)
        self.assertEqual(headers[col], 'Grab Difference')
        self.assertEqual(sheet.ws.cell(6, col + 1).value, 10000)
        self.assertEqual(sheet.build_preview()['totals']['Grab Difference'], 10000)

    def test_ambiguous_and_distant_matches_stay_blank(self):
        transfer = datetime(2026, 12, 1)
        for identifier, store in [('a', 'store'), ('b', 'other')]:
            db.session.add(GrabDisbursement(disbursement_id=identifier, store_id=store, store_name='Store',
                                           transfer_date=transfer, net_amount=100000, status='Completed',
                                           rekening_id=1, account_suffix='0385'))
        db.session.add_all([
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100000),
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=300000),
        ])
        db.session.commit()
        day = date(2026, 11, 30)
        daily = {day: _init_daily_total()}
        reports = _get_grab_disbursements('store', datetime(2026, 11, 30), datetime(2026, 11, 30))
        _match_grab_disbursement_mutations(daily, reports)
        self.assertIsNone(daily[day]['Grab_Mutation'])

    def test_near_amount_and_preassigned_code_are_not_matched(self):
        transfer = datetime(2026, 12, 1)
        db.session.add(GrabDisbursement(disbursement_id='a', store_id='store', store_name='Store',
                                       transfer_date=transfer, net_amount=100000, status='Completed',
                                       rekening_id=1, account_suffix='0385'))
        db.session.add_all([
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100001),
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100000, platform_code='other'),
        ])
        db.session.commit()
        day = date(2026, 11, 30)
        daily = {day: _init_daily_total()}
        reports = _get_grab_disbursements('store', datetime(2026, 11, 30), datetime(2026, 11, 30))
        _match_grab_disbursement_mutations(daily, reports)
        self.assertIsNone(daily[day]['Grab_Mutation'])
        self.assertIsNone(reports[0].mutation_id)
