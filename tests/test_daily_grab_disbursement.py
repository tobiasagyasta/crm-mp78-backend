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
        self.refresh = patch('app.services.mutation_totals_service.rebuild_daily_mutation_totals')
        self.refresh_mock = self.refresh.start()
        self.addCleanup(self.refresh.stop)
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

    def test_duplicate_payouts_share_no_mutation(self):
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
        self.assertEqual(daily[day]['Grab_Mutation'], 100000)
        payouts = GrabDisbursement.query.order_by(GrabDisbursement.id).all()
        self.assertIsNotNone(payouts[0].mutation_id)
        self.assertIsNone(payouts[1].mutation_id)
        self.assertEqual(db.session.get(BankMutation, payouts[0].mutation_id).platform_code, 'store')

    def test_duplicate_credits_use_one_per_payout(self):
        transfer = datetime(2026, 12, 1)
        for identifier, store in [('a', 'store'), ('b', 'other')]:
            db.session.add(GrabDisbursement(disbursement_id=identifier, store_id=store, store_name='Store',
                                           transfer_date=transfer, net_amount=100000, status='Completed',
                                           rekening_id=1, account_suffix='0385'))
        for _ in range(2):
            db.session.add(BankMutation(rekening_number='1230385', tanggal=transfer.date(),
                                        platform_name='Grab', transaction_type='CR', transaction_amount=100000))
        db.session.commit()
        day = date(2026, 11, 30)
        daily = {day: _init_daily_total()}
        reports = _get_grab_disbursements('store', datetime(2026, 11, 30), datetime(2026, 11, 30))
        _match_grab_disbursement_mutations(daily, reports)
        payouts = GrabDisbursement.query.order_by(GrabDisbursement.id).all()
        self.assertEqual(len({p.mutation_id for p in payouts}), 2)
        self.assertTrue(all(p.mutation_id is not None for p in payouts))
        self.assertEqual([db.session.get(BankMutation, p.mutation_id).platform_code for p in payouts],
                         ['store', 'other'])

    def test_outside_tolerance_and_preassigned_code_are_not_matched(self):
        transfer = datetime(2026, 12, 1)
        db.session.add(GrabDisbursement(disbursement_id='a', store_id='store', store_name='Store',
                                       transfer_date=transfer, net_amount=100000, status='Completed',
                                       rekening_id=1, account_suffix='0385'))
        db.session.add_all([
            BankMutation(rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                         transaction_type='CR', transaction_amount=100006),
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

    def test_alternate_historical_account_matches_and_remains_visible(self):
        transfer = datetime(2026, 11, 8, 3, 23)
        payout = GrabDisbursement(disbursement_id='p', store_id='mpr-store', store_name='MPR',
                                  transfer_date=transfer, net_amount=100270, status='Completed',
                                  rekening_id=1, account_suffix='0385')
        db.session.add_all([
            payout,
            BankMutation(rekening_number='alternate0229', tanggal=date(2026, 10, 27),
                         platform_name='Grab', platform_code='mpr-store', transaction_amount=1),
            BankMutation(rekening_number='alternate0229', tanggal=transfer.date(),
                         platform_name='Grab', transaction_type='CR', transaction_amount=100270),
        ])
        db.session.commit()
        day = date(2026, 11, 7)
        daily = {day: _init_daily_total()}
        _match_grab_disbursement_mutations(daily, [payout])
        self.assertEqual(daily[day]['Grab_Mutation'], 100270)
        self.assertEqual(db.session.get(BankMutation, payout.mutation_id).rekening_number, 'alternate0229')
        repeat = {day: _init_daily_total()}
        _match_grab_disbursement_mutations(repeat, [payout])
        self.assertEqual(repeat[day]['Grab_Mutation'], 100270)

    def test_alternate_account_must_be_known_and_unambiguous(self):
        transfer = datetime(2026, 11, 8)
        payout = GrabDisbursement(disbursement_id='p', store_id='store', store_name='Store',
                                  transfer_date=transfer, net_amount=100270, status='Completed',
                                  rekening_id=1, account_suffix='0385')
        db.session.add_all([
            payout,
            BankMutation(rekening_number='alternate', tanggal=transfer.date(),
                         platform_name='Grab', transaction_type='CR', transaction_amount=100270),
        ])
        db.session.commit()
        daily = {date(2026, 11, 7): _init_daily_total()}
        _match_grab_disbursement_mutations(daily, [payout])
        self.assertIsNone(payout.mutation_id)
        db.session.add_all([
            BankMutation(rekening_number='alternate', tanggal=date(2026, 10, 1),
                         platform_name='Grab', platform_code='store', transaction_amount=1),
            BankMutation(rekening_number='alternate', tanggal=transfer.date(),
                         platform_name='Grab', transaction_type='CR', transaction_amount=100270),
        ])
        db.session.commit()
        _match_grab_disbursement_mutations(daily, [payout])
        self.assertIsNone(payout.mutation_id)

    def test_same_account_takes_priority_over_alternate(self):
        transfer = datetime(2026, 11, 8)
        payout = GrabDisbursement(disbursement_id='p', store_id='store', store_name='Store',
                                  transfer_date=transfer, net_amount=100270, status='Completed',
                                  rekening_id=1, account_suffix='0385')
        direct = BankMutation(rekening_number='1230385', tanggal=transfer.date(),
                              platform_name='Grab', transaction_type='CR', transaction_amount=100270)
        db.session.add_all([
            payout, direct,
            BankMutation(rekening_number='alternate', tanggal=date(2026, 10, 1),
                         platform_name='Grab', platform_code='store', transaction_amount=1),
            BankMutation(rekening_number='alternate', tanggal=transfer.date(),
                         platform_name='Grab', transaction_type='CR', transaction_amount=100270),
        ])
        db.session.commit()
        _match_grab_disbursement_mutations({date(2026, 11, 7): _init_daily_total()}, [payout])
        self.assertEqual(payout.mutation_id, direct.id)

    def test_credit_linked_to_payout_outside_scope_is_never_reused(self):
        transfer = datetime(2026, 11, 8)
        db.session.add(Rekening(id=2, name='Other', rekening_number='other-account'))
        credit = BankMutation(rekening_number='1230385', tanggal=transfer.date(),
                              platform_name='Grab', transaction_type='CR', transaction_amount=100270)
        db.session.add(credit)
        db.session.flush()
        requested = GrabDisbursement(disbursement_id='requested', store_id='store', store_name='Store',
                                     transfer_date=transfer, net_amount=100270, status='Completed',
                                     rekening_id=1, account_suffix='0385')
        already = GrabDisbursement(disbursement_id='already', store_id='other', store_name='Other',
                                   transfer_date=transfer, net_amount=100270, status='Completed',
                                   rekening_id=2, account_suffix='0000', mutation_id=credit.id)
        db.session.add_all([requested, already])
        db.session.commit()
        _match_grab_disbursement_mutations({date(2026, 11, 7): _init_daily_total()}, [requested])
        self.assertIsNone(requested.mutation_id)
        self.assertEqual(already.mutation_id, credit.id)

    def test_tolerance_in_both_directions_and_closest_match(self):
        transfer = datetime(2026, 12, 1)
        for index, (amount, credit_amount, should_match) in enumerate([
            (493975, 493974, True), (200000, 200005, True),
            (300000, 299995, True), (400000, 399994, False),
            (500000, 500006, False),
        ]):
            db.session.add(GrabDisbursement(
                disbursement_id=str(index), store_id='store', store_name='Store',
                transfer_date=transfer, net_amount=amount, status='Completed',
                rekening_id=1, account_suffix='0385'))
            db.session.add(BankMutation(
                rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
                transaction_type='CR', transaction_amount=credit_amount))
        # A closer credit must win even when it has a higher ID.
        db.session.add(BankMutation(
            rekening_number='1230385', tanggal=transfer.date(), platform_name='Grab',
            transaction_type='CR', transaction_amount=493975))
        db.session.commit()
        reports = GrabDisbursement.query.order_by(GrabDisbursement.id).all()
        day = date(2026, 11, 30)
        daily = {day: _init_daily_total()}
        self.assertEqual(_match_grab_disbursement_mutations(daily, reports), 3)
        self.assertEqual([p.mutation_id is not None for p in reports],
                         [True, True, True, False, False])
        self.assertEqual(db.session.get(BankMutation, reports[0].mutation_id).transaction_amount, 493975)
        self.assertEqual(daily[day]['Grab_Mutation'], 493975 + 200005 + 299995)
        self.assertTrue(all(db.session.get(BankMutation, p.mutation_id).platform_code == 'store'
                            for p in reports[:3]))
