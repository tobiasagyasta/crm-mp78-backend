from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Flask

from app.controllers import reports_controller as controller
from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.outlet import Outlet
from app.models.qpon_reports import QponReport
from app.models.rekening import Rekening
from app.services.rekening_info_service import OutletRekeningInfo


class ReportsTotalsRekeningsTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()

    def tearDown(self):
        db.session.remove()
        self.context.pop()

    def request_totals(self, code, mapping=None, outlet=None, mpr_outlet=None, dates=''):
        models = [
            controller.GojekReport, controller.GrabFoodReport, controller.ShopeeReport,
            controller.ShopeepayReport, controller.TiktokReport, QponReport,
            controller.WebshopReport, controller.MaximReport, controller.CashReport, controller.ManualEntry,
        ]
        query = MagicMock()
        query.filter.return_value = query
        query.all.return_value = []
        with ExitStack() as stack:
            for model in models:
                stack.enter_context(patch.object(model, 'query', query))
            stack.enter_context(patch.object(controller, 'get_mpr_mapping_for_outlet', return_value=(outlet, mapping)))
            stack.enter_context(patch.object(controller, 'calculate_mpr_totals', return_value={'gojek': 0}))
            outlet_query = stack.enter_context(patch.object(controller.Outlet, 'query'))
            outlet_query.filter_by.return_value.first.return_value = mpr_outlet
            lookup = stack.enter_context(patch.object(controller.RekeningInfoService, 'get_outlet_rekenings'))
            lookup.side_effect = lambda obj, start, end, *, latest: (
                [OutletRekeningInfo(obj.outlet_code, 'Gojek', 'Bank', obj.number)] if obj else []
            )
            with self.app.test_request_context(f'/?outlet_code={code}{dates}'):
                response, status = controller.get_reports_totals()
            return response.get_json(), status, lookup.call_args_list

    def test_mapped_outlet_includes_both_accounts_and_period(self):
        outlet = SimpleNamespace(outlet_code='MP', brand='MP78', number='001')
        mpr = SimpleNamespace(outlet_code='MPR', brand='MPR', number='002')
        mapping = SimpleNamespace(mpr_outlet_code='MPR')
        data, status, calls = self.request_totals(
            'MP', mapping, outlet, mpr, '&start_date=2026-11-01&end_date=2026-11-30'
        )
        self.assertEqual(status, 200)
        self.assertEqual(data['rekenings']['outlet'][0]['rekening_number'], '001')
        self.assertEqual(data['rekenings']['mpr'][0]['rekening_number'], '002')
        self.assertEqual(calls[0].args[1:], (date(2026, 11, 1), date(2026, 11, 30)))
        self.assertEqual(calls[1].args[1:], calls[0].args[1:])
        self.assertIn('mpr_totals', data['totals'])

    def test_latest_accounts_use_outlet_references_and_bank_date(self):
        for model in (BankMutation, Rekening, QponReport, controller.WebshopReport, controller.MaximReport):
            model.__table__.create(db.engine)
        outlet = Outlet(outlet_code='ABC', store_id_shopee='123456')
        db.session.add_all([QponReport(billing_id='bill1', outlet_code='ABC'),
                            QponReport(billing_id='bill2', outlet_code='OTHER')])
        for platform, key, day, account in [
            ('Qpon', 'bill1', 3, 'new'), ('Qpon', 'bill1', 1, 'old'),
            ('Qpon', 'bill2', 4, 'unrelated'), ('Qpon', 'bill1', 6, 'future'),
            ('Shopee', '3456', 2, 'pay'), ('Webshop', 'ABC', 2, 'web'),
        ]:
            db.session.add(BankMutation(platform_name=platform, platform_code=key,
                                        rekening_number=account, tanggal=date(2026, 11, day)))
        rows = controller.RekeningInfoService.get_outlet_rekenings(
            outlet, end_date=date(2026, 11, 5), latest=True)
        self.assertEqual([(r.platform_name, r.rekening_number) for r in rows],
                         [('Qpon', 'new'), ('ShopeePay', 'pay'), ('Webshop', 'web')])

    def test_unmapped_and_all_have_empty_mpr_accounts(self):
        outlet = SimpleNamespace(outlet_code='MP', brand='MP78', number='001')
        data, status, _ = self.request_totals('MP', outlet=outlet)
        self.assertEqual(status, 200)
        self.assertEqual(data['rekenings']['mpr'], [])
        data, status, calls = self.request_totals('ALL')
        self.assertEqual(status, 200)
        self.assertEqual(data['rekenings'], {'outlet': [], 'mpr': []})
        self.assertEqual(calls, [])
