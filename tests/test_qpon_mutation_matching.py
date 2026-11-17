from datetime import date, datetime
from unittest import TestCase

from flask import Flask

from app.controllers.mutations_controller import mutations_bp
from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.qpon_reports import QponReport
from app.services.mutation_report_importer import import_mutation_report_bytes


class QponMutationMatchingTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(mutations_bp)
        self.context = self.app.app_context()
        self.context.push()
        BankMutation.__table__.create(db.engine)
        QponReport.__table__.create(db.engine)

    def tearDown(self):
        db.session.remove()
        QponReport.__table__.drop(db.engine)
        BankMutation.__table__.drop(db.engine)
        self.context.pop()

    def test_import_and_match_full_billing_id(self):
        csv = (
            'Tanggal,Keterangan,Cabang,Jumlah,Saldo\n'
            '11/16/2026,TRSF E-BANKING CR 1611/FTSCY/WS95051 17100.00  '
            'Qpon11141319744316 180247459  QPON DIGITAL INDON,0,"17,100.00 CR","1,404,410,677.12"\n'
            '11/16/2026,TRSF E-BANKING CR 1611/FTSCY/WS95051 8550.00  '
            'Qpon11141319744316 180245983  QPON DIGITAL INDON,0,"8,550.00 CR","1,404,460,552.12"\n'
        )
        result = import_mutation_report_bytes(csv.encode(), 'account', 'statement.csv')
        self.assertEqual(result['inserted_rows'], 2)
        db.session.add(QponReport(billing_id='1319744316180247459',
                                  bill_created_at=datetime(2026, 11, 2),
                                  outlet_code='OUT-1', outlet_name='Outlet', nett_amount=17100))
        db.session.commit()

        response = self.app.test_client().get('/match/qpon?start_date=2026-11-16&end_date=2026-11-16')
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result['pagination']['total_records'], 2)
        by_id = {item['mutation']['platform_code']: item for item in result['data']}
        matched = by_id['1319744316180247459']
        self.assertEqual(matched['mutation']['platform_name'], 'Qpon')
        self.assertEqual(matched['mutation']['transaction_amount'], 17100)
        self.assertEqual(matched['report']['outlet_code'], 'OUT-1')
        self.assertIsNone(by_id['1319744316180245983']['report'])

    def test_incomplete_billing_id_is_not_fabricated(self):
        row = ['11/16/2026', 'QPON DIGITAL INDON', '0', '100.00 CR', '1000.00']
        parsed = BankMutation.parse_mutation_row(row, 'account')
        self.assertEqual(parsed['platform_name'], 'Qpon')
        self.assertIsNone(parsed['platform_code'])
        self.assertEqual(parsed['tanggal'], date(2026, 11, 16))

    def test_split_statement_description(self):
        row = ["'16/11/2026", 'TRSF', 'E-BANKING', 'CR', '1611/FTSCY/WS95051',
               '41325.00', 'Qpon11141319744316', '180244318', 'QPON', 'DIGITAL',
               'INDON', '0', '41325.00', 'CR', '1404452002.12']
        parsed = BankMutation.parse_mutation_row(row, 'account')
        self.assertEqual(parsed['platform_code'], '1319744316180244318')
        self.assertEqual(parsed['transaction_amount'], 41325)
        self.assertEqual(parsed['transaction_type'], 'CR')
