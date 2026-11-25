from datetime import date
from unittest import TestCase

from flask import Flask

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.tiktok_mutation import TikTokMutation
from app.services.mutation_report_importer import import_mutation_report_bytes


class TikTokMutationTest(TestCase):
    def test_wrapped_statement_routes_only_payouts_and_deduplicates(self):
        statement = (
            'Periode : 01/10/2026 - 31/10/2026\n'
            '"Tanggal Transaksi,""Keterangan"",""Cabang"",""Jumlah"",""Saldo"""\n'
            '"01/10,""WD123 TKPD TOKOPEDIA PT"",""0372"",""1,250.00 CR"",""5,000.00"""\n'
            '"01/10,""BIAYA ADM"",""0000"",""30.00 DB"",""4,970.00"""\n'
            '"Saldo Akhir : 4,970.00"\n'
        ).encode()
        app = Flask(__name__)
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        db.init_app(app)
        with app.app_context():
            BankMutation.__table__.create(db.engine)
            TikTokMutation.__table__.create(db.engine)
            result = import_mutation_report_bytes(statement, 'account')
            self.assertEqual(result['inserted_rows'], 2)
            payout = TikTokMutation.query.one()
            self.assertEqual((payout.tanggal, payout.transaction_amount), (date(2026, 10, 1), 1250))
            self.assertEqual(BankMutation.query.one().transaksi, 'BIAYA ADM')
            repeat = import_mutation_report_bytes(statement, 'account')
            self.assertEqual((repeat['inserted_rows'], repeat['duplicate_rows']), (0, 2))
