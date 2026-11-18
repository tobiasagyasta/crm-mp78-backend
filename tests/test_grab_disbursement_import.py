from datetime import datetime
from unittest import TestCase

from flask import Flask
from sqlalchemy import text

from app.extensions import db
from app.models.grab_disbursement import GrabDisbursement
from app.models.rekening import Rekening
from app.services.grab_disbursement_importer import import_grab_disbursement_bytes
from app.services.import_job_issues import IssueCollector


HEADER = ('Tanggal,Nama toko,ID toko,ID pencairan dana,Jumlah bersih,Status,'
          'Tanggal transfer,Kode rekening koran bank,Nama bank,Akun bank\n')
ROW = ('10 Nov 2026 11:31 PM,Store,store-id,{id},463674,Completed,'
       '11 Nov 2026 12:32 AM,123,BCA,{suffix}\n')


class GrabDisbursementImportTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        Rekening.__table__.create(db.engine)
        GrabDisbursement.__table__.create(db.engine)
        db.session.execute(text('CREATE TABLE outlets (store_id_grab VARCHAR, outlet_name_grab VARCHAR, rekening_id INTEGER)'))
        db.session.add(Rekening(name='BCA', rekening_number='123450385'))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        with db.engine.begin() as connection:
            connection.execute(text('DROP TABLE outlets'))
        GrabDisbursement.__table__.drop(db.engine)
        Rekening.__table__.drop(db.engine)
        self.context.pop()

    def test_import_date_suffix_and_duplicates(self):
        content = HEADER + ROW.format(id='SID1', suffix='0385 ') + ROW.format(id='SID1', suffix='385')
        result = import_grab_disbursement_bytes(content.encode())
        db.session.commit()
        report = GrabDisbursement.query.one()
        self.assertEqual(report.transfer_date, datetime(2026, 11, 11, 0, 32))
        self.assertEqual(report.account_suffix, '0385')
        self.assertEqual(report.rekening_name, 'BCA')
        self.assertEqual(report.net_amount, 463674)
        self.assertEqual(result['duplicate_rows'], 1)

    def test_rekening_name_reflects_linked_account(self):
        import_grab_disbursement_bytes((HEADER + ROW.format(id='SID1', suffix='0385')).encode())
        db.session.commit()
        report = GrabDisbursement.query.one()
        report.rekening.name = 'Updated name'
        db.session.commit()
        self.assertEqual(report.rekening_name, 'Updated name')
        result = import_grab_disbursement_bytes((HEADER + ROW.format(id='SID1', suffix='385')).encode())
        self.assertEqual(result['duplicate_rows'], 1)

    def test_ambiguous_missing_and_invalid_rows(self):
        db.session.add(Rekening(name='Other', rekening_number='999385'))
        db.session.commit()
        issues = IssueCollector()
        content = HEADER + ROW.format(id='SID1', suffix='385') + ROW.format(id='SID2', suffix='123')
        result = import_grab_disbursement_bytes(content.encode(), issues=issues)
        self.assertEqual(result['inserted_rows'], 0)
        self.assertEqual([issue['reason_code'] for issue in issues.pending],
                         ['AMBIGUOUS_REKENING', 'REKENING_NOT_FOUND'])

    def test_missing_headers_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Missing required CSV headers'):
            import_grab_disbursement_bytes(b'Tanggal,Nama toko\n')

    def test_ambiguous_suffix_uses_outlet_account(self):
        db.session.add_all([
            Rekening(id=2, name='One', rekening_number='7770001'),
            Rekening(id=3, name='Two', rekening_number='8880001'),
        ])
        db.session.execute(text(
            "INSERT INTO outlets (store_id_grab, outlet_name_grab, rekening_id) "
            "VALUES ('store-id', 'Store', 3)"
        ))
        db.session.commit()
        result = import_grab_disbursement_bytes((HEADER + ROW.format(id='SID1', suffix='0001')).encode())
        self.assertEqual(result['inserted_rows'], 1)
        self.assertEqual(GrabDisbursement.query.one().rekening_id, 3)
        self.assertEqual(GrabDisbursement.query.one().account_suffix, '0001')

    def test_ambiguous_suffix_name_fallback_and_unresolved(self):
        db.session.add_all([
            Rekening(id=2, name='One', rekening_number='7770001'),
            Rekening(id=3, name='Two', rekening_number='8880001'),
        ])
        db.session.execute(text(
            "INSERT INTO outlets (store_id_grab, outlet_name_grab, rekening_id) "
            "VALUES ('different-id', '  store  ', 2)"
        ))
        db.session.commit()
        result = import_grab_disbursement_bytes((HEADER + ROW.format(id='SID1', suffix='0001')).encode())
        self.assertEqual(result['inserted_rows'], 1)
        self.assertEqual(GrabDisbursement.query.one().rekening_id, 2)
        db.session.execute(text("UPDATE outlets SET rekening_id = 1"))
        issues = IssueCollector()
        result = import_grab_disbursement_bytes((HEADER + ROW.format(id='SID2', suffix='0001')).encode(), issues=issues)
        self.assertEqual(result['inserted_rows'], 0)
        self.assertEqual(issues.pending[0]['reason_code'], 'AMBIGUOUS_REKENING')
