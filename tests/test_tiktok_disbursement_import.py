import csv
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO, StringIO
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from flask import Flask

from app.controllers.import_jobs_controller import import_jobs_bp
from app.extensions import db
from app.models.import_job import ImportJob
from app.models.tiktok_disbursement import TiktokDisbursement
from app.services.import_job_issues import IssueCollector
from app.services.tiktok_disbursement_importer import import_tiktok_disbursement_bytes


SAMPLE = Path(__file__).resolve().parents[1] / 'docs' / 'earningsreport.csv'


def csv_bytes(rows):
    output = StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode('utf-8')


class TiktokDisbursementImportTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(import_jobs_bp)
        self.context = self.app.app_context()
        self.context.push()
        TiktokDisbursement.__table__.create(db.engine)
        ImportJob.__table__.create(db.engine)
        with SAMPLE.open(encoding='utf-8-sig', newline='') as source:
            self.rows = list(csv.DictReader(source))

    def tearDown(self):
        db.session.remove()
        ImportJob.__table__.drop(db.engine)
        TiktokDisbursement.__table__.drop(db.engine)
        self.context.pop()

    def test_sample_import_and_reupload(self):
        result = import_tiktok_disbursement_bytes(SAMPLE.read_bytes())
        self.assertEqual(result['inserted_rows'], len(self.rows))
        report = TiktokDisbursement.query.filter_by(item_order_id='4629358253367888089').one()
        self.assertEqual(report.statement_no, '202611120305700020026B2S10Dl6iyi')
        self.assertEqual(report.statement_order_id, '4629358253351110873')
        self.assertEqual(report.store_id, '100336154133763')
        self.assertEqual(report.store_name, 'Martabak Pecenongan 78 - Depok')
        self.assertEqual(report.type, 'Order')
        self.assertEqual(report.order_creation_date, datetime(2026, 11, 13, 23, 44, 29))
        self.assertEqual(report.order_payment_date, date(2026, 11, 17))
        self.assertEqual(
            [report.total_settlement_amount, report.total_commission, report.total_platform_fees,
             report.creator_commission, report.payment_amount, report.refund_amount,
             report.platform_incentive, report.platform_incentive_refunds],
            list(map(Decimal, ['55784', '6930', '3780', '3150', '60480', '0', '2520', '0'])),
        )
        db.session.commit()
        result = import_tiktok_disbursement_bytes(SAMPLE.read_bytes())
        self.assertEqual(result['duplicate_rows'], len(self.rows))
        self.assertEqual(result['inserted_rows'], 0)

    def test_duplicate_identity_and_invalid_rows(self):
        first = self.rows[0].copy()
        other_type = {**first, 'Type': 'Refund', 'Refund amount': '-12.50'}
        other_statement = {**first, 'Statement no.': 'another-statement'}
        invalid = {**first, 'Item order ID': 'bad', 'Order payment date': 'invalid'}
        bad_amount = {**first, 'Item order ID': 'bad-amount', 'Payment amount': 'NaN'}
        issues = IssueCollector()
        result = import_tiktok_disbursement_bytes(
            csv_bytes([first, first, other_type, other_statement, invalid, bad_amount]), issues=issues
        )
        self.assertEqual((result['inserted_rows'], result['duplicate_rows'],
                          result['non_duplicate_skipped_rows']), (3, 1, 2))
        self.assertEqual([issue['row_number'] for issue in issues.pending], [6, 7])
        self.assertEqual(TiktokDisbursement.query.filter_by(type='Refund').one().refund_amount,
                         Decimal('-12.50'))
        db.session.rollback()
        self.assertEqual(TiktokDisbursement.query.count(), 0)

    def test_missing_headers_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Missing required CSV headers'):
            import_tiktok_disbursement_bytes(b'Statement order ID\n1\n')

    def test_upload_creates_background_job(self):
        queue = Mock()
        queue.enqueue.return_value.id = 'rq-id'
        with patch('app.controllers.import_jobs_controller.s3') as storage, \
                patch('app.controllers.import_jobs_controller.get_import_queue', return_value=queue):
            storage.bucket = 'test-bucket'
            response = self.app.test_client().post(
                '/import-jobs/upload/tiktok-disbursement',
                data={'file': (BytesIO(csv_bytes(self.rows[:1])), 'earningsreport.csv')},
                content_type='multipart/form-data',
            )
        self.assertEqual(response.status_code, 202)
        job = ImportJob.query.one()
        self.assertEqual(job.report_type, 'tiktok_disbursement')
        self.assertTrue(job.storage_key.startswith('docs/tiktok_disbursement/'))
        self.assertEqual(response.get_json()['jobs'][0]['import_job_id'], job.id)
        queue.enqueue.assert_called_once()
