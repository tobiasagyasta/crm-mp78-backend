import csv
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from flask import Flask

from app.controllers.import_jobs_controller import import_jobs_bp
from app.extensions import db
from app.models.import_job import ImportJob
from app.models.import_job_issue import ImportJobIssue
from app.services.import_job_issues import IssueCollector
from app.services.tiktok_report_importer import import_tiktok_report_bytes


class ImportJobIssuesTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(import_jobs_bp)
        self.context = self.app.app_context()
        self.context.push()
        ImportJob.__table__.create(db.engine)
        ImportJobIssue.__table__.create(db.engine)
        job = ImportJob(report_type='gojek', status='completed', original_filename='report.csv',
                        storage_bucket='test', storage_key='test/report.csv', attempt_number=2,
                        skipped_rows=61, duplicate_rows=60, non_duplicate_skipped_rows=1)
        db.session.add(job)
        db.session.commit()
        self.job_id = job.id
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        ImportJobIssue.__table__.drop(db.engine)
        ImportJob.__table__.drop(db.engine)
        self.context.pop()

    def test_issue_details_survive_and_are_paginated_by_attempt(self):
        collector = IssueCollector()
        collector.record(63, 'OUTLET_NOT_FOUND', 'No matching outlet',
                         {'Merchant ID': 'missing'}, context={'merchant_id': 'missing'})
        collector.save(self.job_id, 2)
        old = IssueCollector()
        old.record(3, 'INVALID_DATE', 'Bad date', {'Date': 'invalid'})
        old.save(self.job_id, 1)
        db.session.commit()

        detail = self.client.get(f'/import-jobs/{self.job_id}').get_json()
        self.assertEqual(detail['issue_summary'], [{'reason_code': 'OUTLET_NOT_FOUND', 'count': 1}])
        self.assertEqual(detail['duplicate_rows'], 60)

        response = self.client.get(f'/import-jobs/{self.job_id}/issues?per_page=1').get_json()
        self.assertEqual(response['pagination']['total_records'], 1)
        self.assertEqual(response['data'][0]['row_number'], 63)
        self.assertEqual(response['data'][0]['row_data']['Merchant ID'], 'missing')
        self.assertEqual(self.client.get(
            f'/import-jobs/{self.job_id}/issues?reason_code=INVALID_DATE'
        ).get_json()['pagination']['total_records'], 0)
        self.assertEqual(self.client.get(
            f'/import-jobs/{self.job_id}/issues?attempt_number=1'
        ).get_json()['data'][0]['reason_code'], 'INVALID_DATE')

        export = self.client.get(f'/import-jobs/{self.job_id}/issues/export')
        exported = list(csv.DictReader(StringIO(export.get_data(as_text=True))))
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0]['reason_code'], 'OUTLET_NOT_FOUND')

    def test_invalid_attempt_is_rejected(self):
        self.assertEqual(self.client.get(f'/import-jobs/{self.job_id}/issues?attempt_number=3').status_code, 400)

    def test_duplicates_do_not_hide_later_parse_issues(self):
        content = 'Breakdown,Item order ID\n' + 'duplicate,1\n' * 60 + 'bad,2\n'

        def parse(row, header, raise_on_error=False):
            if row[0] == 'bad':
                raise ValueError('Invalid redemption time')
            return {'outlet_code': 'outlet', 'outlet_order_id': '1',
                    'store_name': 'store', 'order_time': 'date', 'settlement_time': 'date',
                    'gross_amount': 1, 'net_amount': 1}

        issues = IssueCollector()
        with patch('app.services.tiktok_report_importer.TiktokReport.parse_tiktok_row', side_effect=parse), \
                patch('app.services.tiktok_report_importer._load_existing_keys', return_value=set()), \
                patch('app.services.tiktok_report_importer._flush_reports', return_value=1):
            result = import_tiktok_report_bytes(content.encode(), issues=issues)

        self.assertEqual(result['duplicate_rows'], 59)
        self.assertEqual(result['non_duplicate_skipped_rows'], 1)
        self.assertEqual(issues.pending[0]['row_number'], 62)
        self.assertEqual(issues.pending[0]['reason_code'], 'PARSE_ERROR')
