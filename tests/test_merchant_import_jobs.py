import csv
from decimal import Decimal
from io import BytesIO, StringIO
from unittest import TestCase
from unittest.mock import Mock, patch

from flask import Flask
from sqlalchemy import text

from app.controllers.import_jobs_controller import import_jobs_bp
from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.import_job import ImportJob
from app.models.maxim_report import MaximReport
from app.models.qpon_reports import QponReport
from app.models.shopee_reports import ShopeeReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.webshop_report import WebshopReport
from app.services.import_job_issues import IssueCollector
from app.services.merchant_report_importers import (
    import_qpon_report_bytes, import_shopee_adjustment_bytes,
    import_shopeepay_report_bytes, import_webshop_report_bytes,
)


def csv_bytes(headers, rows):
    output = StringIO(newline='')
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue().encode()


class MerchantImportJobsTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(import_jobs_bp)
        self.context = self.app.app_context()
        self.context.push()
        self.tables = [ImportJob, ShopeeReport, ShopeepayReport, QponReport,
                       WebshopReport, MaximReport, DailyMerchantTotal]
        for model in self.tables:
            model.__table__.create(db.engine)
        # Outlet has PostgreSQL ARRAY columns, which SQLite cannot create.
        columns = []
        from app.models.outlet import Outlet
        for column in Outlet.__table__.columns:
            columns.append(f'"{column.name}" ' + ('INTEGER PRIMARY KEY' if column.name == 'id' else 'TEXT'))
        db.session.execute(text('CREATE TABLE outlets (' + ', '.join(columns) + ')'))
        db.session.execute(text(
            "INSERT INTO outlets (id, outlet_code, outlet_name_grab, outlet_name_qpon, "
            "outlet_name_webshop, store_id_shopee, brand) "
            "VALUES (1, 'A', 'Store', 'Store', 'Branch', 's1', 'Brand')"
        ))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        db.session.execute(text('DROP TABLE outlets'))
        for model in reversed(self.tables):
            model.__table__.drop(db.engine)
        self.context.pop()

    def test_upload_routes_enqueue_one_job_per_file(self):
        for route, report_type in [('shopee-adjustment', 'shopee_adjustment'),
                                   ('shopeepay', 'shopeepay'), ('qpon', 'qpon'),
                                   ('webshop', 'webshop')]:
            queue = Mock()
            queue.enqueue.return_value.id = 'rq-id'
            with patch('app.controllers.import_jobs_controller.s3') as storage, \
                    patch('app.controllers.import_jobs_controller.get_import_queue', return_value=queue):
                storage.bucket = 'bucket'
                response = self.app.test_client().post(
                    '/import-jobs/upload/' + route,
                    data={'file': [(BytesIO(b'a\n1\n'), 'one.csv'),
                                   (BytesIO(b'a\n2\n'), 'two.csv')]},
                    content_type='multipart/form-data',
                )
            self.assertEqual(response.status_code, 202)
            self.assertEqual(len(response.json['jobs']), 2)
            self.assertEqual(ImportJob.query.filter_by(report_type=report_type).count(), 2)
            self.assertEqual(queue.enqueue.call_count, 2)
            self.assertEqual(self.app.test_client().post('/import-jobs/upload/' + route).status_code, 400)

    def test_adjustment_duplicates_and_bad_rows(self):
        headers = ['Store ID', 'Store Name', 'Wallet Adjustment ID', 'Wallet Adjustment Time',
                   'Wallet Adjustment Amount', 'Wallet Adjustment Reason']
        row = ['s1', 'Store', 'id1', '2026-11-20 12:00:00', '25', 'Refund']
        content = csv_bytes(headers, [row, row, ['missing', 'Unknown', 'id2', 'bad', '0', '']])
        issues = IssueCollector()
        result = import_shopee_adjustment_bytes(content, issues=issues)
        self.assertEqual((result['inserted_rows'], result['duplicate_rows'],
                          result['non_duplicate_skipped_rows']), (1, 1, 1))
        self.assertEqual(issues.pending[0]['reason_code'], 'OUTLET_NOT_FOUND')
        report = ShopeeReport.query.one()
        self.assertEqual((report.transaction_type, report.order_id, report.net_income),
                         ('Adjustment', 'id1', Decimal('25')))
        db.session.commit()
        self.assertEqual(import_shopee_adjustment_bytes(content)['duplicate_rows'], 2)

    def test_shopeepay_consolidates_and_records_errors(self):
        headers = ['Entity ID', 'Merchant/Store Name', 'Transaction ID', 'Create Time',
                   'Transaction Type', 'Transaction Amount', 'Settlement Amount']
        row = ['s1', 'Store', 'id1', '2026-11-20 12:30:00', 'Payment', '100', '90']
        issues = IssueCollector()
        result = import_shopeepay_report_bytes(csv_bytes(headers, [row, row,
                                             ['s1', 'Store', 'id2', 'bad', '', '', '']]), issues=issues)
        self.assertEqual((result['inserted_rows'], result['duplicate_rows'],
                          result['non_duplicate_skipped_rows']), (1, 1, 1))
        self.assertEqual(issues.pending[0]['reason_code'], 'PARSE_ERROR')
        total = DailyMerchantTotal.query.one()
        self.assertEqual((total.total_gross, total.total_net), (Decimal('100'), Decimal('90')))
        db.session.commit()
        self.assertEqual(import_shopeepay_report_bytes(csv_bytes(headers, [row]))['duplicate_rows'], 1)

    def test_qpon_semicolon_and_nett_fallback(self):
        data = ('Waktu Pembuatan Tagihan;Nomor Tagihan;Nama Toko;Total Penerimaan;Nett\n'
                '2026-11-20;id1;Store;100;0\n'
                '2026-11-20;id1;Store;100;0\n'
                '2026-11-20;;Store;100;0\n').encode()
        issues = IssueCollector()
        result = import_qpon_report_bytes(data, issues=issues)
        self.assertEqual((result['inserted_rows'], result['duplicate_rows'],
                          result['non_duplicate_skipped_rows']), (1, 1, 1))
        self.assertEqual(QponReport.query.one().nett_amount, Decimal('99'))
        db.session.commit()
        self.assertEqual(import_qpon_report_bytes(data)['duplicate_rows'], 2)

    def test_webshop_and_maxim_cross_table_duplicates(self):
        headers = ['Order ID', 'Provider', 'Order Source', 'Brand', 'Branch', 'Status',
                   'Net Order Value', 'Created At']
        webshop = ['w1', 'klikit', 'webshop', 'Brand', 'Branch', 'DELIVERED', '100', '2026-11-20 12:00:00']
        maxim = ['m1', 'maxim', 'maxim', 'Brand', 'Branch', 'DELIVERED', '100', '2026-11-20 12:00:00']
        unknown = ['x', 'other', 'other', 'Brand', 'Branch', 'DELIVERED', '100', '']
        issues = IssueCollector()
        result = import_webshop_report_bytes(csv_bytes(headers, [webshop, maxim, maxim, unknown]), issues=issues)
        self.assertEqual((result['inserted_rows'], result['duplicate_rows'],
                          result['non_duplicate_skipped_rows']), (2, 1, 1))
        self.assertEqual(result['platform_counts'], {'webshop': 1, 'maxim': 1})
        self.assertEqual(WebshopReport.query.one().nett_value, Decimal('97'))
        self.assertEqual(MaximReport.query.one().nett_value, Decimal('100'))
        self.assertEqual(issues.pending[0]['reason_code'], 'UNKNOWN_SOURCE')
        db.session.commit()
        self.assertEqual(import_webshop_report_bytes(csv_bytes(headers, [webshop, maxim]))['duplicate_rows'], 2)
        with self.assertRaisesRegex(ValueError, 'Missing Provider'):
            import_webshop_report_bytes(b'Order ID\n1\n')

    def test_worker_dispatches_new_report_types(self):
        from app.workers.report_import_jobs import process_report_import_job
        for report_type, importer in [
            ('shopee_adjustment', 'import_shopee_adjustment_bytes'),
            ('shopeepay', 'import_shopeepay_report_bytes'),
            ('qpon', 'import_qpon_report_bytes'),
            ('webshop', 'import_webshop_report_bytes'),
        ]:
            job = ImportJob(report_type=report_type, status='queued', original_filename='a.csv',
                            storage_provider='test', storage_bucket='bucket', storage_key='key')
            db.session.add(job)
            db.session.commit()
            with patch('app.create_app', return_value=self.app), \
                    patch('app.workers.report_import_jobs.s3') as storage, \
                    patch('app.workers.report_import_jobs.' + importer, return_value={
                        'total_rows': 1, 'processed_rows': 1, 'inserted_rows': 1,
                    }) as process, \
                    patch('app.services.import_job_issues.IssueCollector.save'):
                storage.client.get_object.return_value = {'Body': BytesIO(b'data')}
                result = process_report_import_job(job.id)
            self.assertEqual(result['status'], 'completed')
            db.session.expire_all()
            self.assertEqual(db.session.get(ImportJob, job.id).inserted_rows, 1)
            process.assert_called_once()
