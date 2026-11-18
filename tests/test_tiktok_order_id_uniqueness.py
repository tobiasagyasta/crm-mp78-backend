from datetime import datetime
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch

from flask import Flask
from sqlalchemy.exc import IntegrityError

from app.controllers.reports_controller import reports_bp
from app.extensions import db
from app.models.tiktok_reports import TiktokReport
from app.services.tiktok_report_importer import import_tiktok_report_bytes


def parse_row(row, header, **kwargs):
    return {
        'outlet_code': 'outlet', 'outlet_order_id': row[1],
        'store_name': row[0], 'order_time': datetime(2026, 1, 1),
        'settlement_time': datetime(2026, 1, 2),
        'gross_amount': int(row[2]), 'net_amount': int(row[2]),
    }


class TiktokOrderIdUniquenessTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        app.register_blueprint(reports_bp)
        self.context = app.app_context()
        self.context.push()
        TiktokReport.__table__.create(db.engine)
        self.client = app.test_client()

    def tearDown(self):
        db.session.remove()
        TiktokReport.__table__.drop(db.engine)
        self.context.pop()

    def test_background_duplicate_ids_within_and_across_batches_and_uploads(self):
        content = b'Breakdown,Item order ID,amount\nstore,001,100\nother, 001 ,200\nstore,002,300\n'
        with patch.object(TiktokReport, 'parse_tiktok_row', side_effect=parse_row):
            first = import_tiktok_report_bytes(content, batch_size=1)
            second = import_tiktok_report_bytes(content, batch_size=2)
        self.assertEqual((first['inserted_rows'], first['duplicate_rows']), (2, 1))
        self.assertEqual((second['inserted_rows'], second['duplicate_rows']), (0, 3))
        self.assertEqual(TiktokReport.query.count(), 2)

    def test_direct_upload_checks_ids_within_and_across_files(self):
        first = b'Breakdown,Item order ID,amount\nstore,001,100\nother,001,200\n'
        second = b'Breakdown,Item order ID,amount\nstore,001,300\nstore,002,400\n'
        with patch.object(TiktokReport, 'parse_tiktok_row', side_effect=parse_row):
            response = self.client.post('/reports/upload/tiktok', data={
                'file': [(BytesIO(first), 'first.csv'), (BytesIO(second), 'second.csv')]
            }, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json['total_records'], 2)
        self.assertEqual(response.json['skipped_records'], 2)

    def test_unique_index_rejects_whitespace_variations(self):
        db.session.add(TiktokReport(outlet_order_id='001', order_time=datetime(2026, 1, 1)))
        db.session.commit()
        db.session.add(TiktokReport(outlet_order_id=' 001 ', order_time=datetime(2026, 1, 1)))
        with self.assertRaises(IntegrityError):
            db.session.commit()
        db.session.rollback()
