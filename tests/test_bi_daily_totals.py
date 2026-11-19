from datetime import date
from decimal import Decimal
from unittest import TestCase

from flask import Flask
from sqlalchemy import text

from app.controllers.bi_controller import bi_bp
from app.extensions import db
from app.models.daily_merchant_totals import DailyMerchantTotal


class DailyTotalsEndpointTest(TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(bi_bp)
        self.context = self.app.app_context()
        self.context.push()
        db.session.execute(text('CREATE TABLE outlets (outlet_code VARCHAR PRIMARY KEY, brand VARCHAR, '
                                'outlet_name_gojek VARCHAR, outlet_name_grab VARCHAR, '
                                'outlet_name_webshop VARCHAR, outlet_name_qpon VARCHAR)'))
        db.session.execute(text("INSERT INTO outlets VALUES "
                                "('a', 'MP78', 'Zulu', NULL, NULL, NULL), "
                                "('b', 'MP78', 'alpha', NULL, NULL, NULL), "
                                "('c', 'MPR', NULL, 'Beta', NULL, NULL)"))
        DailyMerchantTotal.__table__.create(db.engine)
        db.session.add_all([
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1), report_type='gojek',
                               total_gross=100, total_net=90),
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 2), report_type='grab',
                               total_gross=50, total_net=40),
            DailyMerchantTotal(outlet_id='b', date=date(2026, 1, 1), report_type='gojek',
                               total_gross=200, total_net=180),
            DailyMerchantTotal(outlet_id='c', date=date(2026, 1, 1), report_type='shopee',
                               total_gross=300, total_net=270),
            DailyMerchantTotal(outlet_id='orphan', date=date(2026, 1, 1), report_type='gojek',
                               total_gross=10, total_net=9),
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1), report_type='shopeepay',
                               total_gross=999, total_net=999),
        ])
        db.session.commit()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        DailyMerchantTotal.__table__.drop(db.engine)
        db.session.execute(text('DROP TABLE outlets'))
        db.session.commit()
        self.context.pop()

    def get(self, query=''):
        return self.client.get('/bi/daily-totals?start_date=2026-01-01&end_date=2026-01-02' + query)

    def test_brand_platform_filter_and_pagination(self):
        response = self.get('&brand=MP78&platform=gojek&include_outlets=true&per_page=1&page=1')
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(Decimal(body['totals']['net']), 270)
        self.assertEqual(len(body['daily']), 1)
        self.assertEqual(body['outlets']['pagination']['total_records'], 2)
        self.assertEqual(body['outlets']['data'][0]['outlet_name'], 'alpha')
        self.assertEqual(body['outlets']['data'][0]['platforms'][0]['platform'], 'gojek')
        second = self.get('&brand=MP78&platform=gojek&include_outlets=true&per_page=1&page=2').get_json()
        self.assertEqual(second['outlets']['data'][0]['outlet_name'], 'Zulu')
        self.assertEqual(second['totals'], body['totals'])

    def test_unassigned_and_multiple_platforms(self):
        body = self.get('&platform=gojek&platform=shopee').get_json()
        self.assertEqual(Decimal(body['totals']['net']), 549)
        self.assertTrue(any(item['brand'] is None for item in body['brands']))
        self.assertNotIn('outlets', body)

    def test_invalid_inputs_and_empty_results(self):
        for query in ('&platform=shopeepay', '&per_page=0', '&page=x', '&include_outlets=yes'):
            self.assertEqual(self.get(query).status_code, 400)
        self.assertEqual(self.client.get('/bi/daily-totals').status_code, 400)
        self.assertEqual(self.client.get('/bi/daily-totals?start_date=2026-01-02&end_date=2026-01-01').status_code, 400)
        body = self.get('&brand=unknown&include_outlets=true').get_json()
        self.assertEqual(body['daily'], [])
        self.assertEqual(body['outlets']['pagination']['total_records'], 0)
