from datetime import date
from decimal import Decimal
from unittest import TestCase

from flask import Flask
from sqlalchemy import text

from app.extensions import db
from app.models.daily_mutation_totals import DailyMutationTotal
from app.services.mutation_totals_service import rebuild_daily_mutation_totals


class MutationTotalsTest(TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(app)
        self.context = app.app_context()
        self.context.push()
        for sql in [
            'CREATE TABLE bank_mutations (id INTEGER PRIMARY KEY, rekening_number VARCHAR, tanggal DATE, transaksi VARCHAR, transaction_type VARCHAR, transaction_id VARCHAR, transaction_amount NUMERIC, platform_code VARCHAR, platform_name VARCHAR, created_at DATETIME)',
            'CREATE TABLE transaction_matches (id INTEGER PRIMARY KEY, mutation_id INTEGER, platform VARCHAR, daily_total_outlet_id VARCHAR, daily_total_date DATE, daily_total_report_type VARCHAR, status VARCHAR)',
            'CREATE TABLE outlets (outlet_code VARCHAR PRIMARY KEY, brand VARCHAR, store_id_grab VARCHAR, store_id_gojek VARCHAR, store_id_shopee VARCHAR)',
            'CREATE TABLE grab_disbursements (id INTEGER PRIMARY KEY, mutation_id INTEGER, transfer_date DATETIME, store_id VARCHAR, status VARCHAR)',
        ]:
            db.session.execute(text(sql))
        DailyMutationTotal.__table__.create(db.engine)
        from app.models.daily_merchant_totals import DailyMerchantTotal
        DailyMerchantTotal.__table__.create(db.engine)
        db.session.execute(text("INSERT INTO outlets VALUES ('a','MP78','store-a','g1','123456'), ('b','MPR','store-b','g2','654321')"))
        db.session.execute(text("INSERT INTO bank_mutations (id,rekening_number,tanggal,transaction_type,transaction_amount,platform_name) VALUES "
                                "(1,'r','2026-01-02','CR',100,'Gojek'),"
                                "(2,'r','2026-01-02',NULL,30,'ShopeeFood'),"
                                "(3,'r','2026-01-02','DB',900,'Gojek'),"
                                "(4,'r','2026-01-02','CR',70,'Grab'),"
                                "(5,'r','2026-01-02','CR',NULL,'Gojek'),"
                                "(6,'r','2026-01-03','CR',90,'Gojek')"))
        db.session.execute(text("INSERT INTO transaction_matches VALUES "
                                "(1,1,'gojek','a','2026-01-01','gojek','manual_matched'),"
                                "(2,4,'grab','b','2026-01-01','grab','matched')"))
        db.session.execute(text("INSERT INTO grab_disbursements VALUES "
                                "(1,4,'2026-01-02 10:00:00','store-a','completed')"))
        db.session.commit()

    def tearDown(self):
        db.session.remove()
        DailyMutationTotal.__table__.drop(db.engine)
        from app.models.daily_merchant_totals import DailyMerchantTotal
        DailyMerchantTotal.__table__.drop(db.engine)
        for table in ('grab_disbursements', 'transaction_matches', 'bank_mutations', 'outlets'):
            db.session.execute(text(f'DROP TABLE {table}'))
        db.session.commit()
        self.context.pop()

    def test_matches_conflicts_h1_and_idempotence(self):
        for _ in range(2):
            result = rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1))
            db.session.commit()
            self.assertEqual(result['mutations'], 4)
        rows = {(r.platform, r.attribution): r for r in DailyMutationTotal.query.all()}
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows['gojek', 'manual'].brand, 'MP78')
        self.assertEqual(rows['gojek', 'manual'].amount, Decimal('100'))
        self.assertEqual(rows['grab', 'disbursement'].amount, Decimal('70'))
        self.assertEqual(rows['grab', 'disbursement'].brand, 'MP78')
        self.assertEqual(rows['gojek', 'unattributed'].missing_amount_count, 1)
        self.assertEqual(rows['shopee', 'unattributed'].transaction_date, date(2026, 1, 1))
        self.assertEqual(rows['shopee', 'unattributed'].bank_date, date(2026, 1, 2))

    def test_platform_filter_and_invalid_range(self):
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['gojek'])
        db.session.commit()
        self.assertTrue(all(row.platform == 'gojek' for row in DailyMutationTotal.query.all()))
        with self.assertRaises(ValueError):
            rebuild_daily_mutation_totals(date(2026, 1, 2), date(2026, 1, 1))

    def test_sid_matching_and_ambiguity(self):
        from app.models.daily_merchant_totals import DailyMerchantTotal
        db.session.execute(text("UPDATE bank_mutations SET platform_code='3456' WHERE id=2"))
        db.session.add(DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1),
                                         report_type='shopee', total_gross=50, total_net=30))
        db.session.commit()
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['shopee'])
        db.session.commit()
        self.assertEqual(DailyMutationTotal.query.one().attribution, 'sid_h1')
        db.session.execute(text("INSERT INTO bank_mutations (id,rekening_number,tanggal,transaction_type,transaction_amount,platform_code,platform_name) VALUES (7,'r','2026-01-02','CR',30,'3456','ShopeeFood')"))
        db.session.commit()
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['shopee'])
        db.session.commit()
        row = DailyMutationTotal.query.one()
        self.assertEqual(row.attribution, 'sid_h1')
        self.assertEqual(row.mutation_count, 2)

    def test_shared_shopee_suffix_is_not_attributed(self):
        from app.models.daily_merchant_totals import DailyMerchantTotal
        db.session.execute(text("UPDATE bank_mutations SET platform_code='3456' WHERE id=2"))
        db.session.execute(text("UPDATE outlets SET store_id_shopee='993456' WHERE outlet_code='b'"))
        db.session.add_all([
            DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1), report_type='shopee', total_net=30),
            DailyMerchantTotal(outlet_id='b', date=date(2026, 1, 1), report_type='shopee', total_net=30),
        ])
        db.session.commit()
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['shopee'])
        db.session.commit()
        self.assertEqual(DailyMutationTotal.query.one().attribution, 'unattributed')

    def test_gojek_uses_full_store_id(self):
        from app.models.daily_merchant_totals import DailyMerchantTotal
        db.session.add(DailyMerchantTotal(outlet_id='b', date=date(2026, 1, 1),
                                         report_type='gojek', total_gross=100, total_net=90))
        db.session.execute(text("UPDATE bank_mutations SET platform_code='g2', transaction_amount=25 WHERE id=5"))
        db.session.commit()
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['gojek'])
        db.session.commit()
        row = DailyMutationTotal.query.filter_by(attribution='sid_h1').one()
        self.assertEqual(row.outlet_code, 'b')
        self.assertEqual(row.amount, Decimal('25'))

    def test_manual_match_does_not_block_other_credits_for_outlet(self):
        from app.models.daily_merchant_totals import DailyMerchantTotal
        db.session.add(DailyMerchantTotal(outlet_id='a', date=date(2026, 1, 1),
                                         report_type='gojek', total_gross=100, total_net=90))
        db.session.execute(text("UPDATE bank_mutations SET platform_code='g1', transaction_amount=25 WHERE id=5"))
        db.session.commit()
        rebuild_daily_mutation_totals(date(2026, 1, 1), date(2026, 1, 1), ['gojek'])
        db.session.commit()
        rows = {r.attribution: r for r in DailyMutationTotal.query.all()}
        self.assertEqual(rows['manual'].outlet_code, 'a')
        self.assertEqual(rows['sid_h1'].outlet_code, 'a')
        self.assertEqual(rows['sid_h1'].amount, Decimal('25'))
