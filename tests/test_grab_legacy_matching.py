from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from app.utils.transaction_matcher import TransactionMatcher


class GrabLegacyMatchingTest(TestCase):
    def setUp(self):
        self.matcher = TransactionMatcher('grab')
        self.total = SimpleNamespace(outlet_id='outlet', date=date(2026, 11, 1), total_net=100000)
        self.outlet = SimpleNamespace(store_id_grab='store', outlet_name_grab='Store')

    def match(self, candidates):
        context = {'outlets_by_code': {'outlet': self.outlet},
                   'mutations_by_date': {date(2026, 11, 2): candidates}}
        return self.matcher.match_transactions(self.total, candidates, context)[1]

    def mutation(self, identifier, amount):
        return SimpleNamespace(id=identifier, transaction_amount=amount, transaction_id=str(identifier),
                               platform_code=None, tanggal=date(2026, 11, 2))

    def test_exact_wins_over_earlier_approximate(self):
        result = self.match([self.mutation(1, Decimal('100500')),
                             self.mutation(2, Decimal('100000'))])
        self.assertEqual(result['transaction_id'], '2')

    def test_closest_then_lowest_id_and_tolerance(self):
        result = self.match([self.mutation(7, Decimal('100500')),
                             self.mutation(4, Decimal('99500')),
                             self.mutation(1, Decimal('120000'))])
        self.assertEqual(result['transaction_id'], '4')
        self.assertIsNone(self.match([self.mutation(1, Decimal('110001')),
                                      self.mutation(2, None)]))
