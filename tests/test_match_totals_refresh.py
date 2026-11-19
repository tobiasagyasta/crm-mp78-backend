from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from app.utils.transaction_matcher import TransactionMatcher


class MatchTotalsRefreshTest(TestCase):
    def test_matching_refreshes_before_commit(self):
        day = date(2026, 1, 1)
        matcher = TransactionMatcher('gojek')
        total = SimpleNamespace(outlet_id='a', date=day, report_type='gojek', total_net=100)
        batch = {'daily_totals': [total], 'mutations': [],
                 'results': [{'daily_total': total, 'mutation': None}],
                 'matched_mutation_ids': set()}
        session = MagicMock()
        with patch('app.utils.transaction_matcher.db') as db_mock, \
                patch.object(matcher, '_manual_match_scope', return_value={'daily_keys': set(), 'mutation_ids': set()}), \
                patch('app.services.mutation_totals_service.rebuild_daily_mutation_totals') as refresh:
            db_mock.session = session
            refresh.side_effect = lambda *args: session.commit.assert_not_called()
            matcher.persist_matches(batch, day, day)
            refresh.assert_called_once_with(day, day, ['gojek'])
            session.commit.assert_called_once()

    def test_refresh_failure_prevents_commit(self):
        day = date(2026, 1, 1)
        matcher = TransactionMatcher('shopee')
        total = SimpleNamespace(outlet_id='a', date=day, report_type='shopee', total_net=100)
        batch = {'daily_totals': [total], 'mutations': [],
                 'results': [{'daily_total': total, 'mutation': None}],
                 'matched_mutation_ids': set()}
        session = MagicMock()
        with patch('app.utils.transaction_matcher.db') as db_mock, \
                patch.object(matcher, '_manual_match_scope', return_value={'daily_keys': set(), 'mutation_ids': set()}), \
                patch('app.services.mutation_totals_service.rebuild_daily_mutation_totals', side_effect=RuntimeError('failed')):
            db_mock.session = session
            with self.assertRaises(RuntimeError):
                matcher.persist_matches(batch, day, day)
            session.commit.assert_not_called()
