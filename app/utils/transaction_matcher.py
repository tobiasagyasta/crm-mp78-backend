from datetime import timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
import logging
import time
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.bank_mutations import BankMutation
from app.models.outlet import Outlet
from app.models.transaction_match import TransactionMatch
from app.extensions import db
from sqlalchemy import and_, or_

logger = logging.getLogger(__name__)

class TransactionMatcher:
    def __init__(self, platform: str):
        self.platform = platform.lower()
        self.platform_configs = {
            'gojek': {
                'platform_name': 'Gojek',
                'store_id_field': 'store_id_gojek',
                'outlet_name_field': 'outlet_name_gojek',
                'days_offset': 1,
                'match_function': self._match_gojek
            },
            'grab': {
                'platform_name': 'Grab',
                'store_id_field': 'store_id_grab',
                'outlet_name_field': 'outlet_name_grab',
                'days_offset': 1,
                'match_function': self._match_grab
            },
            'shopee': {
                'platform_name': 'ShopeeFood',
                'store_id_field': 'store_id_shopee',
                'outlet_name_field': 'outlet_name_gojek',  # Assuming using gojek name for now
                'days_offset': 1,
                'match_function': self._match_shopee
            },
               'shopeepay': {
                'platform_name': 'Shopee', 
                'store_id_field': 'store_id_shopee',
                'outlet_name_field': 'outlet_name_gojek',
                'days_offset': 1,
                'match_function': self._match_shopeepay
            }
        }
        self.config = self.platform_configs.get(self.platform)
        if not self.config:
            raise ValueError(f"Unsupported platform: {platform}")

    def _match_gojek(self, store_id: str, platform_code: str) -> bool:
        """Gojek matches store_id_gojek exactly with platform_code"""
        return store_id == platform_code

    def _match_shopee(self, store_id: str, platform_code: str) -> bool:
        """Shopee matches last 4 or 5 digits of store_id_shopee with platform_code"""
        if not store_id or not platform_code:
            return False
        store_id = store_id.strip()
        platform_code = platform_code.strip()

        # Match if platform_code == last 4 or 5 digits of store_id
        return platform_code == store_id[-4:] or platform_code == store_id[-5:]
    def _match_shopeepay(self, store_id: str, platform_code: str) -> bool:
        """ShopeePay matches last 5 digits of store_id_shopee with platform_code"""
        if not store_id or not platform_code:
            return False
        store_id = store_id.strip()
        platform_code = platform_code.strip()

        # Match if platform_code == last 4 or 5 digits of store_id
        return platform_code == store_id[-4:] or platform_code == store_id[-5:]
    
    def _match_grab(self, transaction_amount: float, daily_total_amount: float, tolerance: float = 10000.0) -> bool:
        """Grab matches only by transaction amount within a tolerance"""
        return abs(float(transaction_amount or 0.0) - float(daily_total_amount or 0.0)) <= tolerance

    def get_daily_totals_query(self, start_date: str, end_date: str, platform_code: str = None) -> db.Query:
        """Get daily totals query with optional platform code filter"""
        query = db.session.query(
            DailyMerchantTotal.outlet_id,
            DailyMerchantTotal.date,
            DailyMerchantTotal.report_type,
            DailyMerchantTotal.total_gross,
            DailyMerchantTotal.total_net
        ).filter(
            DailyMerchantTotal.date >= start_date,
            DailyMerchantTotal.date <= end_date,
            DailyMerchantTotal.report_type == self.platform
        )

        if platform_code:
            # For platform code filtering, we need to handle each platform differently
            outlets = db.session.query(Outlet)
            if self.platform == 'shopee' or self.platform == 'shopeepay':
                # For Shopee, match the last 5 digits
                outlets = outlets.filter(
                    db.func.right(getattr(Outlet, self.config['store_id_field']), 5) == 
                    db.func.right(platform_code, 5)
                )
            else:
                # For other platforms, exact match
                outlets = outlets.filter(
                    getattr(Outlet, self.config['store_id_field']) == platform_code
                )
            
            outlet_codes = [o.outlet_code for o in outlets.all()]
            if outlet_codes:
                query = query.filter(DailyMerchantTotal.outlet_id.in_(outlet_codes))

        return query

    def get_mutations_query(self, start_date: str, end_date: str) -> db.Query:
        date_offset = timedelta(days=self.config['days_offset'])
        return db.session.query(BankMutation).filter(
            BankMutation.platform_name == self.config['platform_name'],
            BankMutation.tanggal >= start_date + date_offset,
            BankMutation.tanggal <= end_date + date_offset
        )

    def build_match_context(
        self,
        daily_totals: List[DailyMerchantTotal] = None,
        mutations: List[BankMutation] = None,
        outlet_codes: List[str] = None,
    ) -> Dict:
        daily_totals = daily_totals or []
        mutations = mutations or []
        outlet_codes_set = {str(code) for code in (outlet_codes or []) if code}
        outlet_codes_set.update(
            str(total.outlet_id)
            for total in daily_totals
            if getattr(total, 'outlet_id', None)
        )

        outlets = []
        if outlet_codes_set:
            outlets = db.session.query(Outlet).filter(
                Outlet.outlet_code.in_(sorted(outlet_codes_set))
            ).all()

        mutations_by_date_code = defaultdict(list)
        mutations_by_date = defaultdict(list)
        mutations_by_data = {}
        for mutation in mutations:
            mutations_by_date[mutation.tanggal].append(mutation)
            if mutation.platform_code:
                mutations_by_date_code[(mutation.tanggal, mutation.platform_code.strip())].append(mutation)
            mutations_by_data[self._mutation_data_identity(mutation)] = mutation

        return {
            'outlets_by_code': {outlet.outlet_code: outlet for outlet in outlets},
            'mutations_by_date_code': mutations_by_date_code,
            'mutations_by_date': mutations_by_date,
            'mutations_by_data': mutations_by_data,
        }

    def _mutation_data_identity(self, mutation: BankMutation) -> Tuple:
        return (
            mutation.transaction_id,
            mutation.platform_code,
            mutation.tanggal,
            float(mutation.transaction_amount or 0.0),
        )

    def _mutation_data(self, mutation: BankMutation) -> Dict:
        return {
            'transaction_id': mutation.transaction_id,
            'platform_code': mutation.platform_code,
            'transaction_date': mutation.tanggal,
            'transaction_amount': float(mutation.transaction_amount or 0.0)
        }

    def _find_code_match(self, context: Dict, match_date, store_id: str):
        if not store_id:
            return None

        if self.platform in ('shopee', 'shopeepay'):
            store_id = store_id.strip()
            platform_codes = [store_id[-4:], store_id[-5:]]
        else:
            platform_codes = [store_id]

        for platform_code in platform_codes:
            if not platform_code:
                continue
            mutations = context['mutations_by_date_code'].get((match_date, platform_code.strip()))
            if mutations:
                return mutations[0]
        return None

    def match_transactions(self, daily_total: DailyMerchantTotal,
        mutations: List[BankMutation], context: Dict = None) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Match a single daily total with mutations using platform-specific matching"""
        if context is None:
            context = self.build_match_context([daily_total], mutations)

        outlet = context['outlets_by_code'].get(str(daily_total.outlet_id))
        if not outlet:
            return None, None

        store_id = getattr(outlet, self.config['store_id_field'])
        outlet_name = getattr(outlet, self.config['outlet_name_field'])
        
        platform_data = {
            'merchant_id': outlet_name,
            'transaction_date': daily_total.date,
            'total_amount': float(daily_total.total_net)
        }

        match_date = daily_total.date + timedelta(days=self.config['days_offset'])
        
        if self.platform == 'grab':
            # Match only by date and amount with tolerance
            mutation = next(
                (m for m in context['mutations_by_date'].get(match_date, [])
                if self._match_grab(m.transaction_amount or 0.0, daily_total.total_net)),
                None
            )
           
            if mutation:
                # NOTE: Platform code update is now handled by the calling function
                # No longer doing individual commits here
                
                return platform_data, self._mutation_data(mutation)
        else:
            # Platform-specific logic for Shopee/Gojek
            mutation = self._find_code_match(context, match_date, store_id)
            # print(f"[DEBUG] Gojek match trial: store_id={store_id}, match_date={match_date}")
            # for m in mutations:
            #     if m.tanggal == match_date:
            #         print(f"  ⤷ Checking platform_code={m.platform_code}")
            #         if match_func(store_id, m.platform_code):
            #             print("  ✅ MATCHED!")
            #         else:
            #             print("  ❌ Not matched")
        if mutation:
            return platform_data, self._mutation_data(mutation)

        return platform_data, None

    def match_batch(self, start_date, end_date, platform_code: str = None) -> Dict:
        daily_totals = self.get_daily_totals_query(start_date, end_date, platform_code).order_by(
            DailyMerchantTotal.date
        ).all()
        mutations = self.get_mutations_query(start_date, end_date).all()
        context = self.build_match_context(daily_totals, mutations)

        results = []
        matched_mutation_ids = set()
        matched_mutation_keys = set()

        for daily_total in daily_totals:
            platform_data, mutation_data = self.match_transactions(daily_total, mutations, context)
            mutation = None
            if mutation_data:
                mutation = context['mutations_by_data'].get((
                    mutation_data.get('transaction_id'),
                    mutation_data.get('platform_code'),
                    mutation_data.get('transaction_date'),
                    mutation_data.get('transaction_amount'),
                ))
                if mutation:
                    matched_mutation_ids.add(mutation.id)
                    matched_mutation_keys.add((mutation.platform_code, mutation.tanggal))

            results.append({
                'daily_total': daily_total,
                'platform_data': platform_data,
                'mutation_data': mutation_data,
                'mutation': mutation,
            })

        matched_count = sum(1 for result in results if result['mutation_data'])
        unmatched_platform_count = sum(1 for result in results if result['platform_data'] and not result['mutation_data'])
        unmatched_mutation_count = sum(
            1 for mutation in mutations
            if (mutation.platform_code, mutation.tanggal) not in matched_mutation_keys
        )
        logger.info(
            "transaction_matcher.batch platform=%s start_date=%s end_date=%s platform_code=%s "
            "daily_totals=%s mutations=%s matches=%s unmatched_platform=%s unmatched_mutations=%s",
            self.platform,
            start_date,
            end_date,
            platform_code,
            len(daily_totals),
            len(mutations),
            matched_count,
            unmatched_platform_count,
            unmatched_mutation_count,
        )

        return {
            'daily_totals': daily_totals,
            'mutations': mutations,
            'results': results,
            'matched_mutation_ids': matched_mutation_ids,
            'matched_mutation_keys': matched_mutation_keys,
        }

    def _manual_match_scope(self, batch_result: Dict) -> Dict:
        daily_keys = set()
        mutation_ids = set()
        filters = [
            TransactionMatch.platform == self.platform,
            TransactionMatch.status == 'manual_matched',
        ]

        daily_totals = batch_result['daily_totals']
        mutations = batch_result['mutations']
        daily_dates = [total.date for total in daily_totals]
        mutation_dates = [mutation.tanggal for mutation in mutations if mutation.tanggal]
        date_filters = []
        if daily_dates:
            date_filters.append(and_(
                TransactionMatch.daily_total_date >= min(daily_dates),
                TransactionMatch.daily_total_date <= max(daily_dates),
            ))
        if mutation_dates:
            date_filters.append(and_(
                TransactionMatch.report_date >= min(mutation_dates),
                TransactionMatch.report_date <= max(mutation_dates),
            ))
        if not date_filters:
            return {'daily_keys': daily_keys, 'mutation_ids': mutation_ids}

        manual_rows = db.session.query(TransactionMatch).filter(*filters, or_(*date_filters)).all()
        for row in manual_rows:
            if row.daily_total_outlet_id and row.daily_total_date and row.daily_total_report_type:
                daily_keys.add((row.daily_total_outlet_id, row.daily_total_date, row.daily_total_report_type))
            if row.mutation_id:
                mutation_ids.add(row.mutation_id)
        return {'daily_keys': daily_keys, 'mutation_ids': mutation_ids}

    def persist_matches(self, batch_result: Dict, start_date=None, end_date=None, include_unmatched_mutations: bool = True) -> Dict:
        started_at = time.perf_counter()
        daily_totals = batch_result['daily_totals']
        mutations = batch_result['mutations']
        if not daily_totals and not mutations:
            return {'inserted': 0, 'preserved_manual': 0, 'duration_seconds': 0.0}

        manual_scope = self._manual_match_scope(batch_result)

        daily_dates = [total.date for total in daily_totals]
        mutation_dates = [mutation.tanggal for mutation in mutations if mutation.tanggal]
        if daily_dates or mutation_dates:
            daily_start = start_date or (min(daily_dates) if daily_dates else None)
            daily_end = end_date or (max(daily_dates) if daily_dates else None)
            delete_filters = []
            if daily_start and daily_end:
                daily_filter = and_(
                    TransactionMatch.daily_total_date >= daily_start,
                    TransactionMatch.daily_total_date <= daily_end,
                )
                if not include_unmatched_mutations:
                    outlet_ids = [total.outlet_id for total in daily_totals if total.outlet_id]
                    daily_filter = and_(
                        daily_filter,
                        TransactionMatch.daily_total_outlet_id.in_(outlet_ids),
                    )
                delete_filters.append(daily_filter)
            if include_unmatched_mutations and mutation_dates:
                delete_filters.append(and_(
                    TransactionMatch.daily_total_date.is_(None),
                    TransactionMatch.report_date >= min(mutation_dates),
                    TransactionMatch.report_date <= max(mutation_dates),
                ))

            if delete_filters:
                db.session.query(TransactionMatch).filter(
                    TransactionMatch.platform == self.platform,
                    TransactionMatch.status != 'manual_matched',
                    or_(*delete_filters),
                ).delete(synchronize_session=False)

        matches = []
        persisted_mutation_ids = set()
        for result in batch_result['results']:
            daily_total = result['daily_total']
            daily_key = (daily_total.outlet_id, daily_total.date, daily_total.report_type)
            if daily_key in manual_scope['daily_keys']:
                continue
            mutation = result['mutation']
            notes = None
            if mutation and mutation.id in manual_scope['mutation_ids']:
                notes = 'Matched mutation preserved by manual_matched row; automatic cache row stored without mutation.'
                mutation = None
            elif mutation and mutation.id in persisted_mutation_ids:
                notes = 'Duplicate mutation match skipped to preserve one active cache row per mutation.'
                mutation = None
            elif mutation:
                persisted_mutation_ids.add(mutation.id)
            platform_amount = daily_total.total_net
            mutation_amount = mutation.transaction_amount if mutation else None
            matches.append(TransactionMatch(
                platform=self.platform,
                outlet_code=str(daily_total.outlet_id) if daily_total.outlet_id else None,
                report_date=daily_total.date,
                daily_total_outlet_id=daily_total.outlet_id,
                daily_total_date=daily_total.date,
                daily_total_report_type=daily_total.report_type,
                mutation_id=mutation.id if mutation else None,
                platform_code=mutation.platform_code if mutation else None,
                platform_amount=platform_amount,
                mutation_amount=mutation_amount,
                difference=(platform_amount - mutation_amount) if mutation else None,
                status='matched' if mutation else 'unmatched_platform',
                match_method='amount_tolerance' if mutation and self.platform == 'grab' else ('platform_code' if mutation else None),
                notes=notes,
            ))

        if include_unmatched_mutations:
            for mutation in mutations:
                if mutation.id in batch_result['matched_mutation_ids']:
                    continue
                if mutation.id in manual_scope['mutation_ids']:
                    continue
                matches.append(TransactionMatch(
                    platform=self.platform,
                    outlet_code=None,
                    report_date=mutation.tanggal,
                    mutation_id=mutation.id,
                    platform_code=mutation.platform_code,
                    mutation_amount=mutation.transaction_amount,
                    status='unmatched_mutation',
                    match_method=None,
                ))

        if matches:
            db.session.bulk_save_objects(matches)
        db.session.commit()
        duration_seconds = time.perf_counter() - started_at
        logger.info(
            "transaction_matcher.persist platform=%s inserted=%s preserved_manual_daily=%s "
            "preserved_manual_mutations=%s include_unmatched_mutations=%s duration_seconds=%.4f",
            self.platform,
            len(matches),
            len(manual_scope['daily_keys']),
            len(manual_scope['mutation_ids']),
            include_unmatched_mutations,
            duration_seconds,
        )
        return {
            'inserted': len(matches),
            'preserved_manual': len(manual_scope['daily_keys']) + len(manual_scope['mutation_ids']),
            'duration_seconds': duration_seconds,
        }

    def safe_rebuild_matches(self, start_date, end_date, platform_code: str = None) -> Dict:
        batch_result = self.match_batch(start_date, end_date, platform_code)
        persist_result = self.persist_matches(
            batch_result,
            start_date,
            end_date,
            include_unmatched_mutations=not platform_code,
        )
        batch_result['persist_result'] = persist_result
        return batch_result

    def verify_batch_parity(self, start_date, end_date, platform_code: str = None) -> Dict:
        batch_result = self.match_batch(start_date, end_date, platform_code)
        mutations = batch_result['mutations']
        mismatches = []

        for index, daily_total in enumerate(batch_result['daily_totals']):
            old_platform_data, old_mutation_data = self.match_transactions(daily_total, mutations)
            new_result = batch_result['results'][index]
            if old_platform_data != new_result['platform_data'] or old_mutation_data != new_result['mutation_data']:
                mismatches.append({
                    'daily_total': {
                        'outlet_id': daily_total.outlet_id,
                        'date': daily_total.date.isoformat() if daily_total.date else None,
                        'report_type': daily_total.report_type,
                    },
                    'single_row': {
                        'platform_data': old_platform_data,
                        'mutation_data': old_mutation_data,
                    },
                    'batch': {
                        'platform_data': new_result['platform_data'],
                        'mutation_data': new_result['mutation_data'],
                    },
                })

        return {
            'platform': self.platform,
            'start_date': start_date.isoformat() if hasattr(start_date, 'isoformat') else start_date,
            'end_date': end_date.isoformat() if hasattr(end_date, 'isoformat') else end_date,
            'platform_code': platform_code,
            'checked': len(batch_result['daily_totals']),
            'mismatch_count': len(mismatches),
            'mismatches': mismatches,
        }

    def match_mutation_without_daily_total(
        self,
        outlet_code: str,
        transaction_date,
        mutations: List[BankMutation],
        context: Dict = None,
    ) -> Optional[Dict]:
        """
        Match a mutation for a date/outlet when platform report data is missing.

        Grab mutations are not supported here because the imported mutation rows do
        not carry an outlet-specific platform code; existing Grab matching depends
        on comparing the mutation amount with the platform net amount.
        """
        if self.platform == 'grab':
            return None

        if context is None:
            context = self.build_match_context(mutations=mutations, outlet_codes=[outlet_code])

        outlet = context['outlets_by_code'].get(str(outlet_code))
        if not outlet:
            return None

        store_id = getattr(outlet, self.config['store_id_field'])
        match_date = transaction_date + timedelta(days=self.config['days_offset'])
        mutation = self._find_code_match(context, match_date, store_id)
        if not mutation:
            return None

        return self._mutation_data(mutation)
