from datetime import timedelta
from typing import List, Dict, Tuple, Optional
from app.models.daily_merchant_totals import DailyMerchantTotal
from app.models.bank_mutations import BankMutation
from app.models.outlet import Outlet
from app.extensions import db

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
            }
        }
        self.config = self.platform_configs.get(self.platform)
        if not self.config:
            raise ValueError(f"Unsupported platform: {platform}")

    def _match_gojek(self, store_id: str, platform_code: str) -> bool:
        """Gojek matches store_id_gojek exactly with platform_code"""
        return store_id == platform_code

    def _match_shopee(self, store_id: str, platform_code: str) -> bool:
        """Shopee matches last 5 digits of store_id_shopee with platform_code"""
        if not store_id or not platform_code or len(store_id) < 5:
            return False
        return platform_code[-5:] in store_id
    def _match_grab(self, transaction_amount: float, daily_total_amount: float, tolerance: float = 50000.0) -> bool:
        """Grab matches only by transaction amount within a tolerance"""
        return abs(transaction_amount - daily_total_amount) <= tolerance

    def get_daily_totals_query(self, start_date: str, end_date: str, platform_code: str = None) -> db.Query:
        """Get daily totals query with optional platform code filter"""
        query = db.session.query(
            DailyMerchantTotal.outlet_id,
            DailyMerchantTotal.date,
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
            if self.platform == 'shopee':
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
        """Get mutations query with platform-specific date offset"""
        date_offset = timedelta(days=self.config['days_offset'])
        return db.session.query(
            BankMutation.transaction_id,
            BankMutation.transaction_amount,
            BankMutation.rekening_number,
            BankMutation.tanggal,
            BankMutation.platform_code
        ).filter(
            BankMutation.platform_name == self.config['platform_name'],
            BankMutation.tanggal >= start_date + date_offset,
            BankMutation.tanggal <= end_date + date_offset
        )

    def match_transactions(self, daily_total: DailyMerchantTotal, 
        mutations: List[BankMutation]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Match a single daily total with mutations using platform-specific matching"""
        outlet = db.session.query(Outlet).filter_by(outlet_code=daily_total.outlet_id).first()
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
        
        # Use platform-specific matching function
        match_func = self.config['match_function']
        if self.platform == 'grab':
            # Match only by date and amount with tolerance
            mutation = next(
                (m for m in mutations
                if m.tanggal == match_date and
                self._match_grab(m.transaction_amount or 0.0, daily_total.total_net)),
                None
            )
        else:
            # Platform-specific logic for Shopee/Gojek
            mutation = next(
                (m for m in mutations
                if m.platform_code and match_func(store_id, m.platform_code)
                and m.tanggal == match_date),
                None
            )
        if mutation:
            mutation_data = {
                'transaction_id': mutation.transaction_id,
                'platform_code': mutation.platform_code,
                'transaction_date': mutation.tanggal,
                'transaction_amount': float(mutation.transaction_amount or 0.0)
            }
            return platform_data, mutation_data

        return platform_data, None