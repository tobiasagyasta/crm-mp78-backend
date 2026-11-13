from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from openpyxl import Workbook

from app.services.excel_export import mpr_calculations as calc
from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.sheets.daily_sheet import DailySheet


class ShopeeSheetCommissionTest(TestCase):
    def test_daily_and_closing_values_for_both_platforms_and_brands(self):
        for brand, expected in [('MPR', 92), ('MPR Mandiri', 92), ('MPR Non MP78', 92),
                                ('MP78', 100), ('Other', 100), ('Es Ce Hun Tiau & Bongko Wendy', 100)]:
            with self.subTest(brand=brand), patch.object(calc, 'ENABLE_SHOPEE_COMMISSION', True):
                outlet = SimpleNamespace(brand=brand)
                totals = {'Shopee_Mutation': 100, 'Shopee_Net': 200,
                          'ShopeePay_Mutation': 0, 'ShopeePay_Net': 100}
                data = {'outlet': outlet, 'grand_totals': totals, 'daily_totals': {'day': totals}}
                daily = DailySheet(None, data)
                closing = ClosingSheet(Workbook(), data)
                for platform in ('Shopee', 'ShopeePay'):
                    self.assertEqual(f'{platform} Net (ac)' in daily._get_headers(), calc.is_mpr_brand(brand))
                    self.assertEqual(closing._shopee_ac_label(platform),
                                     f'{platform} (ac)' if calc.is_mpr_brand(brand) and platform == 'ShopeePay' else platform)
                    rate = closing._get_commission_rate_for_closing_header(f'{platform}_Mutation', 'main')
                    if calc.is_mpr_brand(brand):
                        self.assertAlmostEqual(rate, 0.08)
                    else:
                        self.assertIsNone(rate)
                for platform in ('Shopee', 'ShopeePay'):
                    value = daily._get_daily_value_map()[f'{platform} Net (ac)'](totals, 'day', 0)
                    self.assertAlmostEqual(value, expected)
                    self.assertAlmostEqual(closing._get_platform_daily_value_with_fallback(
                        'main', 'day', f'{platform}_Mutation'), expected)
                    self.assertAlmostEqual(closing._get_closing_grand_total_income_contribution(
                        'main', f'{platform}_Mutation'), expected)

    def test_mapped_mpr_and_disabled_flag(self):
        outlet = SimpleNamespace(brand='MP78')
        mpr_outlet = SimpleNamespace(brand='MPR')
        totals = {'Shopee_Mutation': 100, 'Shopee_Net': 200,
                  'ShopeePay_Mutation': 0, 'ShopeePay_Net': 100}
        mpr_data = {'outlet': mpr_outlet, 'grand_totals': totals, 'daily_totals': {'day': totals}}
        data = {'outlet': outlet, 'grand_totals': totals, 'daily_totals': {'day': totals},
                'mpr_report_data': mpr_data}
        closing = ClosingSheet(Workbook(), data)
        for enabled, expected in [(True, 92), (False, 100)]:
            with self.subTest(enabled=enabled), patch.object(calc, 'ENABLE_SHOPEE_COMMISSION', enabled):
                for platform in ('Shopee', 'ShopeePay'):
                    header = f'{platform}_Net'
                    self.assertAlmostEqual(closing._get_platform_daily_value_with_fallback('mpr', 'day', header), expected)
                    self.assertAlmostEqual(closing._get_closing_grand_total_income_contribution('mpr', header), expected)
                    self.assertAlmostEqual(DailySheet(None, mpr_data)._get_daily_value_map()[
                        f'{platform} Net (ac)'](totals, 'day', 0), expected)
                self.assertEqual(closing._shopee_ac_label('ShopeeFood', 'mpr'),
                                  'ShopeeFood (ac)' if enabled else 'ShopeeFood')
                self.assertEqual(closing._shopee_ac_label('ShopeeFood'), 'ShopeeFood')
                for platform in ('Shopee', 'ShopeePay'):
                    self.assertEqual(f'{platform} Net (ac)' in DailySheet(None, mpr_data)._get_headers(), enabled)
