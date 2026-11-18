from types import SimpleNamespace
from unittest import TestCase

from app.services.report_outlet_matcher import ReportOutletMatcher


def outlet(gojek=None, grab=None, **ids):
    return SimpleNamespace(
        outlet_name_gojek=gojek, outlet_name_grab=grab,
        store_id_gojek=ids.get('gojek'), store_id_grab=ids.get('grab'),
        store_id_shopee=ids.get('shopee'), brand='Existing brand',
    )


class ReportOutletMatcherTest(TestCase):
    def test_each_platform_learns_id_from_either_outlet_name(self):
        for platform in ('gojek', 'grab', 'shopee'):
            with self.subTest(platform=platform):
                target = outlet(gojek='Gojek name', grab='Grab name')
                matcher = ReportOutletMatcher([target], platform)
                self.assertIs(matcher.resolve(" '00123' ", ' grab NAME '), target)
                self.assertEqual(getattr(target, f'store_id_{platform}'), '00123')
                self.assertIs(matcher.resolve('00123', 'different name'), target)
                self.assertEqual(target.brand, 'Existing brand')
                self.assertEqual(matcher.updated_count, 1)

    def test_ambiguous_name_and_conflicting_id_are_not_assigned(self):
        first = outlet(gojek='Same', grab='First')
        second = outlet(grab='Same')
        matcher = ReportOutletMatcher([first, second], 'grab')
        self.assertIsNone(matcher.resolve('123', 'Same'))
        self.assertIsNone(first.store_id_grab)
        self.assertIs(matcher.resolve('123', 'First'), first)
        self.assertIsNone(matcher.resolve('456', 'First'))
        self.assertEqual(first.store_id_grab, '123')
        self.assertIs(matcher.resolve('123', 'Same'), first)

    def test_mpr_brands_match_without_learning_ids(self):
        for brand in ('MPR', 'MPR Mandiri', 'MPR Non MP78', ' mpr '):
            for platform in ('gojek', 'grab', 'shopee'):
                with self.subTest(brand=brand, platform=platform):
                    target = outlet(gojek='Outlet')
                    target.brand = brand
                    matcher = ReportOutletMatcher([target], platform)
                    self.assertIs(matcher.resolve('123', 'Outlet'), target)
                    self.assertIsNone(getattr(target, f'store_id_{platform}'))
                    self.assertEqual(matcher.updated_count, 0)
