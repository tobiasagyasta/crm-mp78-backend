from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from openpyxl import Workbook

from app.services.excel_export.sheets.pukis_closing_sheet import PukisClosingSheet
from app.services.rekening_info_service import OutletRekeningInfo, RekeningInfoService


class PukisClosingRekeningTest(TestCase):
    def test_generated_sheet_includes_period_rekening(self):
        outlet = SimpleNamespace(brand='Pukis & Martabak Kota Baru', outlet_code='PKB-001',
                                 outlet_name_gojek='PKB Outlet', store_id_gojek='00123',
                                 store_id_grab=None, store_id_shopee='SH123')
        start, end = date(2026, 1, 1), date(2026, 1, 31)
        data = {'outlet': outlet, 'start_date': start, 'end_date': end,
                'all_dates': [], 'daily_totals': {}, 'grand_totals': {}, 'pukis_reports': []}
        rekening = OutletRekeningInfo('PKB-001', 'Gojek', 'Bank', '001234')
        sheet = PukisClosingSheet(Workbook(), data)

        with patch.object(RekeningInfoService, 'get_outlet_rekenings', return_value=[rekening]) as lookup:
            sheet.generate()

        lookup.assert_called_once_with(outlet, start, end)
        col = sheet.rekening_col_start
        self.assertEqual([sheet.ws.cell(3, col + i).value for i in range(3)],
                         ['Outlet', 'Platform', 'Rekening'])
        self.assertEqual([sheet.ws.cell(4, col + i).value for i in range(3)],
                         ['PKB-001', 'Gojek', 'Bank - 001234'])
        store_col = sheet.store_id_col_end - 1
        self.assertEqual([sheet.ws.cell(3, store_col + i).value for i in range(2)],
                         ['Platform Name', 'Store ID'])
        self.assertEqual([[sheet.ws.cell(row, store_col + i).value for i in range(2)]
                          for row in range(4, 8)],
                         [['Gojek', '00123'], ['Grab', '-'],
                          ['ShopeeFood', 'SH123'], ['ShopeePay', 'SH123']])
        self.assertEqual(col, sheet.store_id_col_end + 2)
