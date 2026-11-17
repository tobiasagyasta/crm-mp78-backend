from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from openpyxl import Workbook

from app.services.excel_export.sheets.closing_sheet import ClosingSheet


class ClosingGojekCommissionTest(TestCase):
    def test_mpr_grand_total_includes_gojek_after_gofood_commission(self):
        for brand in ('MPR', 'MPR Mandiri', 'MPR Non MP78'):
            with self.subTest(brand=brand):
                data = {
                    'outlet': SimpleNamespace(brand=brand, outlet_name_gojek='Outlet'),
                    'start_date': date(2026, 1, 1),
                    'end_date': date(2026, 1, 31),
                    'grand_totals': {'Gojek_Net': 1000, 'Gojek_QRIS': 250, 'UV': 0},
                    'manual_entries': [],
                }
                sheet = ClosingSheet(Workbook(), data)
                with patch.object(sheet, '_is_closing_platform_disabled', return_value=False):
                    sheet._write_grand_total_section()

                col = sheet.grand_total_col_start
                rows = {sheet.ws.cell(row, col).value: row for row in range(5, sheet.ws.max_row + 1)}
                self.assertAlmostEqual(sheet.ws.cell(rows['Gojek (ac)'], col + 1).value, 940)
                self.assertNotIn('Gojek Manag MPR', rows)
                self.assertAlmostEqual(sheet.ws.cell(4, col + 1).value, 940)
                self.assertAlmostEqual(sheet.ws.cell(4, col + 2).value, 0)
                self.assertAlmostEqual(sheet.ws.cell(4, col + 3).value, 940)
