from datetime import date
from types import SimpleNamespace
from unittest import TestCase

from openpyxl import Workbook

from app.services.excel_export.sheets.closing_sheet import ClosingSheet
from app.services.excel_export.utils.excel_utils import CLOSED_OFF_FILL


class ClosingDisabledPlatformStylesTest(TestCase):
    def test_grand_total_income_cells_match_disabled_platform_labels(self):
        outlet = SimpleNamespace(
            brand='Es Ce Hun Tiau & Bongko Wendy',
            outlet_name_gojek='Outlet',
            disabled_closing_platforms=['gojek'],
        )
        mpr_outlet = SimpleNamespace(
            brand='MPR',
            outlet_name_gojek='MPR Outlet',
            disabled_closing_platforms=['grab'],
        )
        data = {
            'outlet': outlet,
            'start_date': date(2026, 1, 1),
            'end_date': date(2026, 1, 31),
            'grand_totals': {'Gojek_Net': 1000, 'Grab_Net': 500, 'UV': 0},
            'manual_entries': [],
            'mpr_report_data': {
                'outlet': mpr_outlet,
                'grand_totals': {'Gojek_Net': 200, 'Grab_Net': 300},
            },
        }
        sheet = ClosingSheet(Workbook(), data)
        sheet._write_grand_total_section()
        sheet._apply_styles()

        col = sheet.grand_total_col_start
        rows = {sheet.ws.cell(row, col).value: row for row in range(5, sheet.ws.max_row + 1)}
        for label in ('Gojek', 'Grab MPR (ac)'):
            with self.subTest(label=label):
                label_cell = sheet.ws.cell(rows[label], col)
                income_cell = sheet.ws.cell(rows[label], col + 1)
                self.assertEqual(label_cell.fill, CLOSED_OFF_FILL)
                self.assertEqual(income_cell.fill, CLOSED_OFF_FILL)
                self.assertIsNone(income_cell.value)

        for label in ('Grab', 'Gojek MPR (ac)'):
            with self.subTest(label=label):
                income_cell = sheet.ws.cell(rows[label], col + 1)
                self.assertNotEqual(income_cell.fill, CLOSED_OFF_FILL)
                self.assertGreater(income_cell.value, 0)
