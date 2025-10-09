import calendar
from openpyxl.styles import Alignment, Font, NamedStyle
from openpyxl.utils import get_column_letter

from app.services.excel_export.base_sheet import BaseSheet


class MonthlyIncomeSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, "Monthly Net Income", data)

    def generate(self):
        """Generates the monthly net income Excel sheet."""
        self._prepare_styles()
        self._create_header()
        self._populate_data()
        self._set_column_widths()

    def _prepare_styles(self):
        self.header_font = Font(bold=True)
        self.center_align = Alignment(horizontal="center", vertical="center")
        self.number_style = NamedStyle(name='number_style', number_format='#,##0')
        if 'number_style' not in self.wb.style_names:
            self.wb.add_named_style(self.number_style)

    def _create_header(self):
        headers = ['Outlet', 'Closing Day'] + [calendar.month_name[i] for i in range(1, 13)]
        self.ws.append(headers)
        for cell in self.ws[1]:
            cell.font = self.header_font
            cell.alignment = self.center_align

    def _populate_data(self):
        for outlet_code, outlet_data in self.data.items():
            row_data = [
                outlet_data['name'],
                outlet_data['closing_day'],
            ]
            for i in range(1, 13):
                row_data.append(outlet_data['monthly_totals'].get(i, 0))

            self.ws.append(row_data)

            # Apply styles
            self.ws.cell(row=self.ws.max_row, column=2).alignment = self.center_align
            for col_num in range(3, 15):
                self.ws.cell(row=self.ws.max_row, column=col_num).style = 'number_style'

    def _set_column_widths(self):
        self.ws.column_dimensions[get_column_letter(1)].width = 35
        self.ws.column_dimensions[get_column_letter(2)].width = 15
        for i in range(3, 15):
            self.ws.column_dimensions[get_column_letter(i)].width = 15