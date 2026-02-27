from openpyxl.styles import PatternFill
from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, GOJEK_FILL, GRAB_FILL, SHOPEE_FILL,
    SHOPEEPAY_FILL, TIKTOK_FILL, CASH_FILL, DATE_FILL, DIFFERENCE_FILL,
    set_column_widths
)

class DailySheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Daily', data)

    def generate(self):
        self._write_title()
        self._write_headers()
        self._write_data()
        self._write_grand_total()
        set_column_widths(self.ws, {'A': 12, 'B': 15, 'C': 15, 'D': 15, 'E': 15, 'F': 15, 'G': 15, 'H': 15, 'I': 15, 'J': 15, 'K': 15, 'L': 15, 'M': 15, 'N': 15, 'O': 15, 'P': 15, 'Q': 15, 'R': 15, 'S': 15})

    def _write_title(self):
        self.ws['A1'] = 'Sales Report'
        self.ws['A2'] = 'Period:'
        self.ws['B2'] = f"{self.data['start_date'].strftime('%Y-%m-%d')} to {self.data['end_date'].strftime('%Y-%m-%d')}"
        self.ws['A3'] = 'Outlet:'
        self.ws['B3'] = self.data['outlet'].outlet_name_gojek

    def _get_headers(self):
        user_role = self.data.get('user_role')
        outlet_brand = self.data['outlet'].brand

        base_headers = [
            'Date', 'GoFood', 'GO-PAY QRIS', 'Gojek Net', 'Gojek Mutation', 'Gojek Difference',
            'GrabFood', 'GrabOVO', 'Grab Net (ac)', 'Shopee Net', 'Shopee Mutation', 'Shopee Difference',
            'ShopeePay Net', 'ShopeePay Mutation', 'ShopeePay Difference',
            'Tiktok Net', 'Qpon Net', 'Webshop Net', 'UV'
        ]
        extra_headers = ['Cash Income (Admin)', 'Cash Expense (Admin)', 'Sisa Cash (Admin)', 'Minusan (Mutasi)']

        if user_role == "management" and outlet_brand not in ["Pukis & Martabak Kota Baru", "Es Ce Hun Tiau & Bongko Wendy"]:
            base_headers.insert(8, 'Grab Net')

        if outlet_brand == "Pukis & Martabak Kota Baru" or outlet_brand == "Es Ce Hun Tiau & Bongko Wendy":
            base_headers.insert(4, 'Grab Net')
            if 'Grab Net (ac)' in base_headers:
                base_headers.remove('Grab Net (ac)')
            base_headers += extra_headers
        if outlet_brand == "Es Ce Hun Tiau & Bongko Wendy":
            base_headers.insert(9, 'Grab Net (ac)')
            if 'Grab Net' in base_headers:
                base_headers.remove('Grab Net')
        return base_headers

    def _write_headers(self):
        headers = self._get_headers()
        header_colors = {
            'Date': DATE_FILL, 'GoFood': GOJEK_FILL, 'GO-PAY QRIS': GOJEK_FILL, 'Gojek Net': GOJEK_FILL, 'Gojek Mutation': GOJEK_FILL,
            'Gojek Difference': DIFFERENCE_FILL, 'GrabFood': GRAB_FILL, 'GrabOVO': GRAB_FILL, 'Grab Net': GRAB_FILL, 'Grab Net (ac)': GRAB_FILL,
            'Shopee Net': SHOPEE_FILL, 'Shopee Mutation': SHOPEE_FILL, 'Shopee Difference': DIFFERENCE_FILL,
            'ShopeePay Net': SHOPEEPAY_FILL, 'ShopeePay Mutation': SHOPEEPAY_FILL, 'ShopeePay Difference': DIFFERENCE_FILL,
            'Tiktok Net': TIKTOK_FILL, 'Qpon Net': TIKTOK_FILL, 'Webshop Net': TIKTOK_FILL,
            'UV': PatternFill(start_color='35F0F0', end_color='35F0F0', fill_type='solid'),
            'Cash Income (Admin)': CASH_FILL, 'Cash Expense (Admin)': CASH_FILL, 'Sisa Cash (Admin)': CASH_FILL
        }

        for col, header in enumerate(headers, 1):
            cell = self.ws.cell(row=5, column=col)
            cell.value = header
            cell.font = HEADER_FONT
            cell.fill = header_colors.get(header, YELLOW_FILL)
            cell.alignment = CENTER_ALIGN

    def _write_data(self):
        headers = self._get_headers()
        all_dates = self.data['all_dates']
        daily_totals = self.data['daily_totals']
        minusan_by_date = self.data['minusan_by_date']
        outlet_brand = self.data['outlet'].brand
        current_row = 6

        header_value_map = {
            'Date': lambda totals, date, minusan_total: date,
            'GoFood': lambda totals, date, minusan_total: (totals.get('Gojek_Net', 0) - totals.get('Gojek_QRIS', 0)),
            'GO-PAY QRIS': lambda totals, date, minusan_total: totals.get('Gojek_QRIS', 0),
            'Gojek Net': lambda totals, date, minusan_total: totals.get('Gojek_Net', 0),
            'Gojek Mutation': lambda totals, date, minusan_total: totals.get('Gojek_Mutation', 0),
            'Gojek Difference': lambda totals, date, minusan_total: totals.get('Gojek_Difference', 0),
            'GrabFood': lambda totals, date, minusan_total: (totals.get('Grab_Net', 0) - totals.get('GrabOVO_Net', 0)),
            'GrabOVO': lambda totals, date, minusan_total: totals.get('GrabOVO_Net', 0),
            'Grab Net': lambda totals, date, minusan_total: totals.get('Grab_Net', 0),
            'Grab Net (ac)': lambda totals, date, minusan_total: totals.get('Grab_Commission', 0),
            'Shopee Net': lambda totals, date, minusan_total: totals.get('Shopee_Net', 0),
            'Shopee Mutation': lambda totals, date, minusan_total: totals.get('Shopee_Mutation', 0),
            'Shopee Difference': lambda totals, date, minusan_total: totals.get('Shopee_Difference', 0),
            'ShopeePay Net': lambda totals, date, minusan_total: totals.get('ShopeePay_Net', 0),
            'ShopeePay Mutation': lambda totals, date, minusan_total: totals.get('ShopeePay_Mutation', 0),
            'ShopeePay Difference': lambda totals, date, minusan_total: totals.get('ShopeePay_Difference', 0),
            'Tiktok Net': lambda totals, date, minusan_total: totals.get('Tiktok_Net', 0),
            'Qpon Net': lambda totals, date, minusan_total: totals.get('Qpon_Net', 0),
            'Webshop Net': lambda totals, date, minusan_total: totals.get('Webshop_Net', 0),
            'UV': lambda totals, date, minusan_total: totals.get('UV', 0),
            'Cash Income (Admin)': lambda totals, date, minusan_total: totals.get('Cash_Income', 0),
            'Cash Expense (Admin)': lambda totals, date, minusan_total: totals.get('Cash_Expense', 0),
            'Sisa Cash (Admin)': lambda totals, date, minusan_total: totals.get('Cash_Income', 0) - totals.get('Cash_Expense', 0),
            'Minusan (Mutasi)': lambda totals, date, minusan_total: minusan_total,
        }

        for date in all_dates:
            totals = daily_totals.get(date, {})
            minusan_total = minusan_by_date.get(date, 0)

            # Recalculate Grab_Commission for the specific date
            if outlet_brand not in ["Pukis & Martabak Kota Baru"]:
                totals['Grab_Commission'] = totals.get('Grab_Net', 0) - (totals.get('Grab_Net', 0) * 1/74)
            else:
                totals['Grab_Commission'] = 0

            row_data = [
                header_value_map[header](totals, date, minusan_total) if header in header_value_map else None
                for header in headers
            ]

            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=current_row, column=col, value=value)
                if isinstance(value, (int, float)) and col > 1:
                    cell.number_format = '#,##0'

                # Apply conditional formatting for difference columns
                if headers[col-1] in ['Gojek Difference', 'Shopee Difference', 'ShopeePay Difference']:
                    if value is not None:
                        if value > 0:
                            cell.fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # Light green
                        elif value < 0:
                            cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')  # Light red
            current_row += 1

    def _write_grand_total(self):
        headers = self._get_headers()
        grand_totals = self.data['grand_totals']
        all_dates = self.data['all_dates']
        minusan_by_date = self.data['minusan_by_date']
        grand_total_row = self.ws.max_row + 1

        grand_total_value_map = {
            'Date': lambda: 'Grand Total',
            'GoFood': lambda: grand_totals.get('Gojek_Net', 0) - grand_totals.get('Gojek_QRIS', 0),
            'GO-PAY QRIS': lambda: grand_totals.get('Gojek_QRIS', 0),
            'Gojek Net': lambda: grand_totals.get('Gojek_Net', 0),
            'Gojek Mutation': lambda: grand_totals.get('Gojek_Mutation', 0),
            'Gojek Difference': lambda: grand_totals.get('Gojek_Difference', 0),
            'GrabFood': lambda: grand_totals.get('Grab_Net', 0) - grand_totals.get('GrabOVO_Net', 0),
            'GrabOVO': lambda: grand_totals.get('GrabOVO_Net', 0),
            'Grab Net': lambda: grand_totals.get('Grab_Net', 0),
            'Grab Net (ac)': lambda: grand_totals.get('Grab_Net', 0) - grand_totals.get('Grab_Commission', 0),
            'Shopee Net': lambda: grand_totals.get('Shopee_Net', 0),
            'Shopee Mutation': lambda: grand_totals.get('Shopee_Mutation', 0),
            'Shopee Difference': lambda: grand_totals.get('Shopee_Difference', 0),
            'ShopeePay Net': lambda: grand_totals.get('ShopeePay_Net', 0),
            'ShopeePay Mutation': lambda: grand_totals.get('ShopeePay_Mutation', 0),
            'ShopeePay Difference': lambda: grand_totals.get('ShopeePay_Difference', 0),
            'Tiktok Net': lambda: grand_totals.get('Tiktok_Net', 0),
            'Qpon Net': lambda: grand_totals.get('Qpon_Net', 0),
            'Webshop Net': lambda: grand_totals.get('Webshop_Net', 0),
            'UV': lambda: grand_totals.get('UV', 0),
            'Cash Income (Admin)': lambda: grand_totals.get('Cash_Income', 0),
            'Cash Expense (Admin)': lambda: grand_totals.get('Cash_Expense', 0),
            'Sisa Cash (Admin)': lambda: grand_totals.get('Cash_Difference', 0),
            'Minusan (Mutasi)': lambda: sum(minusan_by_date.get(date, 0) for date in all_dates),
        }

        row_data = [
            grand_total_value_map[header]() if header in grand_total_value_map else None
            for header in headers
        ]

        for col, value in enumerate(row_data, 1):
            cell = self.ws.cell(row=grand_total_row, column=col, value=value)
            cell.font = HEADER_FONT
            cell.fill = YELLOW_FILL
            cell.alignment = CENTER_ALIGN
            if isinstance(value, (int, float)):
                cell.number_format = '#,##0'
