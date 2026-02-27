from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, GOJEK_FILL, GRAB_FILL, SHOPEE_FILL,GREY_FILL,
    TIKTOK_FILL, BLUE_FILL, DIFFERENCE_FILL, THIN_BORDER, auto_fit_columns, LEFT_ALIGN, RIGHT_ALIGN
)
from datetime import datetime
import re

class ClosingSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Closing Sheet', data)

    def generate(self):
        self._write_main_table()
        self._write_grand_total_section()
        self._apply_styles()
        auto_fit_columns(self.ws)

    def _write_main_table(self):
        outlet = self.data['outlet']
        grand_totals = self.data['grand_totals']
        all_dates = self.data['all_dates']
        daily_totals = self.data['daily_totals']

        # Header
        self.ws['A1'] = outlet.outlet_name_gojek
        self.ws['A1'].alignment = CENTER_ALIGN
        self.ws['A1'].font = HEADER_FONT
        self.ws['B2'] = outlet.store_id_gojek
        self.ws['B2'].alignment = CENTER_ALIGN
        self.ws['C2'] = outlet.store_id_grab
        self.ws['C2'].alignment = CENTER_ALIGN
        self.ws['D2'] = outlet.store_id_shopee
        self.ws['D2'].alignment = CENTER_ALIGN
        self.ws['E2'] = outlet.store_id_shopee
        self.ws['E2'].alignment = CENTER_ALIGN

        closing_row = 3
        # Updated platform columns to include GrabOVO
        platform_columns = ['Gojek_Mutation', 'Grab_Net', 'Shopee_Net', 'ShopeePay_Net', 'Tiktok_Net', 'Qpon_Net', 'Webshop_Net', 'UV']
        platform_names = ['Gojek', 'Grab', 'ShopeeFood', 'ShopeePay', 'Tiktok', 'Qpon', 'Webshop', 'Ultra Voucher']

        # Merged 'Tanggal' header
        self.ws.merge_cells(start_row=closing_row, start_column=1, end_row=closing_row + 1, end_column=1)
        tanggal_cell = self.ws.cell(row=closing_row, column=1)
        tanggal_cell.value = 'Tanggal'
        tanggal_cell.font = HEADER_FONT
        tanggal_cell.alignment = CENTER_ALIGN

        # Platform totals in the first row of the header
        for col, header in enumerate(platform_columns, 2):
            cell = self.ws.cell(row=closing_row, column=col)
            value = grand_totals.get(header)
            if header.endswith('_Mutation') and not value:
                net_key = header.replace('_Mutation', '_Net')
                value = grand_totals.get(net_key, 0)
            cell.value = value
            cell.font = HEADER_FONT
            cell.alignment = RIGHT_ALIGN
            cell.number_format = '#,##0'
            cell.fill = YELLOW_FILL

        # Platform names in the second row of the header
        for col, name in enumerate(platform_names, 2):
            cell = self.ws.cell(row=closing_row + 1, column=col)
            cell.value = name
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            if name in ['GoFood', 'GO-PAY QRIS']: cell.fill = GOJEK_FILL
            elif name in ['Grab', 'Grab(OVO)']: cell.fill = GRAB_FILL
            elif name in ['ShopeeFood', 'ShopeePay']: cell.fill = SHOPEE_FILL
            elif name in ['Tiktok', 'Qpon', 'Webshop']: cell.fill = TIKTOK_FILL

        closing_row += 2

        # Daily data rows
        for date in all_dates:
            row_data = [date]
            for header in platform_columns:
                value = daily_totals[date].get(header)
                if header.endswith('_Mutation') and not value:
                    net_key = header.replace('_Mutation', '_Net')
                    value = daily_totals[date].get(net_key, 0)
                row_data.append(value)

            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=closing_row, column=col, value=value)
                cell.alignment = RIGHT_ALIGN
                if col == 1:
                    cell.number_format = 'yyyy-mm-dd'
                else:
                    cell.number_format = '#,##0'
            closing_row += 1

    def _get_grand_total_with_fallback(self, header):
        grand_totals = self.data['grand_totals']
        value = grand_totals.get(header)
        if header.endswith('_Mutation') and not value:
            net_key = header.replace('_Mutation', '_Net')
            value = grand_totals.get(net_key, 0)
        return value

    def _write_grand_total_section(self):
        outlet = self.data['outlet']
        start_date = self.data['start_date']
        end_date = self.data['end_date']
        manual_entries = self.data['manual_entries']
        platform_columns = ['Gojek_Mutation', 'Grab_Net', 'Shopee_Net', 'ShopeePay_Net', 'Tiktok_Net', 'Qpon_Net', 'Webshop_Net', 'UV']
        platform_names = ['Gojek', 'Grab', 'ShopeeFood', 'ShopeePay', 'Tiktok', 'Qpon', 'Webshop', 'Ultra Voucher']

        col_start = self.ws.max_column + 3
        row_start = 3

        self.ws.cell(row=row_start, column=col_start, value=outlet.outlet_name_gojek).font = HEADER_FONT
        self.ws.cell(row=row_start, column=col_start).alignment = CENTER_ALIGN
        self.ws.cell(row=row_start, column=col_start).fill = BLUE_FILL
        self.ws.cell(row=row_start + 1, column=col_start, value=f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}').font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start).alignment = CENTER_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start).fill = BLUE_FILL

        total_income = (
            self._get_grand_total_with_fallback('Gojek_Mutation') +
            self._get_grand_total_with_fallback('Grab_Net') +
            self._get_grand_total_with_fallback('Shopee_Net') +
            self._get_grand_total_with_fallback('ShopeePay_Net') +
            self._get_grand_total_with_fallback('Tiktok_Net') +
            self._get_grand_total_with_fallback('Qpon_Net') +
            self._get_grand_total_with_fallback('Webshop_Net') +
            sum(float(entry.amount) for entry, _, _ in manual_entries if entry.entry_type == 'income') +
            self._get_grand_total_with_fallback('UV')
        )
        self.ws.cell(row=row_start + 1, column=col_start + 1, value=total_income).font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start + 1).alignment = RIGHT_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start + 1).number_format = '#,##0'
        self.ws.cell(row=row_start + 1, column=col_start + 1).fill = GRAB_FILL
        self.ws.cell(row=row_start, column=col_start + 1).fill = GRAB_FILL

        total_expense = (
            sum(float(entry.amount) for entry, _, _ in manual_entries if entry.entry_type == 'expense') +
            (self._get_grand_total_with_fallback('Grab_Net') * 1/74)
        )
        self.ws.cell(row=row_start + 1, column=col_start + 2, value=total_expense).font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start + 2).alignment = RIGHT_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start + 2).number_format = '#,##0'
        self.ws.cell(row=row_start + 1, column=col_start + 2).fill = DIFFERENCE_FILL
        self.ws.cell(row=row_start, column=col_start + 2).fill = DIFFERENCE_FILL

        total_all = total_income - total_expense
        self.ws.cell(row=row_start + 1, column=col_start + 3, value=total_all).font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start + 3).alignment = RIGHT_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start + 3).number_format = '#,##0'
        self.ws.cell(row=row_start + 1, column=col_start + 3).fill = GREY_FILL
        self.ws.cell(row=row_start, column=col_start + 3).fill = GREY_FILL

        final_i = 0
        for i, header in enumerate(platform_columns, 1):
            label_row = row_start + 1 + final_i + 1
            self.ws.cell(row=label_row, column=col_start, value=platform_names[i-1]).alignment = LEFT_ALIGN
            self.ws.cell(row=label_row, column=col_start, value=platform_names[i-1]).font = HEADER_FONT
            value_cell = self.ws.cell(row=label_row, column=col_start + 1, value=self._get_grand_total_with_fallback(header))
            value_cell.number_format = '#,##0'
            value_cell.alignment = RIGHT_ALIGN
            final_i += 1
            if platform_names[i-1] == 'Grab':
                grab_mgmt_row = label_row + 1
                self.ws.cell(row=grab_mgmt_row, column=col_start, value="Grab Manag 1%").alignment = LEFT_ALIGN
                self.ws.cell(row=grab_mgmt_row, column=col_start, value="Grab Manag 1%").font = HEADER_FONT
                management_cell_value = self.ws.cell(row=grab_mgmt_row, column=col_start + 2, value=self.data['grand_totals']['Grab_Net'] * 1/74)
                management_cell_value.number_format = '#,##0'
                management_cell_value.alignment = RIGHT_ALIGN
                final_i += 1

        MONTH_MAP = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'Mei': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Des': 12
        }
        def parse_indonesian_date(description):
            match = re.search(r'(\d{1,2})[-\s]?([A-Za-z]+)(?:-(\d{2,4}))?', description)
            if match:
                day, month_str, year_str = match.groups()
                month = MONTH_MAP.get(month_str.capitalize())
                if month:
                    year = int(year_str) if year_str else datetime.now().year
                    if len(str(year)) == 2: year += 2000
                    return datetime(year, month, int(day))
            return datetime.min

        manual_entries_sorted = sorted(manual_entries, key=lambda tup: parse_indonesian_date(tup[0].description))

        for idx, (entry, income_cat, expense_cat) in enumerate(manual_entries_sorted, 1):
            row = row_start + final_i + 1 + idx
            cat_name = (income_cat.name if income_cat else '') if entry.entry_type == 'income' else (expense_cat.name if expense_cat else '')
            desc_text = f"{cat_name}: {entry.description}"
            self.ws.cell(row=row, column=col_start, value=desc_text).alignment = LEFT_ALIGN
            self.ws.cell(row=row, column=col_start, value=desc_text).font = HEADER_FONT


            if entry.entry_type == 'income':
                cell = self.ws.cell(row=row, column=col_start + 1, value=float(entry.amount))
            else:
                cell = self.ws.cell(row=row, column=col_start + 2, value=float(entry.amount))
            cell.number_format = '#,##0'
            cell.alignment = RIGHT_ALIGN

    def _apply_styles(self):
        # Apply borders to the main table
        for row in self.ws.iter_rows(min_row=1, max_row=self.ws.max_row, min_col=1, max_col=9):
            for cell in row:
                cell.border = THIN_BORDER
        # Apply borders to the grand total section
        final_row = 0
        for row in self.ws.iter_rows(min_row=3, min_col=9, max_col=self.ws.max_column):
            for cell in row:
                cell.border = THIN_BORDER
            if row[0].row > final_row:
                final_row = row[0].row
        for row in self.ws.iter_rows(min_row=3, max_row=final_row, min_col=9, max_col=self.ws.max_column):
            for cell in row:
                cell.border = THIN_BORDER
