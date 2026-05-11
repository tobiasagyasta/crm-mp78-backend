from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export import mpr_calculations as mpr_calc
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, GOJEK_FILL, GRAB_FILL, SHOPEE_FILL,GREY_FILL,
    TIKTOK_FILL, BLUE_FILL, DIFFERENCE_FILL, THIN_BORDER, auto_fit_columns, LEFT_ALIGN, RIGHT_ALIGN
)
from datetime import datetime
import re

class ClosingSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Closing Sheet', data)
        self.main_table_col_end = None
        self.grand_total_col_start = None
        self.grand_total_col_end = None
        self.grand_total_row_start = None
        self.store_id_col_start = None
        self.store_id_col_end = None
        self.store_id_row = None
        self.store_id_row_end = None

    def generate(self):
        self._write_main_table()
        self._write_grand_total_section()
        self._write_store_id_table()
        self._apply_styles()
        auto_fit_columns(self.ws)

    def _write_main_table(self):
        outlet = self.data['outlet']
        all_dates = self.data['all_dates']
        platform_definitions = self._get_main_table_platforms()

        # Header
        self.ws['A1'] = outlet.outlet_name_gojek
        self.ws['A1'].alignment = CENTER_ALIGN
        self.ws['A1'].font = HEADER_FONT

        closing_row = 3
        self._write_main_table_group_headers(closing_row - 1, platform_definitions)

        # Merged 'Tanggal' header
        self.ws.merge_cells(start_row=closing_row, start_column=1, end_row=closing_row + 1, end_column=1)
        tanggal_cell = self.ws.cell(row=closing_row, column=1)
        tanggal_cell.value = 'Tanggal'
        tanggal_cell.font = HEADER_FONT
        tanggal_cell.alignment = CENTER_ALIGN

        # Platform totals in the first row of the header
        for col, (name, header, report_type) in enumerate(platform_definitions, 2):
            cell = self.ws.cell(row=closing_row, column=col)
            value = self._get_platform_grand_total_with_fallback(report_type, header)
            cell.value = value
            cell.font = HEADER_FONT
            cell.alignment = RIGHT_ALIGN
            cell.number_format = '#,##0'
            cell.fill = YELLOW_FILL

        # Platform names in the second row of the header
        for col, (name, _, _) in enumerate(platform_definitions, 2):
            cell = self.ws.cell(row=closing_row + 1, column=col)
            cell.value = name
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            if name in ['Gojek', 'Gojek (ac)', 'Gojek MPR (ac)']:
                cell.fill = GOJEK_FILL
            elif name in ['Grab', 'Grab (ac)', 'Grab MPR (ac)', 'Grab(OVO)']:
                cell.fill = GRAB_FILL
            elif name in [
                'ShopeeFood', 'ShopeeFood (ac)', 'ShopeePay', 'ShopeePay (ac)',
                'Shopee MPR (ac)', 'ShopeePay MPR (ac)'
            ]:
                cell.fill = SHOPEE_FILL
            elif name in [
                'Tiktok', 'Tiktok (ac)', 'Tiktok MPR (ac)',
                'Qpon', 'Qpon (ac)', 'Webshop', 'Webshop (ac)'
            ]:
                cell.fill = TIKTOK_FILL

        closing_row += 2

        # Daily data rows
        for date in all_dates:
            row_data = [date]
            for _, header, report_type in platform_definitions:
                value = self._get_platform_daily_value_with_fallback(report_type, date, header)
                row_data.append(value)

            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=closing_row, column=col, value=value)
                cell.alignment = RIGHT_ALIGN
                if col == 1:
                    cell.number_format = 'yyyy-mm-dd'
                else:
                    cell.number_format = '#,##0'
            closing_row += 1

        self.main_table_col_end = len(platform_definitions) + 1

    def _write_main_table_group_headers(self, row, platform_definitions):
        if not self.data.get('mpr_report_data'):
            return

        current_report_type = None
        start_col = None

        for col, (_, _, report_type) in enumerate(platform_definitions, 2):
            if report_type != current_report_type:
                if current_report_type is not None:
                    self._merge_main_table_group_header(row, start_col, col - 1, current_report_type)
                current_report_type = report_type
                start_col = col

        if current_report_type is not None and start_col is not None:
            end_col = len(platform_definitions) + 1
            self._merge_main_table_group_header(row, start_col, end_col, current_report_type)

    def _merge_main_table_group_header(self, row, start_col, end_col, report_type):
        if report_type not in ['main', 'mpr'] or start_col > end_col:
            return

        label = 'MP78 (ac)' if report_type == 'main' else 'MPR (ac)'
        self.ws.merge_cells(start_row=row, start_column=start_col, end_row=row, end_column=end_col)
        cell = self.ws.cell(row=row, column=start_col, value=label)
        cell.font = HEADER_FONT
        cell.alignment = CENTER_ALIGN
        cell.fill = BLUE_FILL

    def _get_grand_total_with_fallback(self, header):
        grand_totals = self.data['grand_totals']
        value = grand_totals.get(header)
        if header.endswith('_Mutation') and not value:
            net_key = header.replace('_Mutation', '_Net')
            value = grand_totals.get(net_key, 0)
        return value

    def _get_mpr_grand_total_with_fallback(self, header):
        mpr_report_data = self.data.get('mpr_report_data')
        if not mpr_report_data:
            return None

        grand_totals = mpr_report_data.get('grand_totals', {})
        value = grand_totals.get(header)
        if header.endswith('_Mutation') and not value:
            net_key = header.replace('_Mutation', '_Net')
            value = grand_totals.get(net_key, 0)
        return value

    def _get_platform_definitions_for_mpr(self):
        return [
            ('Gojek MPR (ac)', 'Gojek_Mutation', 'mpr'),
            ('Grab MPR (ac)', 'Grab_Net', 'mpr'),
            ('Shopee MPR (ac)', 'Shopee_Net', 'mpr'),
            ('ShopeePay MPR (ac)', 'ShopeePay_Net', 'mpr'),
            ('Tiktok MPR (ac)', 'Tiktok_Net', 'mpr'),
        ]

    def _get_main_table_platforms(self):
        platform_definitions = self._get_main_platform_definitions_for_grand_total()

        if not self.data.get('mpr_report_data'):
            return platform_definitions

        return platform_definitions + [
            ('Gojek MPR (ac)', 'Gojek_Mutation', 'mpr'),
            ('Grab MPR (ac)', 'Grab_Net', 'mpr'),
            ('Shopee MPR (ac)', 'Shopee_Net', 'mpr'),
            ('ShopeePay MPR (ac)', 'ShopeePay_Net', 'mpr'),
            ('Tiktok MPR (ac)', 'Tiktok_Net', 'mpr'),
        ]

    def _get_platform_grand_total_with_fallback(self, report_type, header):
        if report_type == 'mpr':
            return self._get_mpr_display_value(header)
        if self._uses_mp78_management_ac():
            return self._get_mp78_display_value(header)
        return self._get_grand_total_with_fallback(header)

    def _get_platform_daily_value_with_fallback(self, report_type, date, header):
        if report_type == 'mpr':
            return self._get_mpr_display_value(header, date)
        if self._uses_mp78_management_ac():
            return self._get_mp78_display_value(header, date)
        return self._get_report_value_with_fallback(self.data, header, date)

    def _write_grand_total_section(self):
        outlet = self.data['outlet']
        start_date = self.data['start_date']
        end_date = self.data['end_date']
        manual_entries = self.data['manual_entries']
        platform_columns = ['Gojek_Mutation', 'Grab_Net', 'Shopee_Net', 'ShopeePay_Net', 'Tiktok_Net', 'Qpon_Net', 'Webshop_Net']
        platform_names = [
            label
            for label, _, _ in self._get_main_platform_definitions_for_grand_total()
        ]

        col_start = self.ws.max_column + 3
        row_start = 3
        self.grand_total_col_start = col_start
        self.grand_total_col_end = col_start + 3
        self.grand_total_row_start = row_start

        self.ws.cell(row=row_start, column=col_start, value=outlet.outlet_name_gojek).font = HEADER_FONT
        self.ws.cell(row=row_start, column=col_start).alignment = CENTER_ALIGN
        self.ws.cell(row=row_start, column=col_start).fill = BLUE_FILL
        self.ws.cell(row=row_start + 1, column=col_start, value=f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}').font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start).alignment = CENTER_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start).fill = BLUE_FILL

        grab_net_total = self._get_grand_total_with_fallback('Grab_Net') or 0
        grab_management_expense = grab_net_total * mpr_calc.MANAGEMENT_COMMISSION_RATE
        total_income = (
            self._get_platform_grand_total_with_fallback('main', 'Gojek_Mutation') +
            self._get_closing_grand_total_income_value('Grab_Net', grab_net_total) +
            self._get_platform_grand_total_with_fallback('main', 'Shopee_Net') +
            self._get_platform_grand_total_with_fallback('main', 'ShopeePay_Net') +
            self._get_platform_grand_total_with_fallback('main', 'Tiktok_Net') +
            self._get_platform_grand_total_with_fallback('main', 'Qpon_Net') +
            self._get_platform_grand_total_with_fallback('main', 'Webshop_Net') +
            (self._get_mpr_display_value('Gojek_Mutation') or 0) +
            (self._get_mpr_display_value('Grab_Net') or 0) +
            (self._get_mpr_display_value('Shopee_Net') or 0) +
            (self._get_mpr_display_value('ShopeePay_Net') or 0) +
            (self._get_mpr_display_value('Tiktok_Net') or 0) +
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
            grab_management_expense
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
            platform_label = self._get_closing_grand_total_platform_label(platform_names[i-1], header)
            self.ws.cell(row=label_row, column=col_start, value=platform_label).alignment = LEFT_ALIGN
            self.ws.cell(row=label_row, column=col_start, value=platform_label).font = HEADER_FONT
            value_cell = self.ws.cell(
                row=label_row,
                column=col_start + 1,
                value=self._get_closing_grand_total_income_value(header, grab_net_total)
            )
            value_cell.number_format = '#,##0'
            value_cell.alignment = RIGHT_ALIGN
            final_i += 1
            if header == 'Grab_Net':
                grab_mgmt_row = label_row + 1
                self.ws.cell(row=grab_mgmt_row, column=col_start, value="Grab Manag 1%").alignment = LEFT_ALIGN
                self.ws.cell(row=grab_mgmt_row, column=col_start, value="Grab Manag 1%").font = HEADER_FONT
                management_cell_value = self.ws.cell(
                    row=grab_mgmt_row,
                    column=col_start + 2,
                    value=grab_management_expense
                )
                management_cell_value.number_format = '#,##0'
                management_cell_value.alignment = RIGHT_ALIGN
                final_i += 1

        mpr_rows = [
            (label, header)
            for label, header, _ in self._get_platform_definitions_for_mpr()
        ]
        for label, header in mpr_rows:
            mpr_value = self._get_mpr_display_value(header)
            if mpr_value is None:
                continue

            label_row = row_start + 1 + final_i + 1
            self.ws.cell(row=label_row, column=col_start, value=label).alignment = LEFT_ALIGN
            self.ws.cell(row=label_row, column=col_start, value=label).font = HEADER_FONT
            value_cell = self.ws.cell(row=label_row, column=col_start + 1, value=mpr_value)
            value_cell.number_format = '#,##0'
            value_cell.alignment = RIGHT_ALIGN
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

    def _write_store_id_table(self):
        outlet = self.data['outlet']
        mpr_report_data = self.data.get('mpr_report_data')
        mpr_outlet = mpr_report_data.get('outlet') if mpr_report_data else None

        row = 3
        start_column = 20  # Column T
        store_id_rows = [
            ('Gojek', outlet.store_id_gojek, GOJEK_FILL),
            ('Grab', outlet.store_id_grab, GRAB_FILL),
            ('ShopeeFood', outlet.store_id_shopee, SHOPEE_FILL),
            ('ShopeePay', outlet.store_id_shopee, SHOPEE_FILL),
        ]

        if mpr_outlet:
            store_id_rows.extend([
                ('Gojek MPR', mpr_outlet.store_id_gojek, GOJEK_FILL),
                ('Grab MPR', mpr_outlet.store_id_grab, GRAB_FILL),
                ('Shopee MPR', mpr_outlet.store_id_shopee, SHOPEE_FILL),
            ])

        self.store_id_col_start = start_column
        self.store_id_col_end = start_column + 1
        self.store_id_row = row
        self.store_id_row_end = row + len(store_id_rows)
        self.ws.row_dimensions[row].height = 18

        header_cells = [
            (start_column, 'Platform Name'),
            (start_column + 1, 'Store ID'),
        ]
        for column, value in header_cells:
            cell = self.ws.cell(row=row, column=column, value=value)
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = BLUE_FILL

        for offset, (label, value, fill) in enumerate(store_id_rows, start=1):
            data_row = row + offset
            self.ws.row_dimensions[data_row].height = 18

            label_cell = self.ws.cell(row=data_row, column=start_column, value=label)
            label_cell.font = HEADER_FONT
            label_cell.alignment = CENTER_ALIGN
            label_cell.fill = fill

            value_cell = self.ws.cell(row=data_row, column=start_column + 1, value=value or "-")
            value_cell.font = HEADER_FONT
            value_cell.alignment = CENTER_ALIGN
            value_cell.fill = fill

    def _get_report_value_with_fallback(self, report_data, header, date=None):
        if not report_data:
            return None

        totals = report_data.get('grand_totals', {})
        if date is not None:
            totals = report_data.get('daily_totals', {}).get(date, {})

        value = totals.get(header)
        if header.endswith('_Mutation') and not value:
            net_key = header.replace('_Mutation', '_Net')
            value = totals.get(net_key, 0)
        return value

    def _is_mp78_brand(self):
        return self.data['outlet'].brand == 'MP78'

    def _uses_mp78_management_ac(self):
        return self._is_mp78_brand() and mpr_calc.ENABLE_MP78_MANAGEMENT_AC

    def _get_mp78_display_value(self, header, date=None):
        totals = self.data.get('grand_totals', {})
        if date is not None:
            totals = self.data.get('daily_totals', {}).get(date, {})

        ac_value = mpr_calc.mp78_ac_value_for_header(totals, header)
        if ac_value is not None:
            return ac_value

        return self._get_report_value_with_fallback(self.data, header, date)

    def _get_mpr_display_value(self, header, date=None):
        report_data = self.data.get('mpr_report_data')
        if not report_data:
            return None

        totals = report_data.get('grand_totals', {})
        if date is not None:
            totals = report_data.get('daily_totals', {}).get(date, {})

        ac_value = mpr_calc.mpr_ac_value_for_header(totals, header)
        if ac_value is not None:
            return ac_value

        return self._get_report_value_with_fallback(report_data, header, date)

    def _get_closing_grand_total_income_value(self, header, grab_net_total=None):
        if header == 'Grab_Net':
            if grab_net_total is not None:
                return grab_net_total
            return self._get_grand_total_with_fallback(header)

        return self._get_platform_grand_total_with_fallback('main', header)

    def _get_closing_grand_total_platform_label(self, label, header):
        if header == 'Grab_Net':
            return 'Grab'

        return label

    def _get_main_platform_definitions_for_grand_total(self):
        if self._uses_mp78_management_ac():
            return [
                ('Gojek (ac)', 'Gojek_Mutation', 'main'),
                ('Grab (ac)', 'Grab_Net', 'main'),
                ('ShopeeFood', 'Shopee_Net', 'main'),
                ('ShopeePay', 'ShopeePay_Net', 'main'),
                ('Tiktok (ac)', 'Tiktok_Net', 'main'),
                ('Qpon', 'Qpon_Net', 'main'),
                ('Webshop', 'Webshop_Net', 'main'),
            ]

        return [
            ('Gojek', 'Gojek_Mutation', 'main'),
            ('Grab', 'Grab_Net', 'main'),
            ('ShopeeFood', 'Shopee_Net', 'main'),
            ('ShopeePay', 'ShopeePay_Net', 'main'),
            ('Tiktok', 'Tiktok_Net', 'main'),
            ('Qpon', 'Qpon_Net', 'main'),
            ('Webshop', 'Webshop_Net', 'main'),
        ]

    def _apply_styles(self):
        # Apply borders to the main table
        if self.main_table_col_end:
            for row in self.ws.iter_rows(
                min_row=1,
                max_row=self.ws.max_row,
                min_col=1,
                max_col=self.main_table_col_end,
            ):
                for cell in row:
                    cell.border = THIN_BORDER

        # Apply borders to the grand total section only.
        if self.grand_total_col_start and self.grand_total_col_end and self.grand_total_row_start:
            for row in self.ws.iter_rows(
                min_row=self.grand_total_row_start,
                max_row=self.ws.max_row,
                min_col=self.grand_total_col_start,
                max_col=self.grand_total_col_end,
            ):
                for cell in row:
                    cell.border = THIN_BORDER

        # Apply borders to the store ID table separately.
        if self.store_id_col_start and self.store_id_col_end and self.store_id_row and self.store_id_row_end:
            for row in self.ws.iter_rows(
                min_row=self.store_id_row,
                max_row=self.store_id_row_end,
                min_col=self.store_id_col_start,
                max_col=self.store_id_col_end,
            ):
                for cell in row:
                    cell.border = THIN_BORDER
