from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export import mpr_calculations as mpr_calc
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, GOJEK_FILL, GRAB_FILL, SHOPEE_FILL,GREY_FILL,
    TIKTOK_FILL, BLUE_FILL, DIFFERENCE_FILL, THIN_BORDER, auto_fit_columns, LEFT_ALIGN, RIGHT_ALIGN,
    CLOSED_OFF_FILL
)
from app.models.mpr_mapping import MprMapping
from app.models.outlet import Outlet
from app.services.closing_platforms import is_platform_disabled, platform_for_header
from app.services.rekening_info_service import OutletRekeningInfo, RekeningInfoService
from datetime import datetime
import re

class ClosingSheet(BaseSheet):
    TIKTOK_NET_HEADER = 'Tiktok_Net'
    TIKTOK_CLOSING_NET_HEADER = 'Tiktok_Closing_Net'
    QPON_NET_HEADER = 'Qpon_Net'
    QPON_CLOSING_NET_HEADER = 'Qpon_Closing_Net'

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
        self.rekening_col_start = None
        self.rekening_col_end = None
        self.rekening_row = None
        self.rekening_row_end = None
        self._mpr_mapping = None
        self._mpr_mapping_loaded = False

    def generate(self):
        self._write_main_table()
        self._write_grand_total_section()
        self._write_store_id_table()
        self._write_rekening_table()
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
            cell.fill = CLOSED_OFF_FILL if self._is_closing_platform_disabled(header, report_type) else YELLOW_FILL

        # Platform names in the second row of the header
        for col, (name, header, report_type) in enumerate(platform_definitions, 2):
            cell = self.ws.cell(row=closing_row + 1, column=col)
            cell.value = name
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            if self._is_closing_platform_disabled(header, report_type):
                cell.fill = CLOSED_OFF_FILL
            elif name in ['Gojek', 'Gojek (ac)', 'Gojek MPR (ac)']:
                cell.fill = GOJEK_FILL
            elif name in ['Grab', 'Grab Net', 'Grab (ac)', 'Grab MPR (ac)', 'Grab(OVO)']:
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
        if self._uses_mp78_management_ac():
            platform_definitions = (
                platform_definitions[:1] +
                [
                    ('Grab Net', 'Grab_Net_Raw', 'main'),
                ] +
                [
                    definition for definition in platform_definitions[1:]
                    if definition[0] != 'Grab (ac)'
                ]
            )

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
        if header == self.TIKTOK_NET_HEADER:
            return self._get_tiktok_closing_display_value(report_type)
        if header == self.QPON_NET_HEADER:
            return self._get_qpon_closing_display_value(report_type)
        if report_type == 'mpr':
            return self._get_mpr_display_value(header)
        if self._is_mpr_brand():
            return self._get_direct_mpr_display_value(header)
        special_value = self._get_main_table_special_value(header)
        if special_value is not None:
            return special_value
        if self._uses_mp78_management_ac():
            return self._get_mp78_display_value(header)
        return self._get_grand_total_with_fallback(header)

    def _get_platform_daily_value_with_fallback(self, report_type, date, header):
        if header == self.TIKTOK_NET_HEADER:
            return self._get_tiktok_closing_display_value(report_type, date)
        if header == self.QPON_NET_HEADER:
            return self._get_qpon_closing_display_value(report_type, date)
        if report_type == 'mpr':
            return self._get_mpr_display_value(header, date)
        if self._is_mpr_brand():
            return self._get_direct_mpr_display_value(header, date)
        special_value = self._get_main_table_special_value(header, date)
        if special_value is not None:
            return special_value
        if self._uses_mp78_management_ac():
            return self._get_mp78_display_value(header, date)
        return self._get_report_value_with_fallback(self.data, header, date)

    def _write_grand_total_section(self):
        outlet = self.data['outlet']
        start_date = self.data['start_date']
        end_date = self.data['end_date']
        manual_entries = self.data['manual_entries']
        mp78_mutations = self.data.get('mp78_mutations', [])
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
        grab_management_expense = self._get_grab_management_commission_expense(grab_net_total)
        mp78_income_total = self._get_mp78_mutation_total(mp78_mutations, 'income')
        mp78_expense_total = self._get_mp78_mutation_total(mp78_mutations, 'expense')
        total_income = (
            self._get_closing_grand_total_income_contribution('main', 'Gojek_Mutation') +
            self._get_closing_grand_total_income_contribution('main', 'Grab_Net', grab_net_total) +
            self._get_closing_grand_total_income_contribution('main', 'Shopee_Net') +
            self._get_closing_grand_total_income_contribution('main', 'ShopeePay_Net') +
            self._get_closing_grand_total_income_contribution('main', 'Tiktok_Net') +
            self._get_closing_grand_total_income_contribution('main', 'Qpon_Net') +
            self._get_closing_grand_total_income_contribution('main', 'Webshop_Net') +
            self._get_closing_grand_total_income_contribution('mpr', 'Gojek_Mutation') +
            self._get_closing_grand_total_income_contribution('mpr', 'Grab_Net') +
            self._get_closing_grand_total_income_contribution('mpr', 'Shopee_Net') +
            self._get_closing_grand_total_income_contribution('mpr', 'ShopeePay_Net') +
            self._get_closing_grand_total_income_contribution('mpr', 'Tiktok_Net') +
            sum(float(entry.amount) for entry, _, _ in manual_entries if entry.entry_type == 'income') +
            mp78_income_total +
            self._get_grand_total_with_fallback('UV')
        )
        self.ws.cell(row=row_start + 1, column=col_start + 1, value=total_income).font = HEADER_FONT
        self.ws.cell(row=row_start + 1, column=col_start + 1).alignment = RIGHT_ALIGN
        self.ws.cell(row=row_start + 1, column=col_start + 1).number_format = '#,##0'
        self.ws.cell(row=row_start + 1, column=col_start + 1).fill = GRAB_FILL
        self.ws.cell(row=row_start, column=col_start + 1).fill = GRAB_FILL

        total_expense = (
            sum(float(entry.amount) for entry, _, _ in manual_entries if entry.entry_type == 'expense') +
            mp78_expense_total +
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
            platform_disabled = self._is_closing_platform_disabled(header, 'main')
            label_cell = self.ws.cell(row=label_row, column=col_start, value=platform_label)
            label_cell.alignment = LEFT_ALIGN
            label_cell.font = HEADER_FONT
            if platform_disabled:
                label_cell.fill = CLOSED_OFF_FILL
            value_cell = self.ws.cell(
                row=label_row,
                column=col_start + 1,
                value=None if platform_disabled else self._get_closing_grand_total_income_value(header, grab_net_total)
            )
            value_cell.number_format = '#,##0'
            value_cell.alignment = RIGHT_ALIGN
            if platform_disabled:
                value_cell.fill = CLOSED_OFF_FILL
            final_i += 1
            if header == 'Grab_Net':
                grab_mgmt_row = label_row + 1
                management_label_cell = self.ws.cell(
                    row=grab_mgmt_row,
                    column=col_start,
                    value=self._get_grab_management_commission_label()
                )
                management_label_cell.alignment = LEFT_ALIGN
                management_label_cell.font = HEADER_FONT
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
            if header == self.TIKTOK_NET_HEADER:
                mpr_value = self._get_tiktok_closing_display_value('mpr')
            else:
                mpr_value = self._get_mpr_display_value(header)
            if mpr_value is None:
                continue

            label_row = row_start + 1 + final_i + 1
            platform_disabled = self._is_closing_platform_disabled(header, 'mpr')
            label_cell = self.ws.cell(row=label_row, column=col_start, value=label)
            label_cell.alignment = LEFT_ALIGN
            label_cell.font = HEADER_FONT
            if platform_disabled:
                label_cell.fill = CLOSED_OFF_FILL
            value_cell = self.ws.cell(
                row=label_row,
                column=col_start + 1,
                value=None if platform_disabled else mpr_value,
            )
            value_cell.number_format = '#,##0'
            value_cell.alignment = RIGHT_ALIGN
            if platform_disabled:
                value_cell.fill = CLOSED_OFF_FILL
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

        grand_total_entries = []
        for idx, (entry, income_cat, expense_cat) in enumerate(manual_entries):
            grand_total_entries.append((
                parse_indonesian_date(entry.description or ''),
                1,
                idx,
                'manual',
                (entry, income_cat, expense_cat),
            ))
        for idx, mutation in enumerate(mp78_mutations):
            mutation_date = mutation.tanggal
            if mutation_date:
                mutation_date = datetime.combine(mutation_date, datetime.min.time())
            else:
                mutation_date = datetime.min
            grand_total_entries.append((mutation_date, 0, idx, 'mp78_mutation', mutation))

        for idx, (_, _, _, row_type, payload) in enumerate(sorted(grand_total_entries), 1):
            row = row_start + final_i + 1 + idx
            if row_type == 'manual':
                entry, income_cat, expense_cat = payload
                cat_name = (income_cat.name if income_cat else '') if entry.entry_type == 'income' else (expense_cat.name if expense_cat else '')
                desc_text = f"{cat_name}: {entry.description}"
                amount = float(entry.amount or 0)
                amount_col = col_start + 1 if entry.entry_type == 'income' else col_start + 2
            else:
                mutation = payload
                desc_text = self._get_mp78_mutation_description(mutation)
                amount = float(mutation.transaction_amount or 0)
                amount_col = col_start + 2 if self._is_mp78_expense_mutation(mutation) else col_start + 1

            self.ws.cell(row=row, column=col_start, value=desc_text).alignment = LEFT_ALIGN
            self.ws.cell(row=row, column=col_start, value=desc_text).font = HEADER_FONT
            cell = self.ws.cell(row=row, column=amount_col, value=amount)
            cell.number_format = '#,##0'
            cell.alignment = RIGHT_ALIGN

    def _is_mp78_expense_mutation(self, mutation):
        return (getattr(mutation, 'transaction_type', '') or '').upper() == 'DB'

    def _get_mp78_mutation_total(self, mutations, entry_type):
        total = 0
        for mutation in mutations:
            is_expense = self._is_mp78_expense_mutation(mutation)
            if (entry_type == 'expense') != is_expense:
                continue
            total += float(getattr(mutation, 'transaction_amount', 0) or 0)
        return total

    def _get_mp78_mutation_description(self, mutation):
        date_label = mutation.tanggal.strftime('%Y-%m-%d') if mutation.tanggal else '-'
        transaction_type = (mutation.transaction_type or '').upper()
        description = mutation.transaksi or ''
        return f"{date_label} : {description} {transaction_type} "

    def _write_store_id_table(self):
        outlet = self.data['outlet']
        mpr_outlet = self._get_mapped_mpr_outlet()

        row = 3
        start_column = (self.grand_total_col_end + 2) if self.grand_total_col_end else 20
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

    def _write_rekening_table(self):
        outlet = self.data['outlet']
        mpr_outlet = self._get_mapped_mpr_outlet()

        row = 3
        start_column = (self.store_id_col_end + 2) if self.store_id_col_end else 22
        rekening_rows = RekeningInfoService.get_outlet_rekenings(outlet)

        if mpr_outlet:
            rekening_rows.extend(RekeningInfoService.get_outlet_rekenings(mpr_outlet))

        if not rekening_rows:
            rekening_rows = [
                OutletRekeningInfo(
                    outlet_label=self._get_rekening_table_outlet_label(outlet),
                    platform_name="-",
                    rekening_name=None,
                    rekening_number=None,
                )
            ]

        self.rekening_col_start = start_column
        self.rekening_col_end = start_column + 2
        self.rekening_row = row
        self.rekening_row_end = row + len(rekening_rows)
        self.ws.row_dimensions[row].height = 18

        header_cells = [
            (start_column, 'Outlet'),
            (start_column + 1, 'Platform'),
            (start_column + 2, 'Rekening'),
        ]
        for column, value in header_cells:
            cell = self.ws.cell(row=row, column=column, value=value)
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = BLUE_FILL

        for offset, rekening_info in enumerate(rekening_rows, start=1):
            data_row = row + offset
            self.ws.row_dimensions[data_row].height = 18

            label_cell = self.ws.cell(row=data_row, column=start_column, value=rekening_info.outlet_label)
            label_cell.font = HEADER_FONT
            label_cell.alignment = CENTER_ALIGN
            label_cell.fill = GREY_FILL

            platform_cell = self.ws.cell(row=data_row, column=start_column + 1, value=rekening_info.platform_name)
            platform_cell.font = HEADER_FONT
            platform_cell.alignment = CENTER_ALIGN
            platform_cell.fill = GREY_FILL

            value_cell = self.ws.cell(
                row=data_row,
                column=start_column + 2,
                value=rekening_info.display_value,
            )
            value_cell.font = HEADER_FONT
            value_cell.alignment = CENTER_ALIGN
            value_cell.fill = GREY_FILL

    def _get_mapped_mpr_outlet(self):
        if not self._is_mp78_brand():
            return None

        mpr_report_data = self.data.get('mpr_report_data')
        if mpr_report_data and mpr_report_data.get('outlet'):
            return mpr_report_data['outlet']

        mapping = self._get_mpr_mapping()
        if not mapping or not mapping.mpr_outlet_code:
            return None

        return Outlet.query.filter_by(outlet_code=mapping.mpr_outlet_code).first()

    def _get_mpr_mapping(self):
        if not self._is_mp78_brand():
            return None

        if not self._mpr_mapping_loaded:
            outlet = self.data['outlet']
            self._mpr_mapping = MprMapping.query.filter_by(mp78_outlet_code=outlet.outlet_code).first()
            self._mpr_mapping_loaded = True

        return self._mpr_mapping

    def _has_mpr_mapping(self):
        return self._get_mpr_mapping() is not None

    def _get_rekening_table_outlet_label(self, outlet):
        if not outlet:
            return "-"
        outlet_code = getattr(outlet, 'outlet_code', None)
        brand = getattr(outlet, 'brand', None)
        if brand and outlet_code:
            return f"{brand} - {outlet_code}"
        return outlet_code or getattr(outlet, 'outlet_name_gojek', None) or "-"

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

    def _get_main_table_special_value(self, header, date=None):
        totals = self.data.get('grand_totals', {})
        if date is not None:
            totals = self.data.get('daily_totals', {}).get(date, {})

        if header == 'Grab_Net_Raw':
            return totals.get('Grab_Net', 0)

        return None

    def _is_mp78_brand(self):
        return self.data['outlet'].brand == 'MP78'

    def _is_mpr_brand(self):
        return mpr_calc.is_mpr_brand(self.data['outlet'].brand)

    def _uses_mp78_management_ac(self):
        return self._is_mp78_brand() and mpr_calc.ENABLE_MP78_MANAGEMENT_AC

    def _get_grab_management_commission_rate(self):
        if self._is_mpr_brand():
            return mpr_calc.MPR_GRAB_MANAGEMENT_COMMISSION_RATE

        return mpr_calc.MANAGEMENT_COMMISSION_RATE

    def _get_grab_management_commission_expense(self, grab_net_total):
        if self._is_mpr_brand():
            return grab_net_total - (self._get_direct_mpr_display_value('Grab_Net') or 0)

        return grab_net_total * self._get_grab_management_commission_rate()

    def _get_grab_management_commission_label(self):
        if self._is_mpr_brand():
            return 'Grab Manag MPR'

        return 'Grab Manag 1%'

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

    def _get_direct_mpr_display_value(self, header, date=None):
        totals = self.data.get('grand_totals', {})
        if date is not None:
            totals = self.data.get('daily_totals', {}).get(date, {})

        ac_value = mpr_calc.mpr_ac_value_for_header(totals, header)
        if ac_value is not None:
            return ac_value

        return self._get_report_value_with_fallback(self.data, header, date)

    def _get_tiktok_closing_display_value(self, report_type, date=None):
        report_data = self.data.get('mpr_report_data') if report_type == 'mpr' else self.data
        if not report_data:
            return None

        totals = report_data.get('grand_totals', {})
        if date is not None:
            totals = report_data.get('daily_totals', {}).get(date, {})

        closing_net = totals.get(self.TIKTOK_CLOSING_NET_HEADER, 0)
        closing_totals = {self.TIKTOK_NET_HEADER: closing_net}

        if report_type == 'mpr' or (report_type == 'main' and self._is_mpr_brand()):
            return mpr_calc.tiktok_net_ac_value(closing_totals, is_mpr=True)

        if self._uses_mp78_management_ac():
            return mpr_calc.mp78_ac_value_for_header(closing_totals, self.TIKTOK_NET_HEADER)

        return closing_net

    def _get_qpon_closing_display_value(self, report_type, date=None):
        report_data = self.data.get('mpr_report_data') if report_type == 'mpr' else self.data
        if not report_data:
            return None

        totals = report_data.get('grand_totals', {})
        if date is not None:
            totals = report_data.get('daily_totals', {}).get(date, {})

        return totals.get(self.QPON_CLOSING_NET_HEADER, 0)

    def _get_closing_grand_total_income_value(self, header, grab_net_total=None):
        if header == 'Grab_Net':
            if grab_net_total is not None:
                return grab_net_total
            return self._get_grand_total_with_fallback(header)

        return self._get_platform_grand_total_with_fallback('main', header)

    def _get_closing_grand_total_income_contribution(self, report_type, header, grab_net_total=None):
        if self._is_closing_platform_disabled(header, report_type):
            return 0

        if report_type == 'mpr':
            if header == self.TIKTOK_NET_HEADER:
                return self._get_tiktok_closing_display_value('mpr') or 0
            return self._get_mpr_display_value(header) or 0

        return self._get_closing_grand_total_income_value(header, grab_net_total) or 0

    def _is_closing_platform_disabled(self, header, report_type='main'):
        platform = platform_for_header(header)
        if not platform:
            return False

        outlet = self._get_outlet_for_report_type(report_type)
        return is_platform_disabled(outlet, platform)

    def _get_outlet_for_report_type(self, report_type):
        if report_type == 'mpr':
            mpr_report_data = self.data.get('mpr_report_data')
            if mpr_report_data:
                return mpr_report_data.get('outlet')
            return self._get_mapped_mpr_outlet()

        return self.data.get('outlet')

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

        # Apply borders to the rekening table separately.
        if self.rekening_col_start and self.rekening_col_end and self.rekening_row and self.rekening_row_end:
            for row in self.ws.iter_rows(
                min_row=self.rekening_row,
                max_row=self.rekening_row_end,
                min_col=self.rekening_col_start,
                max_col=self.rekening_col_end,
            ):
                for cell in row:
                    cell.border = THIN_BORDER
