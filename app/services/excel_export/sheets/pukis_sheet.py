from collections import defaultdict
from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, CENTER_ALIGN, RIGHT_ALIGN,
    PatternFill, auto_fit_columns
)

class PukisSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Pukis Inventory', data)

    def generate(self):
        self._write_title()
        self._write_headers()
        self._write_data()
        self._write_totals()
        auto_fit_columns(self.ws)

    def _write_title(self):
        self.ws['A1'] = 'Pukis Daily Inventory'
        self.ws['A2'] = 'Period:'
        self.ws['B2'] = f"{self.data['start_date'].strftime('%Y-%m-%d')} to {self.data['end_date'].strftime('%Y-%m-%d')}"
        self.ws['A3'] = 'Outlet:'
        self.ws['B3'] = self.data['outlet'].outlet_name_gojek

    def _write_headers(self):
        headers = ['Date', 'Inventory Type', 'Product Type', 'Amount']
        header_fill = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')
        for col, header in enumerate(headers, 1):
            cell = self.ws.cell(row=5, column=col, value=header)
            cell.font = HEADER_FONT
            cell.fill = header_fill
            cell.alignment = CENTER_ALIGN

    def _write_data(self):
        pukis_reports = self.data['pukis_reports']
        pukis_by_date_type = defaultdict(lambda: defaultdict(dict))
        for report in pukis_reports:
            date_str = report.tanggal.strftime('%Y-%m-%d')
            product_type = report.pukis_product_type
            pukis_by_date_type[date_str][product_type][report.pukis_inventory_type] = float(report.amount or 0)

        current_row = 6
        data_fill = PatternFill(start_color='FFFFFF', end_color='FFFFFF', fill_type='solid')
        sisa_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')

        for date_str in sorted(pukis_by_date_type.keys()):
            for product_type in ['jumbo', 'klasik']:
                daily = pukis_by_date_type[date_str][product_type]
                for inv_type in ['produksi', 'terjual', 'retur', 'free']:
                    amount = daily.get(inv_type, 0)
                    self._write_row(current_row, date_str, inv_type.capitalize(), product_type.capitalize(), amount, data_fill)
                    current_row += 1

                # Calculate and write "sisa" row
                produksi = daily.get('produksi', 0)
                terjual = daily.get('terjual', 0)
                retur = daily.get('retur', 0)
                free = daily.get('free', 0)
                sisa = produksi - (terjual + retur + free)
                self._write_row(current_row, date_str, 'Sisa', product_type.capitalize(), sisa, sisa_fill)
                current_row += 1

    def _write_row(self, row, date_str, inv_type, prod_type, amount, fill):
        cell_date = self.ws.cell(row=row, column=1, value=date_str)
        cell_date.number_format = 'yyyy-mm-dd'
        cell_date.alignment = CENTER_ALIGN

        cell_type = self.ws.cell(row=row, column=2, value=inv_type)
        cell_type.alignment = CENTER_ALIGN

        cell_prod = self.ws.cell(row=row, column=3, value=prod_type)
        cell_prod.alignment = CENTER_ALIGN

        cell_amt = self.ws.cell(row=row, column=4, value=amount)
        cell_amt.number_format = '#,##0'
        cell_amt.alignment = RIGHT_ALIGN

        for col in range(1, 5):
            self.ws.cell(row=row, column=col).fill = fill

    def _write_totals(self):
        pukis_reports = self.data['pukis_reports']
        totals = {
            'jumbo': {'produksi': 0, 'terjual': 0, 'retur': 0, 'free': 0},
            'klasik': {'produksi': 0, 'terjual': 0, 'retur': 0, 'free': 0}
        }
        for report in pukis_reports:
            ptype = report.pukis_product_type
            itype = report.pukis_inventory_type
            if ptype in totals and itype in totals[ptype]:
                totals[ptype][itype] += float(report.amount or 0)

        summary_start_row = self.ws.max_row + 2
        for product_type in ['jumbo', 'klasik']:
            for inv_type in ['produksi', 'terjual', 'retur', 'free']:
                self.ws.cell(row=summary_start_row, column=1, value='TOTAL')
                self.ws.cell(row=summary_start_row, column=2, value=inv_type.capitalize())
                self.ws.cell(row=summary_start_row, column=3, value=product_type.capitalize())
                self.ws.cell(row=summary_start_row, column=4, value=totals[product_type][inv_type])
                summary_start_row += 1