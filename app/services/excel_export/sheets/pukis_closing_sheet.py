from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, THIN_BORDER, auto_fit_columns, RIGHT_ALIGN
)
from datetime import timedelta

class PukisClosingSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Pukis Closing Sheet', data)

    def generate(self):
        self._write_header()
        self._write_data()
        self._apply_styles()
        auto_fit_columns(self.ws)

    def _write_header(self):
        outlet = self.data['outlet']
        start_date = self.data['start_date']
        end_date = self.data['end_date']

        self.ws['A1'] = outlet.outlet_name_gojek
        self.ws.merge_cells('A1:Q1')
        self.ws['A1'].alignment = CENTER_ALIGN
        self.ws['A1'].font = HEADER_FONT

        self.ws['A2'] = f"PERIODE {start_date.strftime('%d %b %Y')} - {end_date.strftime('%d %b %Y')}"
        self.ws.merge_cells('A2:Q2')
        self.ws['A2'].alignment = CENTER_ALIGN
        self.ws['A2'].font = HEADER_FONT

        header1 = ["TANGGAL", "PENERIMAAN", None, None, None, None, None, "PUKIS JUMBO TERJUAL", "PUKIS KLASIK TERJUAL", "PUKIS FREE", "PUKIS SISA", "TGL TF REK BARU", "NOMINAL TF", "SELISIH", "KETERANGAN", "NOTE"]
        self.ws.append(header1)
        self.ws.merge_cells(start_row=4, start_column=2, end_row=4, end_column=8)

        header2 = [None, "CASH", "GOJEK", "GRAB", "SHOPEE FOOD", "SHOPEE PAY", "QRIS", "TRF"]
        self.ws.append(header2)

        for cell in self.ws[4]:
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = YELLOW_FILL

        for cell in self.ws[5]:
            if cell.value:
                cell.font = HEADER_FONT
                cell.alignment = CENTER_ALIGN
                cell.fill = YELLOW_FILL

    def _write_data(self):
        all_dates = self.data['all_dates']
        daily_totals = self.data['daily_totals']
        pukis_reports = self.data['pukis_reports']
        pukis_by_date = {p.tanggal.date(): p for p in pukis_reports}

        for date in all_dates:
            pukis_data = pukis_by_date.get(date)
            sisa = 0
            if pukis_data:
                terjual = (pukis_data.jumbo_terjual or 0) + (pukis_data.reguler_terjual or 0)
                sisa = (pukis_data.produksi or 0) - (terjual + (pukis_data.retur or 0) + (pukis_data.free or 0))

            row_data = [
                date.strftime("%d-%b-%y"),
                daily_totals[date]['Cash_Income'],
                daily_totals[date]['Gojek_Net'],
                daily_totals[date]['Grab_Net'],
                daily_totals[date]['Shopee_Net'],
                daily_totals[date]['ShopeePay_Net'],
                0,  # QRIS
                0,  # TRF
                pukis_data.jumbo_terjual if pukis_data else 0,
                pukis_data.reguler_terjual if pukis_data else 0,
                pukis_data.free if pukis_data else 0,
                sisa,
                None,
                None,
                None,
                None,
                None
            ]
            self.ws.append(row_data)

        # Add total row
        grand_totals = self.data['grand_totals']
        total_pukis_jumbo = sum(p.jumbo_terjual for p in pukis_reports if p.jumbo_terjual)
        total_pukis_klasik = sum(p.reguler_terjual for p in pukis_reports if p.reguler_terjual)
        total_pukis_free = sum(p.free for p in pukis_reports if p.free)
        total_pukis_sisa = sum((p.produksi or 0) - ((p.jumbo_terjual or 0) + (p.reguler_terjual or 0) + (p.retur or 0) + (p.free or 0)) for p in pukis_reports)

        total_row = [
            "TOTAL",
            grand_totals.get('Cash_Income', 0),
            grand_totals.get('Gojek_Net', 0),
            grand_totals.get('Grab_Net', 0),
            grand_totals.get('Shopee_Net', 0),
            grand_totals.get('ShopeePay_Net', 0),
            0,
            0,
            total_pukis_jumbo,
            total_pukis_klasik,
            total_pukis_free,
            total_pukis_sisa,
            None, None, None, None, None
        ]
        self.ws.append(total_row)
        self.ws.append(["GRAND TOTAL", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None])

    def _apply_styles(self):
        for row in self.ws.iter_rows(min_row=4, max_row=self.ws.max_row, min_col=1, max_col=self.ws.max_column):
            for cell in row:
                cell.border = THIN_BORDER
                if cell.column > 1:
                    cell.number_format = '#,##0'
                    cell.alignment = RIGHT_ALIGN
