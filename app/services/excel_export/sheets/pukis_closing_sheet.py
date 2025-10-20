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

        header1 = ["TANGGAL", "PENERIMAAN", None, None, None, None, None,None, "PUKIS JUMBO TERJUAL", "PUKIS KLASIK TERJUAL", "PUKIS FREE", "PUKIS SISA", "TGL TF REK BARU", "NOMINAL TF", "SELISIH", "KETERANGAN", "NOTE"]
        self.ws.append(header1)
        self.ws.merge_cells(start_row=4, start_column=2, end_row=4, end_column=8)

        header2 = [None, "CASH", "GOJEK", "GRAB", "SHOPEE FOOD", "SHOPEE PAY", "QRIS", "TRF"]
        self.ws.append(header2)
        for cell in self.ws[3]:
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = YELLOW_FILL

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
        
        # Group pukis reports by date
        pukis_by_date = {}
        for report in pukis_reports:
            date = report.tanggal.date()
            if date not in pukis_by_date:
                pukis_by_date[date] = []
            pukis_by_date[date].append(report)

        for date in all_dates:
            date_reports = pukis_by_date.get(date, [])
            
            # Calculate totals for this date
            jumbo_terjual = sum(float(r.amount or 0) for r in date_reports 
                              if r.pukis_inventory_type == 'terjual' and r.pukis_product_type == 'jumbo')
            reguler_terjual = sum(float(r.amount or 0) for r in date_reports 
                                if r.pukis_inventory_type == 'terjual' and r.pukis_product_type == 'klasik')
            free = sum(float(r.amount or 0) for r in date_reports 
                      if r.pukis_inventory_type == 'free')
            produksi = sum(float(r.amount or 0) for r in date_reports 
                         if r.pukis_inventory_type == 'produksi')
            retur = sum(float(r.amount or 0) for r in date_reports 
                       if r.pukis_inventory_type == 'retur')
            
            # Calculate sisa
            sisa = produksi - (jumbo_terjual + reguler_terjual + retur + free)

            row_data = [
                date.strftime("%d-%b-%y"),
                daily_totals[date]['Cash_Income'] - daily_totals[date]['Cash_Expense'],
                daily_totals[date]['Gojek_Net'],
                daily_totals[date]['Grab_Net'],
                daily_totals[date]['Shopee_Net'],
                daily_totals[date]['ShopeePay_Net'],
                0,  # QRIS
                0,  # TRF
                jumbo_terjual,
                reguler_terjual,
                free,
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
        
        # Calculate grand totals for pukis
        total_jumbo_terjual = sum(float(r.amount or 0) for r in pukis_reports 
                                 if r.pukis_inventory_type == 'terjual' and r.pukis_product_type == 'jumbo')
        total_reguler_terjual = sum(float(r.amount or 0) for r in pukis_reports 
                                   if r.pukis_inventory_type == 'terjual' and r.pukis_product_type == 'klasik')
        total_free = sum(float(r.amount or 0) for r in pukis_reports 
                        if r.pukis_inventory_type == 'free')
        total_produksi = sum(float(r.amount or 0) for r in pukis_reports 
                            if r.pukis_inventory_type == 'produksi')
        total_retur = sum(float(r.amount or 0) for r in pukis_reports 
                         if r.pukis_inventory_type == 'retur')
        
        total_sisa = total_produksi - (total_jumbo_terjual + total_reguler_terjual + total_retur + total_free)

        total_row = [
            "TOTAL",
            grand_totals.get('Cash_Income', 0) - grand_totals.get('Cash_Expense', 0),
            grand_totals.get('Gojek_Net', 0),
            grand_totals.get('Grab_Net', 0),
            grand_totals.get('Shopee_Net', 0),
            grand_totals.get('ShopeePay_Net', 0),
            0,
            0,
            total_jumbo_terjual,
            total_reguler_terjual,
            total_free,
            total_sisa,
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
