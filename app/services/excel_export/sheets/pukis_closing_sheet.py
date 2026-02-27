from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, YELLOW_FILL, CENTER_ALIGN, THIN_BORDER, auto_fit_columns, RIGHT_ALIGN,
    CASH_FILL, GOJEK_FILL, GRAB_FILL, SHOPEE_FILL, TIKTOK_FILL, DATA_FILL, LIGHT_BLUE_FILL
)
from datetime import timedelta

class PukisClosingSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Pukis Closing Sheet', data)

    def generate(self):
        self._write_header()
        self._write_data()
        self._write_expenses_table()
        self._apply_styles()
        auto_fit_columns(self.ws)

    def _write_header(self):
        outlet = self.data['outlet']
        start_date = self.data['start_date']
        end_date = self.data['end_date']

        self.ws['A1'] = outlet.outlet_name_gojek
        self.ws.merge_cells('A1:R1')
        self.ws['A1'].alignment = CENTER_ALIGN
        self.ws['A1'].font = HEADER_FONT

        self.ws['A2'] = f"PERIODE {start_date.strftime('%d %b %Y')} - {end_date.strftime('%d %b %Y')}"
        self.ws.merge_cells('A2:R2')
        self.ws['A2'].alignment = CENTER_ALIGN
        self.ws['A2'].font = HEADER_FONT

        header1 = ["TANGGAL", "PENERIMAAN", None, None, None, None, None, None, None, "PUKIS JUMBO TERJUAL", "PUKIS KLASIK TERJUAL", "PUKIS FREE", "PUKIS SISA", "TGL TF REK BARU", "NOMINAL TF", "SELISIH", "KETERANGAN", "NOTE"]
        self.ws.append(header1)
        self.ws.merge_cells(start_row=4, start_column=2, end_row=4, end_column=9)

        header2 = [None, "CASH", "GOJEK", "GRAB", "SHOPEE FOOD", "TIKTOK", "QPON", "WEBSHOP", "TRF"]
        self.ws.append(header2)
        header2_row = self.ws.max_row
        for cell in self.ws[3]:
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = YELLOW_FILL

        header2_fills = {
            "CASH": CASH_FILL,
            "GOJEK": GOJEK_FILL,
            "GRAB": GRAB_FILL,
            "SHOPEE FOOD": SHOPEE_FILL,
            "TIKTOK": TIKTOK_FILL,
            "QPON": TIKTOK_FILL,
            "WEBSHOP": TIKTOK_FILL,
        }

        for col in range(1, len(header2) + 1):
            cell = self.ws.cell(row=header2_row, column=col)
            cell.font = HEADER_FONT
            cell.alignment = CENTER_ALIGN
            cell.fill = header2_fills.get(cell.value, DATA_FILL)

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

        self.daily_rows_start = self.ws.max_row + 1
        for date in all_dates:
            date_reports = pukis_by_date.get(date, [])
            totals = daily_totals.get(date, {})
            gojek_value = totals.get('Gojek_Mutation') or totals.get('Gojek_Net', 0)
            
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
                gojek_value,
                daily_totals[date]['Grab_Net'],
                daily_totals[date]['Shopee_Net'],
                daily_totals[date]['Tiktok_Net'],
                daily_totals[date]['Qpon_Net'],  # QPON
                daily_totals[date].get('Webshop_Net', 0),  # WEBSHOP
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
        self.daily_rows_end = self.ws.max_row

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
            grand_totals.get('Gojek_Mutation') or grand_totals.get('Gojek_Net', 0),
            grand_totals.get('Grab_Net', 0),
            grand_totals.get('Shopee_Net', 0),
            grand_totals.get('Tiktok_Net', 0),
            grand_totals.get('Qpon_Net', 0),
            grand_totals.get('Webshop_Net', 0),
            0,
            total_jumbo_terjual,
            total_reguler_terjual,
            total_free,
            total_sisa,
            None, None, None, None, None
        ]
        self.ws.append(total_row)

        for i in range(1, 10):
            self.ws.cell(row=self.ws.max_row, column=i).font = HEADER_FONT
        
        # self.ws.append(total_row)
        # self.ws.append(["GRAND TOTAL", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None])

    def _write_expenses_table(self):
        manual_entries = self.data.get('manual_entries', [])

        # Aggregate expenses by category
        aggregated_expenses = {}
        for entry, income_cat, expense_cat in manual_entries:
            if entry.entry_type == 'expense' and entry.category_id != 9:
                category_name = expense_cat.name if expense_cat else 'Uncategorized'
                if category_name not in aggregated_expenses:
                    aggregated_expenses[category_name] = 0
                aggregated_expenses[category_name] += entry.amount

        if not aggregated_expenses:
            return

        # Add a blank row for spacing
        self.ws.append([])
        start_row = self.ws.max_row + 3

        # Header
        # Merging A and B for the main title, then C for the value title
        header_a = self.ws.cell(row=start_row, column=1, value='PENGELUARAN')
        header_a.font = HEADER_FONT
        header_a.fill = YELLOW_FILL
        header_a.alignment = CENTER_ALIGN

        header_b = self.ws.cell(row=start_row, column=2)
        header_b.fill = YELLOW_FILL # Also fill the second cell to make the merge seamless

        self.ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=2)

        # header_c = self.ws.cell(row=start_row, column=3, value='TOTAL')
        # header_c.font = HEADER_FONT
        # header_c.fill = YELLOW_FILL
        # header_c.alignment = CENTER_ALIGN
        

        # Sub-header for categories
        sub_header_row = start_row + 1
        keterangan_header = self.ws.cell(row=sub_header_row, column=2, value='KETERANGAN')
        keterangan_header.font = HEADER_FONT
        keterangan_header.fill = YELLOW_FILL
        keterangan_header.alignment = CENTER_ALIGN

        # Data
        current_row = sub_header_row + 1
        total_expenses = 0

        # Manually define the order of categories
        category_order = [
            "Adm kantor", "Adm gudang", "Gaji karyawan", "Social media",
            "Tagihan gudang", "Seragam", "Tutup loyang", "Sewa"
        ]

        # Create a list of tuples for sorting
        expenses_to_sort = list(aggregated_expenses.items())

        sorted_expenses = sorted(expenses_to_sort, key=lambda item: category_order.index(item[0]) if item[0] in category_order else len(category_order))

        for category, amount in sorted_expenses:
            self.ws.cell(row=current_row, column=2).value = category
            amount_cell = self.ws.cell(row=current_row, column=3)
            amount_cell.value = amount
            amount_cell.number_format = '#,##0'
            amount_cell.alignment = RIGHT_ALIGN
            total_expenses += amount
            current_row += 1

        # Footer
        footer_row_val = 'TOTAL'
        footer_cell_a = self.ws.cell(row=current_row, column=1, value=footer_row_val)
        footer_cell_a.font = HEADER_FONT
        # footer_cell_a.fill = LIGHT_BLUE_FILL
        footer_cell_b = self.ws.cell(row=current_row, column=2) # Empty cell for merging
        # footer_cell_b.fill = LIGHT_BLUE_FILL

        self.ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=2)

        total_cell = self.ws.cell(row=current_row, column=3)
        total_cell.value = total_expenses
        total_cell.number_format = '#,##0'
        total_cell.alignment = RIGHT_ALIGN
        total_cell.font = HEADER_FONT
        # total_cell.fill = LIGHT_BLUE_FILL

        # Apply borders to the new table
        for row in self.ws.iter_rows(min_row=start_row, max_row=current_row, min_col=1, max_col=3):
            for cell in row:
                cell.border = THIN_BORDER

    def _apply_styles(self):
        for row in self.ws.iter_rows(min_row=3, max_row=self.ws.max_row, min_col=1, max_col=self.ws.max_column):
            for cell in row:
                cell.border = THIN_BORDER
                if hasattr(self, "daily_rows_start") and hasattr(self, "daily_rows_end"):
                    if cell.row == self.daily_rows_end + 1:
                        cell.fill = LIGHT_BLUE_FILL

                if cell.row in (3, 4):
                    if cell.value is not None:
                        cell.alignment = CENTER_ALIGN
                    continue

                if cell.column == 1:
                    if cell.value is not None:
                        cell.alignment = CENTER_ALIGN
                    continue

                if isinstance(cell.value, (int, float)):
                    cell.number_format = '#,##0'
                    cell.alignment = RIGHT_ALIGN
                elif cell.value is not None:
                    cell.alignment = CENTER_ALIGN
