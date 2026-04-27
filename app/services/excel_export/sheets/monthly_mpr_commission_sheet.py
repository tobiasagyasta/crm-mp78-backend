import calendar

from openpyxl.styles import Alignment, Font, NamedStyle, PatternFill
from openpyxl.utils import get_column_letter

from app.services.excel_export.base_sheet import BaseSheet


class MonthlyMprCommissionSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, "Monthly MPR Commission", data)

    def generate(self):
        self._prepare_styles()
        self._create_header()
        self._populate_data()
        self._set_column_widths()
        self.ws.freeze_panes = "B3"

    def _prepare_styles(self):
        self.header_font = Font(bold=True)
        self.center_align = Alignment(horizontal="center", vertical="center")
        self.header_fill = PatternFill(
            start_color="D9EAD3",
            end_color="D9EAD3",
            fill_type="solid",
        )
        self.commission_fill = PatternFill(
            start_color="FFF2CC",
            end_color="FFF2CC",
            fill_type="solid",
        )
        self.currency_style = NamedStyle(
            name="mpr_currency_style",
            number_format="#,##0.00",
        )
        if "mpr_currency_style" not in self.wb.style_names:
            self.wb.add_named_style(self.currency_style)

    def _periods(self):
        if isinstance(self.data, dict) and "periods" in self.data:
            return self.data["periods"]
        return list(range(1, 13))

    def _outlets(self):
        if isinstance(self.data, dict) and "outlets" in self.data:
            return self.data.get("outlets", {})
        return self.data

    def _period_label(self, period):
        if isinstance(period, tuple):
            year, month = period
            return f"{calendar.month_name[month]} {year}"
        return calendar.month_name[period]

    def _create_header(self):
        periods = self._periods()

        self.ws.merge_cells(start_row=1, start_column=1, end_row=2, end_column=1)
        self.ws["A1"] = "Outlet"

        current_col = 2
        for period in periods:
            self.ws.merge_cells(
                start_row=1,
                start_column=current_col,
                end_row=1,
                end_column=current_col + 1,
            )
            self.ws.cell(row=1, column=current_col, value=self._period_label(period))
            self.ws.cell(row=2, column=current_col, value="Net Total")
            self.ws.cell(row=2, column=current_col + 1, value="Commission")
            current_col += 2

        self.ws.merge_cells(
            start_row=1,
            start_column=current_col,
            end_row=2,
            end_column=current_col,
        )
        self.ws.cell(row=1, column=current_col, value="Total Commission")

        for row in self.ws.iter_rows(min_row=1, max_row=2, min_col=1, max_col=current_col):
            for cell in row:
                cell.font = self.header_font
                cell.alignment = self.center_align
                cell.fill = self.header_fill

        for commission_col in range(4, current_col + 1, 2):
            self.ws.cell(row=2, column=commission_col).fill = self.commission_fill
        self.ws.cell(row=1, column=current_col).fill = self.commission_fill

    def _populate_data(self):
        periods = self._periods()
        outlets = self._outlets()
        net_totals = {period: 0 for period in periods}
        commission_totals = {period: 0 for period in periods}
        grand_total_commission = 0
        current_row = 3

        for _, outlet_data in outlets.items():
            row_values = [outlet_data["name"]]
            total_commission = outlet_data.get("total_commission", 0)

            for period in periods:
                period_totals = outlet_data["monthly_totals"].get(
                    period,
                    {"net_total": 0, "commission_total": 0},
                )
                net_total = period_totals.get("net_total", 0)
                commission_total = period_totals.get("commission_total", 0)
                net_totals[period] += net_total
                commission_totals[period] += commission_total
                row_values.extend([net_total, commission_total])

            row_values.append(total_commission)
            self.ws.append(row_values)

            for col_num in range(2, len(row_values) + 1):
                self.ws.cell(row=current_row, column=col_num).style = "mpr_currency_style"
            for commission_col in range(3, len(row_values) + 1, 2):
                self.ws.cell(row=current_row, column=commission_col).fill = self.commission_fill
            self.ws.cell(row=current_row, column=len(row_values)).fill = self.commission_fill
            current_row += 1
            grand_total_commission += total_commission

        total_row = ["Grand Total"]
        for period in periods:
            total_row.extend([net_totals[period], commission_totals[period]])
        total_row.append(grand_total_commission)
        self.ws.append(total_row)

        total_row_index = self.ws.max_row
        for col_num in range(1, len(total_row) + 1):
            cell = self.ws.cell(row=total_row_index, column=col_num)
            cell.font = self.header_font
            if col_num == 1:
                cell.alignment = self.center_align
            if col_num >= 2:
                cell.style = "mpr_currency_style"
            cell.fill = self.header_fill

        for commission_col in range(3, len(total_row) + 1, 2):
            self.ws.cell(row=total_row_index, column=commission_col).fill = self.commission_fill
        self.ws.cell(row=total_row_index, column=len(total_row)).fill = self.commission_fill

    def _set_column_widths(self):
        self.ws.column_dimensions[get_column_letter(1)].width = 35

        last_column = self.ws.max_column
        for col_num in range(2, last_column + 1):
            self.ws.column_dimensions[get_column_letter(col_num)].width = 16
