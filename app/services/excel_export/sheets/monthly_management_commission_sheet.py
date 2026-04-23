import calendar

from openpyxl.styles import Alignment, Font, NamedStyle, PatternFill
from openpyxl.utils import get_column_letter

from app.services.excel_export.base_sheet import BaseSheet


class MonthlyManagementCommissionSheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, "Monthly Management Commission", data)

    def generate(self):
        self._prepare_styles()
        self._create_header()
        self._populate_data()
        self._set_column_widths()
        self.ws.freeze_panes = "C3"

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
        self.net_after_fill = PatternFill(
            start_color="DDEBF7",
            end_color="DDEBF7",
            fill_type="solid",
        )
        self.currency_style = NamedStyle(
            name="management_commission_currency_style",
            number_format="#,##0.00",
        )
        if "management_commission_currency_style" not in self.wb.style_names:
            self.wb.add_named_style(self.currency_style)

    def _periods(self):
        return self.data.get("periods", list(range(1, 13)))

    def _outlets(self):
        return self.data.get("outlets", {})

    def _period_label(self, period):
        if isinstance(period, tuple):
            year, month = period
            return f"{calendar.month_name[month]} {year}"
        return calendar.month_name[period]

    def _create_header(self):
        periods = self._periods()

        self.ws.merge_cells(start_row=1, start_column=1, end_row=2, end_column=1)
        self.ws["A1"] = "Outlet"

        self.ws.merge_cells(start_row=1, start_column=2, end_row=2, end_column=2)
        self.ws["B1"] = "Closing Date"

        current_col = 3
        for period in periods:
            self.ws.merge_cells(
                start_row=1,
                start_column=current_col,
                end_row=1,
                end_column=current_col + 2,
            )
            self.ws.cell(row=1, column=current_col, value=self._period_label(period))
            self.ws.cell(row=2, column=current_col, value="Net Total")
            self.ws.cell(row=2, column=current_col + 1, value="Management Commission")
            self.ws.cell(row=2, column=current_col + 2, value="Net After Commission")
            current_col += 3

        self.ws.merge_cells(
            start_row=1,
            start_column=current_col,
            end_row=2,
            end_column=current_col,
        )
        self.ws.cell(row=1, column=current_col, value="Total After Commission")

        for row in self.ws.iter_rows(min_row=1, max_row=2, min_col=1, max_col=current_col):
            for cell in row:
                cell.font = self.header_font
                cell.alignment = self.center_align
                cell.fill = self.header_fill

        for commission_col in range(4, current_col, 3):
            self.ws.cell(row=2, column=commission_col).fill = self.commission_fill
        for net_after_col in range(5, current_col, 3):
            self.ws.cell(row=2, column=net_after_col).fill = self.net_after_fill
        self.ws.cell(row=1, column=current_col).fill = self.net_after_fill

    def _populate_data(self):
        periods = self._periods()
        outlets = self._outlets()
        net_totals = {period: 0 for period in periods}
        commission_totals = {period: 0 for period in periods}
        net_after_totals = {period: 0 for period in periods}
        grand_total_after_commission = 0
        current_row = 3

        for _, outlet_data in outlets.items():
            row_values = [
                outlet_data.get("name", ""),
                outlet_data.get("closing_day", "Calendar"),
            ]

            for period in periods:
                period_totals = outlet_data.get("monthly_totals", {}).get(
                    period,
                    {
                        "net_total": 0,
                        "commission_total": 0,
                        "net_after_commission": 0,
                    },
                )
                net_total = period_totals.get("net_total", 0)
                commission_total = period_totals.get("commission_total", 0)
                net_after_commission = period_totals.get("net_after_commission", 0)
                net_totals[period] += net_total
                commission_totals[period] += commission_total
                net_after_totals[period] += net_after_commission
                row_values.extend([net_total, commission_total, net_after_commission])

            total_after_commission = outlet_data.get("total_after_commission", 0)
            row_values.append(total_after_commission)
            self.ws.append(row_values)

            self.ws.cell(row=current_row, column=2).alignment = self.center_align
            for col_num in range(3, len(row_values) + 1):
                self.ws.cell(row=current_row, column=col_num).style = (
                    "management_commission_currency_style"
                )
            for commission_col in range(4, len(row_values), 3):
                self.ws.cell(row=current_row, column=commission_col).fill = self.commission_fill
            for net_after_col in range(5, len(row_values) + 1, 3):
                self.ws.cell(row=current_row, column=net_after_col).fill = self.net_after_fill
            self.ws.cell(row=current_row, column=len(row_values)).fill = self.net_after_fill

            current_row += 1
            grand_total_after_commission += total_after_commission

        total_row = ["Grand Total", ""]
        for period in periods:
            total_row.extend(
                [
                    net_totals[period],
                    commission_totals[period],
                    net_after_totals[period],
                ]
            )
        total_row.append(grand_total_after_commission)
        self.ws.append(total_row)

        total_row_index = self.ws.max_row
        for col_num in range(1, len(total_row) + 1):
            cell = self.ws.cell(row=total_row_index, column=col_num)
            cell.font = self.header_font
            if col_num <= 2:
                cell.alignment = self.center_align
            else:
                cell.style = "management_commission_currency_style"
            cell.fill = self.header_fill

        for commission_col in range(4, len(total_row), 3):
            self.ws.cell(row=total_row_index, column=commission_col).fill = self.commission_fill
        for net_after_col in range(5, len(total_row) + 1, 3):
            self.ws.cell(row=total_row_index, column=net_after_col).fill = self.net_after_fill
        self.ws.cell(row=total_row_index, column=len(total_row)).fill = self.net_after_fill

    def _set_column_widths(self):
        self.ws.column_dimensions[get_column_letter(1)].width = 35
        self.ws.column_dimensions[get_column_letter(2)].width = 15

        last_column = self.ws.max_column
        for col_num in range(3, last_column + 1):
            self.ws.column_dimensions[get_column_letter(col_num)].width = 18
