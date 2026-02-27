from app.services.excel_export.base_sheet import BaseSheet
from app.services.excel_export.utils.excel_utils import (
    HEADER_FONT, BOLD_RED_FONT, YELLOW_FILL, GRAB_FILL, CASH_FILL, SHOPEE_FILL,
    COMMISSION_FILL, auto_fit_columns
)
from datetime import datetime
import re

class SummarySheet(BaseSheet):
    def __init__(self, workbook, data):
        super().__init__(workbook, 'Summary', data)

    def generate(self):
        self._write_title()
        self._write_online_platform_summary()
        self._write_income_expense_summary()
        self._write_grand_total_net_income()
        if self.data['outlet'].brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan", "Martabak 999 Asli Bandung", "Martabak Surya Kencana", "Martabak Akim"]:
            self._write_commission_summary()
        auto_fit_columns(self.ws)

    def _write_title(self):
        self.ws['A1'] = 'Summary Report'
        self.ws['A2'] = 'Period:'
        self.ws['B2'] = f"{self.data['start_date'].strftime('%Y-%m-%d')} to {self.data['end_date'].strftime('%Y-%m-%d')}"
        self.ws['A3'] = 'Outlet:'
        self.ws['B3'] = self.data['outlet'].outlet_name_gojek

    def _write_online_platform_summary(self):
        current_row = 5
        summary_title_cell = self.ws.cell(row=current_row, column=1, value='Online Platform Summary')
        summary_title_cell.font = HEADER_FONT
        summary_title_cell.fill = GRAB_FILL
        current_row += 1

        platform_headers = ['Platform', 'Gross', 'Net', 'Difference']
        for col, header in enumerate(platform_headers, 1):
            cell = self.ws.cell(row=current_row, column=col)
            cell.value = header
            cell.font = HEADER_FONT
            cell.fill = GRAB_FILL
        current_row += 1

        grand_totals = self.data['grand_totals']
        grabfood_gross_total = self.data['grabfood_gross_total']
        grabovo_gross_total = self.data['grabovo_gross_total']
        grabfood_net_total = self.data['grabfood_net_total']
        grabovo_net_total = self.data['grabovo_net_total']

        platforms = [
            ('Gojek', grand_totals['Gojek_Gross'], grand_totals['Gojek_Net']),
            ('Grab (Total)', grabfood_gross_total + grabovo_gross_total, grabfood_net_total + grabovo_net_total),
            ('Shopee', grand_totals['Shopee_Gross'], grand_totals['Shopee_Net']),
            ('ShopeePay', grand_totals['ShopeePay_Gross'], grand_totals['ShopeePay_Net']),
            ('Tiktok', grand_totals['Tiktok_Gross'], grand_totals['Tiktok_Net']),
            ('Qpon', grand_totals['Qpon_Gross'], grand_totals['Qpon_Net']),
            ('UV', 0, grand_totals['UV']),
        ]

        for platform, gross, net in platforms:
            difference = gross - net if net is not None else 0
            row_data = [platform, gross, net, difference]
            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=current_row, column=col, value=value)
                cell.fill = GRAB_FILL
                if col > 1:
                    cell.number_format = '#,##0'
            current_row += 1
        self.ws.cell(row=current_row, column=1, value='') # Add spacing

    def _write_income_expense_summary(self):
        current_row = self.ws.max_row + 2
        summary_title_cell = self.ws.cell(row=current_row, column=1, value='Income/Expense Summary')
        summary_title_cell.font = HEADER_FONT
        summary_title_cell.fill = CASH_FILL
        current_row += 1

        expense_headers = ['Category', 'Income', 'Expense', 'Net Total', 'Description', 'Date Range']
        for col, header in enumerate(expense_headers, 1):
            cell = self.ws.cell(row=current_row, column=col)
            cell.value = header
            cell.font = HEADER_FONT
            cell.fill = CASH_FILL
        current_row += 1

        grand_totals = self.data['grand_totals']
        manual_entries = self.data['manual_entries']

        cash_net_total = grand_totals['Cash_Income'] - grand_totals['Cash_Expense']
        cash_row = ['Cash', grand_totals['Cash_Income'], grand_totals['Cash_Expense'], cash_net_total, '', '']
        for col, value in enumerate(cash_row, 1):
            cell = self.ws.cell(row=current_row, column=col, value=value)
            cell.fill = CASH_FILL
            if 1 < col <= 4:
                cell.number_format = '#,##0'
        current_row += 1

        # Sort manual entries
        MONTH_MAP = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'Mei': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Des': 12
        }

        def parse_indonesian_date(description):
            match = re.search(r'(\d{1,2})[-\s]?([A-Za-z]+)', description)
            if match:
                day = int(match.group(1))
                month_str = match.group(2).capitalize()
                month = MONTH_MAP.get(month_str)
                if month:
                    year = datetime.now().year
                    return datetime(year, month, day)
            return datetime.min

        manual_entries_sorted = sorted(
            manual_entries,
            key=lambda x: parse_indonesian_date(x[0].description)
        )

        manual_income = 0
        manual_expense = 0
        for entry, income_cat, expense_cat in manual_entries_sorted:
            amount = float(entry.amount or 0)
            income_amount = amount if entry.entry_type == 'income' else 0
            expense_amount = amount if entry.entry_type == 'expense' else 0
            manual_income += income_amount
            manual_expense += expense_amount
            net_amount = income_amount - expense_amount
            category_name = (income_cat.name if income_cat else '') if entry.entry_type == 'income' else (expense_cat.name if expense_cat else '')

            row_data = [
                category_name, income_amount, expense_amount, net_amount,
                entry.description, f"{entry.start_date} - {entry.end_date}"
            ]
            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=current_row, column=col, value=value)
                cell.fill = CASH_FILL
                if 1 < col <= 4:
                    cell.number_format = '#,##0'
            current_row += 1

        total_net = (grand_totals['Cash_Income'] + manual_income) - (grand_totals['Cash_Expense'] + manual_expense)
        total_row = [
            'Total', grand_totals['Cash_Income'] + manual_income,
            grand_totals['Cash_Expense'] + manual_expense, total_net, '', ''
        ]
        for col, value in enumerate(total_row, 1):
            cell = self.ws.cell(row=current_row, column=col, value=value)
            cell.font = HEADER_FONT
            cell.fill = CASH_FILL
            if 1 < col <= 4:
                cell.number_format = '#,##0'

    def _write_grand_total_net_income(self):
        current_row = self.ws.max_row + 2
        grand_totals = self.data['grand_totals']
        manual_entries = self.data['manual_entries']
        outlet = self.data['outlet']
        grabfood_net_total = self.data['grabfood_net_total']

        manual_income = sum(float(entry.amount or 0) for entry, _, _ in manual_entries if entry.entry_type == 'income')
        manual_expense = sum(float(entry.amount or 0) for entry, _, _ in manual_entries if entry.entry_type == 'expense')

        online_net_total = (
            grand_totals['Gojek_Net'] + grand_totals['Grab_Net'] +
            grand_totals['Shopee_Net'] + grand_totals['ShopeePay_Net'] +
            grand_totals['Tiktok_Net'] + grand_totals['Qpon_Net'] + grand_totals['UV']
        )

        management_commission = 0
        if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan", "Martabak 999 Asli Bandung", "Martabak Surya Kencana", "Martabak Akim"]:
            management_commission = grabfood_net_total * 1/74

        online_net_after_commission = online_net_total - management_commission
        cash_manual_net_total = (grand_totals['Cash_Income'] + manual_income) - (grand_totals['Cash_Expense'] + manual_expense)

        summary_data = [('Online Platforms Net', online_net_total)]
        if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan", "Martabak 999 Asli Bandung", "Martabak Surya Kencana", "Martabak Akim"]:
            summary_data.extend([
                ('Grab Management', -management_commission),
                ('Online Net After Commission', online_net_after_commission)
            ])
        summary_data.extend([
            ('Cash & Manual Net', cash_manual_net_total),
            ('GRAND TOTAL NET INCOME', online_net_after_commission + cash_manual_net_total)
        ])

        for label, amount in summary_data:
            cell = self.ws.cell(row=current_row, column=1, value=label)
            amount_cell = self.ws.cell(row=current_row, column=2, value=amount)
            cell.fill = SHOPEE_FILL
            amount_cell.fill = SHOPEE_FILL
            amount_cell.number_format = '#,##0'
            if label == 'GRAND TOTAL NET INCOME':
                cell.font = HEADER_FONT
                amount_cell.font = HEADER_FONT
                cell.fill = YELLOW_FILL
                amount_cell.fill = YELLOW_FILL
            
            current_row += 1

    def _write_commission_summary(self):
        current_row = self.ws.max_row + 2
        summary_title_cell = self.ws.cell(row=current_row, column=1, value='Commission Summary')
        summary_title_cell.font = HEADER_FONT
        summary_title_cell.fill = COMMISSION_FILL
        current_row += 1

        commission_headers = ['Category', 'Rate', 'Commission']
        for col, header in enumerate(commission_headers, 1):
            cell = self.ws.cell(row=current_row, column=col, value=header)
            cell.font = HEADER_FONT
            cell.fill = COMMISSION_FILL
        current_row += 1

        grabfood_net_total = self.data['grabfood_net_total']
        grand_totals = self.data['grand_totals']
        management_commission = grabfood_net_total * 1/74
        partner_commission = grabfood_net_total * 1/74
        tiktok_commission = grand_totals['Tiktok_Net'] * 1/74

        commission_data = [
            ('Management Commission (GrabFood)', '1%', management_commission),
            ('Partner Commission (GrabFood)', '1%', partner_commission),
            ('Management Commission (TikTok)', '1%', tiktok_commission),
        ]

        for category, rate, commission in commission_data:
            row_data = [category, rate, commission]
            for col, value in enumerate(row_data, 1):
                cell = self.ws.cell(row=current_row, column=col, value=value)
                cell.fill = COMMISSION_FILL
                if col == 3:
                    cell.number_format = '#,##0'
            current_row += 1
