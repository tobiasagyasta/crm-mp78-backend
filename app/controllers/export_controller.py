from flask import Blueprint, request, jsonify, send_file
from flask_cors import cross_origin
from datetime import datetime
from io import BytesIO
from app.models.outlet import Outlet
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.models.shopeepay_reports import ShopeepayReport
from app.models.bank_mutations import BankMutation
from app.utils.transaction_matcher import TransactionMatcher
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.workbook.protection import WorkbookProtection  # Add this import at the top
import os

export_bp = Blueprint('export', __name__, url_prefix="/export")

@export_bp.route('', methods=['POST', 'OPTIONS'])
def export_reports():
    try:

          # Define yellow background for grand total
        yellow_fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')  # Yellow background
        if request.method == 'OPTIONS':
            response = jsonify({'status': 'OK'})
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
            response.headers.add('Access-Control-Allow-Methods', 'POST')
            return response, 200

        data = request.get_json()
        outlet_code = data.get('outlet_code')
        start_date = datetime.strptime(data.get('start_date'), '%Y-%m-%d')
        end_date = datetime.strptime(data.get('end_date'), '%Y-%m-%d')

        if not all([outlet_code, start_date, end_date]):
            return jsonify({"error": "Missing required parameters"}), 400

        # Get outlet
        outlet = Outlet.query.filter_by(outlet_code=outlet_code).first()
        if not outlet:
            return jsonify({"error": "Outlet not found"}), 404

        # Initialize daily_totals dictionary
        daily_totals = {}

        # Initialize dictionary structure
        def init_daily_total():
            return {
                'Gojek_Gross': 0, 'Gojek_Net': 0,
                'Grab_Gross': 0, 'Grab_Net': 0,
                'ShopeePay_Gross': 0, 'ShopeePay_Net': 0,
                'Shopee_Gross': 0, 'Shopee_Net': 0,
                'Cash_Income': 0, 'Cash_Expense': 0,
                'Gojek_Mutation': None, 'Gojek_Difference': 0,
                'Grab_Mutation': None, 'Grab_Difference': 0,
                'Shopee_Mutation': None, 'Shopee_Difference': 0
            }
        
        # Add title rows
        dataset = []
        dataset.append(['Sales Report', '', '', '', '', '', '', '', ''])
        dataset.append(['Period:', f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}', '', '', '', '', '', '', ''])
        dataset.append(['Outlet:', outlet.outlet_name_gojek, '', '', '', '', '', '', ''])
        dataset.append([]) # Empty row for spacing
        # Update the initial headers to include ShopeePay and mutation data
        dataset.append([
            'Date',
            'Gojek Net', 'Gojek Mutation', 'Gojek Difference',
            'Grab Net', 'Grab Mutation', 'Grab Difference',
            'Shopee Net', 'Shopee Mutation', 'Shopee Difference',
            'ShopeePay Net', 'Cash Income', 'Cash Expense'
        ])

        # Query reports with inclusive end date
        end_date_inclusive = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
        
        gojek_reports = GojekReport.query.filter(
            GojekReport.outlet_code == outlet_code,
            GojekReport.transaction_date >= start_date,
            GojekReport.transaction_date <= end_date_inclusive
        ).all()

        grab_reports = GrabFoodReport.query.filter(
            GrabFoodReport.outlet_code == outlet_code,
            GrabFoodReport.tanggal_dibuat >= start_date,
            GrabFoodReport.tanggal_dibuat <= end_date_inclusive
        ).all()

        shopee_reports = ShopeeReport.query.filter(
            ShopeeReport.outlet_code == outlet_code,
            ShopeeReport.order_create_time >= start_date,
            ShopeeReport.order_create_time <= end_date_inclusive
        ).all()

        shopeepay_reports = ShopeepayReport.query.filter(
            ShopeepayReport.outlet_code == outlet_code,
            ShopeepayReport.create_time >= start_date,
            ShopeepayReport.create_time <= end_date_inclusive
        ).all()

        # Query cash reports with separate income and expense queries
        cash_income_reports = CashReport.query.filter(
            CashReport.outlet_code == outlet_code,
            CashReport.type == 'income',
            CashReport.tanggal >= start_date,
            CashReport.tanggal <= end_date_inclusive
        ).all()
        
        cash_expense_reports = CashReport.query.filter(
            CashReport.outlet_code == outlet_code,
            CashReport.type == 'expense',
            CashReport.tanggal >= start_date,
            CashReport.tanggal <= end_date_inclusive
        ).all()

        # Aggregate all data by date
        for report in gojek_reports:
            date = report.transaction_date
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Gojek_Net'] += float(report.nett_amount or 0)
            daily_totals[date]['Gojek_Gross'] += float(report.amount or 0)

        # Initialize variables for GrabFood and GrabOVO
        grabfood_gross_total = 0
        grabovo_gross_total = 0
        grabfood_net_total = 0
        grabovo_net_total = 0

        for report in grab_reports:
            date = report.tanggal_dibuat.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Grab_Net'] += float(report.total or 0)
            daily_totals[date]['Grab_Gross'] += float(report.amount or 0)
            
            # Track separate totals for GrabOVO and GrabFood
            if hasattr(report, 'jenis'):
                if report.jenis == 'OVO':
                    grabovo_gross_total += float(report.amount or 0)
                    grabovo_net_total += float(report.total or 0)
                elif report.jenis == 'GrabFood':
                    grabfood_gross_total += float(report.amount or 0)
                    grabfood_net_total += float(report.total or 0)

        for report in shopee_reports:
            if report.order_status != "Cancelled":
                date = report.order_create_time.date()
                if date not in daily_totals:
                    daily_totals[date] = init_daily_total()
                daily_totals[date]['Shopee_Net'] += float(report.net_income or 0)
                daily_totals[date]['Shopee_Gross'] += float(report.order_amount or 0)
        for report in shopeepay_reports:
            if report.transaction_type != "Withdrawal":
                date = report.create_time.date()
                if date not in daily_totals:
                    daily_totals[date] = init_daily_total()
                daily_totals[date]['ShopeePay_Net'] += float(report.settlement_amount or 0)
                daily_totals[date]['ShopeePay_Gross'] += float(report.transaction_amount or 0)
        # Handle cash reports separately for income and expense
        for report in cash_income_reports:
            date = report.tanggal.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Cash_Income'] += float(report.total or 0)

        for report in cash_expense_reports:
            date = report.tanggal.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Cash_Expense'] += float(report.total or 0)

        # Add mutation matching logic for each platform
        platforms = ['gojek', 'grab', 'shopee']
        
        for platform in platforms:
            try:
                # Initialize matcher for current platform
                matcher = TransactionMatcher(platform)
                
                # For Grab, mutations are not mapped to outlet_code, so we match only by date and amount.
                # This is handled by the TransactionMatcher.match_transactions logic for Grab.
                mutations = matcher.get_mutations_query(start_date.date(), end_date.date()).all()
                
                # Process each date in daily_totals
                for date, totals in daily_totals.items():
                    # Skip if no data for this platform on this date
                    platform_net_key = f'{platform.capitalize()}_Net'
                    if platform_net_key not in totals or totals[platform_net_key] == 0:
                        continue
                    print(f"Matching {platform} for date {date} with net {totals[platform_net_key]}")
                    
                    class MockDailyTotal:
                        def __init__(self, outlet_id, date, total_net):
                            self.outlet_id = outlet_id
                            self.date = date
                            self.total_net = total_net

                    mock_total = MockDailyTotal(outlet_code, date, totals[platform_net_key])
                    
                    # Add debug for Shopee
                    if platform == "shopee":
                        outlet = Outlet.query.filter_by(outlet_code=mock_total.outlet_id).first()
                        store_id_shopee = getattr(outlet, "store_id_shopee", None) if outlet else None
                        print(f"[DEBUG] Shopee matching: outlet_code={mock_total.outlet_id}, store_id_shopee={store_id_shopee}")
                        for m in mutations:
                            print(f"[DEBUG] Mutation: platform_code={getattr(m, 'platform_code', None)}, tanggal={getattr(m, 'tanggal', None)}")
                            if store_id_shopee and m.platform_code:
                                match_result = matcher._match_shopee(store_id_shopee, m.platform_code)
                                print(f"[DEBUG] _match_shopee({store_id_shopee}, {m.platform_code}) = {match_result}")

                    # Try to match with mutations
                    platform_data, mutation_data = matcher.match_transactions(mock_total, mutations)
                    
                    if mutation_data:
                        mutation_amount = mutation_data.get('transaction_amount', 0)
                        totals[f'{platform.capitalize()}_Mutation'] = mutation_amount
                        totals[f'{platform.capitalize()}_Difference'] = mutation_amount - totals[platform_net_key]
                    else:
                        totals[f'{platform.capitalize()}_Mutation'] = None
                        totals[f'{platform.capitalize()}_Difference'] = None
                        
            except Exception as e:
                # If matching fails for any platform, continue with others
                print(f"Warning: Mutation matching failed for {platform}: {str(e)}")
                continue

        # Add aggregated data to dataset, sorted by date
        # Update the data output to include ShopeePay and mutation data
        for date in sorted(daily_totals.keys()):
            totals = daily_totals[date]
            dataset.append([
                date,
                totals['Gojek_Net'],
                totals['Gojek_Mutation'],
                totals['Gojek_Difference'],
                totals['Grab_Net'],
                totals['Grab_Mutation'],
                totals['Grab_Difference'],
                totals['Shopee_Net'],
                totals['Shopee_Mutation'],
                totals['Shopee_Difference'],
                totals['ShopeePay_Net'],
                totals['Cash_Income'],
                totals['Cash_Expense']
            ])

        # Update grand totals to include ShopeePay and mutation data
        grand_totals = {
            'Gojek_Gross': sum(day['Gojek_Gross'] for day in daily_totals.values()),
            'Gojek_Net': sum(day['Gojek_Net'] for day in daily_totals.values()),
            'Gojek_Mutation': sum(day['Gojek_Mutation'] for day in daily_totals.values() if day['Gojek_Mutation'] is not None),
            'Gojek_Difference': sum(day['Gojek_Difference'] for day in daily_totals.values() if day['Gojek_Difference'] is not None),
            'Grab_Gross': sum(day['Grab_Gross'] for day in daily_totals.values()),
            'Grab_Net': sum(day['Grab_Net'] for day in daily_totals.values()),
            'Grab_Mutation': sum(day['Grab_Mutation'] for day in daily_totals.values() if day['Grab_Mutation'] is not None),
            'Grab_Difference': sum(day['Grab_Difference'] for day in daily_totals.values() if day['Grab_Difference'] is not None),
            'Shopee_Gross': sum(day['Shopee_Gross'] for day in daily_totals.values()),
            'Shopee_Net': sum(day['Shopee_Net'] for day in daily_totals.values()),
            'Shopee_Mutation': sum(day['Shopee_Mutation'] for day in daily_totals.values() if day['Shopee_Mutation'] is not None),
            'Shopee_Difference': sum(day['Shopee_Difference'] for day in daily_totals.values() if day['Shopee_Difference'] is not None),
            'ShopeePay_Gross': sum(day['ShopeePay_Gross'] for day in daily_totals.values()),
            'ShopeePay_Net': sum(day['ShopeePay_Net'] for day in daily_totals.values()),
            'Cash_Income': sum(day['Cash_Income'] for day in daily_totals.values()),
            'Cash_Expense': sum(day['Cash_Expense'] for day in daily_totals.values())
        }

        # Update grand total row to include ShopeePay and mutation data
        dataset.append([
            'Grand Total',
            grand_totals['Gojek_Net'],
            grand_totals['Gojek_Mutation'],
            grand_totals['Gojek_Difference'],
            grand_totals['Grab_Net'],
            grand_totals['Grab_Mutation'],
            grand_totals['Grab_Difference'],
            grand_totals['Shopee_Net'],
            grand_totals['Shopee_Mutation'],
            grand_totals['Shopee_Difference'],
            grand_totals['ShopeePay_Net'],
            grand_totals['Cash_Income'],
            grand_totals['Cash_Expense']
        ])

        # Create a new workbook and select the active sheet
        wb = Workbook()
        daily_sheet = wb.active
        daily_sheet.title = 'Daily'
        summary_sheet = wb.create_sheet(title='Summary')

        # Style configurations
        header_font = Font(bold=True)
        header_fill = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')
        center_align = Alignment(horizontal='center')

        # Daily Sheet Formatting
        # Add title rows
        daily_sheet['A1'] = 'Sales Report'
        daily_sheet['A2'] = 'Period:'
        daily_sheet['B2'] = f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'
        daily_sheet['A3'] = 'Outlet:'
        daily_sheet['B3'] = outlet.outlet_name_gojek

        # Define platform colors
        date_fill = PatternFill(start_color='C9F0FF', end_color='C9F0FF', fill_type='solid')  # Light green
        gojek_fill = PatternFill(start_color='00AA13', end_color='00AA13', fill_type='solid')  # Light green
        grab_fill = PatternFill(start_color='98FB98', end_color='98FB98', fill_type='solid')   # Pale green
        shopee_fill = PatternFill(start_color='FF7A00', end_color='FF7A00', fill_type='solid') # Light pink
        cash_fill = PatternFill(start_color='ADD8E6', end_color='ADD8E6', fill_type='solid')   # Light blue
        commision_fill = PatternFill(start_color='C6CCB2', end_color='C6CCB2', fill_type='solid') # Light pink
        shopeepay_fill = PatternFill(start_color='E31F26', end_color='E31F26', fill_type='solid')  # Shopee red

        # New: Colors for mutation and difference columns
        mutation_fill = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')   # Light yellow
        difference_fill = PatternFill(start_color='F4CCCC', end_color='F4CCCC', fill_type='solid') # Light red

        # Headers with their corresponding colors
        header_colors = {
            'Date': date_fill,
            'Gojek Net': gojek_fill,
            'Gojek Mutation': gojek_fill, 'Gojek Difference': difference_fill,
            'Grab Net': grab_fill,
            'Grab Mutation': grab_fill, 'Grab Difference': difference_fill,
            'Shopee Net': shopee_fill,
            'Shopee Mutation': shopee_fill, 'Shopee Difference': difference_fill,
            'ShopeePay Net': shopeepay_fill,
            'Cash Income': cash_fill, 'Cash Expense': cash_fill
        }

        headers = [
            'Date',
            'Gojek Net', 'Gojek Mutation', 'Gojek Difference',
            'Grab Net', 'Grab Mutation', 'Grab Difference',
            'Shopee Net', 'Shopee Mutation', 'Shopee Difference',
            'ShopeePay Net', 'Cash Income', 'Cash Expense'
        ]

        # Write headers to Excel
        for col, header in enumerate(headers, 1):
            cell = daily_sheet.cell(row=5, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_colors.get(header, header_fill)
            cell.alignment = center_align

        # Write daily data rows to Excel
        current_row = 6
        for date in sorted(daily_totals.keys()):
            totals = daily_totals[date]
            row_data = [
                date,
                totals['Gojek_Net'],
                totals['Gojek_Mutation'],
                totals['Gojek_Difference'],
                totals['Grab_Net'],
                totals['Grab_Mutation'],
                totals['Grab_Difference'],
                totals['Shopee_Net'],
                totals['Shopee_Mutation'],
                totals['Shopee_Difference'],
                totals['ShopeePay_Net'],
                totals['Cash_Income'],
                totals['Cash_Expense']
            ]
            for col, value in enumerate(row_data, 1):
                cell = daily_sheet.cell(row=current_row, column=col)
                cell.value = value
                if col > 1:  # Format numbers
                    cell.number_format = '#,##0'

                # Apply color for Difference columns (Gojek, Grab, Shopee)
                # Columns: 4 (Gojek Difference), 7 (Grab Difference), 10 (Shopee Difference)
                if col in [4, 7, 10]:
                    if value is not None:
                        if value > 0:
                            cell.fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')  # Light green
                        elif value < 0:
                            cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')  # Light red
                        else:
                            cell.fill = difference_fill  # Neutral color
                    else:
                        cell.fill = difference_fill  # Neutral color

            current_row += 1

        # Update grand total row to match new headers
        grand_total_row = current_row
        daily_sheet.cell(row=grand_total_row, column=1, value='Grand Total').font = header_font
        grand_total_data = [
            grand_totals['Gojek_Net'],
            grand_totals['Gojek_Mutation'],
            grand_totals['Gojek_Difference'],
            grand_totals['Grab_Net'],
            grand_totals['Grab_Mutation'],
            grand_totals['Grab_Difference'],
            grand_totals['Shopee_Net'],
            grand_totals['Shopee_Mutation'],
            grand_totals['Shopee_Difference'],
            grand_totals['ShopeePay_Net'],
            grand_totals['Cash_Income'],
            grand_totals['Cash_Expense']
        ]
        for col, value in enumerate(grand_total_data, 2):
            cell = daily_sheet.cell(row=grand_total_row, column=col)
            cell.value = value
            cell.font = header_font
            cell.fill = yellow_fill
            cell.alignment = Alignment(horizontal='center', vertical='center') 
            cell.number_format = '#,##0'

        # Summary Sheet Formatting
        # Add titles
        summary_sheet['A1'] = 'Summary Report'
        summary_sheet['A2'] = 'Period:'
        summary_sheet['B2'] = f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'
        summary_sheet['A3'] = 'Outlet:'
        summary_sheet['B3'] = outlet.outlet_name_gojek

        # Online Platform Summary
        current_row = 5
        summary_title_cell = summary_sheet.cell(row=current_row, column=1, value='Online Platform Summary')
        summary_title_cell.font = header_font
        summary_title_cell.fill = grab_fill
        current_row += 1
        
        platform_headers = ['Platform', 'Gross', 'Net', 'Difference']
        for col, header in enumerate(platform_headers, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = grab_fill
        current_row += 1

        # Add platform data
        platforms = [
            ('Gojek', 'Gojek_Gross', 'Gojek_Net'),
            ('Grab (Total)', 'Grab_Gross', 'Grab_Net'),
            ('   GrabFood', grabfood_gross_total, grabfood_net_total),  # Added net total
            ('   OVO', grabovo_gross_total, grabovo_net_total),    # Added net total
            ('Shopee', 'Shopee_Gross', 'Shopee_Net'),
            ('ShopeePay', 'ShopeePay_Gross', 'ShopeePay_Net')
        ]
        
        for platform_data in platforms:
            if isinstance(platform_data[1], (int, float)):  # For GrabFood and GrabOVO
                platform, gross, net = platform_data
                row_data = [
                    platform,
                    gross,
                    net,
                    gross - net
                ]
            else:
                platform, gross_key, net_key = platform_data
                row_data = [
                    platform,
                    grand_totals[gross_key],
                    grand_totals[net_key],
                    grand_totals[gross_key] - grand_totals[net_key]
                ]
            
            for col, value in enumerate(row_data, 1):
                cell = summary_sheet.cell(row=current_row, column=col)
                cell.value = value
                cell.fill = grab_fill
                if col > 1:
                    cell.number_format = '#,##0'
            current_row += 1
            
       

        current_row += 1  # Add spacing

        # Income/Expense Summary
        summary_title_cell = summary_sheet.cell(row=current_row, column=1, value='Income/Expense Summary')
        summary_title_cell.font = header_font
        summary_title_cell.fill = cash_fill
        current_row += 1
        
        expense_headers = ['Category', 'Income', 'Expense', 'Net Total', 'Description', 'Date Range']
        for col, header in enumerate(expense_headers, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = cash_fill
        current_row += 1

        # Query manual entries first
        manual_entries = ManualEntry.query.filter(
            ManualEntry.outlet_code == outlet_code,
            ManualEntry.start_date <= end_date,
            ManualEntry.end_date >= start_date
        ).all()

        # Calculate manual entry totals
        manual_income = sum(float(entry.amount or 0) for entry in manual_entries if entry.entry_type == 'income')
        manual_expense = sum(float(entry.amount or 0) for entry in manual_entries if entry.entry_type == 'expense')

        # Add cash data
        cash_net_total = grand_totals['Cash_Income'] - grand_totals['Cash_Expense']
        cash_row = ['Cash', grand_totals['Cash_Income'], grand_totals['Cash_Expense'], cash_net_total, '', '']
        for col, value in enumerate(cash_row, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = value
            cell.fill = cash_fill
            if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                cell.number_format = '#,##0'
        current_row += 1

        # Add manual entries with their details
        for entry in manual_entries:
            amount = float(entry.amount or 0)
            income_amount = amount if entry.entry_type == 'income' else 0
            expense_amount = amount if entry.entry_type == 'expense' else 0
            net_amount = income_amount - expense_amount
            
            row_data = [
                'Manual Entry',
                income_amount,
                expense_amount,
                net_amount,
                entry.description,
                f"{entry.start_date} - {entry.end_date}"
            ]
            for col, value in enumerate(row_data, 1):
                cell = summary_sheet.cell(row=current_row, column=col)
                cell.value = value
                cell.fill = cash_fill
                if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                    cell.number_format = '#,##0'
            current_row += 1

        # Add total row
        total_net = (grand_totals['Cash_Income'] + manual_income) - (grand_totals['Cash_Expense'] + manual_expense)
        total_row = [
            'Total',
            grand_totals['Cash_Income'] + manual_income,
            grand_totals['Cash_Expense'] + manual_expense,
            total_net,
            '',
            ''
        ]
        for col, value in enumerate(total_row, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = value
            cell.font = header_font
            cell.fill = cash_fill
            if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                cell.number_format = '#,##0'
        current_row += 1

        current_row += 1  # Add spacing

        # Calculate total net income from all sources
        online_net_total = (
            grand_totals['Gojek_Net'] +
            grand_totals['Grab_Net'] +
            grand_totals['Shopee_Net'] +
            grand_totals['ShopeePay_Net']
        )
        
        # Calculate commission first (moved from below)
        management_commission = grabfood_gross_total * 0.01 if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan","Martabak 999 Asli Bandung", "Martabak Surya Kencana","Martabak Akim"] else 0
        partner_commission = grabfood_gross_total * 0.01 if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan","Martabak 999 Asli Bandung", "Martabak Surya Kencana","Martabak Akim"] else 0

        cash_manual_net_total = (
            (grand_totals['Cash_Income'] + manual_income) -
            (grand_totals['Cash_Expense'] + manual_expense)
        )
        
        # Calculate final online net total after commission
        online_net_after_commission = online_net_total - management_commission

        summary_data = [('Online Platforms Net', online_net_total)]
        
        # Add commission-related entries only for specific brands
        if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan","Martabak 999 Asli Bandung", "Martabak Surya Kencana","Martabak Akim"]:
            summary_data.extend([
                ('Grab Management', -management_commission),
                ('Online Net After Commission', online_net_after_commission)
            ])
        
        summary_data.extend([
            ('Cash & Manual Net', cash_manual_net_total),
            ('GRAND TOTAL NET INCOME', online_net_after_commission + cash_manual_net_total)
        ])

        for label, amount in summary_data:
            cell = summary_sheet.cell(row=current_row, column=1, value=label)
            amount_cell = summary_sheet.cell(row=current_row, column=2, value=amount)
            cell.fill = shopee_fill
            amount_cell.number_format = '#,##0'
            amount_cell.fill = shopee_fill
            if label == 'GRAND TOTAL NET INCOME':
                cell.font = header_font
                amount_cell.font = header_font
                cell.fill = yellow_fill
                amount_cell.fill = yellow_fill
            elif label == 'Management Commission':
                cell.font = Font(bold=True, color='FF0000')  # Red color for commission
                amount_cell.font = Font(bold=True, color='FF0000')
            current_row += 1

        current_row += 2  # Add spacing


        # Commission Summary - Only show for specific brands
        if outlet.brand in ["MP78", "MP78 Express", "Martabak 777 Sinar Bulan","Martabak 999 Asli Bandung", "Martabak Surya Kencana","Martabak Akim"]:
            summary_title_cell = summary_sheet.cell(row=current_row, column=1, value='Commission Summary')
            summary_title_cell.font = header_font
            summary_title_cell.fill = commision_fill
            current_row += 1
            
            commission_headers = ['Category','Rate', 'Commission']
            for col, header in enumerate(commission_headers, 1):
                cell = summary_sheet.cell(row=current_row, column=col)
                cell.value = header
                cell.font = header_font
                cell.fill = commision_fill
            current_row += 1

            commission_data = [
                ('Management Commission (GrabFood)', '1%', management_commission),
                ('Partner Commission (GrabFood)', '1%', partner_commission),
            ]

            for category, rate, commission in commission_data:
                row_data = [category, rate, commission]
                for col, value in enumerate(row_data, 1):
                    cell = summary_sheet.cell(row=current_row, column=col)
                    cell.value = value
                    cell.fill = commision_fill
                    if col in [3]:  # Format numbers for Base Amount and Commission columns
                        cell.number_format = '#,##0'
                current_row += 1

            current_row += 1  # Add spacing
       

        # Auto-adjust column widths
        for sheet in [daily_sheet, summary_sheet]:
            for column in sheet.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                sheet.column_dimensions[get_column_letter(column[0].column)].width = adjusted_width
            # Protect each worksheet
            sheet.protection.sheet = True
            sheet.protection.enable()
       # Protect workbook structure properly
        wb.security = WorkbookProtection(workbookPassword=None, lockStructure=True)
        # Save to BytesIO
        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)

        # Replace special characters in outlet name to make it filename-safe
        safe_outlet_name = outlet.outlet_name_gojek.replace('/', '_').replace('\\', '_').replace(' ', '_')
        filename = f"reports_{safe_outlet_name}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
        
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        return jsonify({
            "error": "General process failed",
            "details": str(e),
            "type": type(e).__name__,
            "step": "general"
        }), 400
