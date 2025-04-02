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
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
import os

export_bp = Blueprint('export', __name__, url_prefix="/export")

@export_bp.route('', methods=['POST', 'OPTIONS'])
def export_reports():
    try:
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
                'Shopee_Gross': 0, 'Shopee_Net': 0,
                'Cash_Income': 0, 'Cash_Expense': 0
            }
        
        # Add title rows
        dataset = []
        dataset.append(['Sales Report', '', '', '', '', '', '', '', ''])
        dataset.append(['Period:', f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}', '', '', '', '', '', '', ''])
        dataset.append(['Outlet:', outlet.outlet_name_gojek, '', '', '', '', '', '', ''])
        dataset.append([]) # Empty row for spacing
        dataset.append(['Date', 'Gojek Gross', 'Gojek Net', 'Grab Gross', 'Grab Net', 'Shopee Gross', 'Shopee Net', 'Cash Income', 'Cash Expense'])
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

        for report in grab_reports:
            date = report.tanggal_dibuat.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Grab_Net'] += float(report.total or 0)
            daily_totals[date]['Grab_Gross'] += float(report.amount or 0)

        for report in shopee_reports:
            if report.order_status != "Cancelled":
                date = report.order_create_time.date()
                if date not in daily_totals:
                    daily_totals[date] = init_daily_total()
                daily_totals[date]['Shopee_Net'] += float(report.net_income or 0)
                daily_totals[date]['Shopee_Gross'] += float(report.order_amount or 0)

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

        # Add aggregated data to dataset, sorted by date
        for date in sorted(daily_totals.keys()):
            totals = daily_totals[date]
            dataset.append([
                date,
                totals['Gojek_Gross'],
                totals['Gojek_Net'],
                totals['Grab_Gross'],
                totals['Grab_Net'],
                totals['Shopee_Gross'],
                totals['Shopee_Net'],
                totals['Cash_Income'],
                totals['Cash_Expense']
            ])

        # Calculate and append grand totals
        grand_totals = {
            'Gojek_Gross': sum(day['Gojek_Gross'] for day in daily_totals.values()),
            'Gojek_Net': sum(day['Gojek_Net'] for day in daily_totals.values()),
            'Grab_Gross': sum(day['Grab_Gross'] for day in daily_totals.values()),
            'Grab_Net': sum(day['Grab_Net'] for day in daily_totals.values()),
            'Shopee_Gross': sum(day['Shopee_Gross'] for day in daily_totals.values()),
            'Shopee_Net': sum(day['Shopee_Net'] for day in daily_totals.values()),
            'Cash_Income': sum(day['Cash_Income'] for day in daily_totals.values()),
            'Cash_Expense': sum(day['Cash_Expense'] for day in daily_totals.values())
        }

        dataset.append(['Grand Total', 
                       grand_totals['Gojek_Gross'],
                       grand_totals['Gojek_Net'],
                       grand_totals['Grab_Gross'],
                       grand_totals['Grab_Net'],
                       grand_totals['Shopee_Gross'],
                       grand_totals['Shopee_Net'],
                       grand_totals['Cash_Income'],
                       grand_totals['Cash_Expense']])

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
        shopee_fill = PatternFill(start_color='FF6600', end_color='FF6600', fill_type='solid') # Light pink
        cash_fill = PatternFill(start_color='ADD8E6', end_color='ADD8E6', fill_type='solid')   # Light blue

        # Headers with their corresponding colors
        header_colors = {
            'Date': date_fill,
            'Gojek Gross': gojek_fill, 'Gojek Net': gojek_fill,
            'Grab Gross': grab_fill, 'Grab Net': grab_fill,
            'Shopee Gross': shopee_fill, 'Shopee Net': shopee_fill,
            'Cash Income': cash_fill, 'Cash Expense': cash_fill
        }

        headers = ['Date', 'Gojek Gross', 'Gojek Net', 'Grab Gross', 'Grab Net', 
                  'Shopee Gross', 'Shopee Net', 'Cash Income', 'Cash Expense']
        
        dataset.append(headers)

        # Later in the code, update the Excel sheet formatting (around line 190):
        # Headers
        headers = ['Date', 'Gojek Gross', 'Gojek Net', 'Grab Gross', 'Grab Net', 
                  'Shopee Gross', 'Shopee Net', 'Cash Income', 'Cash Expense']
        for col, header in enumerate(headers, 1):
            cell = daily_sheet.cell(row=5, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_colors[header] if header_colors[header] else header_fill
            cell.alignment = center_align

        # Add daily data
        current_row = 6
        for date in sorted(daily_totals.keys()):
            totals = daily_totals[date]
            row_data = [
                date,
                totals['Gojek_Gross'],
                totals['Gojek_Net'],
                totals['Grab_Gross'],
                totals['Grab_Net'],
                totals['Shopee_Gross'],
                totals['Shopee_Net'],
                totals['Cash_Income'],
                totals['Cash_Expense']
            ]
            for col, value in enumerate(row_data, 1):
                cell = daily_sheet.cell(row=current_row, column=col)
                cell.value = value
                if col > 1:  # Format numbers
                    cell.number_format = '#,##0.00'
            current_row += 1

        # Add grand totals
        grand_total_row = current_row
        daily_sheet.cell(row=grand_total_row, column=1, value='Grand Total').font = header_font
        grand_total_data = [
            grand_totals['Gojek_Gross'],
            grand_totals['Gojek_Net'],
            grand_totals['Grab_Gross'],
            grand_totals['Grab_Net'],
            grand_totals['Shopee_Gross'],
            grand_totals['Shopee_Net'],
            grand_totals['Cash_Income'],
            grand_totals['Cash_Expense']
        ]
        for col, value in enumerate(grand_total_data, 2):
            cell = daily_sheet.cell(row=grand_total_row, column=col)
            cell.value = value
            cell.font = header_font
            cell.number_format = '#,##0.00'

        # Summary Sheet Formatting
        # Add titles
        summary_sheet['A1'] = 'Summary Report'
        summary_sheet['A2'] = 'Period:'
        summary_sheet['B2'] = f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}'
        summary_sheet['A3'] = 'Outlet:'
        summary_sheet['B3'] = outlet.outlet_name_gojek

        # Online Platform Summary
        current_row = 5
        summary_sheet.cell(row=current_row, column=1, value='Online Platform Summary').font = header_font
        current_row += 1
        
        platform_headers = ['Platform', 'Gross', 'Net', 'Difference']
        for col, header in enumerate(platform_headers, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_fill
        current_row += 1

        # Add platform data
        platforms = [
            ('Gojek', 'Gojek_Gross', 'Gojek_Net'),
            ('Grab', 'Grab_Gross', 'Grab_Net'),
            ('Shopee', 'Shopee_Gross', 'Shopee_Net')
        ]
        
        for platform, gross_key, net_key in platforms:
            row_data = [
                platform,
                grand_totals[gross_key],
                grand_totals[net_key],
                grand_totals[gross_key] - grand_totals[net_key]
            ]
            for col, value in enumerate(row_data, 1):
                cell = summary_sheet.cell(row=current_row, column=col)
                cell.value = value
                if col > 1:
                    cell.number_format = '#,##0.00'
            current_row += 1

        current_row += 1  # Add spacing

        # Income/Expense Summary
        summary_sheet.cell(row=current_row, column=1, value='Income/Expense Summary').font = header_font
        current_row += 1
        
        expense_headers = ['Category', 'Income', 'Expense', 'Net Total', 'Description', 'Date Range']
        for col, header in enumerate(expense_headers, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_fill
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
            if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                cell.number_format = '#,##0.00'
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
                if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                    cell.number_format = '#,##0.00'
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
            if 1 < col <= 4:  # Format numbers for Income, Expense, and Net Total columns
                cell.number_format = '#,##0.00'
        current_row += 1

        current_row += 1  # Add spacing

        # Calculate total net income from all sources
        online_net_total = (
            grand_totals['Gojek_Net'] +
            grand_totals['Grab_Net'] +
            grand_totals['Shopee_Net']
        )
        
        cash_manual_net_total = (
            (grand_totals['Cash_Income'] + manual_income) -
            (grand_totals['Cash_Expense'] + manual_expense)
        )
        
        total_net_income = online_net_total + cash_manual_net_total

        current_row += 1  # Add spacing

        # Overall Net Income Summary
        summary_sheet.cell(row=current_row, column=1, value='Overall Net Income Summary').font = header_font
        current_row += 1
        
        summary_sheet.cell(row=current_row, column=1, value='Source').font = header_font
        summary_sheet.cell(row=current_row, column=2, value='Amount').font = header_font
        current_row += 1

        summary_data = [
            ('Online Platforms Net', online_net_total),
            ('Cash & Manual Net', cash_manual_net_total),
            ('GRAND TOTAL NET INCOME', total_net_income)
        ]

        # Define yellow background for grand total
        yellow_fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')  # Yellow background
        
        for label, amount in summary_data:
            cell = summary_sheet.cell(row=current_row, column=1, value=label)
            amount_cell = summary_sheet.cell(row=current_row, column=2, value=amount)
            amount_cell.number_format = '#,##0.00'
            if label == 'GRAND TOTAL NET INCOME':
                cell.font = header_font
                amount_cell.font = header_font
                cell.fill = yellow_fill
                amount_cell.fill = yellow_fill
            current_row += 1

        current_row += 2  # Add spacing


        # Commission Summary
        summary_sheet.cell(row=current_row, column=1, value='Commission Summary').font = header_font
        current_row += 1
        
        commission_headers = ['Category','Rate', 'Commission']
        for col, header in enumerate(commission_headers, 1):
            cell = summary_sheet.cell(row=current_row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_fill
        current_row += 1

        # Calculate commissions (1% each)
        management_commission = grand_totals['Grab_Gross'] * 0.01
        partner_commission = grand_totals['Grab_Gross'] * 0.01

        commission_data = [
            ('Management Commission (Grab)', '1%', management_commission),
            ('Partner Commission (Grab)', '1%', partner_commission),
        ]

        for category,rate, commission in commission_data:
            row_data = [category, rate, commission]
            for col, value in enumerate(row_data, 1):
                cell = summary_sheet.cell(row=current_row, column=col)
                cell.value = value
                if col in [3]:  # Format numbers for Base Amount and Commission columns
                    cell.number_format = '#,##0.00'
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

        # Save to BytesIO
        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)

        filename = f"reports_{outlet_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
        
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
