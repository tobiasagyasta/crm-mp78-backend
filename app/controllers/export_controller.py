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
import tablib
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

        # Query reports
        gojek_reports = GojekReport.query.filter(
            GojekReport.outlet_code == outlet_code,
            GojekReport.transaction_date.between(start_date, end_date)
        ).all()

        grab_reports = GrabFoodReport.query.filter(
            GrabFoodReport.outlet_code == outlet_code,
            GrabFoodReport.tanggal_dibuat.between(start_date, end_date)
        ).all()

        shopee_reports = ShopeeReport.query.filter(
            ShopeeReport.outlet_code == outlet_code,
            ShopeeReport.order_create_time.between(start_date, end_date)
        ).all()

        # Query cash reports
        cash_reports = CashReport.query.filter(
            CashReport.outlet_code == outlet_code,
            CashReport.tanggal.between(start_date, end_date)
        ).all()

        # Create dataset with updated headers and initialize totals
        headers = ('Date', 
                  'Gojek Gross', 'Gojek Net', 
                  'Grab Gross', 'Grab Net', 
                  'Shopee Gross', 'Shopee Net', 
                  'Cash Income', 'Cash Expense')
        # Create dataset without headers first
        dataset = tablib.Dataset()
        dataset.title = 'Daily'
        
        # Add title rows
        dataset.append(['Sales Report', '', '', '', '', '', '', '', ''])
        dataset.append(['Period:', f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}', '', '', '', '', '', '', ''])
        dataset.append(['Outlet:', outlet.outlet_name_gojek, '', '', '', '', '', '', ''])
        dataset.append([]) # Empty row for spacing
        
        # Add the header row as regular data
        dataset.append([
            'Date', 
            'Gojek Gross', 'Gojek Net', 
            'Grab Gross', 'Grab Net', 
            'Shopee Gross', 'Shopee Net', 
            'Cash Income', 'Cash Expense'
        ])
     
        daily_totals = {}

        # Initialize dictionary structure
        def init_daily_total():
            return {
                'Gojek_Gross': 0, 'Gojek_Net': 0,
                'Grab_Gross': 0, 'Grab_Net': 0,
                'Shopee_Gross': 0, 'Shopee_Net': 0,
                'Cash_Income': 0, 'Cash_Expense': 0
            }

        # Aggregate all data by date
        for report in gojek_reports:
            date = report.transaction_date
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Gojek_Gross'] += report.amount or 0
            daily_totals[date]['Gojek_Net'] += report.nett_amount or 0

        for report in grab_reports:
            date = report.tanggal_dibuat.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Grab_Gross'] += report.amount or 0
            daily_totals[date]['Grab_Net'] += report.total or 0

        for report in shopee_reports:
            date = report.order_create_time.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            daily_totals[date]['Shopee_Gross'] += report.order_amount or 0
            daily_totals[date]['Shopee_Net'] += report.net_income or 0

        for report in cash_reports:
            date = report.tanggal.date()
            if date not in daily_totals:
                daily_totals[date] = init_daily_total()
            if report.type == 'income':
                daily_totals[date]['Cash_Income'] += report.total or 0
            else:
                daily_totals[date]['Cash_Expense'] += report.total or 0

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

        # Export to Excel
        # Export first sheet with daily details
        daily_sheet = dataset.export('xlsx')

        # Create new dataset for summary
        summary_dataset = tablib.Dataset()
        summary_dataset.title = 'Summary'
        
        # Add title for summary (match the 9 columns from main dataset)
        summary_dataset.append(['Summary Report', '', '', '', '', '', '', '', ''])
        summary_dataset.append(['Period:', f'{start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}', '', '', '', '', '', '', ''])
        summary_dataset.append(['Outlet:', outlet.outlet_name_gojek, '', '', '', '', '', '', ''])
        summary_dataset.append(['', '', '', '', '', '', '', '', '']) # Empty row
        
        # Add grand totals summary (match the 9 columns)
        summary_dataset.append(['Online Platform Summary', '', '', '', '', '', '', '', ''])
        summary_dataset.append(['Platform', 'Gross', 'Net', 'Difference', '', '', '', '', ''])
        summary_dataset.append(['Gojek', grand_totals['Gojek_Gross'], grand_totals['Gojek_Net'],
                              grand_totals['Gojek_Gross'] - grand_totals['Gojek_Net'], '', '', '', '', ''])
        summary_dataset.append(['Grab', grand_totals['Grab_Gross'], grand_totals['Grab_Net'],
                              grand_totals['Grab_Gross'] - grand_totals['Grab_Net'], '', '', '', '', ''])
        summary_dataset.append(['Shopee', grand_totals['Shopee_Gross'], grand_totals['Shopee_Net'],
                              grand_totals['Shopee_Gross'] - grand_totals['Shopee_Net'], '', '', '', '', ''])
        summary_dataset.append(['', '', '', '', '', '', '', '', '']) # Empty row

        # Query manual entries first
        manual_entries = ManualEntry.query.filter(
            ManualEntry.outlet_code == outlet_code,
            ManualEntry.start_date <= end_date,
            ManualEntry.end_date >= start_date
        ).all()

        # Calculate manual entry totals
        manual_income = sum(entry.amount for entry in manual_entries if entry.entry_type == 'income')
        manual_expense = sum(entry.amount for entry in manual_entries if entry.entry_type == 'expense')

        # Add combined income/expense summary
        summary_dataset.append(['Income/Expense Summary', '', '', '', '', '', '', '', ''])
        summary_dataset.append(['Category', 'Income', 'Expense', 'Net Total', '', '', '', '', ''])
        summary_dataset.append(['Cash', 
                              grand_totals['Cash_Income'],
                              grand_totals['Cash_Expense'],
                              grand_totals['Cash_Income'] - grand_totals['Cash_Expense'],
                              '', '', '', '', ''])
        summary_dataset.append(['Manual Entries',
                              manual_income,
                              manual_expense,
                              manual_income - manual_expense,
                              '', '', '', '', ''])
        summary_dataset.append(['Total',
                              grand_totals['Cash_Income'] + manual_income,
                              grand_totals['Cash_Expense'] + manual_expense,
                              (grand_totals['Cash_Income'] + manual_income) - (grand_totals['Cash_Expense'] + manual_expense),
                              '', '', '', '', ''])
        summary_dataset.append(['', '', '', '', '', '', '', '', '']) # Empty row

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

        # Add grand total net income summary
        summary_dataset.append(['Overall Net Income Summary', '', '', '', '', '', '', '', ''])
        summary_dataset.append(['Source', 'Amount', '', '', '', '', '', '', ''])
        summary_dataset.append(['Online Platforms Net', online_net_total, '', '', '', '', '', '', ''])
        summary_dataset.append(['Cash & Manual Net', cash_manual_net_total, '', '', '', '', '', '', ''])
        summary_dataset.append(['GRAND TOTAL NET INCOME', total_net_income, '', '', '', '', '', '', ''])
        summary_dataset.append(['', '', '', '', '', '', '', '', '']) # Empty row

        # Add manual entries detail if exists
        if manual_entries:
            summary_dataset.append(['Manual Entries Detail', '', '', '', '', '', '', '', ''])
            summary_dataset.append(['Date Range', 'Description', 'Amount', 'Type', '', '', '', '', ''])
            for entry in manual_entries:
                summary_dataset.append([
                    f"{entry.start_date} - {entry.end_date}",
                    entry.description,
                    entry.amount,
                    entry.entry_type,
                    '', '', '', '', ''
                ])

        # Export both sheets to Excel
        try:
            book = tablib.Databook([dataset, summary_dataset])
            output = book.export('xlsx')
            
            filename = f"reports_{outlet_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
            
            response = jsonify()
            response.data = output
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f'attachment; filename={filename}'
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            return response

        except Exception as excel_error:
            return jsonify({
                "error": "Excel generation failed",
                "details": str(excel_error),
                "step": "excel_export"
            }), 400

    except Exception as e:
        return jsonify({
            "error": "General process failed",
            "details": str(e),
            "type": type(e).__name__,
            "step": "general"
        }), 400
