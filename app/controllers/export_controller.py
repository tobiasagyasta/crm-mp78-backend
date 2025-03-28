from flask import Blueprint, request, jsonify, send_file
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

@export_bp.route('/', methods=['POST'])
def export_reports():
    try:
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
            daily_totals[date]['Grab_Net'] += report.penjualan_bersih or 0

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
        output = dataset.export('xlsx')

        # Save locally first
        local_path = f"c:\\Users\\agyas\\Documents\\Repos\\crm-mp78-backend\\exports\\reports_{outlet_code}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

        # Ensure exports directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Save file locally
        with open(local_path, 'wb') as f:
            f.write(output)

        # Return file for download
        return send_file(
            local_path,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=os.path.basename(local_path)
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 400
