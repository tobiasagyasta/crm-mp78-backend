from flask import Blueprint, jsonify, request, Response, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.extensions import db
# import pdfkit
import csv
from io import StringIO
from sqlalchemy import func
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from app.models.outlet import Outlet
from app.utils.report_generator import generate_daily_report

# config = pdfkit.configuration(wkhtmltopdf='C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe')
reports_bp = Blueprint('reports', __name__, url_prefix='/reports')

import os


def update_store_ids_batch(store_id_map, platform):
    """Update store IDs in batches to prevent timeout"""
    batch_size = 100
    updated = 0
    
    # Process in batches
    store_items = list(store_id_map.items())
    for i in range(0, len(store_items), batch_size):
        batch = store_items[i:i + batch_size]
        for store_name, store_id in batch:
            try:
                # For Gojek
                if platform == 'gojek':
                    # Check if store_id already exists
                    existing_outlet = Outlet.query.filter_by(store_id_gojek=store_id).first()
                    if existing_outlet:
                        continue
                    
                    outlet = Outlet.query.filter_by(outlet_name_gojek=store_name).first()
                    if outlet and not outlet.store_id_gojek:
                        outlet.store_id_gojek = store_id
                        updated += 1
                
                # For Grab
                elif platform == 'grab':
                    existing_outlet = Outlet.query.filter_by(store_id_grab=store_id).first()
                    if existing_outlet:
                        continue
                    
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                    if outlet and not outlet.store_id_grab:
                        outlet.store_id_grab = store_id
                        updated += 1

                # For Shopee
                elif platform == 'shopee':
                    existing_outlet = Outlet.query.filter_by(store_id_shopee=store_id).first()
                    if existing_outlet:
                        continue
                    
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                    if outlet and not outlet.store_id_shopee:
                        outlet.store_id_shopee = store_id
                        updated += 1
                
            except Exception as e:
                print(f"Error updating {store_name}: {str(e)}")
                continue
        
        # Commit each batch
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"Error committing batch: {str(e)}")

    return updated

@reports_bp.route('/upload/gojek', methods=['POST'])
def upload_report_gojek():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        store_id_map = {}  # To collect store IDs
        
        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)
            
            reports = []
            for row in reader:
                merchant_name = row.get('Merchant name', '').strip()
                merchant_id = row.get('Merchant ID')
                if merchant_name and merchant_id:
                    store_id_map[merchant_name] = merchant_id

                # Get outlet code from store ID first, then fallback to name
                outlet = None
                if merchant_id:
                    outlet = Outlet.query.filter_by(store_id_gojek=merchant_id).first()
                if not outlet and merchant_name:
                    outlet = Outlet.query.filter_by(outlet_name_gojek=merchant_name).first()
                if not outlet:
                    continue  # Skip if outlet not found by either ID or name

                # Check if transaction already exists
                order_no = row.get('Order No', '')
                if order_no:
                    existing_report = GojekReport.query.filter_by(nomor_pesanan=order_no).first()
                    if existing_report:
                        skipped_reports += 1
                        continue

                try:
                    # Parse the date and time
                    date_str = row.get('Transaction Date', '')
                    time_str = row.get('Transaction time', '')
                    
                    try:
                        # Parse MM/DD/YYYY format for date
                        transaction_date = datetime.strptime(date_str, '%m/%d/%Y').date()
                    except ValueError:
                        print(f"Error parsing date: {date_str}")
                        continue
                    
                    # Parse ISO format timestamp for time
                    try:
                        transaction_time = datetime.fromisoformat(time_str.replace('Z', '+00:00')).time()
                    except ValueError:
                        transaction_time = None
                    
                    report = GojekReport(
                        brand_name=outlet.brand,
                        outlet_code=outlet.outlet_code,
                        transaction_id=row.get('Transaction ID', '').strip("'"),
                        transaction_date=transaction_date,
                        transaction_time=transaction_time,
                        stan=row.get('Stan', ''),
                        nett_amount=row.get('Nett Amount', 0),
                        amount=row.get('Amount', 0),
                        transaction_status=row.get('Transaction Status', ''),
                        transaction_reference=row.get('Transaction Reference', '').strip("'"),
                        order_id=row.get('Order ID', '').strip("'"),
                        feature=row.get('Feature', ''),
                        payment_type=row.get('Payment Type', ''),
                        merchant_name=merchant_name,
                        merchant_id=merchant_id,
                        promo_type=row.get('Promo Type', ''),
                        promo_name=row.get('Promo Name', ''),
                        gopay_promo=float(row.get('Gopay promo', 0).replace(',', '') or 0),
                        gofood_discount=float(row.get('GoFood discount', 0).replace(',', '') or 0),
                        voucher_commission=float(row.get('Voucher commission', 0).replace(',', '') or 0),
                        tax=float(row.get('Tax', 0).replace(',', '') or 0),
                        witholding_tax=float(row.get('Witholding tax', 0).replace(',', '') or 0),
                        currency=row.get('Currency', 'IDR')
                    )
                    reports.append(report)
                    total_reports += 1
                except (ValueError, TypeError) as e:
                    print(f"Error processing row: {e}")
                    continue

            # Bulk save reports for this file
            if reports:
                try:
                    db.session.bulk_save_objects(reports)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    # Handle reports one by one if bulk insert fails
                    for report in reports:
                        try:
                            db.session.add(report)
                            db.session.commit()
                        except IntegrityError:
                            db.session.rollback()
                            skipped_reports += 1
                        except Exception as e:
                            db.session.rollback()
                            print(f"Error saving report: {str(e)}")

        # Update store IDs in batches
        updated_count = update_store_ids_batch(store_id_map, 'gojek')
        
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'store_ids_updated': updated_count
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@reports_bp.route('/upload/grab', methods=['POST'])
def upload_report_grab():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        store_id_map = {}  # To collect store IDs
        
        for file in files:
            # Process CSV and collect store IDs
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)
            
            reports = []
            for row in reader:
                store_name = row.get('Nama toko', '').strip()
                store_id = row.get('ID toko')
                if store_name and store_id:
                    store_id_map[store_name] = store_id

                outlet = None
                if store_id:
                    outlet = Outlet.query.filter_by(store_id_grab=store_id).first()
                if not outlet and store_name:
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                if not outlet:
                    continue  # Skip if outlet not found by either ID or name

                # Check if transaction already exists
                transaction_id = row.get('ID transaksi', '')
                if transaction_id:
                    existing_report = GrabFoodReport.query.filter_by(id_transaksi=transaction_id).first()
                    if existing_report:
                        skipped_reports += 1
                        continue

                try:
                    # Create GrabFoodReport instance
                    # Parse the date with the correct format
                    date_str = row.get('Tanggal dibuat', '')
                    try:
                        tanggal_dibuat = datetime.strptime(date_str, '%d %b %Y %I:%M %p')
                    except ValueError:
                        # Try alternate format if first attempt fails
                        tanggal_dibuat = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

                    def safe_float(value):
                        if not value:
                            return 0
                        try:
                            # Remove commas and convert to float
                            return float(str(value).replace(',', ''))
                        except (ValueError, TypeError):
                            return 0

                    report = GrabFoodReport(
                        brand_name=outlet.brand,
                        outlet_code=outlet.outlet_code,
                        # nama_merchant=row.get('Nama merchant', ''),
                        # id_merchant=row.get('ID merchant', ''),
                        nama_toko=store_name,
                        id_toko=store_id,
                        tanggal_dibuat=tanggal_dibuat,
                        jenis=row.get('Jenis', ''),
                        kategori=row.get('Kategori', ''),
                        subkategori=row.get('Subkategori', ''),
                        status=row.get('Status', ''),
                        id_transaksi=row.get('ID transaksi', ''),
                        # poin_diberikan=safe_float(row.get('Poin diberikan')),
                        komisi_grabkitchen=safe_float(row.get('Komisi GrabKitchen')),
                        total=safe_float(row.get('Total')),
                        amount=safe_float(row.get('Amount')),
                        penjualan_bersih=safe_float(row.get('Penjualan bersih'))
                    )
                    reports.append(report)
                    total_reports += 1
                except (ValueError, TypeError) as e:
                    print(f"Error processing row: {e}")
                    continue

            # Bulk save reports for this file
            if reports:
                try:
                    db.session.bulk_save_objects(reports)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    # Handle reports one by one if bulk insert fails
                    for report in reports:
                        try:
                            db.session.add(report)
                            db.session.commit()
                            total_reports += 1
                        except IntegrityError:
                            db.session.rollback()
                            skipped_reports += 1
                        except Exception as e:
                            db.session.rollback()
                            print(f"Error saving report: {str(e)}")

        # Update store IDs in batches
        updated_count = update_store_ids_batch(store_id_map, 'grab')
        
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'store_ids_updated': updated_count
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@reports_bp.route('/upload/shopee', methods=['POST'])
def upload_report_shopee():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        store_id_map = {}  # To collect store IDs
        
        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)
            
            reports = []
            for row in reader:
                store_name = row.get('Store Name', '').strip()
                store_id = row.get('Store ID')
                if store_name and store_id:
                    store_id_map[store_name] = store_id

                # Get outlet code from store name
                # Get outlet code from store ID first, then fallback to name
                outlet = None
                if store_id:
                    outlet = Outlet.query.filter_by(store_id_shopee=store_id).first()
                if not outlet and store_name:
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                if not outlet:
                    continue  # Skip if outlet not found by either ID or name

                # Check if order already exists
                order_id = row.get('Order ID', '')
                if order_id:
                    existing_report = ShopeeReport.query.filter_by(order_id=order_id).first()
                    if existing_report:
                        skipped_reports += 1
                        continue

                try:
                    # Parse dates with the correct format (dd/MM/yyyy HH:mm:ss)
                    create_time = datetime.strptime(row.get('Order Create Time', ''), '%d/%m/%Y %H:%M:%S')
                    complete_time = None
                    if row.get('Order Complete/Cancel Time'):
                        complete_time = datetime.strptime(row.get('Order Complete/Cancel Time', ''), '%d/%m/%Y %H:%M:%S')

                    report = ShopeeReport(
                        brand_name=outlet.brand,
                        outlet_code=outlet.outlet_code,
                        transaction_type=row.get('Transaction Type', ''),
                        order_id=row.get('Order ID', ''),
                        order_pick_up_id=row.get('Order Pick up ID', ''),
                        store_id=store_id,
                        store_name=store_name,
                        order_create_time=create_time,
                        order_complete_cancel_time=complete_time,
                        order_amount=float(row.get('Order Amount', 0) or 0),
                        merchant_service_charge=float(row.get('Merchant Service Charge', 0) or 0),
                        pb1=float(row.get('PB1', 0) or 0),
                        merchant_surcharge_fee=float(row.get('Merchant Surcharge Fee', 0) or 0),
                        merchant_shipping_fee_voucher_subsidy=float(row.get('Merchant Shipping Fee Voucher Subsidy', 0) or 0),
                        food_direct_discount=float(row.get('Food Direct Discount', 0) or 0),
                        merchant_food_voucher_subsidy=float(row.get('Merchant Food Voucher Subsidy', 0) or 0),
                        subtotal=float(row.get('Subtotal', 0) or 0),
                        total=float(row.get('Total', 0) or 0),
                        commission=float(row.get('Commission', 0) or 0),
                        net_income=float(row.get('Net Income', 0) or 0),
                        order_status=row.get('Order Status', ''),
                        order_type=row.get('Order Type', '')
                    )
                    reports.append(report)
                    total_reports += 1
                except (ValueError, TypeError) as e:
                    print(f"Error processing row: {e}")
                    continue

            # Bulk save reports for this file
            if reports:
                try:
                    db.session.bulk_save_objects(reports)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    # Handle reports one by one if bulk insert fails
                    for report in reports:
                        try:
                            db.session.add(report)
                            db.session.commit()
                        except IntegrityError:
                            db.session.rollback()
                            skipped_reports += 1
                        except Exception as e:
                            db.session.rollback()
                            print(f"Error saving report: {str(e)}")

        # Update store IDs in batches
        updated_count = update_store_ids_batch(store_id_map, 'shopee')
        
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'store_ids_updated': updated_count
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@reports_bp.route('/', methods=['GET'])
def get_reports():
    report_type = request.args.get('type')
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    brand_name = request.args.get('brand_name')
    outlet_code = request.args.get('outlet_code')

    if not report_type:
        return jsonify({"error": "Report type is required. Choose from 'gojek', 'grabfood', or 'shopee'."}), 400

    try:
        # Determine the model and date attribute based on the report type
        if report_type == 'gojek':
            model = GojekReport
            date_attr = GojekReport.transaction_time
            convert_to_dict = convert_gojek_report_to_dict
        elif report_type == 'grabfood':
            model = GrabFoodReport
            date_attr = GrabFoodReport.tanggal_dibuat
            convert_to_dict = convert_grabfood_report_to_dict
        elif report_type == 'shopee':
            model = ShopeeReport
            date_attr = ShopeeReport.order_create_time
            convert_to_dict = convert_shopee_report_to_dict
        else:
            return jsonify({"error": "Invalid report type. Choose from 'gojek', 'grabfood', or 'shopee'."}), 400

        # Start with base query
        query = model.query

        # Apply filters if provided
        if brand_name:
            query = query.filter(model.brand_name == brand_name)
        if outlet_code:
            query = query.filter(model.outlet_code == outlet_code)

        # Apply date filters
        if start_date_param and end_date_param:
            start_date_obj = datetime.strptime(start_date_param, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date_param, '%Y-%m-%d')
            query = query.filter(date_attr >= start_date_obj,
                               date_attr <= end_date_obj)
        elif start_date_param:
            start_date_obj = datetime.strptime(start_date_param, '%Y-%m-%d')
            query = query.filter(date_attr >= start_date_obj)

        # Execute query
        reports = query.all()

    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    # Convert reports to a list of dictionaries using the appropriate function
    reports_data = [convert_to_dict(report) for report in reports]

    return jsonify(reports_data)

def convert_gojek_report_to_dict(report):
    return {
        "brand_name": report.brand_name,
        "outlet_code": report.outlet_code,
        "serial_no": report.serial_no,
        "waktu_transaksi": report.waktu_transaksi.strftime('%Y-%m-%d %H:%M'),
        "nomor_pesanan": report.nomor_pesanan,
        "total": report.gross_sales,
        "komisi_program": report.komisi_program,
        "nama_program": report.nama_program,
        "diskon_mitra_usaha": report.diskon_ditanggung_mitra,
        "total_biaya_komisi": report.total_biaya_komisi,
        "net_income": report.nett_sales,
    }

def convert_grabfood_report_to_dict(report):
    # Calculate commission safely handling None values
    points = float(report.poin_diberikan or 0)
    commission = float(report.komisi_grabkitchen or 0)
    total_commission = points - commission

    return {
        "brand_name": report.brand_name,
        "outlet_code": report.outlet_code,
        "nama_merchant": report.nama_merchant,
        "id_merchant": report.id_merchant,
        "nama_toko": report.nama_toko,
        "id_toko": report.id_toko,
        "waktu_transaksi": report.tanggal_dibuat.strftime('%Y-%m-%d %H:%M'),
        "jenis": report.jenis,
        "kategori": report.kategori,
        "subkategori": report.subkategori,
        "status": report.status,
        "id_transaksi": report.id_transaksi,
        "total_biaya_komisi": total_commission,
        "total": points,
        "net_income": commission,
    }

def convert_shopee_report_to_dict(report):
    return {
        "brand_name": report.brand_name,
        "outlet_code": report.outlet_code,
        "transaction_type": report.transaction_type,
        "order_id": report.order_id,
        "waktu_transaksi": report.order_create_time.strftime('%Y-%m-%d %H:%M'),
        "nama_merchant": report.store_name,
        "order_amount": report.order_amount,
        "total_biaya_komisi": report.commission,
        "total": report.total,
        "net_income": report.net_income,
    }



@reports_bp.route('/upload/cash', methods=['POST'])
def upload_cash_report():
    file = request.files.get('file')
    outlet_code = request.form.get('outlet_code')
    brand_name = request.form.get('brand_name')
    
    if not file:
        return jsonify({'msg': 'No file uploaded'}), 400
    if not outlet_code:
        return jsonify({'msg': 'Outlet code is required'}), 400

    try:
        # Read the file contents and parse as CSV
        file_contents = file.read().decode('utf-8')
        csv_file = StringIO(file_contents)
        reader = csv.DictReader(csv_file)

        reports = []
        for row in reader:
            # Skip empty rows or rows with insufficient data
            if not row['Tanggal'] or not row['Keterangan 1'] or not row[' Total']:
                continue

            # Process only if Keterangan 1 is either 'Penerimaan' or 'Pengeluaran'
            if row['Keterangan 1'].lower() in ['penerimaan', 'pengeluaran']:
                try:
                    # Parse the date
                    date = datetime.strptime(row['Tanggal'].strip(), '%d %b %Y')
                    
                    # Clean and parse the total amount
                    total_str = row[' Total'].strip().replace('.', '').replace(',', '.')
                    total = float(total_str)

                    # Map 'Penerimaan'/'Pengeluaran' to 'income'/'expense'
                    type_mapping = {
                        'penerimaan': 'income',
                        'pengeluaran': 'expense'
                    }
                    
                    # Create CashReport instance
                    report = CashReport(
                        tanggal=date,
                        outlet_code=outlet_code,
                        brand_name = brand_name,
                        type=type_mapping[row['Keterangan 1'].lower()],
                        details=row.get('Keterangan 2', ''),
                        total=total
                    )
                    reports.append(report)
                    
                except (ValueError, KeyError) as e:
                    continue

        # Bulk save all reports
        db.session.bulk_save_objects(reports)
        db.session.commit()

        return jsonify({'msg': 'Reports uploaded successfully', 'count': len(reports)}), 201

    except IntegrityError:
        db.session.rollback()
        return jsonify({'msg': 'Duplicate entry error'}), 500
    except Exception as e:
        db.session.rollback()
        return jsonify({'msg': str(e)}), 500


@reports_bp.route('/totals', methods=['GET'])
def get_reports_totals():
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    outlet_code = request.args.get('outlet_code')

    try:
        # Initialize queries
        gojek_query = GojekReport.query
        grab_query = GrabFoodReport.query
        shopee_query = ShopeeReport.query
        cash_query = CashReport.query
        manual_entries_query = ManualEntry.query

        # Apply outlet_code filter if provided
        if outlet_code:
            gojek_query = gojek_query.filter(GojekReport.outlet_code == outlet_code)
            grab_query = grab_query.filter(GrabFoodReport.outlet_code == outlet_code)
            shopee_query = shopee_query.filter(ShopeeReport.outlet_code == outlet_code)
            cash_query = cash_query.filter(CashReport.outlet_code == outlet_code)
            manual_entries_query = manual_entries_query.filter(ManualEntry.outlet_code == outlet_code)


        # Apply date filters if provided
        if start_date_param and end_date_param:
            start_date = datetime.strptime(start_date_param, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_param, '%Y-%m-%d')
            
            # Add one day to end_date to include the entire end date
            end_date_inclusive = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
            
            gojek_query = gojek_query.filter(
                GojekReport.transaction_date >= start_date,
                GojekReport.transaction_date <= end_date_inclusive
            )
            grab_query = grab_query.filter(
                GrabFoodReport.tanggal_dibuat >= start_date,
                GrabFoodReport.tanggal_dibuat <= end_date_inclusive
            )
            shopee_query = shopee_query.filter(
                ShopeeReport.order_create_time >= start_date,
                ShopeeReport.order_create_time <= end_date_inclusive
            )
            
            # Use SQLAlchemy filtering for cash reports instead of post-filtering
            cash_income_query = cash_query.filter(
                CashReport.type == 'income',
                CashReport.tanggal >= start_date,
                CashReport.tanggal <= end_date_inclusive
            )
            
            cash_expense_query = cash_query.filter(
                CashReport.type == 'expense',
                CashReport.tanggal >= start_date,
                CashReport.tanggal <= end_date_inclusive
            )
            
            manual_entries_query = manual_entries_query.filter(
                ManualEntry.start_date >= start_date,
                ManualEntry.end_date <= end_date_inclusive
            )

        # Calculate totals for each platform
        gojek_total = sum(float(report.nett_amount or 0) for report in gojek_query.all())
        grab_total = sum(float(report.total or 0) for report in grab_query.all())
        shopee_reports = shopee_query.all()
        shopee_total = sum(float(report.net_income or 0) for report in shopee_reports if report.order_status != "Cancelled")
        
        # Calculate cash totals using the filtered queries
        cash_income = sum(float(report.total or 0) for report in cash_income_query.all())
        cash_expense = sum(float(report.total or 0) for report in cash_expense_query.all())
        cash_net = cash_income - cash_expense

        # Calculate manual entries totals (expenses)
        manual_entries_total = sum(float(entry.amount or 0) for entry in manual_entries_query.all())

        # Calculate running total
        running_total = gojek_total + grab_total + shopee_total + cash_net - manual_entries_total

        # Prepare response
        response = {
            'outlet_code': outlet_code,
            'period': {
                'start_date': start_date_param,
                'end_date': end_date_param
            },
            'totals': {
                'gojek': round(gojek_total, 2),
                'grab': round(grab_total, 2),
                'shopee': round(shopee_total, 2),
                'cash': {
                    'income': round(cash_income, 2),
                    'expense': round(cash_expense, 2),
                    'net': round(cash_net, 2)
                },
                'manual_entries': round(manual_entries_total, 2),
                'running_total': round(running_total, 2)
            }
        }

        return jsonify(response), 200

    except ValueError as e:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        print(f"Error calculating totals: {str(e)}")
        return jsonify({'error': str(e)}), 500



# @reports_bp.route('/generate', methods=['GET'])
# def generate_report():
#     # Get parameters from query string
#     outlet_code = request.args.get('outlet_code')
#     start_date = request.args.get('start_date')
#     end_date = request.args.get('end_date')

#     # Validate parameters
#     if not all([outlet_code, start_date, end_date]):
#         return jsonify({'error': 'Missing required parameters: outlet_code, start_date, end_date'}), 400

#     try:
#         # Convert date strings to datetime objects
#         start_date = datetime.strptime(start_date, '%Y-%m-%d')
#         end_date = datetime.strptime(end_date, '%Y-%m-%d')

#         # Generate the report
#         filename = generate_daily_report(start_date, end_date, outlet_code)

#         # Return the generated PDF file
#         return send_file(
#             f'reports/{filename}',
#             mimetype='application/pdf',
#             as_attachment=True,
#             download_name=filename
#         )

#     except ValueError as e:
#         return jsonify({'error': str(e)}), 400
#     except Exception as e:
#         print(f"Error generating report: {str(e)}")
#         return jsonify({'error': str(e)}), 500
