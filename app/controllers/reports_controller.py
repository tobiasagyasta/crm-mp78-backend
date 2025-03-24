from flask import Blueprint, jsonify, request, Response, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.extensions import db
import pdfkit
import csv
from io import StringIO
from sqlalchemy import func
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from app.models.outlet import Outlet
from app.utils.report_generator import generate_daily_report

config = pdfkit.configuration(wkhtmltopdf='C:/Program Files/wkhtmltopdf/bin/wkhtmltopdf.exe')
reports_bp = Blueprint('reports', __name__, url_prefix='/reports')

import os


@reports_bp.route('/upload/grab', methods=['POST'])
def upload_report_grab():
    # Handle both single and multiple file uploads
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    outlet_code = request.form.get('outlet_code')
    brand_name = request.form.get('brand_name')
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400
    if not outlet_code:
        return jsonify({'msg': 'Outlet code is required'}), 400
    if not brand_name:
        return jsonify({'msg': 'Brand name is required'}), 400

    try:
        total_reports = 0
        for file in files:
            # Read the file contents and parse as CSV
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)

            reports = []
            for row in reader:
                def parse_number(value):
                    if value is None or value == '':
                        return None
                    # Remove thousand separators and handle negative values
                    cleaned_value = value.replace(',', '')
                    try:
                        # Convert to float directly to handle decimal points
                        return float(cleaned_value)
                    except ValueError:
                        return None

                def parse_date(date_str):
                    if not date_str:
                        return None
                    try:
                        return datetime.strptime(date_str.strip(), "%d %b %Y %I:%M %p")
                    except ValueError:
                        return None

                # Skip empty rows
                if not row.get('ID transaksi'):
                    continue

                report = GrabFoodReport(
                    outlet_code=outlet_code,
                    brand_name=brand_name,
                    nama_merchant=row.get('Nama Merchant','').strip(),  # Get first column and ensure it's not empty
                    id_merchant=row.get('ID Merchant'),
                    nama_toko=row.get('Nama toko'),
                    id_toko=row.get('ID toko'),
                    diperbarui_pada=parse_date(row.get('Diperbarui Pada')),
                    tanggal_dibuat=parse_date(row.get('Tanggal dibuat')),
                    jenis=row.get('Jenis'),
                    kategori=row.get('Kategori'),
                    subkategori=row.get('Subkategori'),
                    status=row.get('Status'),
                    id_transaksi=row.get('ID transaksi'),
                    id_transaksi_dihubungkan=row.get('ID Transaksi yang Dihubungkan'),
                    id_transaksi_partner_1=row.get('ID transaksi partner 1'),
                    id_transaksi_partner_2=row.get('ID transaksi partner 2'),
                    id_pesanan_panjang=row.get('ID pesanan panjang') or '',
                    id_pesanan_pendek=row.get('ID pesanan pendek') or '',
                    kode_booking=row.get('Kode booking') or '',
                    saluran_pesanan=row.get('Saluran pesanan') or '',
                    jenis_pesanan=row.get('Jenis pesanan') or '',
                    metode_pembayaran=row.get('Metode pembayaran') or '',
                    id_terminal=row.get('ID terminal'),
                    saluran=row.get('Saluran'),
                    tipe_promo=row.get('Tipe promo'),
                    biaya_grab_persen=parse_number(row.get('Biaya Grab (%)')),
                    pengali_poin=parse_number(row.get('Pengali Poin')),
                    poin_diberikan=parse_number(row.get('Poin yang Diberikan')),
                    id_pencairan_dana=row.get('ID pencairan dana'),
                    tanggal_transfer=parse_date(row.get('Tanggal transfer')),
                    amount=parse_number(row.get('Amount')) or 0,
                    pajak_atas_pesanan=parse_number(row.get('Pajak atas pesanan')),
                    biaya_kemasan=parse_number(row.get('Biaya kemasan')),
                    biaya_pelanggan_tidak_ikut_keanggotaan=parse_number(row.get('Biaya untuk Pelanggan yang Tidak Ikut Keanggotaan')),
                    biaya_layanan_restoran=parse_number(row.get('Biaya layanan restoran')),
                    promo=parse_number(row.get('Promo')),
                    diskon_dibiayai_merchant=parse_number(row.get('Diskon (Dibiayai Merchant)')),
                    diskon_ongkos_kirim_dibiayai_merchant=parse_number(row.get('Diskon Ongkos Kirim (Dibiayai Merchant)')),
                    ongkos_kirim_ditanggung_merchant_online=parse_number(row.get('Ongkos Kirim yang Ditanggung Merchant (untuk Toko Online Grab)')),
                    ongkos_kirim_ditanggung_merchant_pengantaran=parse_number(row.get('Ongkos Kirim yang Ditanggung Merchant (Pengantaran oleh Merchant)')),
                    biaya_layanan_pengiriman_grabexpress=parse_number(row.get('Biaya Layanan Pengiriman GrabExpress')),
                    penjualan_bersih=parse_number(row.get('Penjualan bersih')) or 0,
                    nilai_mdr_bersih=parse_number(row.get('Nilai MDR bersih')),
                    pajak_mdr=parse_number(row.get('Pajak MDR')),
                    biaya_grab=parse_number(row.get('Biaya Grab')) or 0,
                    biaya_sukses_pemasaran=parse_number(row.get('Biaya sukses pemasaran')) or 0,
                    komisi_pengantaran=parse_number(row.get('Komisi pengantaran')),
                    komisi_saluran=parse_number(row.get('Komisi saluran')),
                    komisi_pesanan=parse_number(row.get('Komisi Pesanan')),
                    komisi_lain_grabfood_grabmart=parse_number(row.get('Komisi Lain dari GrabFood / GrabMart')),
                    komisi_grabkitchen=parse_number(row.get('Komisi GrabKitchen')),
                    komisi_lain_grabkitchen=parse_number(row.get('Komisi Lain dari GrabKitchen')),
                    pajak_pemotongan=parse_number(row.get('Pajak pemotongan')),
                    total=parse_number(row.get('Total')) or 0,
                    pajak_atas_mdr_persen=parse_number(row.get('Pajak atas MDR (%)')),
                    komisi_pengantaran_persen=parse_number(row.get('Komisi Pengantaran (%)')),
                    komisi_saluran_persen=parse_number(row.get('Komisi Saluran (%)')),
                    komisi_pesanan_persen=parse_number(row.get('Komisi Pesanan (%)')),
                    pajak_atas_komisi_grabfood_grabmart=parse_number(row.get('Pajak atas Komisi GrabFood / GrabMart, Penyesuaian, Iklan')) or 0,
                    penyesuaian_iklan=parse_number(row.get('Penyesuaian Iklan')),
                    pajak_atas_total_komisi_grabkitchen=parse_number(row.get('Pajak atas Total Komisi GrabKitchen')),
                    alasan_pembatalan=row.get('Alasan pembatalan'),
                    dibatalkan_oleh=row.get('Dibatalkan oleh'),
                    alasan_pengembalian_dana=row.get('Alasan pengembalian dana'),
                    deskripsi=row.get('Deskripsi'),
                    kelompok_insiden=row.get('Kelompok insiden'),
                    nama_insiden=row.get('Nama insiden'),
                    item_terdampak=row.get('Item yang terdampak'),
                    link_untuk_banding=row.get('Link untuk banding'),
                    status_banding=row.get('Status banding')
                )
                reports.append(report)

            if reports:
                db.session.bulk_save_objects(reports)
                total_reports += len(reports)

        # Commit all changes after processing all files
        if total_reports > 0:
            db.session.commit()
            return jsonify({
                'msg': 'Reports uploaded successfully',
                'type': 'GrabFood',
                'files_processed': len(files),
                'total_records': total_reports
            }), 201
        else:
            return jsonify({'msg': 'No valid records found in the CSV files'}), 400

    except IntegrityError as e:
        db.session.rollback()
        print(f"IntegrityError details: {str(e)}")
        return jsonify({'msg': 'Duplicate entry error', 'error': str(e)}), 400
    except Exception as e:
        db.session.rollback()
        print(f"Error details: {str(e)}")
        return jsonify({'msg': str(e)}), 500
  

@reports_bp.route('/upload/shopee', methods=['POST'])
def upload_report_shopee():
    # Handle both single and multiple file uploads
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    outlet_code = request.form.get('outlet_code')
    brand_name = request.form.get('brand_name')
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400
    if not outlet_code:
        return jsonify({'msg': 'Outlet code is required'}), 400

    try:
        total_reports = 0
        for file in files:
            # Read the file contents and parse as CSV
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)

            reports = []
            for row in reader:
                # Convert empty strings to None or 0 where appropriate
                def parse_number(value):
                    return None if value == '' else float(value) if value.replace('.', '', 1).isdigit() else None

                report = ShopeeReport(
                    outlet_code=outlet_code,  # Add this line
                    brand_name = brand_name,
                    transaction_type=row['Transaction Type'],
                    order_id=row['Order ID'],
                    order_pick_up_id=row.get('Order Pick up ID'),
                    store_id=row.get('Store ID'),
                    store_name=row.get('Store Name'),
                    order_create_time=datetime.strptime(row['Order Create Time'], "%d/%m/%Y %H:%M:%S"),
                    order_complete_cancel_time=datetime.strptime(row['Order Complete/Cancel Time'], "%d/%m/%Y %H:%M:%S") if row.get('Order Complete/Cancel Time') else None,
                    order_amount=parse_number(row['Order Amount']),
                    merchant_service_charge=parse_number(row['Merchant Service Charge']),
                    pb1=parse_number(row['PB1']),
                    merchant_surcharge_fee=parse_number(row['Merchant Surcharge Fee']),
                    merchant_shipping_fee_voucher_subsidy=parse_number(row['Merchant Shipping Fee Voucher Subsidy']),
                    food_direct_discount=parse_number(row['Food Direct Discount']),
                    merchant_food_voucher_subsidy=parse_number(row['Merchant Food Voucher Subsidy']),
                    subtotal=parse_number(row['Subtotal']),
                    total=parse_number(row['Total']),
                    commission=parse_number(row['Commission']),
                    net_income=parse_number(row['Net Income']),
                    order_status=row.get('Order Status'),
                    order_type=row.get('Order Type')
                )
                reports.append(report)

            # Insert reports into the database
            db.session.bulk_save_objects(reports)
            total_reports += len(reports)

        # Commit all changes after processing all files
        db.session.commit()
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'type': 'Shopee',
            'files_processed': len(files),
            'total_records': total_reports
        }), 201

    except IntegrityError as e:
        db.session.rollback()
        print(f"IntegrityError details: {str(e)}")  # Add this for debugging
        return jsonify({'msg': f'Duplicate entry error: {str(e)}'}), 500
    except Exception as e:
        db.session.rollback()
        return jsonify({'msg': str(e)}), 500


@reports_bp.route('/upload/gojek', methods=['POST'])
def upload_report_gojek():
    # Handle both single and multiple file uploads
    files = request.files.getlist('file')  # Changed from 'file[]' to 'file'
    if not files:
        files = [request.files.get('file')]  # Try single file upload
    
    outlet_code = request.form.get('outlet_code')
    brand_name = request.form.get('brand_name')
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400
    if not outlet_code:
        return jsonify({'msg': 'Outlet code is required'}), 400

    try:
        total_reports = 0
        for file in files:
            # Read the file contents and parse as CSV
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.DictReader(csv_file)

            reports = []
            for row in reader:
                # Convert empty strings to None or 0 where appropriate
                def parse_number(value):
                    return None if value == '' else float(value) if value.replace('.', '', 1).isdigit() else None

                report = GojekReport(
                    outlet_code=outlet_code,
                    brand_name=brand_name,
                    serial_no=row['Serial No'],
                    waktu_transaksi=row['Waktu Transaksi'],
                    nomor_pesanan=row['Nomor Pesanan'],
                    currency=row['Currency'],
                    gross_sales=parse_number(row['Gross Sales']),
                    komisi_program=parse_number(row['Komisi Program']),
                    nama_program=row['Nama Program'],
                    biaya_komisi=parse_number(row['Biaya Komisi (diluar komisi program)']),
                    diskon_ditanggung_mitra=parse_number(row['Diskon ditanggung Mitra Usaha']),
                    voucher_commission=parse_number(row.get('Voucher commission')),
                    total_biaya_komisi=parse_number(row['Total Biaya Komisi']),
                    nett_sales=parse_number(row['Nett Sales']),
                )
                reports.append(report)

            # Sort the reports by 'waktu_transaksi' (oldest to newest)
            reports.sort(key=lambda x: datetime.strptime(x.waktu_transaksi, "%Y-%m-%dT%H:%M:%S.%f%z"))
            
            # Insert reports into the database
            db.session.bulk_save_objects(reports)
            total_reports += len(reports)

        # Commit all changes after processing all files
        db.session.commit()
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'type': 'Gojek',
            'files_processed': len(files),
            'total_records': total_reports
        }), 201

    except IntegrityError:
        db.session.rollback()
        print(f"IntegrityError details: {str(e)}")  # Add this for debugging
        return jsonify({'msg': f'Duplicate entry error: {str(e)}'}), 500
    except Exception as e:
        db.session.rollback()
        return jsonify({'msg': str(e)}), 500



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
            date_attr = GojekReport.waktu_transaksi
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
                GojekReport.waktu_transaksi >= start_date,
                GojekReport.waktu_transaksi <= end_date_inclusive
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
        gojek_total = sum(float(report.nett_sales or 0) for report in gojek_query.all())
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



@reports_bp.route('/generate', methods=['GET'])
def generate_report():
    # Get parameters from query string
    outlet_code = request.args.get('outlet_code')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Validate parameters
    if not all([outlet_code, start_date, end_date]):
        return jsonify({'error': 'Missing required parameters: outlet_code, start_date, end_date'}), 400

    try:
        # Convert date strings to datetime objects
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # Generate the report
        filename = generate_daily_report(start_date, end_date, outlet_code)

        # Return the generated PDF file
        return send_file(
            f'reports/{filename}',
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        return jsonify({'error': str(e)}), 500
