from flask import Blueprint, jsonify, request, Response, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.models.gojek_reports import GojekReport
from app.models.shopee_reports import ShopeeReport
from app.models.grabfood_reports import GrabFoodReport
from app.models.cash_reports import CashReport
from app.models.manual_entry import ManualEntry
from app.models.shopeepay_reports import ShopeepayReport
from app.models.bank_mutations import BankMutation
from app.models.expense_category import ExpenseCategory
from app.models.income_category import IncomeCategory
from app.models.pukis import Pukis
from app.models.tiktok_reports import TiktokReport
from app.extensions import db
import sys

from datetime import datetime

def parse_date(date_str, default_year=None):
    date_str = date_str.strip()
    formats = [
        '%d-%b-%y',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y-%m-%d',
        '%d-%b'  # Add this format for '10-Apr'
    ]
    for fmt in formats:
        try:
            if fmt == '%d-%b':
                # If no year, use current year or provided default_year
                dt = datetime.strptime(date_str, fmt)
                year = default_year or datetime.now().year
                return dt.replace(year=year)
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unknown date format: {date_str}")
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

@reports_bp.route('/upload/shopee_adjustment', methods=['POST'])
def upload_report_shopee_adjustment():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        store_id_map = {}
        
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

                outlet = None
                if store_id:
                    outlet = Outlet.query.filter_by(store_id_shopee=store_id).first()
                if not outlet and store_name:
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                if not outlet:
                    print(f"SKIPPED: Outlet not found for Store Name '{store_name}', Store ID '{store_id}' | Row: {row}")
                    skipped_reports += 1
                    continue

                # Check if adjustment already exists
                refund_id = row.get('Wallet Adjustment ID', '')
                adjustment_time_str = row.get('Wallet Adjustment Time', '')
                adjustment_time = None
                if adjustment_time_str:
                    try:
                        adjustment_time = datetime.strptime(adjustment_time_str, '%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        print(f"SKIPPED: Invalid adjustment time format: {adjustment_time_str} | Row: {row}")
                        skipped_reports += 1
                        continue

                if refund_id and adjustment_time:
                    existing_report = ShopeeReport.query.filter_by(
                        order_id=refund_id,
                        transaction_type='Adjustment',
                        order_create_time=adjustment_time
                    ).first()
                    if existing_report:
                        print(f"SKIPPED: Duplicate adjustment for Wallet Adjustment ID '{refund_id}' and time '{adjustment_time}' | Row: {row}")
                        skipped_reports += 1
                        continue

                try:
                    # Parse the adjustment time
                    adjustment_time = datetime.strptime(row.get('Wallet Adjustment Time', ''), '%Y-%m-%d %H:%M:%S')

                    report = ShopeeReport(
                        brand_name=outlet.brand,
                        outlet_code=outlet.outlet_code,
                        transaction_type='Adjustment',  # Mark as adjustment
                        order_id=row.get('Wallet Adjustment ID', ''),  # Use refund ID as order ID
                        store_id=store_id,
                        store_name=store_name,
                        order_create_time=adjustment_time,
                        order_amount=float(row.get('Wallet Adjustment Amount', 0) or 0),
                        total=float(row.get('Wallet Adjustment Amount', 0) or 0),
                        net_income=float(row.get('Wallet Adjustment Amount', 0) or 0),
                        order_status='Completed',
                        order_type=row.get('Wallet Adjustment Reason', 'Adjustment')
                    )
                    reports.append(report)
                    total_reports += 1
                except (ValueError, TypeError) as e:
                    print(f"SKIPPED: Error processing row: {e} | Row: {row}")
                    skipped_reports += 1
                    continue

            if reports:
                try:
                    db.session.bulk_save_objects(reports)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
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

        updated_count = update_store_ids_batch(store_id_map, 'shopee')
        
        return jsonify({
            'msg': 'Adjustment reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'store_ids_updated': updated_count
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@reports_bp.route('/upload/mutation', methods=['POST'])
def upload_report_mutation():
    files = request.files.getlist('file')
    rekening_number = request.form.get('rekening_number')

    if not files:
        files = [request.files.get('file')]
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400
    if not rekening_number:
        return jsonify({'msg': 'Rekening number is required'}), 400

    total_mutations = 0
    skipped_mutations = 0
    debug_skipped = []

    try:
        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip header

            mutations = []
            for idx, row in enumerate(reader):
                try:
                    # Skip if the date column is 'PEND'
                    if row and row[0].strip().upper() == 'PEND':
                        skipped_mutations += 1
                        debug_skipped.append({
                            'row_number': idx + 2,
                            'reason': "Date column is 'PEND'",
                            'row': row
                        })
                        continue

                    parser = BankMutation.detect_platform(row)
                    if not parser:
                        skipped_mutations += 1
                        debug_skipped.append({
                            'row_number': idx + 2,
                            'reason': 'Unknown platform',
                            'row': row
                        })
                        continue

                    parsed = parser(row)
                    # if not parsed:
                    #     skipped_mutations += 1
                    #     debug_skipped.append({
                    #         'row_number': idx + 2,
                    #         'reason': 'Parse failed (parser returned None)',
                    #         'row': row
                    #     })
                    #     continue

                    mutation = BankMutation(
                        rekening_number=rekening_number,
                        **parsed
                    )

                    # Check for duplicates in the database only
                    exists = BankMutation.query.filter_by(
                        tanggal=mutation.tanggal,
                        transaction_amount=mutation.transaction_amount,
                        platform_code=mutation.platform_code,
                    ).first()
                    if exists:
                        skipped_mutations += 1
                        debug_skipped.append({
                            'row_number': idx + 2,
                            'reason': 'Duplicate mutation entry',
                            'row': row
                        })
                        continue

                    mutations.append(mutation)
                    total_mutations += 1

                except Exception as e:
                    skipped_mutations += 1
                    debug_skipped.append({
                        'row_number': idx + 2,
                        'reason': f'Exception: {str(e)}',
                        'row': row
                    })

            # Bulk save mutations for this file
            if mutations:
                try:
                    db.session.bulk_save_objects(mutations)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    # Handle mutations one by one if bulk insert fails
                    for mutation in mutations:
                        try:
                            db.session.add(mutation)
                            db.session.commit()
                        except IntegrityError:
                            db.session.rollback()
                            skipped_mutations += 1
                        except Exception as e:
                            db.session.rollback()
                            debug_skipped.append({
                                'row_number': None,
                                'reason': f'Error saving mutation: {str(e)}',
                                'row': None
                            })

      
        seen_reasons = set()
        one_per_reason = []

        for entry in debug_skipped:
            reason = entry['reason']
            if reason not in seen_reasons:
                one_per_reason.append(entry)
                seen_reasons.add(reason)

        return jsonify({
            'msg': 'Bank mutations uploaded successfully',
            'total_records': total_mutations,
            'skipped_records': skipped_mutations,
            'skipped_rows_debug': one_per_reason
        }), 201


    except Exception as e:
        db.session.rollback()
        return jsonify({
            'error': str(e),
            'skipped_rows_debug': debug_skipped
        }), 500

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

@reports_bp.route('/upload/shopeepay', methods=['POST'])
def upload_report_shopeepay():
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
                store_name = row.get('Merchant/Store Name', '').strip()
                entity_id = row.get('Entity ID')
                if store_name and entity_id:
                    store_id_map[store_name] = entity_id

                # Get outlet code from entity ID first, then fallback to name
                outlet = None
                if entity_id:
                    outlet = Outlet.query.filter_by(store_id_shopee=entity_id).first()
                if not outlet and store_name:
                    outlet = Outlet.query.filter_by(outlet_name_grab=store_name).first()
                if not outlet:
                    continue  # Skip if outlet not found by either ID or name

                # Check if transaction already exists
                transaction_id = row.get('Transaction ID', '')
                if transaction_id:
                    existing_report = ShopeepayReport.query.filter_by(transaction_id=transaction_id).first()
                    if existing_report:
                        skipped_reports += 1
                        continue

                try:
                    # Parse dates with the correct format
                    create_time = datetime.strptime(row.get('Create Time', ''), '%Y-%m-%d %H:%M:%S')
                    update_time = None
                    if row.get('Update Time'):
                        update_time = datetime.strptime(row.get('Update Time', ''), '%Y-%m-%d %H:%M:%S')

                    def safe_float(value):
                        if not value:
                            return 0
                        try:
                            return float(str(value).replace(',', ''))
                        except (ValueError, TypeError):
                            return 0

                    report = ShopeepayReport(
                        brand_name=outlet.brand,
                        outlet_code=outlet.outlet_code,
                        merchant_host=row.get('Merchant Host', ''),
                        partner_merchant_id=row.get('Partner Merchant ID', ''),
                        merchant_store_name=store_name,
                        transaction_type=row.get('Transaction Type', ''),
                        merchant_scope=row.get('Merchant Scope', ''),
                        transaction_id=transaction_id,
                        reference_id=row.get('Reference ID', ''),
                        parent_id=row.get('Parent ID', ''),
                        external_reference_id=row.get('External Reference ID', ''),
                        issuer_identifier=row.get('Issuer Identifier', ''),
                        transaction_amount=safe_float(row.get('Transaction Amount')),
                        fee_mdr=safe_float(row.get('Fee (MDR)')),
                        settlement_amount=safe_float(row.get('Settlement Amount')),
                        terminal_id=row.get('Terminal ID', ''),
                        create_time=create_time,
                        update_time=update_time,
                        adjustment_reason=row.get('Adjustment Reason', ''),
                        entity_id=entity_id,
                        fee_cofunding=safe_float(row.get('Fee (Cofunding)')),
                        reward_amount=safe_float(row.get('Reward Amount')),
                        reward_type=row.get('Reward Type', ''),
                        promo_type=row.get('Promo Type', ''),
                        payment_method=row.get('Payment Method', ''),
                        currency_code=row.get('Currency Code', ''),
                        voucher_promotion_event_name=row.get('Voucher Promotion Event Name', ''),
                        payment_option=row.get('Payment Option', ''),
                        fee_withdrawal=safe_float(row.get('Fee (Withdrawal)')),
                        fee_handling=safe_float(row.get('Fee (Handling)'))
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
                            print(f"Error saving report - Store: {report.store_name}, Order ID: {report.order_id}, Error: {str(e)}")

        # Update store IDs in batches
        updated_count = update_store_ids_batch(store_id_map, 'shopeepay')
        
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'store_ids_updated': updated_count
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    
@reports_bp.route('/upload/tiktok', methods=['POST'])
def upload_report_tiktok():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_reports = 0
        skipped_reports = 0
        debug_skipped = []

        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.reader(csv_file)
            for _ in range(4):
                next(reader, None)  # Skip first 4 rows

            reports = []
            for idx, row in enumerate(reader):
                parsed = TiktokReport.parse_tiktok_row(row)
                print(f"DEBUG Parsed Row {idx+1}: {parsed}") 

                if parsed:
                     # Duplicate check: adjust fields as needed for your business logic
                    exists = TiktokReport.query.filter_by(
                        outlet_order_id=parsed['outlet_order_id'],
                        order_time=parsed['order_time']
                    ).first()
                    if exists:
                        skipped_reports += 1
                        debug_skipped.append({
                            'row_number': idx + 2,
                            'reason': 'Duplicate entry',
                            'row': row
                        })
                        continue
                    report = TiktokReport(**parsed)
                    reports.append(report)
                    total_reports += 1
                else:
                    skipped_reports += 1
                    debug_skipped.append({
                        'row_number': idx + 2,
                        'reason': 'Parse failed or outlet not found',
                        'row': row
                    })

            if reports:
                db.session.bulk_save_objects(reports)
                db.session.commit()

        return jsonify({
            'msg': 'Tiktok reports uploaded successfully',
            'total_records': total_reports,
            'skipped_records': skipped_reports,
            'skipped_rows_debug': debug_skipped
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500



@reports_bp.route('/upload/manual', methods=['POST'])
def upload_manual_entry():
    files = request.files.getlist('file')
    if not files:
        files = [request.files.get('file')]
    
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400

    try:
        total_entries = 0
        skipped_entries = 0
        
        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.reader(csv_file)
            
            # Skip the first three rows (header and title)
            for _ in range(3):
                next(reader)
            
            entries = []
            for row in reader:
                try:
                    if len(row) < 7:  # Ensure we have all required columns
                        skipped_entries += 1
                        continue

                    # Skip empty or malformed date fields
                    date_str = row[0].strip()
                    if not date_str:
                        skipped_entries += 1
                        continue

                    try:
                        entry_date = parse_date(date_str, default_year=datetime.now().year)
                    except ValueError:
                        skipped_entries += 1
                        continue

                    # Parse amount (remove commas and convert to float)
                    amount_str = row[2].replace('.','').replace(',', '').strip()
                    if not amount_str:
                        skipped_entries += 1
                        continue

                    try:
                        amount = float(amount_str)
                    except ValueError:
                        skipped_entries += 1
                        continue

                    # Determine entry type and category
                    entry_type_str = row[3].strip().lower()
                    category_name = row[4].strip()
                    if entry_type_str == 'pengeluaran':
                        entry_type = 'expense'
                        category = ExpenseCategory.query.filter_by(name=category_name).first()
                    elif entry_type_str == 'penerimaan':
                        entry_type = 'income'
                        category = IncomeCategory.query.filter_by(name=category_name).first()
                    else:
                        skipped_entries += 1
                        continue

                    if not category:
                        skipped_entries += 1
                        continue

                      # Create manual entry
                    entry_date = parse_date(date_str, default_year=datetime.now().year)
                    outlet_code = row[6].strip()
                    category_id = category.id if category else None

                    # Check for existing entry
                    existing_entry = ManualEntry.query.filter(
                        ManualEntry.start_date == entry_date,
                        ManualEntry.outlet_code == outlet_code,
                        ManualEntry.category_id == category_id,
                        ManualEntry.amount == amount,
                        ManualEntry.description == row[5].strip()
                    ).first()

                    if existing_entry:
                        skipped_entries += 1
                        continue

                    # Create manual entry
                    entry = ManualEntry(
                        outlet_code=outlet_code,
                        brand_name='MP78',  # Assuming brand is always '78'
                        entry_type=entry_type,
                        amount=amount,
                        description=row[5].strip(),
                        start_date=entry_date,
                        end_date=entry_date,
                        category_id=category_id
                    )
                    entries.append(entry)
                    total_entries += 1


                except (ValueError, IndexError) as e:
                    print(f"Error processing row: {e}")
                    skipped_entries += 1
                    continue

            # Bulk save entries
            if entries:
                try:
                    db.session.bulk_save_objects(entries)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    # Handle entries one by one if bulk insert fails
                    for entry in entries:
                        try:
                            db.session.add(entry)
                            db.session.commit()
                        except IntegrityError:
                            db.session.rollback()
                            skipped_entries += 1
                        except Exception as e:
                            db.session.rollback()
                            print(f"Error saving entry: {str(e)}")

        return jsonify({
            'msg': 'Manual entries uploaded successfully',
            'total_records': total_entries,
            'skipped_records': skipped_entries
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
            
            # Debug: Print column names
            print(f"Raw fieldnames: {reader.fieldnames}", file=sys.stderr, flush=True)
            
            # Clean column names by stripping spaces
            reader.fieldnames = [field.strip() for field in reader.fieldnames]
            
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


@reports_bp.route('/upload/cash', methods=['POST'])
def upload_cash_report():
    file = request.files.get('file')
    outlet_code = request.form.get('outlet_code')
    brand_name = request.form.get('brand_name')
    
    if not file:
        return jsonify({'msg': 'No file uploaded'}), 400
    if not outlet_code:
        return jsonify({'msg': 'Outlet code is required'}), 400

    pukis_mapping = {
        'Pukis Produksi': ('produksi', 'jumbo'),
        'Pukis Terjual Total Jumbo': ('terjual', 'jumbo'),
        'Pukis Retur': ('retur', 'jumbo'),
        'Pukis Free': ('free', 'jumbo'),
        'Pukis Produksi KLASIK': ('produksi', 'klasik'),
        'Pukis Terjual Total KLASIK': ('terjual', 'klasik'),
        'Pukis Retur KLASIK': ('retur', 'klasik'),
        'Pukis Free KLASIK': ('free', 'klasik'),
    }

    try:
        file_contents = file.read().decode('utf-8')
        csv_file = StringIO(file_contents)
        reader = csv.reader(csv_file)
        reports = []
        pukis_reports = []
        skipped_duplicates = 0
        skipped_pukis_duplicates = 0
        debug_skipped = []
        header = next(reader, None)  # Skip header row
        for idx, row in enumerate(reader):
            if len(row) < 5:
                debug_skipped.append({
                    'row_number': idx + 2,
                    'reason': 'Not enough columns',
                    'row': row
                })
                continue
            date_str = row[0].strip()
            keterangan1 = row[2].strip()
            details = row[3].strip()
            total_str = row[4].strip().replace('.', '').replace(',', '.')
            if not date_str or not keterangan1 or not total_str:
                debug_skipped.append({
                    'row_number': idx + 2,
                    'reason': 'Missing required fields',
                    'row': row
                })
                continue
            # Handle Pukis rows
            if keterangan1 in pukis_mapping:
                try:
                    tanggal = datetime.strptime(date_str, '%d %b %Y')
                    amount = float(total_str)
                    pukis_inventory_type, pukis_product_type = pukis_mapping[keterangan1]
                    # Duplicate check for Pukis
                    existing_pukis = Pukis.query.filter_by(
                        tanggal=tanggal,
                        outlet_code=outlet_code,
                        brand_name=brand_name,
                        pukis_inventory_type=pukis_inventory_type,
                        pukis_product_type=pukis_product_type
                    ).first()
                    if existing_pukis:
                        skipped_pukis_duplicates += 1
                        debug_skipped.append({
                            'row_number': idx + 2,
                            'reason': 'Duplicate Pukis entry',
                            'row': row
                        })
                        continue
                    pukis_report = Pukis(
                        tanggal=tanggal,
                        outlet_code=outlet_code,
                        brand_name=brand_name,
                        pukis_inventory_type=pukis_inventory_type,
                        pukis_product_type=pukis_product_type,
                        amount=amount
                    )
                    pukis_reports.append(pukis_report)
                except Exception as e:
                    debug_skipped.append({
                        'row_number': idx + 2,
                        'reason': f'Pukis parse error: {str(e)}',
                        'row': row
                    })
                continue  # Don't process as CashReport
            # ...existing code for cash report rows...
            type_str = keterangan1.lower()
            if type_str not in ['penerimaan', 'pengeluaran']:
                debug_skipped.append({
                    'row_number': idx + 2,
                    'reason': f'Unknown type: {type_str}',
                    'row': row
                })
                continue
            try:
                date = datetime.strptime(date_str, '%d %b %Y')
                total = float(total_str)
            except Exception as e:
                debug_skipped.append({
                    'row_number': idx + 2,
                    'reason': f'Parse error: {str(e)}',
                    'row': row
                })
                continue
            existing_record = CashReport.query.filter(
                CashReport.tanggal == date,
                CashReport.total == total,
                CashReport.outlet_code == outlet_code
            ).first()
            if existing_record:
                skipped_duplicates += 1
                debug_skipped.append({
                    'row_number': idx + 2,
                    'reason': 'Duplicate entry',
                    'row': row
                })
                continue
            type_mapping = {
                'penerimaan': 'income',
                'pengeluaran': 'expense'
            }
            report = CashReport(
                tanggal=date,
                outlet_code=outlet_code,
                brand_name=brand_name,
                type=type_mapping[type_str],
                details=details,
                total=total
            )
            reports.append(report)
        if reports:
            db.session.bulk_save_objects(reports)
        if pukis_reports:
            db.session.bulk_save_objects(pukis_reports)
        if reports or pukis_reports:
            db.session.commit()
        return jsonify({
            'msg': 'Reports uploaded successfully',
            'cash_count': len(reports),
            'pukis_count': len(pukis_reports),
            'skipped_duplicates': skipped_duplicates,
            'skipped_pukis_duplicates': skipped_pukis_duplicates,
            'skipped_rows_debug': debug_skipped
        }), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({'msg': 'Duplicate entry error'}), 500
    except Exception as e:
        db.session.rollback()
        return jsonify({'msg': str(e)}), 500

@reports_bp.route('/upload/pkb', methods=['POST'])
def upload_report_pkb():
    files = request.files.getlist('file')
    rekening_number = request.form.get('rekening_number')

    if not files:
        files = [request.files.get('file')]
    if not files or not files[0]:
        return jsonify({'msg': 'No files uploaded'}), 400
    if not rekening_number:
        return jsonify({'msg': 'Rekening number is required'}), 400

    try:
        total_mutations = 0
        skipped_mutations = 0

        for file in files:
            file_contents = file.read().decode('utf-8')
            csv_file = StringIO(file_contents)
            reader = csv.reader(csv_file)

            # Skip header
            next(reader, None)


            mutations = []
            sosmed_total = 0
            sosmed_date = None
            sosmed_area = None
            sosmed_type = None
            avanger_totals = {
                'UM AVANGER': {'total': 0, 'date': None},
                'GAJI AVANGER': {'total': 0, 'date': None}
            }

            for row in reader:
                try:
                    if len(row) < 2:
                        continue  # Not enough columns

                    # Parse date from column 0
                    try:
                        tanggal = datetime.strptime(row[0].strip(), '%d/%m/%Y').date()
                    except ValueError:
                        skipped_mutations += 1
                        continue

                    transaksi_text = row[1].strip()
                    if not transaksi_text:
                        skipped_mutations += 1
                        continue

                    mutation = BankMutation(
                        rekening_number=rekening_number,
                        tanggal=tanggal,
                        transaksi=transaksi_text,
                    )
                    mutation.parse_pkb_transaction()
                    sosmed_info = mutation.parse_pkb_sosmed_row(row)
                    dana_info = mutation.parse_pkb_dana_row(row)
                    avanger_info = mutation.parse_pkb_avanger_row(row)
                    if sosmed_info:
                        sosmed_total += sosmed_info['amount']
                        sosmed_date = sosmed_info['tanggal']
                        sosmed_type = sosmed_info.get('type', 'ALL')
                        sosmed_area = sosmed_info.get('area')
                        continue  # Do not process this row as a mutation
                    # Process DANA row: match phone to Outlet.pic_phone and create manual entry
                    if dana_info:
                        outlet = Outlet.query.filter_by(pic_phone=dana_info['phone']).first()
                        if not outlet:
                            print("Outlet not found for phone:", dana_info['phone'])
                        if outlet:
                            category = ExpenseCategory.query.filter_by(name='DANA Transfer').first()
                            existing_entry = ManualEntry.query.filter(
                                ManualEntry.outlet_code == outlet.outlet_code,
                                ManualEntry.category_id == (category.id if category else None),
                                ManualEntry.start_date == dana_info['tanggal'],
                                ManualEntry.end_date == dana_info['tanggal'],
                                ManualEntry.entry_type == 'expense',
                                func.abs(ManualEntry.amount - dana_info['amount']) < 0.01
                            ).first()
                            if not existing_entry:
                                manual_entry = ManualEntry(
                                    outlet_code=outlet.outlet_code,
                                    brand_name=outlet.brand,
                                    entry_type='expense',
                                    amount=dana_info['amount'],
                                    description=f"DANA Transfer at {dana_info['tanggal'].strftime('%d %b %Y')}",
                                    start_date=dana_info['tanggal'],
                                    end_date=dana_info['tanggal'],
                                    category_id=category.id if category else None
                                )
                                db.session.add(manual_entry)
                                db.session.commit()
                        continue  # Do not process this row as a mutation
                    # Process AVANGER row: accumulate totals for UM/GAJI AVANGER
                    if avanger_info:
                        av_type = avanger_info['type']
                        avanger_totals[av_type]['total'] += avanger_info['amount']
                        avanger_totals[av_type]['date'] = avanger_info['tanggal']
                        avanger_totals[av_type]['description'] = avanger_info['description']  # <-- Add this line

                        continue  # Do not process this row as a mutation
                    # Only save valid PKB mutations
                    if mutation.platform_name == "PKB" and mutation.platform_code:
                        exists = BankMutation.query.filter_by(
                            tanggal=mutation.tanggal,
                            transaction_amount=mutation.transaction_amount,
                            platform_code=mutation.platform_code,
                        ).first()
                        if exists:
                            skipped_mutations += 1
                            continue

                        mutations.append(mutation)
                        total_mutations += 1

                except Exception as e:
                    print(f"Error parsing row: {e}")
                    skipped_mutations += 1
                    continue

            # Save to DB
            if mutations:
                try:
                    db.session.bulk_save_objects(mutations)
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    for mutation in mutations:
                        try:
                            db.session.add(mutation)
                            db.session.commit()
                        except Exception as e:
                            db.session.rollback()
                            print(f"Error saving mutation: {e}")
                            skipped_mutations += 1

            # After processing all rows, distribute sosmed_total as manual entries
            outlets = None
            n = 0
            if sosmed_type == 'AREA' and sosmed_area:
                outlets = Outlet.query.filter_by(
                    brand='Pukis & Martabak Kota Baru',
                    status='Active',
                    area=sosmed_area,
                    is_global=True
                ).filter(Outlet.pkb_code.isnot(None)).all()
                n = len(outlets)
            else:
                outlets = Outlet.query.filter_by(
                    brand='Pukis & Martabak Kota Baru',
                    status='Active',
                    is_global=True
                ).filter(Outlet.pkb_code.isnot(None)).all()
                n = len(outlets)
            # Distribute SOSMED GLOBAL
            if sosmed_total > 0 and n > 0:
                per_outlet_amount = sosmed_total / n
                category = ExpenseCategory.query.filter_by(name='Sosmed Global').first()
                for outlet in outlets:
                    existing_entry = ManualEntry.query.filter(
                        ManualEntry.outlet_code == outlet.outlet_code,
                        ManualEntry.category_id == (category.id if category else None),
                        ManualEntry.start_date == (sosmed_date or datetime.now().date()),
                        ManualEntry.end_date == (sosmed_date or datetime.now().date()),
                        ManualEntry.entry_type == 'expense',
                        func.abs(ManualEntry.amount - per_outlet_amount) < 0.01
                    ).first()
                    if existing_entry:
                        continue  # Skip duplicate
                    manual_entry = ManualEntry(
                        outlet_code=outlet.outlet_code,
                        brand_name=outlet.brand,
                        entry_type='expense',
                        amount=per_outlet_amount,
                        description=f'SOSMED GLOBAL ({sosmed_area}) PKB' if sosmed_type == 'AREA' and sosmed_area else 'SOSMED GLOBAL (ALL) PKB',
                        start_date=sosmed_date or datetime.now().date(),
                        end_date=sosmed_date or datetime.now().date(),
                        category_id=category.id if category else None
                    )
                    db.session.add(manual_entry)
                db.session.commit()
            # Distribute AVANGER rows
            for av_type, av_data in avanger_totals.items():
                if av_data['total'] > 0 and n > 0:
                    per_outlet_amount = av_data['total'] / n
                    category = ExpenseCategory.query.filter_by(name="Avanger").first()
                    for outlet in outlets:
                        existing_entry = ManualEntry.query.filter(
                            ManualEntry.outlet_code == outlet.outlet_code,
                            ManualEntry.category_id == (category.id if category else None),
                            ManualEntry.start_date == (av_data['date'] or datetime.now().date()),
                            ManualEntry.end_date == (av_data['date'] or datetime.now().date()),
                            ManualEntry.entry_type == 'expense',
                            func.abs(ManualEntry.amount - per_outlet_amount) < 0.01
                        ).first()
                        if existing_entry:
                            continue  # Skip duplicate
                        manual_entry = ManualEntry(
                            outlet_code=outlet.outlet_code,
                            brand_name=outlet.brand,
                            entry_type='expense',
                            amount=per_outlet_amount,
                            description= f"{av_data['description']} PKB",
                            start_date=av_data['date'] or datetime.now().date(),
                            end_date=av_data['date'] or datetime.now().date(),
                            category_id=category.id if category else None
                        )
                        db.session.add(manual_entry)
                    db.session.commit()
                            

        return jsonify({
            'msg': 'PKB mutations uploaded successfully',
            'total_records': total_mutations,
            'skipped_records': skipped_mutations
        }), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


@reports_bp.route('/commission-totals', methods=['GET'])
def get_commission_totals():
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    outlet_code = request.args.get('outlet_code')
    brand_name = request.args.get('brand_name')

    try:
        # Initialize query for GrabFood reports
        grab_query = GrabFoodReport.query

        

        # Apply filters based on outlet_code and brand_name
        if outlet_code and outlet_code.upper() != "ALL":
            grab_query = grab_query.filter(GrabFoodReport.outlet_code == outlet_code)
        
        if brand_name and brand_name != "ALL":
             grab_query = grab_query.filter(GrabFoodReport.brand_name.in_(["MP78", "MP78 Express"]))
        if brand_name and brand_name not in ["MP78", "MP78 Express"]:
            return jsonify({'error': 'Commission calculation is only available for MP78 brands'}), 400
        else:
            # Only calculate commission for MP78 brands if no specific brand is selected
            grab_query = grab_query.filter(GrabFoodReport.brand_name.in_(["MP78", "MP78 Express"]))

        # Apply date filters
        if start_date_param and end_date_param:
            start_date = datetime.strptime(start_date_param, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_param, '%Y-%m-%d')
            end_date_inclusive = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)
            
            grab_query = grab_query.filter(
                GrabFoodReport.tanggal_dibuat >= start_date,
                GrabFoodReport.tanggal_dibuat <= end_date_inclusive,
                GrabFoodReport.jenis == "GrabFood"
            )

        # Calculate totals and commission
        grab_total = sum(float(report.total or 0) for report in grab_query.all())
        management_commission = round(grab_total * 1/74, 2)  # 1% commission
        partner_commission = round(grab_total * 1/74, 2)    # 1% commission

        response = {
            'period': {
                'start_date': start_date_param,
                'end_date': end_date_param
            },
            'outlet_code': outlet_code,
            'brand_name': brand_name,
            'totals': {
                'grab_food_total': round(grab_total, 2),
                'management_commission': management_commission,
                'partner_commission': partner_commission,
                'total_commission': management_commission + partner_commission
            }
        }
        
        return jsonify(response), 200

    except ValueError as e:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        print(f"Error calculating commission totals: {str(e)}")
        return jsonify({'error': str(e)}), 500


@reports_bp.route('/totals', methods=['GET'])
def get_reports_totals():
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    outlet_code = request.args.get('outlet_code')
    brand_name = request.args.get('brand_name')

    try:
        # Initialize queries
        gojek_query = GojekReport.query
        grab_query = GrabFoodReport.query
        shopee_query = ShopeeReport.query
        shopeepay_query = ShopeepayReport.query
        tiktok_query = TiktokReport.query
        cash_query = CashReport.query
        manual_entries_query = ManualEntry.query
        # Return error if outlet_code is not provided
        if not outlet_code:
            return jsonify({'error': 'outlet_code is required'}), 400
         # Apply outlet_code filter if provided and not "ALL"
        if outlet_code.upper() != "ALL":
            gojek_query = gojek_query.filter(GojekReport.outlet_code == outlet_code)
            grab_query = grab_query.filter(GrabFoodReport.outlet_code == outlet_code)
            shopee_query = shopee_query.filter(ShopeeReport.outlet_code == outlet_code)
            shopeepay_query = shopeepay_query.filter(ShopeepayReport.outlet_code == outlet_code)
            tiktok_query = tiktok_query.filter(TiktokReport.outlet_code == outlet_code)
            cash_query = cash_query.filter(CashReport.outlet_code == outlet_code)
            manual_entries_query = manual_entries_query.filter(ManualEntry.outlet_code == outlet_code)

        if outlet_code.upper() == "ALL" and brand_name != "ALL":
            gojek_query = gojek_query.filter(GojekReport.brand_name == brand_name)
            grab_query = grab_query.filter(GrabFoodReport.brand_name == brand_name)
            shopee_query = shopee_query.filter(ShopeeReport.brand_name == brand_name)
            cash_query = cash_query.filter(CashReport.brand_name == brand_name)
            shopeepay_query = shopeepay_query.filter(ShopeepayReport.brand_name == brand_name)
            tiktok_query = tiktok_query.filter(TiktokReport.brand_name == brand_name)
            manual_entries_query = manual_entries_query.filter(ManualEntry.brand_name == brand_name)

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
            shopeepay_query = shopeepay_query.filter(
                ShopeepayReport.create_time >= start_date,
                ShopeepayReport.create_time <= end_date_inclusive
            )
            tiktok_query = tiktok_query.filter(
                TiktokReport.order_time >= start_date,
                TiktokReport.order_time <= end_date_inclusive
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
        shopeepay_total = sum(
            float(report.settlement_amount or 0) 
            for report in shopeepay_query.all() 
            if report.transaction_type != "Withdrawal"
        )
        tiktok_total = sum(float(report.net_amount or 0) for report in tiktok_query.all())   
        
        # Calculate cash totals using the filtered queries
        cash_income = sum(float(report.total or 0) for report in cash_income_query.all())
        cash_expense = sum(float(report.total or 0) for report in cash_expense_query.all())
        cash_net = cash_income - cash_expense

        # Calculate manual entries totals (expenses)
        manual_entries_total = sum(float(entry.amount or 0) for entry in manual_entries_query.all())

        # Calculate running total
        running_total = gojek_total + grab_total + shopee_total + cash_net + shopeepay_total - manual_entries_total

        response = {
            'outlet_code': outlet_code,
            'brand_name': brand_name,
            'period': {
                'start_date': start_date_param,
                'end_date': end_date_param
            },
            'totals': {
                'gojek': round(gojek_total, 2),
                'grab': round(grab_total, 2),
                'shopee': round(shopee_total, 2),
                'shopeepay': round(shopeepay_total, 2),
                'tiktok': round(tiktok_total, 2),
                'cash': {
                    'income': round(cash_income, 2),
                    'expense': round(cash_expense, 2),
                    'net': round(cash_net, 2)
                },
                'manual_entries': round(manual_entries_total, 2),
                'running_total': round(running_total, 2),
                # 'mp78_commission': mp78_commission,  # Added MP78 commission
                # 'mp78_grab_total': round(mp78_grab_total, 2)  # Added for reference
            }
        }
        return jsonify(response), 200

    except ValueError as e:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        print(f"Error calculating totals: {str(e)}")
        return jsonify({'error': str(e)}), 500

@reports_bp.route('/top-outlets', methods=['GET'])
def get_top_outlets():
    start_date_param = request.args.get('start_date')
    end_date_param = request.args.get('end_date')
    brand_name = request.args.get('brand_name')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))

    if not start_date_param or not end_date_param or not brand_name:
        return jsonify({'error': 'start_date, end_date, and brand_name are required'}), 400

    try:
        start_date = datetime.strptime(start_date_param, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_param, '%Y-%m-%d')
        end_date_inclusive = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)

        # Fetch all relevant data by brand and date
        gojek_data = GojekReport.query.filter(
            GojekReport.brand_name == brand_name,
            GojekReport.transaction_date >= start_date,
            GojekReport.transaction_date <= end_date_inclusive
        ).all()

        grab_data = GrabFoodReport.query.filter(
            GrabFoodReport.brand_name == brand_name,
            GrabFoodReport.tanggal_dibuat >= start_date,
            GrabFoodReport.tanggal_dibuat <= end_date_inclusive
        ).all()

        shopee_data = ShopeeReport.query.filter(
            ShopeeReport.brand_name == brand_name,
            ShopeeReport.order_create_time >= start_date,
            ShopeeReport.order_create_time <= end_date_inclusive
        ).all()

        shopeepay_data = ShopeepayReport.query.filter(
            ShopeepayReport.brand_name == brand_name,
            ShopeepayReport.create_time >= start_date,
            ShopeepayReport.create_time <= end_date_inclusive
        ).all()

        cash_data = CashReport.query.filter(
            CashReport.brand_name == brand_name,
            CashReport.tanggal >= start_date,
            CashReport.tanggal <= end_date_inclusive
        ).all()

        manual_entries_data = ManualEntry.query.filter(
            ManualEntry.brand_name == brand_name,
            ManualEntry.start_date >= start_date,
            ManualEntry.end_date <= end_date_inclusive
        ).all()

        outlet_totals = {}

        # Helper to accumulate totals
        def add_total(outlet, amount):
            outlet_totals[outlet] = outlet_totals.get(outlet, 0) + amount

        for report in gojek_data:
            add_total(report.outlet_code, float(report.nett_amount or 0))

        for report in grab_data:
            add_total(report.outlet_code, float(report.total or 0))

        for report in shopee_data:
            if report.order_status != "Cancelled":
                add_total(report.outlet_code, float(report.net_income or 0))

        for report in shopeepay_data:
            if report.transaction_type != "Withdrawal":
                add_total(report.outlet_code, float(report.settlement_amount or 0))

        # For cash: income - expense
        cash_summary = {}
        for report in cash_data:
            code = report.outlet_code
            amount = float(report.total or 0)
            if code not in cash_summary:
                cash_summary[code] = {'income': 0, 'expense': 0}
            if report.type == 'income':
                cash_summary[code]['income'] += amount
            elif report.type == 'expense':
                cash_summary[code]['expense'] += amount

        for code, summary in cash_summary.items():
            net_cash = summary['income'] - summary['expense']
            add_total(code, net_cash)

        # Subtract manual entry expenses
        for entry in manual_entries_data:
            add_total(entry.outlet_code, -float(entry.amount or 0))

        # Sort outlets by running total descending
        top_outlets = sorted(outlet_totals.items(), key=lambda x: x[1], reverse=True)
        # Filter to only valid outlets with matching brand and non-null name BEFORE pagination
        valid_outlets = []
        for outlet, total in top_outlets:
            outlet_obj = Outlet.query.filter_by(outlet_code=outlet, brand=brand_name).first()
            if outlet_obj and outlet_obj.brand == brand_name and outlet_obj.outlet_name_gojek:
                valid_outlets.append({
                    'outlet_code': outlet,
                    'outlet_brand': outlet_obj.brand,
                    'outlet_name': outlet_obj.outlet_name_gojek,
                    'running_total': round(total, 2)
                })

        total_records = len(valid_outlets)
        total_pages = (total_records + per_page - 1) // per_page
        start = (page - 1) * per_page
        end = start + per_page
        paginated_outlets = valid_outlets[start:end]

        response = {
            'top_outlets': paginated_outlets,
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_records': total_records
            }
        }

        return jsonify(response), 200

    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        print(f"Error in /top-outlets: {str(e)}")
        return jsonify({'error': str(e)}), 500

