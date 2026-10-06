import csv
from datetime import datetime
from io import StringIO

from app.extensions import db
from app.models.import_job import ImportJob
from app.models.outlet import Outlet
from app.models.shopee_reports import ShopeeReport
from app.services.consolidation_service import update_daily_total_for_outlet


def _safe_float(value):
    if not value:
        return 0
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return 0


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def import_shopee_report_bytes(file_bytes, import_job_id=None):
    total_reports = 0
    skipped_reports = 0
    processed_rows = 0
    store_id_map = {}
    affected_outlets = set()
    debug_skipped = []

    file_contents = file_bytes.decode('utf-8-sig')
    csv_file = StringIO(file_contents)
    reader = csv.DictReader(csv_file)
    if reader.fieldnames:
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
    rows = list(reader)

    _update_import_job_progress(
        import_job_id,
        total_rows=len(rows),
        processed_rows=0,
        inserted_rows=0,
        skipped_rows=0,
        failed_rows=0,
    )

    outlets = Outlet.query.all()
    outlets_by_store_id = {outlet.store_id_shopee: outlet for outlet in outlets if outlet.store_id_shopee}
    outlets_by_name = {outlet.outlet_name_grab: outlet for outlet in outlets if outlet.outlet_name_grab}
    upload_order_ids = {row.get('Order ID', '') for row in rows if row.get('Order ID', '')}
    existing_order_ids = set()
    if upload_order_ids:
        existing_order_ids = {
            order_id
            for (order_id,) in db.session.query(ShopeeReport.order_id)
            .filter(ShopeeReport.order_id.in_(upload_order_ids))
            .all()
        }
    seen_order_ids = set()

    reports = []
    for row_number, row in enumerate(rows, start=2):
        processed_rows += 1
        store_name = row.get('Store Name', '').strip()
        store_id = row.get('Store ID')
        if store_name and store_id:
            store_id_map[store_name] = store_id

        outlet = None
        if store_id:
            outlet = outlets_by_store_id.get(store_id)
        if not outlet and store_name:
            outlet = outlets_by_name.get(store_name)
        if not outlet:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': 'Outlet not found', 'row': row})
            continue

        order_id = row.get('Order ID', '')
        if order_id and (order_id in existing_order_ids or order_id in seen_order_ids):
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': 'Duplicate Shopee order ID', 'row': row})
            continue

        try:
            create_time = datetime.strptime(row.get('Order Create Time', ''), '%d/%m/%Y %H:%M:%S')
            complete_time = None
            if row.get('Order Complete/Cancel Time'):
                complete_time = datetime.strptime(row.get('Order Complete/Cancel Time', ''), '%d/%m/%Y %H:%M:%S')

            reports.append({
                'brand_name': outlet.brand,
                'outlet_code': outlet.outlet_code,
                'transaction_type': row.get('Transaction Type', ''),
                'order_id': order_id,
                'order_pick_up_id': row.get('Order Pick up ID', ''),
                'store_id': store_id,
                'store_name': store_name,
                'order_create_time': create_time,
                'order_complete_cancel_time': complete_time,
                'order_amount': _safe_float(row.get('Order Amount')),
                'merchant_service_charge': _safe_float(row.get('Merchant Service Charge')),
                'pb1': _safe_float(row.get('PB1')),
                'merchant_surcharge_fee': _safe_float(row.get('Merchant Surcharge Fee')),
                'merchant_shipping_fee_voucher_subsidy': _safe_float(row.get('Merchant Shipping Fee Voucher Subsidy')),
                'food_direct_discount': _safe_float(row.get('Food Direct Discount')),
                'merchant_food_voucher_subsidy': _safe_float(row.get('Merchant Food Voucher Subsidy')),
                'subtotal': _safe_float(row.get('Subtotal')),
                'total': _safe_float(row.get('Total')),
                'commission': _safe_float(row.get('Commission')),
                'net_income': _safe_float(row.get('Net Income')),
                'order_status': row.get('Order Status', ''),
                'order_type': row.get('Order Type', ''),
            })
            if order_id:
                seen_order_ids.add(order_id)
            affected_outlets.add((outlet.outlet_code, create_time.date()))
            total_reports += 1
        except (ValueError, TypeError) as exc:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': f'Parse error: {str(exc)}', 'row': row})

    if reports:
        db.session.bulk_insert_mappings(ShopeeReport, reports)

    for outlet_id, date in affected_outlets:
        update_daily_total_for_outlet(outlet_id, date, 'shopee')

    _update_import_job_progress(
        import_job_id,
        processed_rows=processed_rows,
        inserted_rows=total_reports,
        skipped_rows=skipped_reports,
        failed_rows=0,
    )

    return {
        'total_rows': len(rows),
        'processed_rows': processed_rows,
        'inserted_rows': total_reports,
        'skipped_rows': skipped_reports,
        'failed_rows': 0,
        'store_id_map': store_id_map,
        'affected_outlets': [
            {'outlet_code': outlet_code, 'date': date.isoformat()}
            for outlet_code, date in affected_outlets
        ],
        'skipped_rows_debug': debug_skipped,
    }
