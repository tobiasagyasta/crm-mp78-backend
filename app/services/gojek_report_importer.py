import csv
from datetime import datetime
from io import StringIO

from app.extensions import db
from app.models.gojek_reports import GojekReport
from app.models.import_job import ImportJob
from app.models.outlet import Outlet
from app.services.consolidation_service import update_daily_total_for_outlet


def _clean_identifier(value):
    if value is None:
        return None
    value = str(value).strip().strip("'")
    return value or None


def _safe_float(value):
    if not value:
        return 0
    try:
        return float(str(value).replace(',', '').replace("'", ''))
    except (ValueError, TypeError):
        return 0


def _chunks(values, size=1000):
    values = list(values)
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def _load_existing_gojek_identifiers(transaction_references, transaction_ids):
    existing_transaction_references = set()
    existing_transaction_ids = set()

    for batch in _chunks(transaction_references):
        existing_transaction_references.update(
            identifier
            for (identifier,) in db.session.query(GojekReport.transaction_reference)
            .filter(GojekReport.transaction_reference.in_(batch))
            .all()
            if identifier
        )

    for batch in _chunks(transaction_ids):
        existing_transaction_ids.update(
            identifier
            for (identifier,) in db.session.query(GojekReport.transaction_id)
            .filter(GojekReport.transaction_id.in_(batch))
            .all()
            if identifier
        )

    return existing_transaction_references, existing_transaction_ids


def import_gojek_report_bytes(file_bytes, import_job_id=None):
    total_reports = 0
    skipped_reports = 0
    processed_rows = 0
    store_id_map = {}
    affected_outlets = set()
    seen_transaction_references = set()
    seen_transaction_ids = set()
    debug_skipped = []

    file_contents = file_bytes.decode('utf-8-sig')
    csv_file = StringIO(file_contents)
    reader = csv.DictReader(csv_file)
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
    outlets_by_store_id = {outlet.store_id_gojek: outlet for outlet in outlets if outlet.store_id_gojek}
    outlets_by_name = {outlet.outlet_name_gojek: outlet for outlet in outlets if outlet.outlet_name_gojek}

    transaction_references = set()
    transaction_ids = set()
    for row in rows:
        transaction_reference = _clean_identifier(row.get('Transaction Reference'))
        transaction_id = _clean_identifier(row.get('Transaction ID'))
        if transaction_reference:
            transaction_references.add(transaction_reference)
        if transaction_id:
            transaction_ids.add(transaction_id)

    existing_transaction_references, existing_transaction_ids = _load_existing_gojek_identifiers(
        transaction_references,
        transaction_ids,
    )

    reports = []
    for row_number, row in enumerate(rows, start=2):
        processed_rows += 1
        merchant_name = row.get('Merchant name', '').strip()
        merchant_id = row.get('Merchant ID')
        if merchant_name and merchant_id:
            store_id_map[merchant_name] = merchant_id

        outlet = None
        if merchant_id:
            outlet = outlets_by_store_id.get(merchant_id)
        if not outlet and merchant_name:
            outlet = outlets_by_name.get(merchant_name)
        if not outlet:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': 'Outlet not found', 'row': row})
            continue

        transaction_id = _clean_identifier(row.get('Transaction ID'))
        transaction_reference = _clean_identifier(row.get('Transaction Reference'))
        order_id = _clean_identifier(row.get('Order ID'))
        duplicate = (
            (transaction_reference and transaction_reference in seen_transaction_references)
            or (transaction_id and transaction_id in seen_transaction_ids)
            or (transaction_reference and transaction_reference in existing_transaction_references)
            or (transaction_id and transaction_id in existing_transaction_ids)
        )
        if duplicate:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': 'Duplicate Gojek identifier', 'row': row})
            continue

        try:
            transaction_date = datetime.strptime(row.get('Transaction Date', ''), '%m/%d/%Y').date()
            try:
                transaction_time = datetime.fromisoformat(row.get('Transaction time', '').replace('Z', '+00:00')).time()
            except ValueError:
                transaction_time = None

            reports.append({
                'brand_name': outlet.brand,
                'outlet_code': outlet.outlet_code,
                'transaction_id': transaction_id or '',
                'transaction_date': transaction_date,
                'transaction_time': transaction_time,
                'stan': row.get('Stan', ''),
                'nett_amount': _safe_float(row.get('Nett Amount')),
                'amount': _safe_float(row.get('Amount')),
                'transaction_status': row.get('Transaction Status', ''),
                'transaction_reference': transaction_reference or '',
                'order_id': order_id or '',
                'feature': row.get('Feature', ''),
                'payment_type': row.get('Payment Type', ''),
                'merchant_name': merchant_name,
                'merchant_id': merchant_id,
                'promo_type': row.get('Promo Type', ''),
                'promo_name': row.get('Promo Name', ''),
                'gopay_promo': _safe_float(row.get('Gopay promo')),
                'gofood_discount': _safe_float(row.get('GoFood discount')),
                'voucher_commission': _safe_float(row.get('Voucher commission')),
                'tax': _safe_float(row.get('Tax')),
                'witholding_tax': _safe_float(row.get('Witholding tax')),
                'currency': row.get('Currency', 'IDR'),
            })
            if transaction_reference:
                seen_transaction_references.add(transaction_reference)
            if transaction_id:
                seen_transaction_ids.add(transaction_id)
            affected_outlets.add((outlet.outlet_code, transaction_date))
            total_reports += 1
        except (ValueError, TypeError) as exc:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({'row_number': row_number, 'reason': f'Parse error: {str(exc)}', 'row': row})

    if reports:
        db.session.bulk_insert_mappings(GojekReport, reports)

    for outlet_id, date in affected_outlets:
        update_daily_total_for_outlet(outlet_id, date, 'gojek')

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
