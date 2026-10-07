import csv
from datetime import datetime
from io import StringIO

from app.extensions import db
from app.models.grabfood_reports import GrabFoodReport
from app.models.import_job import ImportJob
from app.models.outlet import Outlet
from app.services.consolidation_service import update_daily_total_for_outlet


def _clean_identifier(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _row_value(row, *field_names):
    for field_name in field_names:
        value = row.get(field_name)
        if value not in (None, ''):
            return value
    return None


def _is_blank_row(row):
    return not any(str(value).strip() for value in row.values() if value is not None)


def _safe_float(value):
    if not value:
        return 0
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return 0


def _parse_grab_datetime(value):
    value = str(value or '').strip()
    for date_format in ('%d %b %Y %I:%M %p', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M'):
        try:
            return datetime.strptime(value, date_format)
        except ValueError:
            continue
    raise ValueError(f"time data {value!r} does not match supported Grab date formats")


def _chunks(values, size=1000):
    values = list(values)
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _add_skipped_debug(debug_skipped, row_number, reason, row):
    if len(debug_skipped) >= 50:
        return
    debug_skipped.append({
        'row_number': row_number,
        'reason': reason,
        'store_name': row.get('Nama toko'),
        'store_id': row.get('ID toko'),
        'transaction_id': row.get('ID transaksi'),
        'long_order_id': _row_value(row, 'ID pesanan (panjang)', 'ID pesanan panjang'),
        'short_order_id': _row_value(row, 'ID pesanan (pendek)', 'ID pesanan pendek'),
        'amount': _row_value(row, 'Amount', 'Jumlah'),
        'total': row.get('Total'),
    })


def _load_existing_grab_identifiers(transaction_ids, short_order_ids):
    existing_transaction_ids = set()
    existing_order_id_pairs = set()

    for batch in _chunks(transaction_ids):
        existing_transaction_ids.update(
            identifier
            for (identifier,) in db.session.query(GrabFoodReport.id_transaksi)
            .filter(GrabFoodReport.id_transaksi.in_(batch))
            .all()
            if identifier
        )

    for batch in _chunks(short_order_ids):
        existing_order_id_pairs.update(
            (short_order_id, long_order_id)
            for short_order_id, long_order_id in db.session.query(
                GrabFoodReport.id_pesanan_pendek,
                GrabFoodReport.id_pesanan_panjang,
            )
            .filter(GrabFoodReport.id_pesanan_pendek.in_(batch))
            .all()
            if short_order_id and long_order_id
        )

    return existing_transaction_ids, existing_order_id_pairs


def _has_duplicate_grab_identifier(
    transaction_id,
    short_order_id,
    long_order_id,
    allow_duplicate_order_pair,
    existing_transaction_ids,
    existing_order_id_pairs,
    seen_transaction_ids,
    seen_order_id_pairs,
):
    order_id_pair = (short_order_id, long_order_id) if short_order_id and long_order_id else None
    if transaction_id and transaction_id in seen_transaction_ids:
        return 'Duplicate transaction ID within upload'
    if not allow_duplicate_order_pair and order_id_pair and order_id_pair in seen_order_id_pairs:
        return f'Duplicate order ID pair within upload: {short_order_id} / {long_order_id}'
    if transaction_id and transaction_id in existing_transaction_ids:
        return 'Duplicate transaction ID already exists'
    if not allow_duplicate_order_pair and order_id_pair and order_id_pair in existing_order_id_pairs:
        return f'Duplicate order ID pair already exists: {short_order_id} / {long_order_id}'
    return None


def _remember_grab_identifiers(transaction_id, short_order_id, long_order_id, seen_transaction_ids, seen_order_id_pairs):
    if transaction_id:
        seen_transaction_ids.add(transaction_id)
    if short_order_id and long_order_id:
        seen_order_id_pairs.add((short_order_id, long_order_id))


def _is_grab_compensation_adjustment(row):
    kategori = str(row.get('Kategori') or '').strip().lower()
    subkategori = str(row.get('Subkategori') or '').strip().lower()
    return kategori in ('penyesuaian', 'adjustment') and 'kompensasi' in subkategori


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def import_grab_report_bytes(file_bytes, import_job_id=None):
    total_reports = 0
    skipped_reports = 0
    processed_rows = 0
    store_id_map = {}
    affected_outlets = set()
    seen_transaction_ids = set()
    seen_order_id_pairs = set()
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
    outlets_by_store_id = {outlet.store_id_grab: outlet for outlet in outlets if outlet.store_id_grab}
    outlets_by_name = {outlet.outlet_name_grab: outlet for outlet in outlets if outlet.outlet_name_grab}

    transaction_ids = set()
    short_order_ids = set()
    for row in rows:
        transaction_id = _clean_identifier(row.get('ID transaksi'))
        short_order_id = _clean_identifier(_row_value(row, 'ID pesanan (pendek)', 'ID pesanan pendek'))
        if transaction_id:
            transaction_ids.add(transaction_id)
        if short_order_id:
            short_order_ids.add(short_order_id)

    existing_transaction_ids, existing_order_id_pairs = _load_existing_grab_identifiers(
        transaction_ids,
        short_order_ids,
    )

    reports = []
    for row_number, row in enumerate(rows, start=2):
        processed_rows += 1
        if _is_blank_row(row):
            continue

        store_name = row.get('Nama toko', '').strip()
        store_id = row.get('ID toko')
        if store_name and store_id:
            store_id_map[store_name] = store_id

        outlet = None
        if store_id:
            outlet = outlets_by_store_id.get(store_id)
        if not outlet and store_name:
            outlet = outlets_by_name.get(store_name)
        if not outlet:
            skipped_reports += 1
            _add_skipped_debug(debug_skipped, row_number, 'Outlet not found for Grab store ID or name', row)
            continue

        transaction_id = _clean_identifier(row.get('ID transaksi'))
        long_order_id = _clean_identifier(_row_value(row, 'ID pesanan (panjang)', 'ID pesanan panjang'))
        short_order_id = _clean_identifier(_row_value(row, 'ID pesanan (pendek)', 'ID pesanan pendek'))
        duplicate_reason = _has_duplicate_grab_identifier(
            transaction_id,
            short_order_id,
            long_order_id,
            _is_grab_compensation_adjustment(row),
            existing_transaction_ids,
            existing_order_id_pairs,
            seen_transaction_ids,
            seen_order_id_pairs,
        )
        if duplicate_reason:
            skipped_reports += 1
            _add_skipped_debug(debug_skipped, row_number, duplicate_reason, row)
            continue

        try:
            tanggal_dibuat = _parse_grab_datetime(row.get('Tanggal dibuat', ''))
            tanggal_diperbarui = _parse_grab_datetime(row.get('Diperbarui Pada', ''))
            amount = _safe_float(_row_value(row, 'Amount', 'Jumlah'))
            total = _safe_float(row.get('Total'))
            if amount == 0 or total == 0:
                skipped_reports += 1
                _add_skipped_debug(debug_skipped, row_number, 'Amount or total is zero', row)
                continue

            reports.append({
                'brand_name': outlet.brand,
                'outlet_code': outlet.outlet_code,
                'nama_toko': store_name,
                'id_toko': store_id,
                'tanggal_dibuat': tanggal_dibuat,
                'diperbarui_pada': tanggal_diperbarui,
                'jenis': row.get('Jenis', ''),
                'kategori': row.get('Kategori', ''),
                'subkategori': row.get('Subkategori', ''),
                'status': row.get('Status', ''),
                'id_transaksi': transaction_id,
                'id_pesanan_panjang': long_order_id,
                'id_pesanan_pendek': short_order_id,
                'komisi_grabkitchen': _safe_float(row.get('Komisi GrabKitchen')),
                'total': total,
                'amount': amount,
                'penjualan_bersih': _safe_float(row.get('Penjualan bersih')),
            })
            _remember_grab_identifiers(
                transaction_id,
                short_order_id,
                long_order_id,
                seen_transaction_ids,
                seen_order_id_pairs,
            )
            affected_outlets.add((outlet.outlet_code, tanggal_diperbarui.date()))
            total_reports += 1
        except (ValueError, TypeError) as e:
            skipped_reports += 1
            _add_skipped_debug(debug_skipped, row_number, f'Parse error: {str(e)}', row)

    if reports:
        db.session.bulk_insert_mappings(GrabFoodReport, reports)

    for outlet_id, date in affected_outlets:
        update_daily_total_for_outlet(outlet_id, date, 'grab')

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
