import csv
from io import StringIO

from app.extensions import db
from app.models.import_job import ImportJob
from app.models.tiktok_reports import TiktokReport


def _chunks(values, size=1000):
    values = list(values)
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _duplicate_key(parsed):
    return (
        parsed['store_name'],
        parsed['outlet_order_id'],
        parsed['order_time'],
        parsed['settlement_time'],
        float(parsed['gross_amount'] or 0),
        float(parsed['net_amount'] or 0),
    )


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def _load_existing_keys(outlet_order_ids):
    existing_keys = set()

    for batch in _chunks(outlet_order_ids):
        rows = db.session.query(
            TiktokReport.store_name,
            TiktokReport.outlet_order_id,
            TiktokReport.order_time,
            TiktokReport.settlement_time,
            TiktokReport.gross_amount,
            TiktokReport.net_amount,
        ).filter(
            TiktokReport.outlet_order_id.in_(batch)
        ).all()

        existing_keys.update(
            (
                store_name,
                outlet_order_id,
                order_time,
                settlement_time,
                float(gross_amount or 0),
                float(net_amount or 0),
            )
            for store_name, outlet_order_id, order_time, settlement_time, gross_amount, net_amount in rows
        )

    return existing_keys


def _flush_reports(reports):
    if not reports:
        return 0

    db.session.bulk_insert_mappings(TiktokReport, reports)
    return len(reports)


def import_tiktok_report_bytes(file_bytes, import_job_id=None, batch_size=1000):
    file_contents = file_bytes.decode('utf-8-sig')
    csv_file = StringIO(file_contents)
    reader = csv.reader(csv_file)
    header = None

    for row in reader:
        if row and row[0].strip() == 'Breakdown':
            header = row
            break

    data_rows = [row for row in reader if row and any(cell.strip() for cell in row)]

    _update_import_job_progress(
        import_job_id,
        total_rows=len(data_rows),
        processed_rows=0,
        inserted_rows=0,
        skipped_rows=0,
        failed_rows=0,
    )

    parsed_rows = []
    skipped_reports = 0
    failed_reports = 0
    debug_skipped = []

    for index, row in enumerate(data_rows, start=1):
        parsed = TiktokReport.parse_tiktok_row(row, header)
        if not parsed:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({
                    'row_number': index,
                    'reason': 'Parse failed or outlet not found',
                    'row': row,
                })
            continue
        parsed_rows.append((index, row, parsed))

    outlet_order_ids = {
        parsed['outlet_order_id']
        for _, _, parsed in parsed_rows
        if parsed.get('outlet_order_id')
    }
    existing_keys = _load_existing_keys(outlet_order_ids)
    seen_keys = set()
    reports = []
    inserted_reports = 0
    processed_rows = 0

    for index, row, parsed in parsed_rows:
        processed_rows += 1
        key = _duplicate_key(parsed)

        if key in existing_keys or key in seen_keys:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({
                    'row_number': index,
                    'reason': 'Duplicate entry',
                    'row': row,
                })
            continue

        reports.append(parsed)
        seen_keys.add(key)

        if len(reports) >= batch_size:
            inserted_reports += _flush_reports(reports)
            reports.clear()
            _update_import_job_progress(
                import_job_id,
                processed_rows=processed_rows,
                inserted_rows=inserted_reports,
                skipped_rows=skipped_reports,
                failed_rows=failed_reports,
            )

    inserted_reports += _flush_reports(reports)

    _update_import_job_progress(
        import_job_id,
        processed_rows=len(data_rows),
        inserted_rows=inserted_reports,
        skipped_rows=skipped_reports,
        failed_rows=failed_reports,
    )

    return {
        'total_rows': len(data_rows),
        'processed_rows': len(data_rows),
        'inserted_rows': inserted_reports,
        'skipped_rows': skipped_reports,
        'failed_rows': failed_reports,
        'skipped_rows_debug': debug_skipped,
    }
