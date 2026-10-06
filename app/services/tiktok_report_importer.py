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


def _process_parsed_batch(parsed_batch, seen_keys, debug_skipped):
    outlet_order_ids = {
        parsed['outlet_order_id']
        for _, _, parsed in parsed_batch
        if parsed.get('outlet_order_id')
    }
    existing_keys = _load_existing_keys(outlet_order_ids)
    reports = []
    skipped_reports = 0

    for row_number, row, parsed in parsed_batch:
        key = _duplicate_key(parsed)

        if key in seen_keys:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({
                    'row_number': row_number,
                    'reason': 'Duplicate entry within upload',
                    'row': row,
                })
            continue

        if key in existing_keys:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({
                    'row_number': row_number,
                    'reason': 'Duplicate entry already exists',
                    'row': row,
                })
            continue

        reports.append(parsed)
        seen_keys.add(key)

    return _flush_reports(reports), skipped_reports


def import_tiktok_report_bytes(file_bytes, import_job_id=None, batch_size=1000):
    file_contents = file_bytes.decode('utf-8-sig')
    csv_file = StringIO(file_contents)
    reader = csv.reader(csv_file)
    header = None

    for row in reader:
        if row and row[0].strip() == 'Breakdown':
            header = row
            break

    rows = [
        (row_number, row)
        for row_number, row in enumerate(reader, start=1)
        if row and any(cell.strip() for cell in row)
    ]

    _update_import_job_progress(
        import_job_id,
        total_rows=len(rows),
        processed_rows=0,
        inserted_rows=0,
        skipped_rows=0,
        failed_rows=0,
    )

    total_rows = len(rows)
    processed_rows = 0
    inserted_reports = 0
    skipped_reports = 0
    failed_reports = 0
    debug_skipped = []
    seen_keys = set()
    parsed_batch = []

    for row_number, row in rows:
        processed_rows += 1
        parsed = TiktokReport.parse_tiktok_row(row, header)

        if not parsed:
            skipped_reports += 1
            if len(debug_skipped) < 50:
                debug_skipped.append({
                    'row_number': row_number,
                    'reason': 'Parse failed or outlet not found',
                    'row': row,
                })
            continue

        parsed_batch.append((row_number, row, parsed))

        if len(parsed_batch) >= batch_size:
            batch_inserted, batch_skipped = _process_parsed_batch(
                parsed_batch,
                seen_keys,
                debug_skipped,
            )
            inserted_reports += batch_inserted
            skipped_reports += batch_skipped
            db.session.commit()
            parsed_batch.clear()
            _update_import_job_progress(
                import_job_id,
                processed_rows=processed_rows,
                inserted_rows=inserted_reports,
                skipped_rows=skipped_reports,
                failed_rows=failed_reports,
            )
            db.session.commit()
            print(
                f'TikTok import job {import_job_id}: '
                f'processed={processed_rows} inserted={inserted_reports} skipped={skipped_reports}'
            )

    if parsed_batch:
        batch_inserted, batch_skipped = _process_parsed_batch(
            parsed_batch,
            seen_keys,
            debug_skipped,
        )
        inserted_reports += batch_inserted
        skipped_reports += batch_skipped

    db.session.commit()

    _update_import_job_progress(
        import_job_id,
        total_rows=total_rows,
        processed_rows=processed_rows,
        inserted_rows=inserted_reports,
        skipped_rows=skipped_reports,
        failed_rows=failed_reports,
    )
    db.session.commit()

    print(
        f'TikTok import job {import_job_id} complete: '
        f'processed={processed_rows} inserted={inserted_reports} skipped={skipped_reports}'
    )

    return {
        'total_rows': total_rows,
        'processed_rows': processed_rows,
        'inserted_rows': inserted_reports,
        'skipped_rows': skipped_reports,
        'failed_rows': failed_reports,
        'skipped_rows_debug': debug_skipped,
    }
