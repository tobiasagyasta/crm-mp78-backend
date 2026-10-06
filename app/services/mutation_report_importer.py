import csv
from io import BytesIO, StringIO

import openpyxl
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.import_job import ImportJob
from app.models.mp78_mutations import MP78Mutation


def _is_blank_row(row):
    return not row or not any(str(value).strip() for value in row if value is not None)


def _is_mutation_table_header(row):
    row_text = ' '.join(str(value).strip().upper() for value in row if value is not None)
    return (
        ('TANGGAL' in row_text or 'DATE' in row_text)
        and ('KETERANGAN' in row_text or 'DESCRIPTION' in row_text)
        and ('CABANG' in row_text or 'BRANCH' in row_text)
        and ('JUMLAH' in row_text or 'AMOUNT' in row_text)
        and ('SALDO' in row_text or 'BALANCE' in row_text)
    )


def _debug_row(row):
    return [value.isoformat() if hasattr(value, 'isoformat') else value for value in row]


def _mutation_rows_from_table(numbered_rows, min_header_row=1):
    rows = []
    in_mutation_table = False
    header_row = None

    for row_number, row in numbered_rows:
        row = list(row)
        if _is_blank_row(row):
            continue
        if row_number >= min_header_row and _is_mutation_table_header(row):
            in_mutation_table = True
            header_row = row
            continue
        if not in_mutation_table:
            continue

        first_cell = str(row[0]).strip().upper() if row[0] is not None else ''
        if first_cell.startswith(('SALDO', 'MUTASI')):
            break

        rows.append((row_number, header_row, row))

    return rows


def _mutation_rows_from_excel(file_bytes):
    workbook = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True, read_only=True)
    rows = []

    for worksheet in workbook.worksheets:
        rows.extend(_mutation_rows_from_table(enumerate(worksheet.iter_rows(values_only=True), start=1)))

    return rows


def _mutation_rows_from_upload(file_bytes, filename=''):
    filename = (filename or '').lower()
    if filename.endswith(('.xlsx', '.xlsm')):
        return _mutation_rows_from_excel(file_bytes)

    file_contents = file_bytes.decode('utf-8-sig')
    csv_file = StringIO(file_contents)
    reader = csv.reader(csv_file)
    rows = _mutation_rows_from_table(enumerate(reader, start=1))
    if rows:
        return rows

    csv_file.seek(0)
    reader = csv.reader(csv_file)
    next(reader, None)
    return [(idx, None, row) for idx, row in enumerate(reader, start=2)]


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def _save_objects_with_integrity_fallback(objects):
    if not objects:
        return 0

    try:
        db.session.bulk_save_objects(objects)
        db.session.flush()
        return 0
    except IntegrityError:
        db.session.rollback()

    skipped = 0
    for obj in objects:
        try:
            with db.session.begin_nested():
                db.session.add(obj)
        except IntegrityError:
            skipped += 1
    db.session.flush()
    return skipped


def import_mutation_report_bytes(file_bytes, rekening_number, filename='', import_job_id=None):
    total_mutations = 0
    skipped_mutations = 0
    processed_rows = 0
    debug_skipped = []
    rows = _mutation_rows_from_upload(file_bytes, filename)

    _update_import_job_progress(
        import_job_id,
        total_rows=len(rows),
        processed_rows=0,
        inserted_rows=0,
        skipped_rows=0,
        failed_rows=0,
    )

    mutations = []
    mp78_mutations = []
    for row_number, header_row, row in rows:
        processed_rows += 1
        try:
            first_cell = str(row[0]).strip().upper() if row and row[0] is not None else ''
            if first_cell == 'PEND':
                skipped_mutations += 1
                debug_skipped.append({'row_number': row_number, 'reason': "Date column is 'PEND'", 'row': _debug_row(row)})
                continue

            parsed = BankMutation.parse_mutation_row(row, rekening_number)
            if not parsed:
                skipped_mutations += 1
                debug_skipped.append({
                    'row_number': row_number,
                    'reason': 'Unknown platform or parse failed',
                    'header': _debug_row(header_row) if header_row else None,
                    'row': _debug_row(row),
                })
                continue

            mutation_model = parsed.pop('_mutation_model', 'bank')
            if mutation_model == 'mp78':
                mutation = MP78Mutation(rekening_number=rekening_number, **parsed)
                exists = MP78Mutation.query.filter_by(transaction_id=mutation.transaction_id).first()
                if exists:
                    skipped_mutations += 1
                    debug_skipped.append({'row_number': row_number, 'reason': 'Duplicate MP78 mutation entry', 'row': _debug_row(row)})
                    continue
                mp78_mutations.append(mutation)
                total_mutations += 1
                continue

            mutation = BankMutation(rekening_number=rekening_number, **parsed)
            if mutation.platform_name == 'Unknown' and mutation.transaction_id:
                exists = BankMutation.query.filter_by(transaction_id=mutation.transaction_id).first()
            else:
                exists = BankMutation.query.filter_by(
                    tanggal=mutation.tanggal,
                    transaction_amount=mutation.transaction_amount,
                    platform_code=mutation.platform_code,
                ).first()
            if exists:
                skipped_mutations += 1
                debug_skipped.append({'row_number': row_number, 'reason': 'Duplicate mutation entry', 'row': _debug_row(row)})
                continue

            mutations.append(mutation)
            total_mutations += 1
        except Exception as exc:
            skipped_mutations += 1
            debug_skipped.append({'row_number': row_number, 'reason': f'Exception: {str(exc)}', 'row': _debug_row(row)})

    integrity_skips = _save_objects_with_integrity_fallback(mutations)
    integrity_skips += _save_objects_with_integrity_fallback(mp78_mutations)
    if integrity_skips:
        skipped_mutations += integrity_skips
        total_mutations -= integrity_skips

    _update_import_job_progress(
        import_job_id,
        processed_rows=processed_rows,
        inserted_rows=total_mutations,
        skipped_rows=skipped_mutations,
        failed_rows=0,
    )

    seen_reasons = set()
    one_per_reason = []
    for entry in debug_skipped:
        reason = entry['reason']
        if reason not in seen_reasons:
            one_per_reason.append(entry)
            seen_reasons.add(reason)

    return {
        'total_rows': len(rows),
        'processed_rows': processed_rows,
        'inserted_rows': total_mutations,
        'skipped_rows': skipped_mutations,
        'failed_rows': 0,
        'skipped_rows_debug': one_per_reason,
    }
