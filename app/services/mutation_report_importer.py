import csv
import re
from datetime import datetime
from io import BytesIO, StringIO

import openpyxl
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models.bank_mutations import BankMutation
from app.models.import_job import ImportJob
from app.models.mp78_mutations import MP78Mutation
from app.models.tiktok_mutation import TikTokMutation
from app.services.import_job_issues import IssueCollector


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
        rows.extend((number, header, row, worksheet.title) for number, header, row in
                    _mutation_rows_from_table(enumerate(worksheet.iter_rows(values_only=True), start=1)))

    return rows


def _statement_csv_rows(text, delimiter):
    for row in csv.reader(StringIO(text), delimiter=delimiter):
        if len(row) == 1 and delimiter in row[0]:
            row = next(csv.reader([row[0]], delimiter=delimiter))
        yield row


def _dated_statement_rows(rows):
    period = None
    for row in rows:
        if row:
            match = re.fullmatch(r'Periode\s*:\s*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})',
                                 row[0].strip(), re.I)
            if match:
                period = tuple(datetime.strptime(s, '%d/%m/%Y').date() for s in match.groups())
            elif period and re.fullmatch(r'\d{2}/\d{2}', row[0].strip()):
                for year in range(period[0].year, period[1].year + 1):
                    try:
                        day = datetime.strptime(f'{row[0].strip()}/{year}', '%d/%m/%Y').date()
                    except ValueError:
                        continue
                    if period[0] <= day <= period[1]:
                        row[0] = day.isoformat()
                        break
        yield row


def _mutation_rows_from_upload(file_bytes, filename=''):
    filename = (filename or '').lower()
    if filename.endswith(('.xlsx', '.xlsm')):
        return _mutation_rows_from_excel(file_bytes)

    file_contents = file_bytes.decode('utf-8-sig')
    # Statement exports often contain metadata before the table, so identify
    # the delimiter from the table header rather than relying on the first line.
    delimiter = None
    for candidate in (',', ';'):
        reader = _statement_csv_rows(file_contents, candidate)
        if any(len(row) > 1 and _is_mutation_table_header(row) for row in reader):
            delimiter = candidate
            break
    if delimiter is None:
        try:
            delimiter = csv.Sniffer().sniff(file_contents[:8192], delimiters=',;').delimiter
        except csv.Error:
            delimiter = ','

    reader = _dated_statement_rows(_statement_csv_rows(file_contents, delimiter))
    rows = [(number, header, row, None) for number, header, row in
            _mutation_rows_from_table(enumerate(reader, start=1))]
    if rows:
        return rows

    reader = csv.reader(StringIO(file_contents), delimiter=delimiter)
    next(reader, None)
    return [(idx, None, row, None) for idx, row in enumerate(reader, start=2)]


def _update_import_job_progress(import_job_id, **values):
    if not import_job_id:
        return

    import_job = ImportJob.query.get(import_job_id)
    if not import_job:
        return

    for key, value in values.items():
        setattr(import_job, key, value)
    db.session.flush()


def _save_objects_with_integrity_fallback(objects, issues):
    if not objects:
        return 0, 0

    try:
        with db.session.begin_nested():
            db.session.bulk_save_objects([obj for obj, _, _, _ in objects])
        return 0, 0
    except IntegrityError:
        pass

    skipped = 0
    duplicates = 0
    for obj, row_number, row, sheet_name in objects:
        try:
            with db.session.begin_nested():
                db.session.add(obj)
                db.session.flush()
        except IntegrityError as exc:
            skipped += 1
            orig = getattr(exc, 'orig', None)
            is_unique = (getattr(orig, 'pgcode', None) == '23505'
                         or getattr(orig, 'sqlstate', None) == '23505'
                         or 'UNIQUE constraint failed' in str(orig))
            code = 'DUPLICATE_ENTRY' if is_unique else 'DATABASE_CONSTRAINT_ERROR'
            if is_unique:
                duplicates += 1
            if code != 'DUPLICATE_ENTRY':
                issues.record(row_number, code, 'Database constraint rejected mutation row', row,
                              sheet_name=sheet_name)
    db.session.flush()
    return skipped, duplicates


def import_mutation_report_bytes(file_bytes, rekening_number, filename='', import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    total_mutations = 0
    skipped_mutations = 0
    processed_rows = 0
    duplicate_rows = 0
    rows = _mutation_rows_from_upload(file_bytes, filename)

    _update_import_job_progress(
        import_job_id,
        total_rows=len(rows),
        processed_rows=0,
        inserted_rows=0,
        skipped_rows=0,
        duplicate_rows=0,
        non_duplicate_skipped_rows=0,
        failed_rows=0,
    )

    mutations = []
    separate_mutations = []
    for row_number, header_row, row, sheet_name in rows:
        processed_rows += 1
        try:
            first_cell = str(row[0]).strip().upper() if row and row[0] is not None else ''
            if first_cell == 'PEND':
                skipped_mutations += 1
                issues.record(row_number, 'PENDING_TRANSACTION', "Date column is 'PEND'", row,
                              field_name='Date', sheet_name=sheet_name)
                continue

            parsed = BankMutation.parse_mutation_row(row, rekening_number)
            if not parsed:
                skipped_mutations += 1
                issues.record(row_number, 'PARSE_ERROR', 'Mutation row could not be parsed', row,
                              context={'header': _debug_row(header_row) if header_row else None},
                              sheet_name=sheet_name)
                continue

            mutation_model = parsed.pop('_mutation_model', 'bank')
            if mutation_model in ('mp78', 'tiktok'):
                model = TikTokMutation if mutation_model == 'tiktok' else MP78Mutation
                mutation = model(rekening_number=rekening_number, **parsed)
                exists = model.query.filter_by(transaction_id=mutation.transaction_id).first()
                if exists:
                    skipped_mutations += 1
                    duplicate_rows += 1
                    continue
                separate_mutations.append((mutation, row_number, row, sheet_name))
                total_mutations += 1
                continue

            mutation = BankMutation(rekening_number=rekening_number, **parsed)
            if mutation.platform_name == 'Qpon' and not mutation.platform_code:
                normalized_row = BankMutation._normalize_statement_mutation_row(row)
                mutation.transaction_id = BankMutation._build_mp78_transaction_id(
                    rekening_number, mutation.tanggal, mutation.transaksi,
                    normalized_row[3], normalized_row[4],
                )
            if mutation.platform_name in ('Unknown', 'Qpon') and mutation.transaction_id:
                exists = BankMutation.query.filter_by(transaction_id=mutation.transaction_id).first()
            else:
                exists = BankMutation.query.filter_by(
                    tanggal=mutation.tanggal,
                    transaction_amount=mutation.transaction_amount,
                    platform_code=mutation.platform_code,
                ).first()
            if exists:
                skipped_mutations += 1
                duplicate_rows += 1
                continue

            mutations.append((mutation, row_number, row, sheet_name))
            total_mutations += 1
        except Exception as exc:
            skipped_mutations += 1
            issues.record(row_number, 'PARSE_ERROR', f'Parse error: {exc}', row, sheet_name=sheet_name)

    integrity_skips, integrity_duplicates = _save_objects_with_integrity_fallback(mutations, issues)
    separate_skips, separate_duplicates = _save_objects_with_integrity_fallback(separate_mutations, issues)
    integrity_skips += separate_skips
    duplicate_rows += integrity_duplicates + separate_duplicates
    if integrity_skips:
        skipped_mutations += integrity_skips
        total_mutations -= integrity_skips

    _update_import_job_progress(
        import_job_id,
        processed_rows=processed_rows,
        inserted_rows=total_mutations,
        skipped_rows=skipped_mutations,
        duplicate_rows=duplicate_rows,
        non_duplicate_skipped_rows=skipped_mutations - duplicate_rows,
        failed_rows=0,
    )

    return {
        'total_rows': len(rows),
        'processed_rows': processed_rows,
        'inserted_rows': total_mutations,
        'skipped_rows': skipped_mutations,
        'duplicate_rows': duplicate_rows,
        'non_duplicate_skipped_rows': skipped_mutations - duplicate_rows,
        'failed_rows': 0,
        'skipped_rows_debug': issues.preview,
    }
