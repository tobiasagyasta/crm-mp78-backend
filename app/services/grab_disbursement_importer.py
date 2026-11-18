import csv
import re
from decimal import Decimal, InvalidOperation
from io import StringIO

from app.extensions import db
from app.models.grab_disbursement import GrabDisbursement
from app.models.outlet import Outlet
from app.models.rekening import Rekening
from app.services.grab_report_importer import _parse_grab_datetime
from app.services.import_job_issues import IssueCollector
from sqlalchemy import func


REQUIRED_HEADERS = {
    'Nama toko', 'ID toko', 'ID pencairan dana', 'Jumlah bersih',
    'Status', 'Tanggal transfer', 'Akun bank',
}


def _outlet_rekening_id(store_id, store_name):
    query = db.session.query(Outlet.rekening_id)
    outlets = query.filter(Outlet.store_id_grab == store_id).all()
    if not outlets:
        outlets = query.filter(
            func.lower(func.trim(Outlet.outlet_name_grab)) == store_name.lower()
        ).all()
    if len(outlets) == 1:
        return outlets[0][0]
    return None


def import_grab_disbursement_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    reader = csv.DictReader(StringIO(file_bytes.decode('utf-8-sig'), newline=''))
    if reader.fieldnames:
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
    missing = REQUIRED_HEADERS - set(reader.fieldnames or [])
    if missing:
        raise ValueError('Missing required CSV headers: ' + ', '.join(sorted(missing)))

    rows = list(reader)
    accounts = Rekening.query.all()
    ids = {str(row.get('ID pencairan dana') or '').strip() for row in rows} - {''}
    existing = set()
    ids = list(ids)
    for start in range(0, len(ids), 500):
        existing.update(value for (value,) in db.session.query(GrabDisbursement.disbursement_id)
                        .filter(GrabDisbursement.disbursement_id.in_(ids[start:start + 500])).all())

    seen = set()
    inserted = duplicates = skipped = 0
    for row_number, row in enumerate(rows, 2):
        identifier = (row.get('ID pencairan dana') or '').strip()
        if identifier and (identifier in existing or identifier in seen):
            duplicates += 1
            continue
        try:
            store_id = (row.get('ID toko') or '').strip()
            store_name = (row.get('Nama toko') or '').strip()
            status = (row.get('Status') or '').strip()
            suffix = (row.get('Akun bank') or '').strip()
            if not identifier or not store_id or not store_name or not status:
                raise ValueError('ID pencairan dana, ID toko, Nama toko and Status are required')
            if not re.fullmatch(r'\d{3,4}', suffix):
                raise ValueError('Akun bank must contain exactly 3 or 4 digits')
            matches = [account for account in accounts
                       if (account.rekening_number or '').strip().endswith(suffix)]
            if len(matches) > 1:
                outlet_rekening_id = _outlet_rekening_id(store_id, store_name)
                outlet_matches = [account for account in matches if account.id == outlet_rekening_id]
                if len(outlet_matches) == 1:
                    matches = outlet_matches
            if len(matches) != 1:
                code = 'REKENING_NOT_FOUND' if not matches else 'AMBIGUOUS_REKENING'
                issues.record(row_number, code, 'Account suffix must match exactly one rekening or the outlet’s linked rekening',
                              row, field_name='Akun bank', context={'account_suffix': suffix, 'store_id': store_id})
                skipped += 1
                continue
            transfer_date = _parse_grab_datetime(row.get('Tanggal transfer'))
            amount_text = (row.get('Jumlah bersih') or '').strip()
            if not re.fullmatch(r'-?\d+(?:\.\d{1,2})?', amount_text):
                raise ValueError('Jumlah bersih must be a numeric amount')
            amount = Decimal(amount_text)
            db.session.add(GrabDisbursement(
                disbursement_id=identifier, store_id=store_id, store_name=store_name,
                transfer_date=transfer_date, net_amount=amount, status=status,
                rekening_id=matches[0].id, account_suffix=suffix,
            ))
            seen.add(identifier)
            inserted += 1
        except (ValueError, TypeError, InvalidOperation) as exc:
            skipped += 1
            issues.record(row_number, 'PARSE_ERROR', str(exc), row)

    db.session.flush()
    return {
        'total_rows': len(rows), 'processed_rows': len(rows), 'inserted_rows': inserted,
        'skipped_rows': skipped + duplicates, 'duplicate_rows': duplicates,
        'non_duplicate_skipped_rows': skipped, 'failed_rows': 0,
        'skipped_rows_debug': issues.preview,
    }
