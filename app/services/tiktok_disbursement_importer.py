import csv
import re
from datetime import datetime
from decimal import Decimal
from io import StringIO

from sqlalchemy import tuple_

from app.extensions import db
from app.models.tiktok_disbursement import TiktokDisbursement
from app.services.import_job_issues import IssueCollector


TEXT_FIELDS = {
    'Statement no.': 'statement_no',
    'Statement order ID': 'statement_order_id',
    'Item order ID': 'item_order_id',
    'Store ID': 'store_id',
    'Store name': 'store_name',
    'Type': 'type',
}
AMOUNT_FIELDS = {
    'Total settlement amount': 'total_settlement_amount',
    'Total commission': 'total_commission',
    'Total platform fees': 'total_platform_fees',
    'Creator commission': 'creator_commission',
    'Payment amount': 'payment_amount',
    'Refund amount': 'refund_amount',
    'Platform incentive': 'platform_incentive',
    'Platform incentive refunds': 'platform_incentive_refunds',
}
REQUIRED_HEADERS = set(TEXT_FIELDS) | set(AMOUNT_FIELDS) | {
    'Order creation date', 'Order payment date',
}


def _required_value(row, header):
    value = (row.get(header) or '').strip()
    if not value:
        raise ValueError(f'{header} is required')
    return value


def _parse_row(row):
    values = {field: _required_value(row, header) for header, field in TEXT_FIELDS.items()}
    for header, field in AMOUNT_FIELDS.items():
        value = _required_value(row, header)
        if not re.fullmatch(r'-?\d{1,16}(?:\.\d{1,2})?', value):
            raise ValueError(f'{header} must be a numeric amount with at most two decimal places')
        values[field] = Decimal(value)
    for header, field, fmt in (
        ('Order creation date', 'order_creation_date', '%m/%d/%Y %H:%M:%S'),
        ('Order payment date', 'order_payment_date', '%m/%d/%Y'),
    ):
        value = _required_value(row, header)
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            raise ValueError(f'{header} has an invalid date: {value}') from None
        values[field] = parsed.date() if field == 'order_payment_date' else parsed
    return values


def import_tiktok_disbursement_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    reader = csv.DictReader(StringIO(file_bytes.decode('utf-8-sig'), newline=''))
    if reader.fieldnames:
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
    missing = REQUIRED_HEADERS - set(reader.fieldnames or [])
    if missing:
        raise ValueError('Missing required CSV headers: ' + ', '.join(sorted(missing)))

    rows = list(reader)
    keys = list({
        tuple((row.get(header) or '').strip() for header in ('Statement no.', 'Item order ID', 'Type'))
        for row in rows
    })
    existing = set()
    for start in range(0, len(keys), 300):
        existing.update(db.session.query(
            TiktokDisbursement.statement_no, TiktokDisbursement.item_order_id, TiktokDisbursement.type
        ).filter(tuple_(
            TiktokDisbursement.statement_no, TiktokDisbursement.item_order_id, TiktokDisbursement.type
        ).in_(keys[start:start + 300])).all())

    seen = set()
    inserted = duplicates = skipped = 0
    for row_number, row in enumerate(rows, 2):
        key = tuple((row.get(header) or '').strip()
                    for header in ('Statement no.', 'Item order ID', 'Type'))
        if all(key) and (key in existing or key in seen):
            duplicates += 1
            continue
        try:
            values = _parse_row(row)
            db.session.add(TiktokDisbursement(**values))
            seen.add(key)
            inserted += 1
        except (ValueError, TypeError) as exc:
            skipped += 1
            issues.record(row_number, 'PARSE_ERROR', str(exc), row)

    db.session.flush()
    return {
        'total_rows': len(rows), 'processed_rows': len(rows), 'inserted_rows': inserted,
        'skipped_rows': skipped + duplicates, 'duplicate_rows': duplicates,
        'non_duplicate_skipped_rows': skipped, 'failed_rows': 0,
        'skipped_rows_debug': issues.preview,
    }
