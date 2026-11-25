"""Background importers for the remaining merchant CSV exports."""
import csv
from datetime import datetime
from decimal import Decimal
from io import StringIO

from app.extensions import db
from app.models.maxim_report import MaximReport
from app.models.outlet import Outlet
from app.models.qpon_reports import QponReport, qpon_csv_reader
from app.models.shopee_reports import ShopeeReport
from app.models.shopeepay_reports import ShopeepayReport
from app.models.webshop_report import WebshopReport
from app.services.consolidation_service import update_daily_total_for_outlet
from app.services.import_job_issues import IssueCollector
from app.services.shopee_report_importer import _safe_float, _update_import_job_progress
from app.services.webshop_report_source import classify_report_source


def _rows(file_bytes, *, dictionaries=False, qpon=False):
    source = StringIO(file_bytes.decode('utf-8-sig', errors='replace'))
    reader = csv.DictReader(source) if dictionaries else (
        qpon_csv_reader(source) if qpon else csv.reader(source)
    )
    if dictionaries:
        headers = reader.fieldnames
        if headers:
            reader.fieldnames = [header.strip() for header in headers]
    else:
        headers = next(reader, None)
    if not headers:
        raise ValueError('Missing CSV header row')
    return headers, list(reader)


def _finish(rows, inserted, duplicates, issues, import_job_id):
    skipped = len(rows) - inserted
    result = {
        'total_rows': len(rows), 'processed_rows': len(rows),
        'inserted_rows': inserted, 'skipped_rows': skipped,
        'duplicate_rows': duplicates,
        'non_duplicate_skipped_rows': skipped - duplicates,
        'failed_rows': 0, 'skipped_rows_debug': issues.preview,
    }
    _update_import_job_progress(import_job_id, **{
        key: value for key, value in result.items() if key != 'skipped_rows_debug'
    })
    return result


def _outlets():
    outlets = Outlet.query.all()
    return (
        {outlet.store_id_shopee: outlet for outlet in outlets if outlet.store_id_shopee},
        {outlet.outlet_name_grab: outlet for outlet in outlets if outlet.outlet_name_grab},
    )


def import_shopee_adjustment_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    _, rows = _rows(file_bytes, dictionaries=True)
    by_id, by_name = _outlets()
    seen = set()
    reports = []
    duplicates = 0
    for row_number, row in enumerate(rows, 2):
        store_id = row.get('Store ID')
        store_name = (row.get('Store Name') or '').strip()
        outlet = by_id.get(store_id) or by_name.get(store_name)
        if not outlet:
            issues.record(row_number, 'OUTLET_NOT_FOUND', 'No outlet matches Shopee Store ID or name', row)
            continue
        try:
            adjustment_time = datetime.strptime(row.get('Wallet Adjustment Time', ''), '%Y-%m-%d %H:%M:%S')
            amount = float(row.get('Wallet Adjustment Amount', 0) or 0)
        except (TypeError, ValueError) as exc:
            issues.record(row_number, 'PARSE_ERROR', f'Parse error: {exc}', row)
            continue
        order_id = row.get('Wallet Adjustment ID', '')
        identity = (order_id, adjustment_time)
        if order_id and (identity in seen or ShopeeReport.query.filter_by(
                order_id=order_id, transaction_type='Adjustment',
                order_create_time=adjustment_time).first()):
            duplicates += 1
            continue
        reports.append({
            'brand_name': outlet.brand, 'outlet_code': outlet.outlet_code,
            'transaction_type': 'Adjustment', 'order_id': order_id,
            'store_id': store_id, 'store_name': store_name,
            'order_create_time': adjustment_time, 'order_amount': amount,
            'total': amount, 'net_income': amount, 'order_status': 'Completed',
            'order_type': row.get('Wallet Adjustment Reason', 'Adjustment'),
        })
        if order_id:
            seen.add(identity)
        # The legacy adjustment upload fills missing Shopee store IDs by name.
        if store_id and not outlet.store_id_shopee and store_id not in by_id:
            outlet.store_id_shopee = store_id
            by_id[store_id] = outlet
    if reports:
        db.session.bulk_insert_mappings(ShopeeReport, reports)
    return _finish(rows, len(reports), duplicates, issues, import_job_id)


def import_shopeepay_report_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    _, rows = _rows(file_bytes, dictionaries=True)
    by_id, by_name = _outlets()
    seen = set()
    reports = []
    affected = set()
    duplicates = 0
    text_fields = {
        'merchant_host': 'Merchant Host', 'partner_merchant_id': 'Partner Merchant ID',
        'transaction_type': 'Transaction Type', 'merchant_scope': 'Merchant Scope',
        'reference_id': 'Reference ID', 'parent_id': 'Parent ID',
        'external_reference_id': 'External Reference ID', 'issuer_identifier': 'Issuer Identifier',
        'terminal_id': 'Terminal ID', 'adjustment_reason': 'Adjustment Reason',
        'reward_type': 'Reward Type', 'promo_type': 'Promo Type',
        'payment_method': 'Payment Method', 'currency_code': 'Currency Code',
        'voucher_promotion_event_name': 'Voucher Promotion Event Name', 'payment_option': 'Payment Option',
    }
    amount_fields = {
        'transaction_amount': 'Transaction Amount', 'fee_mdr': 'Fee (MDR)',
        'settlement_amount': 'Settlement Amount', 'fee_cofunding': 'Fee (Cofunding)',
        'reward_amount': 'Reward Amount', 'fee_withdrawal': 'Fee (Withdrawal)',
        'fee_handling': 'Fee (Handling)',
    }
    for row_number, row in enumerate(rows, 2):
        store_name = (row.get('Merchant/Store Name') or '').strip()
        entity_id = row.get('Entity ID')
        outlet = by_id.get(entity_id) or by_name.get(store_name)
        if not outlet:
            issues.record(row_number, 'OUTLET_NOT_FOUND', 'No outlet matches ShopeePay Entity ID or name', row)
            continue
        transaction_id = row.get('Transaction ID', '')
        if transaction_id and (transaction_id in seen or
                               ShopeepayReport.query.filter_by(transaction_id=transaction_id).first()):
            duplicates += 1
            continue
        try:
            create_time = datetime.strptime(row.get('Create Time', ''), '%Y-%m-%d %H:%M:%S')
            update_time = (datetime.strptime(row['Update Time'], '%Y-%m-%d %H:%M:%S')
                           if row.get('Update Time') else None)
        except (TypeError, ValueError) as exc:
            issues.record(row_number, 'PARSE_ERROR', f'Parse error: {exc}', row)
            continue
        report = {field: row.get(header, '') for field, header in text_fields.items()}
        report.update({field: _safe_float(row.get(header)) for field, header in amount_fields.items()})
        report.update(brand_name=outlet.brand, outlet_code=outlet.outlet_code,
                      merchant_store_name=store_name, entity_id=entity_id,
                      transaction_id=transaction_id, create_time=create_time, update_time=update_time)
        reports.append(report)
        if transaction_id:
            seen.add(transaction_id)
        affected.add((outlet.outlet_code, create_time.date()))
    if reports:
        db.session.bulk_insert_mappings(ShopeepayReport, reports)
    for outlet_code, date in affected:
        update_daily_total_for_outlet(outlet_code, date, 'shopeepay')
    return _finish(rows, len(reports), duplicates, issues, import_job_id)


def import_qpon_report_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    headers, rows = _rows(file_bytes, qpon=True)
    columns = QponReport._resolve_columns(headers)
    seen = set()
    reports = []
    duplicates = 0
    for row_number, row in enumerate(rows, 2):
        parsed = QponReport.parse_qpon_row(row, columns)
        if not parsed or not parsed.get('billing_id'):
            issues.record(row_number, 'PARSE_ERROR', 'Parse failed or missing billing ID', row)
            continue
        billing_id = parsed['billing_id']
        if billing_id in seen or QponReport.query.filter_by(billing_id=billing_id).first():
            duplicates += 1
            continue
        seen.add(billing_id)
        reports.append(parsed)
    if reports:
        db.session.bulk_insert_mappings(QponReport, reports)
    return _finish(rows, len(reports), duplicates, issues, import_job_id)


def import_webshop_report_bytes(file_bytes, import_job_id=None, issues=None):
    issues = issues if issues is not None else IssueCollector()
    headers, rows = _rows(file_bytes)
    columns = WebshopReport._resolve_columns(headers)
    if 'provider' not in columns or 'order_source' not in columns:
        raise ValueError('Missing Provider or Order Source CSV header')
    seen = set()
    reports = {'webshop': [], 'maxim': []}
    duplicates = 0
    for row_number, row in enumerate(rows, 2):
        platform = classify_report_source(row, columns)
        if platform is None:
            issues.record(row_number, 'UNKNOWN_SOURCE', 'Unknown or ambiguous provider/order source', row)
            continue
        rate = Decimal('0') if platform == 'maxim' else Decimal('0.03')
        parsed = WebshopReport.parse_order_row(row, columns, rate)
        if not parsed or not parsed.get('order_id'):
            issues.record(row_number, 'PARSE_ERROR', 'Parse failed or missing order ID', row)
            continue
        order_id = parsed['order_id']
        if (order_id in seen or WebshopReport.query.filter_by(order_id=order_id).first()
                or MaximReport.query.filter_by(order_id=order_id).first()):
            duplicates += 1
            continue
        seen.add(order_id)
        reports[platform].append(parsed)
    for platform, model in (('webshop', WebshopReport), ('maxim', MaximReport)):
        if reports[platform]:
            db.session.bulk_insert_mappings(model, reports[platform])
    result = _finish(rows, sum(map(len, reports.values())), duplicates, issues, import_job_id)
    result['platform_counts'] = {platform: len(items) for platform, items in reports.items()}
    return result
